package client

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbitmq-common/metrics"
	"github.com/rafapasa/rabbitmq-common/middleware"
	"github.com/rafapasa/rabbitmq-common/queue"
)

// Consumer handles message consumption with DLQ and metrics
type Consumer struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
	metrics    *metrics.Metrics
}

// NewConsumer creates a new consumer
func NewConsumer(conn *amqp091.Connection) (*Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Consumer{
		connection: conn,
		channel:    ch,
		metrics:    metrics.GetMetrics(),
	}, nil
}

// SetupQueue configures the queue with DLQ
func (c *Consumer) SetupQueue(queueName string) error {
	config, exists := queue.QueueConfigs[queueName]
	if !exists {
		return fmt.Errorf("queue %s not configured", queueName)
	}

	// Declare main queue (with DLQ settings)
	_, err := c.channel.QueueDeclare(
		config.Name,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return err
	}

	// If DLQ is enabled, declare the dead letter queue
	if config.DLQEnabled {
		dlqConfig := queue.QueueConfigs[config.DLQName]
		_, err = c.channel.QueueDeclare(
			dlqConfig.Name,
			dlqConfig.Durable,
			dlqConfig.AutoDelete,
			dlqConfig.Exclusive,
			dlqConfig.NoWait,
			dlqConfig.Args,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// Consume starts consuming messages with DLQ and middlewares
func (c *Consumer) Consume(
	queueName string,
	handler middleware.HandlerFunc,
	workers int,
) error {
	// Setup the queue
	if err := c.SetupQueue(queueName); err != nil {
		return err
	}

	// Prepare handler with middlewares and metrics
	finalHandler := middleware.Chain(
		handler,
		middleware.LoggingMiddleware,
		c.metricsMiddleware,
		c.dlqMiddleware(queueName), // Last = executes first
	)

	// Start consuming
	deliveries, err := c.channel.Consume(
		queueName,
		"",    // consumer tag
		false, // auto-ack = false
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return err
	}

	// Start workers
	c.metrics.SetActiveWorkers(workers)

	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for delivery := range deliveries {
				ctx := context.Background()

				// Execute handler with middlewares
				err := finalHandler(ctx, delivery)

				if err != nil {
					log.Printf("⚠️ Worker %d final error: %v", workerID, err)
					// DLQ middleware already handles the reject
				}
			}
		}(i)
	}

	return nil
}

// metricsMiddleware records metrics
func (c *Consumer) metricsMiddleware(next middleware.HandlerFunc) middleware.HandlerFunc {
	return func(ctx context.Context, delivery amqp091.Delivery) error {
		queueName := delivery.RoutingKey

		// Metrics before
		start := time.Now()
		c.metrics.RecordConsumed(queueName, delivery.RoutingKey)
		c.metrics.RecordMessageSize(queueName, len(delivery.Body))

		// Execute next handler
		err := next(ctx, delivery)

		// Metrics after
		c.metrics.RecordProcessingDuration(queueName, time.Since(start))

		if err != nil {
			c.metrics.RecordFailure(queueName, fmt.Sprintf("%T", err))
		}

		return err
	}
}

// dlqMiddleware handles Dead Letter Queue logic
func (c *Consumer) dlqMiddleware(queueName string) middleware.Middleware {
	return func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(ctx context.Context, delivery amqp091.Delivery) error {
			// Execute the handler
			err := next(ctx, delivery)

			if err != nil {
				// Count how many times this message has been retried
				retryCount := getRetryCount(delivery)
				config := queue.QueueConfigs[queueName]

				if retryCount < config.DLQMaxRetries {
					// Reject and re-queue (with new header)
					log.Printf("🔄 Attempt %d/%d for message %s",
						retryCount+1,
						config.DLQMaxRetries,
						delivery.MessageId,
					)

					// Increment retry counter
					if delivery.Headers == nil {
						delivery.Headers = make(amqp091.Table)
					}
					delivery.Headers["x-retry-count"] = retryCount + 1

					// Reject with requeue=true (goes back to queue)
					return delivery.Reject(true)
				}

				// Max retries exceeded -> send to DLQ
				log.Printf("💀 Sending message %s to DLQ after %d attempts",
					delivery.MessageId,
					retryCount,
				)

				c.metrics.RecordDLQ(queueName)

				// Publish to DLQ
				return c.publishToDLQ(delivery, config.DLQName)
			}

			// Success! Acknowledge the message
			return delivery.Ack(false)
		}
	}
}

// publishToDLQ publishes a message to the Dead Letter Queue
func (c *Consumer) publishToDLQ(delivery amqp091.Delivery, dlqName string) error {
	// Add headers with error reason
	if delivery.Headers == nil {
		delivery.Headers = make(amqp091.Table)
	}
	delivery.Headers["x-dlq-reason"] = "max_retries_exceeded"
	delivery.Headers["x-dlq-timestamp"] = time.Now()

	// Publish to DLQ
	err := c.channel.Publish(
		"",      // exchange
		dlqName, // routing key
		false,   // mandatory
		false,   // immediate
		amqp091.Publishing{
			ContentType:  delivery.ContentType,
			Body:         delivery.Body,
			Headers:      delivery.Headers,
			DeliveryMode: delivery.DeliveryMode,
			Timestamp:    time.Now(),
		},
	)

	if err != nil {
		return fmt.Errorf("error publishing to DLQ: %w", err)
	}

	// Acknowledge the original message (remove from main queue)
	return delivery.Ack(false)
}

// Helper to extract retry count
func getRetryCount(delivery amqp091.Delivery) int {
	if delivery.Headers == nil {
		return 0
	}

	if val, ok := delivery.Headers["x-retry-count"]; ok {
		if count, ok := val.(int32); ok {
			return int(count)
		}
		if count, ok := val.(int64); ok {
			return int(count)
		}
		if count, ok := val.(int); ok {
			return count
		}
	}

	return 0
}

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

// Consumer handles message consumption (generic)
type Consumer struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
	metrics    *metrics.Metrics
}

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

// Consume starts consuming from a queue
// O projeto que usa decide como processar (handler)
func (c *Consumer) Consume(
	queueName string,
	handler middleware.HandlerFunc, // Handler genérico!
	workers int,
) error {
	// Setup queue with DLQ
	if err := c.setupQueue(queueName); err != nil {
		return err
	}

	// Apply middlewares (genéricos)
	finalHandler := middleware.Chain(
		handler,
		middleware.LoggingMiddleware,
		c.metricsMiddleware,
		c.dlqMiddleware(queueName),
	)

	// Start consuming
	deliveries, err := c.channel.Consume(
		queueName,
		"",
		false, // auto-ack = false
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	c.metrics.SetActiveWorkers(workers)

	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for delivery := range deliveries {
				ctx := context.Background()
				if err := finalHandler(ctx, delivery); err != nil {
					log.Printf("⚠️ Worker %d error: %v", workerID, err)
				}
			}
		}(i)
	}

	return nil
}

// setupQueue configura a fila com DLQ
func (c *Consumer) setupQueue(queueName string) error {
	config, exists := queue.QueueConfigs[queueName]
	if !exists {
		return fmt.Errorf("queue %s not configured", queueName)
	}

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

// dlqMiddleware (mesmo código anterior, genérico)
func (c *Consumer) dlqMiddleware(queueName string) middleware.Middleware {
	return func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(ctx context.Context, delivery amqp091.Delivery) error {
			err := next(ctx, delivery)
			if err != nil {
				retryCount := getRetryCount(delivery)
				config := queue.QueueConfigs[queueName]

				if retryCount < config.DLQMaxRetries {
					if delivery.Headers == nil {
						delivery.Headers = make(amqp091.Table)
					}
					delivery.Headers["x-retry-count"] = retryCount + 1
					return delivery.Reject(true)
				}

				c.metrics.RecordDLQ(queueName)
				return c.publishToDLQ(delivery, config.DLQName)
			}
			return delivery.Ack(false)
		}
	}
}

// metricsMiddleware (mesmo código anterior)
func (c *Consumer) metricsMiddleware(next middleware.HandlerFunc) middleware.HandlerFunc {
	return func(ctx context.Context, delivery amqp091.Delivery) error {
		queueName := delivery.RoutingKey
		start := time.Now()
		c.metrics.RecordConsumed(queueName, delivery.RoutingKey)
		c.metrics.RecordMessageSize(queueName, len(delivery.Body))

		err := next(ctx, delivery)

		c.metrics.RecordProcessingDuration(queueName, time.Since(start))
		if err != nil {
			c.metrics.RecordFailure(queueName, fmt.Sprintf("%T", err))
		}
		return err
	}
}

// publishToDLQ (mesmo código anterior)
func (c *Consumer) publishToDLQ(delivery amqp091.Delivery, dlqName string) error {
	if delivery.Headers == nil {
		delivery.Headers = make(amqp091.Table)
	}
	delivery.Headers["x-dlq-reason"] = "max_retries_exceeded"
	delivery.Headers["x-dlq-timestamp"] = time.Now()

	err := c.channel.Publish(
		"",
		dlqName,
		false,
		false,
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
	return delivery.Ack(false)
}

// getRetryCount (mesmo código anterior)
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

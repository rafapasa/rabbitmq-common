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

// Consumer agora recebe um QueueManager
type Consumer struct {
	connection   *amqp091.Connection
	channel      *amqp091.Channel
	metrics      *metrics.Metrics
	queueManager *queue.QueueManager // Gerenciador de filas do projeto
}

// NewConsumer cria um consumer com um gerenciador de filas específico
func NewConsumer(conn *amqp091.Connection, qm *queue.QueueManager) (*Consumer, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Consumer{
		connection:   conn,
		channel:      ch,
		metrics:      metrics.GetMetrics(),
		queueManager: qm,
	}, nil
}

// Consume inicia o consumo de uma fila específica
func (c *Consumer) Consume(
	queueName string,
	handler middleware.HandlerFunc,
	workers int,
) error {
	// Pega a configuração da fila do gerenciador do projeto
	config, exists := c.queueManager.GetQueueConfig(queueName)
	if !exists {
		return fmt.Errorf("queue %s not registered in QueueManager", queueName)
	}

	// Configura a fila com DLQ usando as configurações do projeto
	if err := c.setupQueue(config); err != nil {
		return err
	}

	// Aplica middlewares
	finalHandler := middleware.Chain(
		handler,
		middleware.LoggingMiddleware,
		c.metricsMiddleware,
		c.dlqMiddleware(config), // Passa a configuração específica
	)

	// Inicia consumo
	deliveries, err := c.channel.Consume(
		queueName,
		"",
		false,
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

// setupQueue configura uma fila com suas configurações específicas
func (c *Consumer) setupQueue(config queue.QueueConfig) error {
	// Declara fila principal
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

	// Se DLQ estiver habilitada, declara a fila morta
	if config.DLQEnabled {
		dlqConfig, exists := c.queueManager.GetQueueConfig(config.DLQName)
		if !exists {
			return fmt.Errorf("DLQ %s not registered in QueueManager", config.DLQName)
		}

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

// dlqMiddleware agora usa a configuração específica
func (c *Consumer) dlqMiddleware(config queue.QueueConfig) middleware.Middleware {
	return func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(ctx context.Context, delivery amqp091.Delivery) error {
			err := next(ctx, delivery)
			if err != nil {
				retryCount := getRetryCount(delivery)

				if retryCount < config.DLQMaxRetries {
					if delivery.Headers == nil {
						delivery.Headers = make(amqp091.Table)
					}
					delivery.Headers["x-retry-count"] = retryCount + 1
					return delivery.Reject(true)
				}

				c.metrics.RecordDLQ(config.Name)
				return c.publishToDLQ(delivery, config.DLQName)
			}
			return delivery.Ack(false)
		}
	}
}

// metricsMiddleware (mesmo de antes)
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

// publishToDLQ (mesmo de antes)
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

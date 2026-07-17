package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbitmq-common/metrics"
	"github.com/rafapasa/rabbitmq-common/queue"
)

// Publisher handles message publishing (generic)
type Publisher struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
	metrics    *metrics.Metrics
}

func NewPublisher(conn *amqp091.Connection) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &Publisher{
		connection: conn,
		channel:    ch,
		metrics:    metrics.GetMetrics(),
	}, nil
}

// Publish publishes ANY message with a routing key
// O projeto que usa decide o que vai no payload
func (p *Publisher) Publish(ctx context.Context, routingKey string, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error serializing: %w", err)
	}

	// Get queue name from routing key (optional, for metrics)
	queueName := queue.RoutingToQueue[routingKey]
	if queueName == "" {
		return fmt.Errorf("invalid routing key: %s", routingKey)
	}

	// Metrics
	p.metrics.RecordPublished(queueName, routingKey)
	p.metrics.RecordMessageSize(queueName, len(body))

	// Publish
	err = p.channel.PublishWithContext(
		ctx,
		queue.ExchangeMain,
		routingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType:  "application/json",
			Body:         body,
			Timestamp:    time.Now(),
			DeliveryMode: amqp091.Persistent,
			MessageId:    uuid.New().String(),
		},
	)

	if err != nil {
		p.metrics.RecordFailure(queueName, "publish_error")
		return fmt.Errorf("error publishing: %w", err)
	}

	return nil
}

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbitmq-common/events"
	"github.com/rafapasa/rabbitmq-common/metrics"
	"github.com/rafapasa/rabbitmq-common/queue"
)

// Publisher handles message publishing with metrics
type Publisher struct {
	connection *amqp091.Connection
	channel    *amqp091.Channel
	metrics    *metrics.Metrics
}

// NewPublisher creates a new publisher
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

// PublishEvent publishes a generic event
func (p *Publisher) PublishEvent(ctx context.Context, routingKey string, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error serializing: %w", err)
	}

	// Get queue name from routing key
	queueName := queue.RoutingToQueue[routingKey]

	// Metrics: publish
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

// PublishOrderCreated publishes an order created event
func (p *Publisher) PublishOrderCreated(ctx context.Context, order events.OrderCreatedPayload) error {
	order.EventID = uuid.New().String()
	order.EventType = events.EventTypeOrderCreated
	order.Timestamp = time.Now()
	order.Version = 1

	return p.PublishEvent(ctx, queue.RoutingKeyOrderCreated, order)
}

// PublishOrderPaid publishes an order paid event
func (p *Publisher) PublishOrderPaid(ctx context.Context, payment events.OrderPaidPayload) error {
	payment.EventID = uuid.New().String()
	payment.EventType = events.EventTypeOrderPaid
	payment.Timestamp = time.Now()
	payment.Version = 1

	return p.PublishEvent(ctx, queue.RoutingKeyOrderPaid, payment)
}

// PublishOrderCanceled publishes an order canceled event
func (p *Publisher) PublishOrderCanceled(ctx context.Context, canceled events.OrderCanceledPayload) error {
	canceled.EventID = uuid.New().String()
	canceled.EventType = events.EventTypeOrderCanceled
	canceled.Timestamp = time.Now()
	canceled.Version = 1

	return p.PublishEvent(ctx, queue.RoutingKeyOrderCanceled, canceled)
}

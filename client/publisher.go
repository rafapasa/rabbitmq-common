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

type Publisher struct {
	connManager ConnectionManager
	//channel      *amqp091.Channel
	metrics      *metrics.Metrics
	queueManager *queue.QueueManager
}

func NewPublisher(cm ConnectionManager, qm *queue.QueueManager) (*Publisher, error) {
	// ch, err := cm.GetChannel()
	// if err != nil {
	// 	return nil, err
	// }

	return &Publisher{
		connManager: cm,
		// channel:      ch,
		metrics:      metrics.GetMetrics(),
		queueManager: qm,
	}, nil
}

// Publish publica uma mensagem com routing key
func (p *Publisher) Publish(ctx context.Context, routingKey string, payload interface{}) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error serializing: %w", err)
	}

	// Obtém o nome da fila a partir do routing key (usando o manager do projeto)
	queueName, exists := p.queueManager.GetQueueByRouting(routingKey)
	if !exists {
		return fmt.Errorf("routing key %s not registered", routingKey)
	}

	// Métricas
	p.metrics.RecordPublished(queueName, routingKey)
	p.metrics.RecordMessageSize(queueName, len(body))

	// Publica
	conn, err := p.connManager.GetConnection()
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.PublishWithContext(
		ctx,
		"", // exchange vazia = default
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

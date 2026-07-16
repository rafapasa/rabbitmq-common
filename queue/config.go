package queue

import (
	"github.com/rabbitmq/amqp091-go"
)

// QueueConfig with DLQ support
type QueueConfig struct {
	Name          string
	Durable       bool
	AutoDelete    bool
	Exclusive     bool
	NoWait        bool
	Args          amqp091.Table
	DLQEnabled    bool
	DLQName       string // Dead Letter Queue name
	DLQMaxRetries int    // Maximum number of retry attempts
}

// Queue configurations with DLQ
var QueueConfigs = map[string]QueueConfig{
	QueueOrders: {
		Name:       QueueOrders,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: amqp091.Table{
			"x-max-priority": 10,
			// DLQ configurations
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": QueueOrders + ".dlq",
		},
		DLQEnabled:    true,
		DLQName:       QueueOrders + ".dlq",
		DLQMaxRetries: 3,
	},
	QueuePayments: {
		Name:       QueuePayments,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: amqp091.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": QueuePayments + ".dlq",
		},
		DLQEnabled:    true,
		DLQName:       QueuePayments + ".dlq",
		DLQMaxRetries: 3,
	},
	QueueNotifications: {
		Name:       QueueNotifications,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: amqp091.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": QueueNotifications + ".dlq",
		},
		DLQEnabled:    true,
		DLQName:       QueueNotifications + ".dlq",
		DLQMaxRetries: 3,
	},
}

// Mapping of Routing Key -> Queue (for publishers)
var RoutingToQueue = map[string]string{
	RoutingKeyOrderCreated:  QueueOrders,
	RoutingKeyOrderPaid:     QueueOrders,
	RoutingKeyOrderCanceled: QueueOrders,
}

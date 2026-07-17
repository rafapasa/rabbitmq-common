package queue

import "github.com/rabbitmq/amqp091-go"

type QueueConfig struct {
	Name          string
	Durable       bool
	AutoDelete    bool
	Exclusive     bool
	NoWait        bool
	Args          amqp091.Table
	DLQEnabled    bool
	DLQName       string
	DLQMaxRetries int
}

var QueueConfigs = map[string]QueueConfig{
	QueueOrders: {
		Name:       QueueOrders,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: amqp091.Table{
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
}

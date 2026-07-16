package main

import (
	"context"
	"log"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbitmq-common/client"
	"github.com/rafapasa/rabbitmq-common/events"
)

func main() {
	// Connect to RabbitMQ
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Create publisher
	publisher, err := client.NewPublisher(conn)
	if err != nil {
		log.Fatal(err)
	}

	// Create order event
	order := events.OrderCreatedPayload{
		OrderID:    "ORD-12345",
		CustomerID: "CUS-6789",
		TotalValue: 750.00,
		Items: []events.Item{
			{ProductID: "PROD-1", Quantity: 2, UnitPrice: 150.00},
			{ProductID: "PROD-2", Quantity: 3, UnitPrice: 150.00},
		},
	}

	// Publish event
	err = publisher.PublishOrderCreated(context.Background(), order)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("✅ Order published: %s", order.OrderID)
}

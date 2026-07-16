package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbitmq-common/client"
	"github.com/rafapasa/rabbitmq-common/events"
	"github.com/rafapasa/rabbitmq-common/queue"
)

func main() {
	// 1. Start metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	// 2. Connect to RabbitMQ
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// 3. Create consumer
	consumer, err := client.NewConsumer(conn)
	if err != nil {
		log.Fatal(err)
	}

	// 4. Define business handler
	handler := func(ctx context.Context, delivery amqp091.Delivery) error {
		var order events.OrderCreatedPayload
		if err := json.Unmarshal(delivery.Body, &order); err != nil {
			return err
		}

		// Process order
		log.Printf("📦 Processing order: %s", order.OrderID)

		// Simulate error for testing DLQ
		if order.TotalValue > 1000 {
			return fmt.Errorf("order above R$1000 needs approval")
		}

		return nil
	}

	// 5. Consume with DLQ, metrics and middlewares
	err = consumer.Consume(
		queue.QueueOrders,
		handler,
		5, // 5 workers
	)

	if err != nil {
		log.Fatal(err)
	}

	// 6. Keep running
	log.Println("🚀 Consumer started. Press CTRL+C to stop.")
	select {}
}

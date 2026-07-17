package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbitmq-common/client"
	"github.com/rafapasa/rabbitmq-common/queue"
)

type OrderCreated struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	TotalValue float64 `json:"total_value"`
	Items      []Item  `json:"items"`
}

type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unit_price"`
}

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Println("Erro ao conectar ao RabbitMQ:", err)
		return
	}
	defer conn.Close()

	// 1. Usando o publisher genérico
	publisher, err := client.NewPublisher(conn)
	if err != nil {
		fmt.Println("Erro ao criar Publisher:", err)
		return
	}

	// Publica SEM acoplamento
	order := OrderCreated{
		OrderID:    "ORD-123",
		CustomerID: "CUS-456",
		TotalValue: 750.00,
	}

	// O projeto decide o que publicar e em qual routing key
	publisher.Publish(context.Background(), queue.RoutingKeyOrders, order)

	// 2. Usando o consumer genérico
	consumer, _ := client.NewConsumer(conn)

	// Handler específico do projeto
	handler := func(ctx context.Context, delivery amqp091.Delivery) error {
		var order OrderCreated
		if err := json.Unmarshal(delivery.Body, &order); err != nil {
			return err
		}

		log.Printf("📦 Processando pedido: %s", order.OrderID)
		// Lógica de negócio específica...

		return nil
	}

	consumer.Consume(queue.QueueOrders, handler, 5)

	select {}
}

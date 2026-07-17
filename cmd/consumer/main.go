// projeto-payments/cmd/main.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbitmq-common/client"
	"github.com/rafapasa/rabbitmq-common/queue"
)

type PaymentProcessed struct {
	PaymentID   string    `json:"payment_id"`
	OrderID     string    `json:"order_id"`
	Amount      float64   `json:"amount"`
	ProcessedAt time.Time `json:"processed_at"`
	Status      string    `json:"status"`
}

func main() {
	conn, _ := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	defer conn.Close()

	// Publisher genérico para pagamentos
	publisher, _ := client.NewPublisher(conn)

	payment := PaymentProcessed{
		PaymentID: "PAY-789",
		OrderID:   "ORD-123",
		Amount:    750.00,
		Status:    "approved",
	}

	publisher.Publish(context.Background(), queue.RoutingKeyPayments, payment)

	// Consumer específico para pagamentos
	consumer, _ := client.NewConsumer(conn)

	handler := func(ctx context.Context, delivery amqp091.Delivery) error {
		var payment PaymentProcessed
		if err := json.Unmarshal(delivery.Body, &payment); err != nil {
			return err
		}

		log.Printf("💳 Processando pagamento: %s", payment.PaymentID)
		// Lógica de negócio específica de pagamentos...

		return nil
	}

	consumer.Consume(queue.QueuePayments, handler, 3)

	select {}
}

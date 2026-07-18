# rabbitmq-common
rabbit-common	Infraestrutura genérica (conexão, middlewares, métricas, estrutura de config)

Benefícios dessa abordagem:
Benefício	Explicação
✅ Zero acoplamento: O pacote comum não conhece nenhum domínio
✅ Configuração por projeto: Cada time define suas filas, retries e DLQ
✅ Reutilização total: O mesmo pacote serve para todos os projetos
✅ Flexibilidade: Cada projeto pode ter configurações diferentes (ex: retries)
✅ Escalabilidade: Novos projetos só criam seu QueueManager
✅ Testabilidade: Cada projeto testa suas próprias configurações


Como gerenciar versões?
# Equipe que mantém o pacote
git tag v1.0.0
git push origin v1.0.0

# Equipe que usa o pacote
go get github.com/rafapasa/rabbit-common@v1.0.0

# Para atualizar
go get -u github.com/rafapasa/rabbit-common

# Como utilizar

##### Arquivo de configurações
// projeto-orders/internal/queue/setup.go
package queue

import (
	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbit-common/queue"
)

// Constantes específicas do projeto de Orders
const (
	QueueOrders        = "orders"
	QueueOrdersDLQ     = "orders.dlq"
	RoutingKeyOrders   = "orders"
	ExchangeOrders     = "exchange_orders"
)

// SetupQueueManager configura o gerenciador de filas específico para Orders
func SetupQueueManager() *queue.QueueManager {
	qm := queue.NewQueueManager()

	// Registra a fila principal de Orders
	qm.RegisterQueue(queue.QueueConfig{
		Name:       QueueOrders,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args: amqp091.Table{
			"x-max-priority": 10,
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": QueueOrdersDLQ,
		},
		DLQEnabled:    true,
		DLQName:       QueueOrdersDLQ,
		DLQMaxRetries: 3,
	})

	// Registra a DLQ de Orders
	qm.RegisterQueue(queue.QueueConfig{
		Name:       QueueOrdersDLQ,
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
		DLQEnabled: false,
	})

	// Registra o mapeamento de routing key para fila
	qm.RegisterRouting(RoutingKeyOrders, QueueOrders)

	return qm
}

###### 

// projeto-orders/cmd/main.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbit-common/client"
	"github.com/rafapasa/rabbit-common/middleware"
	"projeto-orders/internal/events"
	"projeto-orders/internal/queue"
)

func main() {
	// 1. Inicia métricas
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	// 2. Conecta no RabbitMQ
	conn, _ := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	defer conn.Close()

	// 3. Configura o gerenciador de filas específico do projeto
	queueManager := queue.SetupQueueManager()

	// 4. Cria publisher com o gerenciador específico
	publisher, _ := client.NewPublisher(conn, queueManager)

	// 5. Publica uma mensagem (usa routing key específica do projeto)
	order := events.OrderCreated{
		OrderID:    "ORD-123",
		CustomerID: "CUS-456",
		TotalValue: 750.00,
	}
	publisher.Publish(context.Background(), queue.RoutingKeyOrders, order)

	// 6. Cria consumer com o gerenciador específico
	consumer, _ := client.NewConsumer(conn, queueManager)

	// 7. Define handler específico
	handler := func(ctx context.Context, delivery amqp091.Delivery) error {
		var order events.OrderCreated
		if err := json.Unmarshal(delivery.Body, &order); err != nil {
			return err
		}
		log.Printf("📦 Processando pedido: %s", order.OrderID)
		return nil
	}

	// 8. Consome da fila específica
	consumer.Consume(queue.QueueOrders, handler, 5)

	select {}
}
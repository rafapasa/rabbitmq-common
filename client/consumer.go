package client

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rafapasa/rabbitmq-common/metrics"
	"github.com/rafapasa/rabbitmq-common/middleware"
	"github.com/rafapasa/rabbitmq-common/models"
	"github.com/rafapasa/rabbitmq-common/queue"
)

// Consumer agora recebe um QueueManager e um ConnectionManager
type Consumer struct {
	connManager  ConnectionManager
	queueManager *queue.QueueManager
	metrics      *metrics.Metrics
	mu           sync.RWMutex
	activeQueues map[string]*models.ActiveQueue
	stopChan     chan struct{}
	isRunning    bool
}

// NewConsumer cria um consumer com gerenciadores
func NewConsumer(qm *queue.QueueManager, connManager ConnectionManager) *Consumer {
	return &Consumer{
		connManager:  connManager,
		queueManager: qm,
		metrics:      metrics.GetMetrics(),
		activeQueues: make(map[string]*models.ActiveQueue),
		stopChan:     make(chan struct{}),
	}
}

// Consume inicia o consumo com reconexão automática
func (c *Consumer) Consume(
	queueName string,
	handler middleware.HandlerFunc,
	workers int,
) error {
	c.mu.Lock()
	if _, exists := c.activeQueues[queueName]; exists {
		c.mu.Unlock()
		return fmt.Errorf("consumer for queue %s is already running", queueName)
	}
	c.activeQueues[queueName] = &models.ActiveQueue{StopChan: make(chan struct{})}
	c.mu.Unlock()

	go c.consumeWithReconnect(queueName, handler, workers, c.activeQueues[queueName].StopChan)
	return nil
}

// consumeWithReconnect mantém o consumo rodando mesmo com falhas
func (c *Consumer) consumeWithReconnect(queueName string, handler middleware.HandlerFunc, workers int, stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			log.Printf("🛑 Consumidor parado para fila: %s", queueName)
			return
		case <-c.stopChan: // Stop global
			log.Printf("🛑 Consumidor global parado, encerrando fila: %s", queueName)
			return
		default:
			if err := c.consume(queueName, handler, workers); err != nil {
				log.Printf("❌ Erro no consumo da fila %s: %v", queueName, err)
				log.Printf("⏳ Tentando reconectar em 5 segundos...")
				time.Sleep(5 * time.Second)

				// Tenta reconectar
				if err := c.connManager.Connect(); err != nil {
					log.Printf("❌ Falha na reconexão: %v", err)
					continue
				}
			}
		}
	}
}

// consume executa o consumo propriamente dito
func (c *Consumer) consume(queueName string, handler middleware.HandlerFunc, workers int) error {
	// 1. Obtém a configuração da fila
	config, exists := c.queueManager.GetQueueConfig(queueName)
	if !exists {
		return fmt.Errorf("queue %s not registered in QueueManager", queueName)
	}

	// 2. Obtém um canal ativo
	ch, err := c.connManager.GetChannel()
	if err != nil {
		return err
	}

	// 3. Configura a fila
	if err := c.setupQueueWithChannel(ch, config); err != nil {
		return err
	}

	// 4. Aplica middlewares
	finalHandler := middleware.Chain(
		handler,
		middleware.LoggingMiddleware,
		middleware.OpenTelemetryMiddleware, // Adicionado
		c.metricsMiddleware(queueName),     // Passa o nome da fila
		c.dlqMiddleware(config),
	)

	// 5. Inicia consumo
	deliveries, err := ch.Consume(
		queueName,
		"",
		false, // auto-ack = false
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	log.Printf("📨 Consumindo da fila: %s com %d workers", queueName, workers)
	c.metrics.SetActiveWorkers(workers)

	// 6. Dispara workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case delivery, ok := <-deliveries:
					if !ok {
						log.Printf("Canal de entregas fechado para fila %s, worker %d encerrando.", queueName, workerID)
						return
					}
					ctx := context.Background()
					if err := finalHandler(ctx, delivery); err != nil {
						log.Printf("⚠️ Worker %d error: %v", workerID, err)
					}
				case <-c.stopChan:
					log.Printf("Parada global solicitada, worker %d da fila %d encerrando.", workerID, workerID)
					return
				}
			}
		}(i)
	}

	// Aguarda todos os workers terminarem
	wg.Wait()
	return nil
}

// setupQueueWithChannel configura uma fila com um canal específico
func (c *Consumer) setupQueueWithChannel(ch *amqp091.Channel, config queue.QueueConfig) error {
	if ch == nil || ch.IsClosed() {
		return fmt.Errorf("channel is closed")
	}

	// Declara fila principal
	_, err := ch.QueueDeclare(
		config.Name,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return err
	}

	// Se DLQ estiver habilitada, declara a fila morta
	if config.DLQEnabled {
		dlqConfig, exists := c.queueManager.GetQueueConfig(config.DLQName)
		if !exists {
			return fmt.Errorf("DLQ %s not registered in QueueManager", config.DLQName)
		}

		_, err = ch.QueueDeclare(
			dlqConfig.Name,
			dlqConfig.Durable,
			dlqConfig.AutoDelete,
			dlqConfig.Exclusive,
			dlqConfig.NoWait,
			dlqConfig.Args,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// dlqMiddleware (mesmo de antes)
func (c *Consumer) dlqMiddleware(config queue.QueueConfig) middleware.Middleware {
	return func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(ctx context.Context, delivery amqp091.Delivery) error {
			err := next(ctx, delivery)
			if err != nil {
				retryCount := getRetryCount(delivery)

				if retryCount < config.DLQMaxRetries {
					if delivery.Headers == nil {
						delivery.Headers = make(amqp091.Table)
					}
					delivery.Headers["x-retry-count"] = retryCount + 1
					return delivery.Reject(true)
				}

				c.metrics.RecordDLQ(config.Name)
				return c.publishToDLQ(delivery, config.DLQName)
			}
			return delivery.Ack(false)
		}
	}
}

// metricsMiddleware (mesmo de antes)
func (c *Consumer) metricsMiddleware(queueName string) middleware.Middleware {
	return func(next middleware.HandlerFunc) middleware.HandlerFunc {
		return func(ctx context.Context, delivery amqp091.Delivery) error {
			start := time.Now()
			c.metrics.RecordConsumed(queueName, delivery.RoutingKey)
			c.metrics.RecordMessageSize(queueName, len(delivery.Body))

			err := next(ctx, delivery)

			c.metrics.RecordProcessingDuration(queueName, time.Since(start))
			if err != nil {
				c.metrics.RecordFailure(queueName, fmt.Sprintf("%T", err))
			}
			return err
		}
	}
}

// publishToDLQ (mesmo de antes)
func (c *Consumer) publishToDLQ(delivery amqp091.Delivery, dlqName string) error {
	if delivery.Headers == nil {
		delivery.Headers = make(amqp091.Table)
	}
	delivery.Headers["x-dlq-reason"] = "max_retries_exceeded"
	delivery.Headers["x-dlq-timestamp"] = time.Now()

	ch, err := c.connManager.GetChannel()
	if err != nil {
		return fmt.Errorf("error getting channel for DLQ publish: %w", err)
	}
	defer ch.Close()

	err = ch.Publish(
		"",
		dlqName,
		false,
		false,
		amqp091.Publishing{
			ContentType:  delivery.ContentType,
			Body:         delivery.Body,
			Headers:      delivery.Headers,
			DeliveryMode: delivery.DeliveryMode,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("error publishing to DLQ: %w", err)
	}
	return delivery.Ack(false)
}

// Stop para o consumidor
func (c *Consumer) Stop(queueName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if aq, exists := c.activeQueues[queueName]; exists {
		close(aq.StopChan)
		delete(c.activeQueues, queueName)
		log.Printf("Solicitando parada para o consumidor da fila: %s", queueName)
	}
}

// StopAll para todos os consumidores ativos gerenciados por esta instância
func (c *Consumer) StopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isRunning { // Previne fechamento duplo do canal global
		return
	}
	c.isRunning = true // Marca que o processo de parada global foi iniciado

	log.Printf("Solicitando parada para todos os consumidores...")
	close(c.stopChan)
}

// getRetryCount (mesmo de antes)
func getRetryCount(delivery amqp091.Delivery) int {
	if delivery.Headers == nil {
		return 0
	}
	if val, ok := delivery.Headers["x-retry-count"]; ok {
		if count, ok := val.(int32); ok {
			return int(count)
		}
		if count, ok := val.(int64); ok {
			return int(count)
		}
		if count, ok := val.(int); ok {
			return count
		}
	}
	return 0
}

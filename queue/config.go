package queue

import "github.com/rabbitmq/amqp091-go"

// QueueConfig representa a configuração de UMA fila
// O projeto que usa o pacote é responsável por definir suas filas
type QueueConfig struct {
	Name          string
	Durable       bool
	AutoDelete    bool
	Exclusive     bool
	NoWait        bool
	Args          amqp091.Table
	DLQEnabled    bool
	DLQName       string // Nome da Dead Letter Queue
	DLQMaxRetries int    // Número máximo de tentativas
}

// QueueManager gerencia as filas de um projeto
// Cada projeto cria seu próprio manager com suas filas específicas
type QueueManager struct {
	configs       map[string]QueueConfig
	routingToQueue map[string]string
}

// NewQueueManager cria um novo gerenciador de filas
func NewQueueManager() *QueueManager {
	return &QueueManager{
		configs:       make(map[string]QueueConfig),
		routingToQueue: make(map[string]string),
	}
}

// RegisterQueue registra uma fila no gerenciador
func (qm *QueueManager) RegisterQueue(config QueueConfig) {
	qm.configs[config.Name] = config
}

// RegisterRouting registra um mapeamento de routing key para fila
func (qm *QueueManager) RegisterRouting(routingKey, queueName string) {
	qm.routingToQueue[routingKey] = queueName
}

// GetQueueConfig retorna a configuração de uma fila
func (qm *QueueManager) GetQueueConfig(queueName string) (QueueConfig, bool) {
	config, exists := qm.configs[queueName]
	return config, exists
}

// GetQueueByRouting retorna o nome da fila a partir da routing key
func (qm *QueueManager) GetQueueByRouting(routingKey string) (string, bool) {
	queue, exists := qm.routingToQueue[routingKey]
	return queue, exists
}

// GetAllQueueConfigs retorna todas as configurações registradas
func (qm *QueueManager) GetAllQueueConfigs() map[string]QueueConfig {
	return qm.configs
}
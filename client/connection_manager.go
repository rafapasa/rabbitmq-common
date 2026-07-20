package client

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

// ConnectionManager interface para gerenciamento de conexão
type ConnectionManager interface {
	GetConnection() (*amqp091.Connection, error)
	GetChannel() (*amqp091.Channel, error)
	IsConnected() bool
	Connect() error
	Close()
}

// Garante que connectionManager implementa a interface ConnectionManager.
var _ ConnectionManager = (*connectionManager)(nil)

// connectionManager gerencia o ciclo de vida de uma conexão RabbitMQ,
// incluindo a reconexão automática.
type connectionManager struct {
	url         string
	connection  *amqp091.Connection
	mu          sync.RWMutex
	isConnected bool
	stopChan    chan struct{}
}

// NewConnectionManager cria e inicializa um novo gerenciador de conexão.
// Ele tenta se conectar imediatamente e inicia o monitoramento para reconexão.
func NewConnectionManager(url string) (ConnectionManager, error) {
	cm := &connectionManager{
		url:      url,
		stopChan: make(chan struct{}),
	}

	if err := cm.Connect(); err != nil {
		log.Printf("Falha na conexão inicial, continuará tentando em segundo plano: %v", err)
	}

	go cm.handleReconnect()

	return cm, nil
}

// Connect tenta estabelecer uma conexão com o RabbitMQ.
func (cm *connectionManager) Connect() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	conn, err := amqp091.Dial(cm.url)
	if err != nil {
		return err
	}

	cm.connection = conn
	cm.isConnected = true
	log.Println("✅ Conexão com RabbitMQ estabelecida com sucesso!")

	// Inicia o monitoramento de fechamento da conexão
	go func() {
		errChan := cm.connection.NotifyClose(make(chan *amqp091.Error))
		err := <-errChan
		log.Printf("🔌 Conexão com RabbitMQ perdida: %v", err)
		cm.mu.Lock()
		cm.isConnected = false
		cm.mu.Unlock()
	}()

	return nil
}

// handleReconnect é executado em uma goroutine para gerenciar a reconexão.
func (cm *connectionManager) handleReconnect() {
	reconnectInterval := 5 * time.Second
	for {
		select {
		case <-cm.stopChan:
			log.Println("🛑 Gerenciador de conexão parado.")
			return
		default:
			if !cm.IsConnected() {
				log.Printf("⏳ Tentando reconectar ao RabbitMQ em %v...", reconnectInterval)
				if err := cm.Connect(); err != nil {
					log.Printf("❌ Falha na tentativa de reconexão: %v", err)
					time.Sleep(reconnectInterval)
				}
			} else {
				// Se conectado, espera um pouco antes de verificar novamente.
				time.Sleep(1 * time.Second)
			}
		}
	}
}

// GetConnection retorna a instância da conexão ativa.
// Retorna um erro se não estiver conectado.
func (cm *connectionManager) GetConnection() (*amqp091.Connection, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if !cm.isConnected || cm.connection == nil {
		return nil, errors.New("não conectado ao RabbitMQ")
	}
	return cm.connection, nil
}

// GetChannel cria e retorna um novo canal a partir da conexão ativa.
func (cm *connectionManager) GetChannel() (*amqp091.Channel, error) {
	conn, err := cm.GetConnection()
	if err != nil {
		return nil, err
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if conn.IsClosed() {
		return nil, errors.New("a conexão está fechada")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// IsConnected retorna o status atual da conexão.
func (cm *connectionManager) IsConnected() bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.isConnected && cm.connection != nil && !cm.connection.IsClosed()
}

// Close fecha a conexão e para o processo de reconexão.
func (cm *connectionManager) Close() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	close(cm.stopChan)

	if cm.connection != nil && !cm.connection.IsClosed() {
		if err := cm.connection.Close(); err != nil {
			log.Printf("Erro ao fechar a conexão com RabbitMQ: %v", err)
		}
	}
	cm.isConnected = false
	log.Println("Conexão com RabbitMQ fechada.")
}

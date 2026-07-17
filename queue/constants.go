package queue

const (
	// Infrastructure constants (sem domínio específico)
	ExchangeMain = "exchange_main"

	// Routing keys são apenas identificadores técnicos
	// Quem usa o pacote decide o que cada routing key significa
	RoutingKeyOrders   = "orders"
	RoutingKeyPayments = "payments"
)

// Queue names (apenas nomes técnicos)
const (
	QueueOrders        = "orders"
	QueuePayments      = "payments"
	QueueNotifications = "notifications"
)

// RoutingToQueue mapeia routing keys para filas (configuração técnica)
var RoutingToQueue = map[string]string{
	RoutingKeyOrders:   QueueOrders,
	RoutingKeyPayments: QueuePayments,
}
package queue

const (
	// Queue names
	QueueOrders        = "orders"
	QueuePayments      = "payments"
	QueueNotifications = "notifications"

	// Exchanges
	ExchangeMain = "exchange_main"

	// Routing Keys
	RoutingKeyOrderCreated  = "order.created"
	RoutingKeyOrderPaid     = "order.paid"
	RoutingKeyOrderCanceled = "order.canceled"
)

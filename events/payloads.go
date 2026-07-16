package events

import "time"

// OrderCreatedPayload is the structure sent in the message
type OrderCreatedPayload struct {
	EventBase
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	TotalValue float64 `json:"total_value"`
	Items      []Item  `json:"items"`
}

// OrderPaidPayload is the structure sent in the message
type OrderPaidPayload struct {
	EventBase
	OrderID       string    `json:"order_id"`
	PaymentID     string    `json:"payment_id"`
	PaymentDate   time.Time `json:"payment_date"`
	PaymentAmount float64   `json:"payment_amount"`
}

// OrderCanceledPayload is the structure sent in the message
type OrderCanceledPayload struct {
	EventBase
	OrderID      string `json:"order_id"`
	CancelReason string `json:"cancel_reason"`
	CanceledBy   string `json:"canceled_by"`
}
type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unit_price"`
}

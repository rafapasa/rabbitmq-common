package events

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event constants
const (
	EventTypeOrderCreated   = "order.created"
	EventTypeOrderPaid      = "order.paid"
	EventTypeOrderCanceled  = "order.canceled"
	EventTypeOrderDelivered = "order.delivered"
)

// Event is the interface that ALL events must implement
type Event interface {
	GetEventID() string
	GetEventType() string
	GetTimestamp() time.Time
	GetVersion() int
	Validate() error
	ToJSON() ([]byte, error)
}

// EventBase implements common methods for all events
type EventBase struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"`
	Timestamp time.Time `json:"timestamp"`
	Version   int       `json:"version"`
}

func (e EventBase) GetEventID() string      { return e.EventID }
func (e EventBase) GetEventType() string    { return e.EventType }
func (e EventBase) GetTimestamp() time.Time { return e.Timestamp }
func (e EventBase) GetVersion() int         { return e.Version }

// OrderCreatedEvent implements the Event interface
type OrderCreatedEvent struct {
	EventBase
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	TotalValue float64 `json:"total_value"`
	Items      []Item  `json:"items"`
}

func (e OrderCreatedEvent) Validate() error {
	if e.OrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if e.CustomerID == "" {
		return fmt.Errorf("customer_id is required")
	}
	if e.TotalValue <= 0 {
		return fmt.Errorf("total_value must be greater than zero")
	}
	if len(e.Items) == 0 {
		return fmt.Errorf("items cannot be empty")
	}
	return nil
}

func (e OrderCreatedEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// OrderPaidEvent implements the Event interface
type OrderPaidEvent struct {
	EventBase
	OrderID       string    `json:"order_id"`
	PaymentID     string    `json:"payment_id"`
	PaymentDate   time.Time `json:"payment_date"`
	PaymentAmount float64   `json:"payment_amount"`
}

func (e OrderPaidEvent) Validate() error {
	if e.OrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if e.PaymentID == "" {
		return fmt.Errorf("payment_id is required")
	}
	if e.PaymentDate.IsZero() {
		return fmt.Errorf("payment_date is required")
	}
	if e.PaymentAmount <= 0 {
		return fmt.Errorf("payment_amount must be greater than zero")
	}
	return nil
}

func (e OrderPaidEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// Helper function to generate event ID
func generateEventID() string {
	return fmt.Sprintf("evt-%d", time.Now().UnixNano())
}

// NewOrderCreatedEvent is a factory for creating events
func NewOrderCreatedEvent(orderID, customerID string, totalValue float64, items []Item) OrderCreatedEvent {
	return OrderCreatedEvent{
		EventBase: EventBase{
			EventID:   generateEventID(),
			EventType: EventTypeOrderCreated,
			Timestamp: time.Now(),
			Version:   1,
		},
		OrderID:    orderID,
		CustomerID: customerID,
		TotalValue: totalValue,
		Items:      items,
	}
}

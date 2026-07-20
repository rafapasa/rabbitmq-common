package events

import (
	"time"
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

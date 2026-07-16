package middleware

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

// HandlerFunc is the function that processes the message
type HandlerFunc func(ctx context.Context, delivery amqp091.Delivery) error

// Middleware is a function that wraps a HandlerFunc
type Middleware func(HandlerFunc) HandlerFunc

// Chain chains multiple middlewares
func Chain(handler HandlerFunc, middlewares ...Middleware) HandlerFunc {
	// Apply middlewares in reverse order (last is executed first)
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// RecoveryMiddleware recovers from panics
func RecoveryMiddleware(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, delivery amqp091.Delivery) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &PanicError{Recovered: r}
			}
		}()
		return next(ctx, delivery)
	}
}

// PanicError represents a panic recovered
type PanicError struct {
	Recovered interface{}
}

func (e *PanicError) Error() string {
	return "panic recovered"
}

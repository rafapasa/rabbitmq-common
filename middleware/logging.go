package middleware

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

// Context keys for tracing
type ContextKey string

const (
	TraceIDKey ContextKey = "trace_id"
	SpanIDKey  ContextKey = "span_id"
)

// LoggingMiddleware adds structured logging
func LoggingMiddleware(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, delivery amqp091.Delivery) error {
		// Get or generate TraceID
		traceID := ctx.Value(TraceIDKey)
		if traceID == nil {
			traceID = uuid.New().String()
			ctx = context.WithValue(ctx, TraceIDKey, traceID)
		}

		// Start log
		start := time.Now()
		log.Printf("📨 [%s] Starting processing | RoutingKey: %s | Size: %d bytes",
			traceID,
			delivery.RoutingKey,
			len(delivery.Body),
		)

		// Execute next middleware/handler
		err := next(ctx, delivery)

		// End log
		duration := time.Since(start)
		if err != nil {
			log.Printf("❌ [%s] Error after %v | Error: %v", traceID, duration, err)
		} else {
			log.Printf("✅ [%s] Completed successfully in %v", traceID, duration)
		}

		return err
	}
}

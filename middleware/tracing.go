package middleware

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// OpenTelemetryMiddleware adds distributed tracing
func OpenTelemetryMiddleware(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, delivery amqp091.Delivery) error {
		tracer := otel.Tracer("rabbitmq-consumer")

		// Start a span
		ctx, span := tracer.Start(ctx, "process_message")
		defer span.End()

		// Add attributes
		span.SetAttributes(
			attribute.String("messaging.system", "rabbitmq"),
			attribute.String("messaging.destination", delivery.RoutingKey),
			attribute.String("messaging.message_id", delivery.MessageId),
			attribute.Int64("messaging.message_size", int64(len(delivery.Body))),
		)

		// Inject trace into context (for propagation)
		ctx = context.WithValue(ctx, TraceIDKey, span.SpanContext().TraceID().String())

		// Execute next handler
		err := next(ctx, delivery)

		if err != nil {
			span.RecordError(err)
			span.SetAttributes(attribute.Bool("error", true))
		}

		return err
	}
}

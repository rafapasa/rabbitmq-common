package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	once     sync.Once
	instance *Metrics
)

// Metrics centralizes all metrics
type Metrics struct {
	// Counters
	MessagesConsumed  *prometheus.CounterVec
	MessagesPublished *prometheus.CounterVec
	MessagesFailed    *prometheus.CounterVec
	MessagesDLQ       *prometheus.CounterVec

	// Histograms
	ProcessingDuration *prometheus.HistogramVec
	MessageSize        *prometheus.HistogramVec

	// Gauges (current values)
	ActiveWorkers prometheus.Gauge
	QueueDepth    *prometheus.GaugeVec
}

// GetMetrics returns the singleton instance
func GetMetrics() *Metrics {
	once.Do(func() {
		instance = &Metrics{
			// Total messages consumed
			MessagesConsumed: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "rabbitmq_messages_consumed_total",
					Help: "Total messages consumed",
				},
				[]string{"queue", "routing_key"},
			),

			// Total messages published
			MessagesPublished: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "rabbitmq_messages_published_total",
					Help: "Total messages published",
				},
				[]string{"queue", "routing_key"},
			),

			// Total failures
			MessagesFailed: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "rabbitmq_messages_failed_total",
					Help: "Total messages with errors",
				},
				[]string{"queue", "error_type"},
			),

			// Total in DLQ
			MessagesDLQ: promauto.NewCounterVec(
				prometheus.CounterOpts{
					Name: "rabbitmq_messages_dlq_total",
					Help: "Total messages sent to DLQ",
				},
				[]string{"queue"},
			),

			// Processing time
			ProcessingDuration: promauto.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "rabbitmq_processing_duration_seconds",
					Help:    "Duration of message processing",
					Buckets: prometheus.DefBuckets,
				},
				[]string{"queue"},
			),

			// Message size
			MessageSize: promauto.NewHistogramVec(
				prometheus.HistogramOpts{
					Name:    "rabbitmq_message_size_bytes",
					Help:    "Message size in bytes",
					Buckets: []float64{64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384},
				},
				[]string{"queue"},
			),

			// Active workers
			ActiveWorkers: promauto.NewGauge(
				prometheus.GaugeOpts{
					Name: "rabbitmq_active_workers",
					Help: "Number of active workers",
				},
			),

			// Queue depth
			QueueDepth: promauto.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "rabbitmq_queue_depth",
					Help: "Queue depth",
				},
				[]string{"queue"},
			),
		}
	})

	return instance
}

// Helper methods

func (m *Metrics) RecordConsumed(queue, routingKey string) {
	m.MessagesConsumed.WithLabelValues(queue, routingKey).Inc()
}

func (m *Metrics) RecordPublished(queue, routingKey string) {
	m.MessagesPublished.WithLabelValues(queue, routingKey).Inc()
}

func (m *Metrics) RecordFailure(queue, errorType string) {
	m.MessagesFailed.WithLabelValues(queue, errorType).Inc()
}

func (m *Metrics) RecordDLQ(queue string) {
	m.MessagesDLQ.WithLabelValues(queue).Inc()
}

func (m *Metrics) RecordProcessingDuration(queue string, duration time.Duration) {
	m.ProcessingDuration.WithLabelValues(queue).Observe(duration.Seconds())
}

func (m *Metrics) RecordMessageSize(queue string, size int) {
	m.MessageSize.WithLabelValues(queue).Observe(float64(size))
}

func (m *Metrics) SetActiveWorkers(count int) {
	m.ActiveWorkers.Set(float64(count))
}

func (m *Metrics) SetQueueDepth(queue string, depth int) {
	m.QueueDepth.WithLabelValues(queue).Set(float64(depth))
}

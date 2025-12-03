package etcd

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics defines etcd-related monitoring metrics
type Metrics struct {
	// Client operation metrics
	clientOperationsTotal    *prometheus.CounterVec
	clientOperationsDuration *prometheus.HistogramVec
	clientErrorsTotal        *prometheus.CounterVec

	// Configuration management metrics
	configOperationsTotal    *prometheus.CounterVec
	configOperationsDuration *prometheus.HistogramVec
	configChangesTotal       *prometheus.CounterVec

	// Health check metrics
	healthCheckTotal    *prometheus.CounterVec
	healthCheckDuration *prometheus.HistogramVec
	healthCheckFailed   *prometheus.CounterVec

	// Connection metrics
	connectionTotal       *prometheus.GaugeVec
	connectionErrorsTotal *prometheus.CounterVec

	// Cache metrics
	cacheHitsTotal   *prometheus.CounterVec
	cacheMissesTotal *prometheus.CounterVec
}

// NewEtcdMetrics creates new monitoring metrics instance
func NewEtcdMetrics() *Metrics {
	return &Metrics{
		// Client operation metrics
		clientOperationsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "client_operations_total",
				Help:      "Total number of client operations",
			},
			[]string{"operation", "status"},
		),
		clientOperationsDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "client_operations_duration_seconds",
				Help:      "Duration of client operations",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"operation"},
		),
		clientErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "client_errors_total",
				Help:      "Total number of client errors",
			},
			[]string{"operation", "error_type"},
		),

		// Configuration management metrics
		configOperationsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "config_operations_total",
				Help:      "Total number of configuration operations",
			},
			[]string{"prefix", "operation", "status"},
		),
		configOperationsDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "config_operations_duration_seconds",
				Help:      "Duration of configuration operations",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"prefix", "operation"},
		),
		configChangesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "config_changes_total",
				Help:      "Total number of configuration changes",
			},
			[]string{"prefix"},
		),

		// Health check metrics
		healthCheckTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "health_check_total",
				Help:      "Total number of health checks",
			},
			[]string{"status"},
		),
		healthCheckDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "health_check_duration_seconds",
				Help:      "Duration of health checks",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{},
		),
		healthCheckFailed: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "health_check_failed_total",
				Help:      "Total number of failed health checks",
			},
			[]string{"error_type"},
		),

		// Connection metrics
		connectionTotal: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "connection_total",
				Help:      "Total number of connections",
			},
			[]string{"status"},
		),
		connectionErrorsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "connection_errors_total",
				Help:      "Total number of connection errors",
			},
			[]string{"error_type"},
		),

		// Cache metrics
		cacheHitsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "cache_hits_total",
				Help:      "Total number of cache hits",
			},
			[]string{"prefix"},
		),
		cacheMissesTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "lynx",
				Subsystem: "etcd",
				Name:      "cache_misses_total",
				Help:      "Total number of cache misses",
			},
			[]string{"prefix"},
		),
	}
}

// RecordClientOperation records client operation
func (m *Metrics) RecordClientOperation(operation, status string) {
	if m == nil {
		return
	}
	m.clientOperationsTotal.WithLabelValues(operation, status).Inc()
}

// RecordConfigOperation records configuration operation
func (m *Metrics) RecordConfigOperation(prefix, operation, status string) {
	if m == nil {
		return
	}
	m.configOperationsTotal.WithLabelValues(prefix, operation, status).Inc()
}

// RecordConfigChange records configuration change
func (m *Metrics) RecordConfigChange(prefix string) {
	if m == nil {
		return
	}
	m.configChangesTotal.WithLabelValues(prefix).Inc()
}

// RecordHealthCheck records health check
func (m *Metrics) RecordHealthCheck(status string) {
	if m == nil {
		return
	}
	m.healthCheckTotal.WithLabelValues(status).Inc()
}

// RecordConnectionError records connection error
func (m *Metrics) RecordConnectionError(errorType string) {
	if m == nil {
		return
	}
	m.connectionErrorsTotal.WithLabelValues(errorType).Inc()
}

// RecordCacheHit records cache hit
func (m *Metrics) RecordCacheHit(prefix string) {
	if m == nil {
		return
	}
	m.cacheHitsTotal.WithLabelValues(prefix).Inc()
}

// RecordCacheMiss records cache miss
func (m *Metrics) RecordCacheMiss(prefix string) {
	if m == nil {
		return
	}
	m.cacheMissesTotal.WithLabelValues(prefix).Inc()
}

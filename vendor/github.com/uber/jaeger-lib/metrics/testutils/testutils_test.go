package testutils

import (
	"testing"

	"github.com/uber/jaeger-lib/metrics"
)

func TestAssertMetrics(t *testing.T) {
	f := metrics.NewLocalFactory(0)
	tags := map[string]string{"key": "value"}
	f.IncCounter("counter", tags, 1)
	f.UpdateGauge("gauge", tags, 11)

	AssertCounterMetrics(t, f, ExpectedMetric{Name: "counter", Tags: tags, Value: 1})
	AssertGaugeMetrics(t, f, ExpectedMetric{Name: "gauge", Tags: tags, Value: 11})
}

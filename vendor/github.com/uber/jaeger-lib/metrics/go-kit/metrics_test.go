package xkit

import (
	"testing"
	"time"

	"github.com/go-kit/kit/metrics/generic"
	"github.com/stretchr/testify/assert"

	"github.com/uber/jaeger-lib/metrics"
)

func TestCounter(t *testing.T) {
	kitCounter := generic.NewCounter("abc")
	var counter metrics.Counter = NewCounter(kitCounter)
	counter.Inc(123)
	assert.EqualValues(t, 123, kitCounter.Value())
}

func TestGauge(t *testing.T) {
	kitGauge := generic.NewGauge("abc")
	var gauge metrics.Gauge = NewGauge(kitGauge)
	gauge.Update(123)
	assert.EqualValues(t, 123, kitGauge.Value())
}

func TestTimer(t *testing.T) {
	kitHist := generic.NewHistogram("abc", 10)
	var timer metrics.Timer = NewTimer(kitHist)
	timer.Record(100*time.Millisecond + 500*time.Microsecond) // 100.5 milliseconds
	assert.EqualValues(t, 0.1005, kitHist.Quantile(0.9))
}

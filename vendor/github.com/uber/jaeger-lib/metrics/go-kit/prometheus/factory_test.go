package prometheus

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	xkit "github.com/uber/jaeger-lib/metrics/go-kit"
)

func TestCounter(t *testing.T) {
	f := NewFactory("namespace", "subsystem", nil)
	assert.True(t, f.Capabilities().Tagging)
	c := f.Counter("gokit.prom-counter")
	c.Add(42)
	m := findMetric(t, "namespace_subsystem_gokit_prom_counter")
	require.NotNil(t, m)
	assert.Equal(t, 42.0, m[0].GetCounter().GetValue())
}

func TestGauge(t *testing.T) {
	f := NewFactory("namespace", "subsystem", nil)
	g := f.Gauge("gokit.prom-gauge")
	g.Set(42)
	m := findMetric(t, "namespace_subsystem_gokit_prom_gauge")
	require.NotNil(t, m)
	assert.Equal(t, 42.0, m[0].GetGauge().GetValue())
}

func TestHistogram(t *testing.T) {
	f := NewFactory("namespace", "subsystem", nil)
	hist := f.Histogram("gokit.prom-hist")
	hist.Observe(10.5)
	m := findMetric(t, "namespace_subsystem_gokit_prom_hist")
	require.NotNil(t, m)
	assert.Equal(t, 10.5, m[0].GetHistogram().GetSampleSum())
}

func TestWrapper(t *testing.T) {
	f := NewFactory("", "", nil)
	wf := xkit.Wrap("foo", f, xkit.ScopeSeparator(":"))
	wf = wf.Namespace("bar", nil)
	c := wf.Counter("gokit.prom-wrapped-counter", nil)
	// TODO tags currently do not work because Prometheus requires to predeclate tag keys.
	// c := wf.Counter("gokit.prom-wrapped-counter", map[string]string{"x": "y"})
	c.Inc(42)
	m := findMetric(t, "foo:bar:gokit_prom_wrapped_counter")
	require.NotNil(t, m)
	assert.Equal(t, 42.0, m[0].GetCounter().GetValue())
}

func findMetric(t *testing.T, key string) []*dto.Metric {
	nf, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, nf := range nf {
		if nf.GetName() == key {
			return nf.GetMetric()
		}
	}
	return nil
}

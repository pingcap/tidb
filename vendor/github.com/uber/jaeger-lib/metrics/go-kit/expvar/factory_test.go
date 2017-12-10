package expvar

import (
	"expvar"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCounter(t *testing.T) {
	f := NewFactory(10)
	assert.False(t, f.Capabilities().Tagging)
	c := f.Counter("gokit_expvar_counter")
	c.Add(42)
	kv := findExpvar("gokit_expvar_counter")
	assert.Equal(t, "42", kv.Value.String())
}

func TestGauge(t *testing.T) {
	f := NewFactory(10)
	g := f.Gauge("gokit_expvar_gauge")
	g.Set(42)
	kv := findExpvar("gokit_expvar_gauge")
	assert.Equal(t, "42", kv.Value.String())
}

func TestHistogram(t *testing.T) {
	f := NewFactory(10)
	hist := f.Histogram("gokit_expvar_hist")
	hist.Observe(10.5)
	kv := findExpvar("gokit_expvar_hist.p50")
	assert.Equal(t, "10.5", kv.Value.String())
}

func findExpvar(key string) *expvar.KeyValue {
	var kv *expvar.KeyValue
	expvar.Do(func(v expvar.KeyValue) {
		if v.Key == key {
			kv = &v
		}
	})
	return kv
}

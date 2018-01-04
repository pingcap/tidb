package xkit

import (
	"sort"
	"testing"
	"time"

	kit "github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/generic"
	"github.com/stretchr/testify/assert"

	"github.com/uber/jaeger-lib/metrics"
)

type genericFactory struct{}

func (f genericFactory) Counter(name string) kit.Counter     { return generic.NewCounter(name) }
func (f genericFactory) Gauge(name string) kit.Gauge         { return generic.NewGauge(name) }
func (f genericFactory) Histogram(name string) kit.Histogram { return generic.NewHistogram(name, 10) }
func (f genericFactory) Capabilities() Capabilities          { return Capabilities{Tagging: true} }

// noTagsFactory is similar to genericFactory but overrides With() methods to no-op
type noTagsFactory struct{}

func (f noTagsFactory) Counter(name string) kit.Counter {
	return noTagsCounter{generic.NewCounter(name)}
}
func (f noTagsFactory) Gauge(name string) kit.Gauge         { return generic.NewGauge(name) }
func (f noTagsFactory) Histogram(name string) kit.Histogram { return generic.NewHistogram(name, 10) }
func (f noTagsFactory) Capabilities() Capabilities          { return Capabilities{Tagging: false} }

type noTagsCounter struct {
	counter *generic.Counter
}

func (c noTagsCounter) Add(delta float64)                      { c.counter.Add(delta) }
func (c noTagsCounter) With(labelValues ...string) kit.Counter { return c }

type noTagsGauge struct {
	gauge *generic.Gauge
}

func (g noTagsGauge) Set(value float64)                    { g.gauge.Set(value) }
func (g noTagsGauge) Add(delta float64)                    { g.gauge.Add(delta) }
func (g noTagsGauge) With(labelValues ...string) kit.Gauge { return g }

type noTagsHistogram struct {
	hist *generic.Histogram
}

func (h noTagsHistogram) Observe(value float64)                    { h.hist.Observe(value) }
func (h noTagsHistogram) With(labelValues ...string) kit.Histogram { return h }

type Tags map[string]string
type metricFunc func(t *testing.T, testCase testCase, f metrics.Factory) (name func() string, labels func() []string)

type testCase struct {
	f Factory

	prefix string
	name   string
	tags   Tags

	useNamespace  bool
	namespace     string
	namespaceTags Tags
	options       []FactoryOption

	expName string
	expTags []string
}

func TestFactoryScoping(t *testing.T) {
	genericFactory := genericFactory{}
	noTagsFactory := noTagsFactory{}
	testSuites := []struct {
		metricType string
		metricFunc metricFunc
	}{
		{"counter", testCounter},
		{"gauge", testGauge},
		{"timer", testTimer},
	}
	for _, ts := range testSuites {
		testSuite := ts // capture loop var
		testCases := []testCase{
			{f: genericFactory, prefix: "x", name: "", expName: "x"},
			{f: genericFactory, prefix: "", name: "y", expName: "y"},
			{f: genericFactory, prefix: "x", name: "y", expName: "x.y"},
			{f: genericFactory, prefix: "x", name: "z", expName: "x.z", tags: Tags{"a": "b"}, expTags: []string{"a", "b"}},
			{
				f:            genericFactory,
				name:         "x",
				useNamespace: true,
				namespace:    "w",
				expName:      "w.x",
			},
			{
				f:             genericFactory,
				name:          "y",
				useNamespace:  true,
				namespace:     "w",
				namespaceTags: Tags{"a": "b"},
				expName:       "w.y",
				expTags:       []string{"a", "b"},
			},
			{
				f:             genericFactory,
				name:          "z",
				tags:          Tags{"a": "b"},
				useNamespace:  true,
				namespace:     "w",
				namespaceTags: Tags{"c": "d"},
				expName:       "w.z",
				expTags:       []string{"a", "b", "c", "d"},
			},
			{f: noTagsFactory, prefix: "x", name: "", expName: "x"},
			{f: noTagsFactory, prefix: "", name: "y", expName: "y"},
			{f: noTagsFactory, prefix: "x", name: "y", expName: "x.y"},
			{f: noTagsFactory, prefix: "x", name: "z", expName: "x.z.a_b", tags: Tags{"a": "b"}},
			{
				f:            noTagsFactory,
				name:         "x",
				useNamespace: true,
				namespace:    "w",
				expName:      "w.x",
			},
			{
				f:             noTagsFactory,
				name:          "y",
				useNamespace:  true,
				namespace:     "w",
				namespaceTags: Tags{"a": "b"},
				expName:       "w.y.a_b",
			},
			{
				f:             noTagsFactory,
				options:       []FactoryOption{ScopeSeparator(":"), TagsSeparator(":")},
				name:          "z",
				tags:          Tags{"a": "b"},
				useNamespace:  true,
				namespace:     "w",
				namespaceTags: Tags{"c": "d"},
				expName:       "w:z:a_b:c_d",
			},
		}
		for _, tc := range testCases {
			testCase := tc // capture loop var
			factoryName := "genericFactory"
			if testCase.f == noTagsFactory {
				factoryName = "noTagsFactory"
			}
			t.Run(factoryName+"_"+testSuite.metricType+"_"+testCase.expName, func(t *testing.T) {
				f := Wrap(testCase.prefix, testCase.f, testCase.options...)
				if testCase.useNamespace {
					f = f.Namespace(testCase.namespace, testCase.namespaceTags)
				}
				name, labels := testSuite.metricFunc(t, testCase, f)
				assert.Equal(t, testCase.expName, name())
				actualTags := labels()
				sort.Strings(actualTags)
				assert.Equal(t, testCase.expTags, actualTags)
			})
		}
	}
}

func testCounter(t *testing.T, testCase testCase, f metrics.Factory) (name func() string, labels func() []string) {
	c := f.Counter(testCase.name, testCase.tags)
	c.Inc(123)
	kc := c.(*Counter).counter
	var gc *generic.Counter
	if c, ok := kc.(*generic.Counter); ok {
		gc = c
	} else {
		gc = kc.(noTagsCounter).counter
	}
	assert.EqualValues(t, 123.0, gc.Value())
	name = func() string { return gc.Name }
	labels = gc.LabelValues
	return
}

func testGauge(t *testing.T, testCase testCase, f metrics.Factory) (name func() string, labels func() []string) {
	g := f.Gauge(testCase.name, testCase.tags)
	g.Update(123)
	gg := g.(*Gauge).gauge.(*generic.Gauge)
	assert.EqualValues(t, 123.0, gg.Value())
	name = func() string { return gg.Name }
	labels = gg.LabelValues
	return
}

func testTimer(t *testing.T, testCase testCase, f metrics.Factory) (name func() string, labels func() []string) {
	tm := f.Timer(testCase.name, testCase.tags)
	tm.Record(123 * time.Millisecond)
	gt := tm.(*Timer).hist.(*generic.Histogram)
	assert.InDelta(t, 0.123, gt.Quantile(0.9), 0.00001)
	name = func() string { return gt.Name }
	labels = gt.LabelValues
	return
}

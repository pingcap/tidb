package tally

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/uber-go/tally"
)

func TestFactory(t *testing.T) {
	testScope := tally.NewTestScope("pre", map[string]string{"a": "b"})
	factory := Wrap(testScope).Namespace("fix", map[string]string{"c": "d"})
	counter := factory.Counter("counter", map[string]string{"x": "y"})
	counter.Inc(42)
	gauge := factory.Gauge("gauge", map[string]string{"x": "y"})
	gauge.Update(42)
	timer := factory.Timer("timer", map[string]string{"x": "y"})
	timer.Record(42 * time.Millisecond)
	snapshot := testScope.Snapshot()

	// tally v3 includes tags in the name, so look
	c := snapshot.Counters()["pre.fix.counter"]
	if c == nil {
		// tally v3 includes tags in the name.
		c = snapshot.Counters()["pre.fix.counter+a=b,c=d,x=y"]
	}

	g := snapshot.Gauges()["pre.fix.gauge"]
	if g == nil {
		g = snapshot.Gauges()["pre.fix.gauge+a=b,c=d,x=y"]
	}

	h := snapshot.Timers()["pre.fix.timer"]
	if h == nil {
		h = snapshot.Timers()["pre.fix.timer+a=b,c=d,x=y"]
	}

	expectedTags := map[string]string{"a": "b", "c": "d", "x": "y"}
	assert.EqualValues(t, 42, c.Value())
	assert.EqualValues(t, expectedTags, c.Tags())
	assert.EqualValues(t, 42, g.Value())
	assert.EqualValues(t, expectedTags, g.Tags())
	assert.Equal(t, []time.Duration{42 * time.Millisecond}, h.Values())
	assert.EqualValues(t, expectedTags, h.Tags())
}

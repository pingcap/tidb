package basictracer

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInMemoryRecorderSpans(t *testing.T) {
	recorder := NewInMemoryRecorder()
	var apiRecorder SpanRecorder = recorder
	span := RawSpan{
		Context:   SpanContext{},
		Operation: "test-span",
		Start:     time.Now(),
		Duration:  -1,
	}
	apiRecorder.RecordSpan(span)
	assert.Equal(t, []RawSpan{span}, recorder.GetSpans())
	assert.Equal(t, []RawSpan{}, recorder.GetSampledSpans())
}

type CountingRecorder int32

func (c *CountingRecorder) RecordSpan(r RawSpan) {
	atomic.AddInt32((*int32)(c), 1)
}

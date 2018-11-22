// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package rpcmetrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/testutils"
)

// E.g. tags("key", "value", "key", "value")
func tags(kv ...string) map[string]string {
	m := make(map[string]string)
	for i := 0; i < len(kv)-1; i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

func endpointTags(endpoint string, kv ...string) map[string]string {
	return tags(append([]string{"endpoint", endpoint}, kv...)...)
}

func TestMetricsByEndpoint(t *testing.T) {
	met := metrics.NewLocalFactory(0)
	mbe := newMetricsByEndpoint(met, DefaultNameNormalizer, 2)

	m1 := mbe.get("abc1")
	m2 := mbe.get("abc1")               // from cache
	m2a := mbe.getWithWriteLock("abc1") // from cache in double-checked lock
	assert.Equal(t, m1, m2)
	assert.Equal(t, m1, m2a)

	m3 := mbe.get("abc3")
	m4 := mbe.get("overflow")
	m5 := mbe.get("overflow2")

	for _, m := range []*Metrics{m1, m2, m2a, m3, m4, m5} {
		m.RequestCountSuccess.Inc(1)
	}

	testutils.AssertCounterMetrics(t, met,
		testutils.ExpectedMetric{Name: "requests", Tags: endpointTags("abc1", "error", "false"), Value: 3},
		testutils.ExpectedMetric{Name: "requests", Tags: endpointTags("abc3", "error", "false"), Value: 1},
		testutils.ExpectedMetric{Name: "requests", Tags: endpointTags("other", "error", "false"), Value: 2},
	)
}

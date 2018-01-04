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

package config

import (
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger-lib/metrics/testutils"

	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/log"
)

func TestNewSamplerConst(t *testing.T) {
	constTests := []struct {
		param    float64
		decision bool
	}{{1, true}, {0, false}}
	for _, tst := range constTests {
		cfg := &SamplerConfig{Type: jaeger.SamplerTypeConst, Param: tst.param}
		s, err := cfg.NewSampler("x", nil)
		require.NoError(t, err)
		s1, ok := s.(*jaeger.ConstSampler)
		require.True(t, ok, "converted to constSampler")
		require.Equal(t, tst.decision, s1.Decision, "decision")
	}
}

func TestNewSamplerProbabilistic(t *testing.T) {
	constTests := []struct {
		param float64
		error bool
	}{{1.5, true}, {0.5, false}}
	for _, tst := range constTests {
		cfg := &SamplerConfig{Type: jaeger.SamplerTypeProbabilistic, Param: tst.param}
		s, err := cfg.NewSampler("x", nil)
		if tst.error {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			_, ok := s.(*jaeger.ProbabilisticSampler)
			require.True(t, ok, "converted to ProbabilisticSampler")
		}
	}
}

func TestDefaultSampler(t *testing.T) {
	cfg := Configuration{
		Sampler: &SamplerConfig{Type: "InvalidType"},
	}
	_, _, err := cfg.New("testService")
	require.Error(t, err)
}

func TestInvalidSamplerType(t *testing.T) {
	cfg := &SamplerConfig{MaxOperations: 10}
	s, err := cfg.NewSampler("x", jaeger.NewNullMetrics())
	require.NoError(t, err)
	rcs, ok := s.(*jaeger.RemotelyControlledSampler)
	require.True(t, ok, "converted to RemotelyControlledSampler")
	rcs.Close()
}

func TestDefaultConfig(t *testing.T) {
	cfg := Configuration{}
	_, _, err := cfg.New("", Metrics(metrics.NullFactory), Logger(log.NullLogger))
	require.EqualError(t, err, "no service name provided")

	_, closer, err := cfg.New("testService")
	defer closer.Close()
	require.NoError(t, err)
}

func TestDisabledFlag(t *testing.T) {
	cfg := Configuration{Disabled: true}
	_, closer, err := cfg.New("testService")
	defer closer.Close()
	require.NoError(t, err)
}

func TestNewReporterError(t *testing.T) {
	cfg := Configuration{
		Reporter: &ReporterConfig{LocalAgentHostPort: "bad_local_agent"},
	}
	_, _, err := cfg.New("testService")
	require.Error(t, err)
}

func TestInitGlobalTracer(t *testing.T) {
	// Save the existing GlobalTracer and replace after finishing function
	prevTracer := opentracing.GlobalTracer()
	defer opentracing.InitGlobalTracer(prevTracer)
	noopTracer := opentracing.NoopTracer{}

	tests := []struct {
		cfg           Configuration
		shouldErr     bool
		tracerChanged bool
	}{
		{Configuration{Disabled: true}, false, false},
		{Configuration{Sampler: &SamplerConfig{Type: "InvalidType"}}, true, false},
		{Configuration{}, false, true},
	}
	for _, test := range tests {
		opentracing.InitGlobalTracer(noopTracer)
		_, err := test.cfg.InitGlobalTracer("testService")
		if test.shouldErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		if test.tracerChanged {
			require.NotEqual(t, noopTracer, opentracing.GlobalTracer())
		} else {
			require.Equal(t, noopTracer, opentracing.GlobalTracer())
		}
	}
}

func TestConfigWithReporter(t *testing.T) {
	c := Configuration{
		Sampler: &SamplerConfig{
			Type:  "const",
			Param: 1,
		},
	}
	r := jaeger.NewInMemoryReporter()
	tracer, closer, err := c.New("test", Reporter(r))
	require.NoError(t, err)
	defer closer.Close()

	tracer.StartSpan("test").Finish()
	assert.Len(t, r.GetSpans(), 1)
}

func TestConfigWithRPCMetrics(t *testing.T) {
	metrics := metrics.NewLocalFactory(0)
	c := Configuration{
		Sampler: &SamplerConfig{
			Type:  "const",
			Param: 1,
		},
		RPCMetrics: true,
	}
	r := jaeger.NewInMemoryReporter()
	tracer, closer, err := c.New("test", Reporter(r), Metrics(metrics))
	require.NoError(t, err)
	defer closer.Close()

	tracer.StartSpan("test", ext.SpanKindRPCServer).Finish()

	testutils.AssertCounterMetrics(t, metrics,
		testutils.ExpectedMetric{
			Name:  "jaeger-rpc.requests",
			Tags:  map[string]string{"component": "jaeger", "endpoint": "test", "error": "false"},
			Value: 1,
		},
	)
}

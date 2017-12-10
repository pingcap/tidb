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

package jaeger

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-lib/metrics"
	mTestutils "github.com/uber/jaeger-lib/metrics/testutils"

	"github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-client-go/testutils"
	"github.com/uber/jaeger-client-go/thrift-gen/sampling"
	"github.com/uber/jaeger-client-go/utils"
)

const (
	testOperationName          = "op"
	testFirstTimeOperationName = "firstTimeOp"

	testDefaultSamplingProbability = 0.5
	testMaxID                      = uint64(1) << 62
	testDefaultMaxOperations       = 10
)

var (
	testProbabilisticExpectedTags = []Tag{
		{"sampler.type", "probabilistic"},
		{"sampler.param", 0.5},
	}
	testLowerBoundExpectedTags = []Tag{
		{"sampler.type", "lowerbound"},
		{"sampler.param", 0.5},
	}
)

func TestSamplerTags(t *testing.T) {
	prob, err := NewProbabilisticSampler(0.1)
	require.NoError(t, err)
	rate := NewRateLimitingSampler(0.1)
	remote := &RemotelyControlledSampler{}
	remote.sampler = NewConstSampler(true)
	tests := []struct {
		sampler  Sampler
		typeTag  string
		paramTag interface{}
	}{
		{NewConstSampler(true), "const", true},
		{NewConstSampler(false), "const", false},
		{prob, "probabilistic", 0.1},
		{rate, "ratelimiting", 0.1},
		{remote, "const", true},
	}
	for _, test := range tests {
		_, tags := test.sampler.IsSampled(TraceID{}, testOperationName)
		count := 0
		for _, tag := range tags {
			if tag.key == SamplerTypeTagKey {
				assert.Equal(t, test.typeTag, tag.value)
				count++
			}
			if tag.key == SamplerParamTagKey {
				assert.Equal(t, test.paramTag, tag.value)
				count++
			}
		}
		assert.Equal(t, 2, count)
	}
}

func TestApplySamplerOptions(t *testing.T) {
	options := applySamplerOptions()
	sampler, ok := options.sampler.(*ProbabilisticSampler)
	assert.True(t, ok)
	assert.Equal(t, 0.001, sampler.samplingRate)

	assert.NotNil(t, options.logger)
	assert.NotZero(t, options.maxOperations)
	assert.NotEmpty(t, options.samplingServerURL)
	assert.NotNil(t, options.metrics)
	assert.NotZero(t, options.samplingRefreshInterval)
}

func TestProbabilisticSamplerErrors(t *testing.T) {
	_, err := NewProbabilisticSampler(-0.1)
	assert.Error(t, err)
	_, err = NewProbabilisticSampler(1.1)
	assert.Error(t, err)
}

func TestProbabilisticSampler(t *testing.T) {
	sampler, _ := NewProbabilisticSampler(0.5)
	sampled, tags := sampler.IsSampled(TraceID{Low: testMaxID + 10}, testOperationName)
	assert.False(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)
	sampled, tags = sampler.IsSampled(TraceID{Low: testMaxID - 20}, testOperationName)
	assert.True(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)
	sampler2, _ := NewProbabilisticSampler(0.5)
	assert.True(t, sampler.Equal(sampler2))
	assert.False(t, sampler.Equal(NewConstSampler(true)))
}

func TestProbabilisticSamplerPerformance(t *testing.T) {
	t.Skip("Skipped performance test")
	sampler, _ := NewProbabilisticSampler(0.01)
	rand := utils.NewRand(8736823764)
	var count uint64
	for i := 0; i < 100000000; i++ {
		id := TraceID{Low: uint64(rand.Int63())}
		if sampled, _ := sampler.IsSampled(id, testOperationName); sampled {
			count++
		}
	}
	println("Sampled:", count, "rate=", float64(count)/float64(100000000))
	// Sampled: 999829 rate= 0.009998290
}

func TestRateLimitingSampler(t *testing.T) {
	sampler := NewRateLimitingSampler(2)
	sampler2 := NewRateLimitingSampler(2)
	sampler3 := NewRateLimitingSampler(3)
	assert.True(t, sampler.Equal(sampler2))
	assert.False(t, sampler.Equal(sampler3))
	assert.False(t, sampler.Equal(NewConstSampler(false)))

	sampler = NewRateLimitingSampler(2)
	sampled, _ := sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.False(t, sampled)

	sampler = NewRateLimitingSampler(0.1)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.True(t, sampled)
	sampled, _ = sampler.IsSampled(TraceID{}, testOperationName)
	assert.False(t, sampled)
}

func TestGuaranteedThroughputProbabilisticSamplerUpdate(t *testing.T) {
	samplingRate := 0.5
	lowerBound := 2.0
	sampler, err := NewGuaranteedThroughputProbabilisticSampler(lowerBound, samplingRate)
	assert.NoError(t, err)

	assert.Equal(t, lowerBound, sampler.lowerBound)
	assert.Equal(t, samplingRate, sampler.samplingRate)

	newSamplingRate := 0.6
	newLowerBound := 1.0
	sampler.update(newLowerBound, newSamplingRate)
	assert.Equal(t, newLowerBound, sampler.lowerBound)
	assert.Equal(t, newSamplingRate, sampler.samplingRate)

	newSamplingRate = 1.1
	sampler.update(newLowerBound, newSamplingRate)
	assert.Equal(t, 1.0, sampler.samplingRate)
}

func TestAdaptiveSampler(t *testing.T) {
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: testDefaultSamplingProbability},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 1.0,
		PerOperationStrategies:           samplingRates,
	}

	sampler, err := NewAdaptiveSampler(strategies, testDefaultMaxOperations)
	require.NoError(t, err)
	defer sampler.Close()

	sampled, tags := sampler.IsSampled(TraceID{Low: testMaxID + 10}, testOperationName)
	assert.True(t, sampled)
	assert.Equal(t, testLowerBoundExpectedTags, tags)

	sampled, tags = sampler.IsSampled(TraceID{Low: testMaxID - 20}, testOperationName)
	assert.True(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)

	sampled, tags = sampler.IsSampled(TraceID{Low: testMaxID + 10}, testOperationName)
	assert.False(t, sampled)

	// This operation is seen for the first time by the sampler
	sampled, tags = sampler.IsSampled(TraceID{Low: testMaxID}, testFirstTimeOperationName)
	assert.True(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)
}

func TestAdaptiveSamplerErrors(t *testing.T) {
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 2.0,
		PerOperationStrategies: []*sampling.OperationSamplingStrategy{
			{
				Operation:             testOperationName,
				ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: -0.1},
			},
		},
	}

	sampler, err := NewAdaptiveSampler(strategies, testDefaultMaxOperations)
	assert.NoError(t, err)
	assert.Equal(t, 0.0, sampler.(*adaptiveSampler).samplers[testOperationName].samplingRate)

	strategies.PerOperationStrategies[0].ProbabilisticSampling.SamplingRate = 1.1
	sampler, err = NewAdaptiveSampler(strategies, testDefaultMaxOperations)
	assert.NoError(t, err)
	assert.Equal(t, 1.0, sampler.(*adaptiveSampler).samplers[testOperationName].samplingRate)
}

func TestAdaptiveSamplerUpdate(t *testing.T) {
	samplingRate := 0.1
	lowerBound := 2.0
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: samplingRate},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: lowerBound,
		PerOperationStrategies:           samplingRates,
	}

	s, err := NewAdaptiveSampler(strategies, testDefaultMaxOperations)
	assert.NoError(t, err)

	sampler, ok := s.(*adaptiveSampler)
	assert.True(t, ok)
	assert.Equal(t, lowerBound, sampler.lowerBound)
	assert.Equal(t, testDefaultSamplingProbability, sampler.defaultSampler.SamplingRate())
	assert.Len(t, sampler.samplers, 1)

	// Update the sampler with new sampling rates
	newSamplingRate := 0.2
	newLowerBound := 3.0
	newDefaultSamplingProbability := 0.1
	newSamplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: newSamplingRate},
		},
		{
			Operation:             testFirstTimeOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: newSamplingRate},
		},
	}
	strategies = &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       newDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: newLowerBound,
		PerOperationStrategies:           newSamplingRates,
	}

	sampler.update(strategies)
	assert.Equal(t, newLowerBound, sampler.lowerBound)
	assert.Equal(t, newDefaultSamplingProbability, sampler.defaultSampler.SamplingRate())
	assert.Len(t, sampler.samplers, 2)
}

func initAgent(t *testing.T) (*testutils.MockAgent, *RemotelyControlledSampler, *metrics.LocalFactory) {
	agent, err := testutils.StartMockAgent()
	require.NoError(t, err)

	metricsFactory := metrics.NewLocalFactory(0)
	metrics := NewMetrics(metricsFactory, nil)

	initialSampler, _ := NewProbabilisticSampler(0.001)
	sampler := NewRemotelyControlledSampler(
		"client app",
		SamplerOptions.Metrics(metrics),
		SamplerOptions.SamplingServerURL("http://"+agent.SamplingServerAddr()),
		SamplerOptions.MaxOperations(testDefaultMaxOperations),
		SamplerOptions.InitialSampler(initialSampler),
		SamplerOptions.Logger(log.NullLogger),
		SamplerOptions.SamplingRefreshInterval(time.Minute),
	)
	sampler.Close() // stop timer-based updates, we want to call them manually

	return agent, sampler, metricsFactory
}

func TestRemotelyControlledSampler(t *testing.T) {
	agent, remoteSampler, metricsFactory := initAgent(t)
	defer agent.Close()

	initSampler, ok := remoteSampler.sampler.(*ProbabilisticSampler)
	assert.True(t, ok)

	agent.AddSamplingStrategy("client app",
		getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, testDefaultSamplingProbability))
	remoteSampler.updateSampler()
	mTestutils.AssertCounterMetrics(t, metricsFactory, []mTestutils.ExpectedMetric{
		{Name: "jaeger.sampler", Tags: map[string]string{"state": "retrieved"}, Value: 1},
		{Name: "jaeger.sampler", Tags: map[string]string{"state": "updated"}, Value: 1},
	}...)
	_, ok = remoteSampler.sampler.(*ProbabilisticSampler)
	assert.True(t, ok)
	assert.NotEqual(t, initSampler, remoteSampler.sampler, "Sampler should have been updated")

	sampled, tags := remoteSampler.IsSampled(TraceID{Low: testMaxID + 10}, testOperationName)
	assert.False(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)
	sampled, tags = remoteSampler.IsSampled(TraceID{Low: testMaxID - 10}, testOperationName)
	assert.True(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)

	remoteSampler.sampler = initSampler
	c := make(chan time.Time)
	remoteSampler.Lock()
	remoteSampler.timer = &time.Ticker{C: c}
	remoteSampler.Unlock()
	go remoteSampler.pollController()

	c <- time.Now() // force update based on timer
	time.Sleep(10 * time.Millisecond)
	remoteSampler.Close()

	_, ok = remoteSampler.sampler.(*ProbabilisticSampler)
	assert.True(t, ok)
	assert.NotEqual(t, initSampler, remoteSampler.sampler, "Sampler should have been updated from timer")

	assert.True(t, remoteSampler.Equal(remoteSampler))
}

func generateTags(key string, value float64) []Tag {
	return []Tag{
		{"sampler.type", key},
		{"sampler.param", value},
	}
}

func TestRemotelyControlledSampler_updateSampler(t *testing.T) {
	tests := []struct {
		probabilities              map[string]float64
		defaultProbability         float64
		expectedDefaultProbability float64
		expectedTags               []Tag
	}{
		{
			probabilities:              map[string]float64{testOperationName: 1.1},
			defaultProbability:         testDefaultSamplingProbability,
			expectedDefaultProbability: testDefaultSamplingProbability,
			expectedTags:               generateTags("probabilistic", 1.0),
		},
		{
			probabilities:              map[string]float64{testOperationName: testDefaultSamplingProbability},
			defaultProbability:         testDefaultSamplingProbability,
			expectedDefaultProbability: testDefaultSamplingProbability,
			expectedTags:               testProbabilisticExpectedTags,
		},
		{
			probabilities: map[string]float64{
				testOperationName:          testDefaultSamplingProbability,
				testFirstTimeOperationName: testDefaultSamplingProbability,
			},
			defaultProbability:         testDefaultSamplingProbability,
			expectedDefaultProbability: testDefaultSamplingProbability,
			expectedTags:               testProbabilisticExpectedTags,
		},
		{
			probabilities:              map[string]float64{"new op": 1.1},
			defaultProbability:         testDefaultSamplingProbability,
			expectedDefaultProbability: testDefaultSamplingProbability,
			expectedTags:               testProbabilisticExpectedTags,
		},
		{
			probabilities:              map[string]float64{"new op": 1.1},
			defaultProbability:         1.1,
			expectedDefaultProbability: 1.0,
			expectedTags:               generateTags("probabilistic", 1.0),
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			agent, sampler, metricsFactory := initAgent(t)
			defer agent.Close()

			initSampler, ok := sampler.sampler.(*ProbabilisticSampler)
			assert.True(t, ok)

			res := &sampling.SamplingStrategyResponse{
				StrategyType: sampling.SamplingStrategyType_PROBABILISTIC,
				OperationSampling: &sampling.PerOperationSamplingStrategies{
					DefaultSamplingProbability:       test.defaultProbability,
					DefaultLowerBoundTracesPerSecond: 0.001,
				},
			}
			for opName, prob := range test.probabilities {
				res.OperationSampling.PerOperationStrategies = append(res.OperationSampling.PerOperationStrategies,
					&sampling.OperationSamplingStrategy{
						Operation: opName,
						ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{
							SamplingRate: prob,
						},
					},
				)
			}

			agent.AddSamplingStrategy("client app", res)
			sampler.updateSampler()

			mTestutils.AssertCounterMetrics(t, metricsFactory,
				mTestutils.ExpectedMetric{
					Name: "jaeger.sampler" + "|state=updated", Value: 1,
				},
			)

			s, ok := sampler.sampler.(*adaptiveSampler)
			assert.True(t, ok)
			assert.NotEqual(t, initSampler, sampler.sampler, "Sampler should have been updated")
			assert.Equal(t, test.expectedDefaultProbability, s.defaultSampler.SamplingRate())

			// First call is always sampled
			sampled, tags := sampler.IsSampled(TraceID{Low: testMaxID + 10}, testOperationName)
			assert.True(t, sampled)

			sampled, tags = sampler.IsSampled(TraceID{Low: testMaxID - 10}, testOperationName)
			assert.True(t, sampled)
			assert.Equal(t, test.expectedTags, tags)
		})
	}
}

func TestMaxOperations(t *testing.T) {
	samplingRates := []*sampling.OperationSamplingStrategy{
		{
			Operation:             testOperationName,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{SamplingRate: 0.1},
		},
	}
	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 2.0,
		PerOperationStrategies:           samplingRates,
	}

	sampler, err := NewAdaptiveSampler(strategies, 1)
	assert.NoError(t, err)

	sampled, tags := sampler.IsSampled(TraceID{Low: testMaxID - 10}, testFirstTimeOperationName)
	assert.True(t, sampled)
	assert.Equal(t, testProbabilisticExpectedTags, tags)
}

func TestSamplerQueryError(t *testing.T) {
	agent, sampler, metricsFactory := initAgent(t)
	defer agent.Close()

	// override the actual handler
	sampler.manager = &fakeSamplingManager{}

	initSampler, ok := sampler.sampler.(*ProbabilisticSampler)
	assert.True(t, ok)

	sampler.Close() // stop timer-based updates, we want to call them manually

	sampler.updateSampler()
	assert.Equal(t, initSampler, sampler.sampler, "Sampler should not have been updated due to query error")

	mTestutils.AssertCounterMetrics(t, metricsFactory,
		mTestutils.ExpectedMetric{
			Name:  "jaeger.sampler",
			Tags:  map[string]string{"phase": "query", "state": "failure"},
			Value: 1,
		},
	)
}

type fakeSamplingManager struct{}

func (c *fakeSamplingManager) GetSamplingStrategy(serviceName string) (*sampling.SamplingStrategyResponse, error) {
	return nil, errors.New("query error")
}

func TestRemotelyControlledSampler_updateSamplerFromAdaptiveSampler(t *testing.T) {
	agent, remoteSampler, metricsFactory := initAgent(t)
	defer agent.Close()
	remoteSampler.Close() // stop timer-based updates, we want to call them manually

	strategies := &sampling.PerOperationSamplingStrategies{
		DefaultSamplingProbability:       testDefaultSamplingProbability,
		DefaultLowerBoundTracesPerSecond: 1.0,
	}

	adaptiveSampler, err := NewAdaptiveSampler(strategies, testDefaultMaxOperations)
	require.NoError(t, err)

	// Overwrite the sampler with an adaptive sampler
	remoteSampler.sampler = adaptiveSampler

	agent.AddSamplingStrategy("client app",
		getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, 0.5))
	remoteSampler.updateSampler()

	// Sampler should have been updated to probabilistic
	_, ok := remoteSampler.sampler.(*ProbabilisticSampler)
	require.True(t, ok)

	// Overwrite the sampler with an adaptive sampler
	remoteSampler.sampler = adaptiveSampler

	agent.AddSamplingStrategy("client app",
		getSamplingStrategyResponse(sampling.SamplingStrategyType_RATE_LIMITING, 1))
	remoteSampler.updateSampler()

	// Sampler should have been updated to ratelimiting
	_, ok = remoteSampler.sampler.(*rateLimitingSampler)
	require.True(t, ok)

	// Overwrite the sampler with an adaptive sampler
	remoteSampler.sampler = adaptiveSampler

	// Update existing adaptive sampler
	agent.AddSamplingStrategy("client app", &sampling.SamplingStrategyResponse{OperationSampling: strategies})
	remoteSampler.updateSampler()

	mTestutils.AssertCounterMetrics(t, metricsFactory,
		mTestutils.ExpectedMetric{
			Name:  "jaeger.sampler",
			Tags:  map[string]string{"state": "retrieved"},
			Value: 3,
		},
		mTestutils.ExpectedMetric{
			Name:  "jaeger.sampler",
			Tags:  map[string]string{"state": "updated"},
			Value: 3,
		},
	)
}

func TestRemotelyControlledSampler_updateRateLimitingOrProbabilisticSampler(t *testing.T) {
	probabilisticSampler, err := NewProbabilisticSampler(0.002)
	require.NoError(t, err)
	otherProbabilisticSampler, err := NewProbabilisticSampler(0.003)
	require.NoError(t, err)
	maxProbabilisticSampler, err := NewProbabilisticSampler(1.0)
	require.NoError(t, err)

	rateLimitingSampler := NewRateLimitingSampler(2)
	otherRateLimitingSampler := NewRateLimitingSampler(3)

	testCases := []struct {
		res                  *sampling.SamplingStrategyResponse
		initSampler          Sampler
		expectedSampler      Sampler
		shouldErr            bool
		referenceEquivalence bool
		caption              string
	}{
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, 1.5),
			initSampler:          probabilisticSampler,
			expectedSampler:      maxProbabilisticSampler,
			shouldErr:            false,
			referenceEquivalence: false,
			caption:              "invalid probabilistic strategy",
		},
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, 0.002),
			initSampler:          probabilisticSampler,
			expectedSampler:      probabilisticSampler,
			shouldErr:            false,
			referenceEquivalence: true,
			caption:              "unchanged probabilistic strategy",
		},
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_PROBABILISTIC, 0.003),
			initSampler:          probabilisticSampler,
			expectedSampler:      otherProbabilisticSampler,
			shouldErr:            false,
			referenceEquivalence: false,
			caption:              "valid probabilistic strategy",
		},
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_RATE_LIMITING, 2),
			initSampler:          rateLimitingSampler,
			expectedSampler:      rateLimitingSampler,
			shouldErr:            false,
			referenceEquivalence: true,
			caption:              "unchanged rate limiting strategy",
		},
		{
			res:                  getSamplingStrategyResponse(sampling.SamplingStrategyType_RATE_LIMITING, 3),
			initSampler:          rateLimitingSampler,
			expectedSampler:      otherRateLimitingSampler,
			shouldErr:            false,
			referenceEquivalence: false,
			caption:              "valid rate limiting strategy",
		},
		{
			res:                  &sampling.SamplingStrategyResponse{},
			initSampler:          rateLimitingSampler,
			expectedSampler:      rateLimitingSampler,
			shouldErr:            true,
			referenceEquivalence: true,
			caption:              "invalid strategy",
		},
	}

	for _, tc := range testCases {
		testCase := tc // capture loop var
		t.Run(testCase.caption, func(t *testing.T) {
			remoteSampler := &RemotelyControlledSampler{samplerOptions: samplerOptions{sampler: testCase.initSampler}}
			err := remoteSampler.updateRateLimitingOrProbabilisticSampler(testCase.res)
			if testCase.shouldErr {
				require.Error(t, err)
			}
			if testCase.referenceEquivalence {
				assert.Equal(t, testCase.expectedSampler, remoteSampler.sampler)
			} else {
				assert.True(t, testCase.expectedSampler.Equal(remoteSampler.sampler))
			}
		})
	}
}

func getSamplingStrategyResponse(strategyType sampling.SamplingStrategyType, value float64) *sampling.SamplingStrategyResponse {
	if strategyType == sampling.SamplingStrategyType_PROBABILISTIC {
		return &sampling.SamplingStrategyResponse{
			StrategyType: sampling.SamplingStrategyType_PROBABILISTIC,
			ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{
				SamplingRate: value,
			},
		}
	}
	if strategyType == sampling.SamplingStrategyType_RATE_LIMITING {
		return &sampling.SamplingStrategyResponse{
			StrategyType: sampling.SamplingStrategyType_RATE_LIMITING,
			RateLimitingSampling: &sampling.RateLimitingSamplingStrategy{
				MaxTracesPerSecond: int16(value),
			},
		}
	}
	return nil
}

func TestAdaptiveSampler_lockRaceCondition(t *testing.T) {
	agent, remoteSampler, _ := initAgent(t)
	defer agent.Close()
	remoteSampler.Close() // stop timer-based updates, we want to call them manually

	numOperations := 1000
	adaptiveSampler, err := NewAdaptiveSampler(
		&sampling.PerOperationSamplingStrategies{
			DefaultSamplingProbability: 1,
		},
		2000,
	)
	require.NoError(t, err)

	// Overwrite the sampler with an adaptive sampler
	remoteSampler.sampler = adaptiveSampler

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(2)

	// Start 2 go routines that will simulate simultaneous calls to IsSampled
	go func() {
		defer wg.Done()
		isSampled(t, remoteSampler, numOperations, "a")
	}()
	go func() {
		defer wg.Done()
		isSampled(t, remoteSampler, numOperations, "b")
	}()
}

func isSampled(t *testing.T, remoteSampler *RemotelyControlledSampler, numOperations int, operationNamePrefix string) {
	for i := 0; i < numOperations; i++ {
		runtime.Gosched()
		sampled, _ := remoteSampler.IsSampled(TraceID{}, fmt.Sprintf("%s%d", operationNamePrefix, i))
		assert.True(t, sampled)
	}
}

// Copyright (c) 2016 Uber Technologies, Inc.
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

package testutils

import (
	"sync"

	"github.com/uber/jaeger-client-go/thrift-gen/sampling"
)

func newSamplingManager() *samplingManager {
	return &samplingManager{
		sampling: make(map[string]*sampling.SamplingStrategyResponse),
	}
}

type samplingManager struct {
	sampling map[string]*sampling.SamplingStrategyResponse
	mutex    sync.Mutex
}

// GetSamplingStrategy implements handler method of sampling.SamplingManager
func (s *samplingManager) GetSamplingStrategy(serviceName string) (*sampling.SamplingStrategyResponse, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if strategy, ok := s.sampling[serviceName]; ok {
		return strategy, nil
	}
	return &sampling.SamplingStrategyResponse{
		StrategyType: sampling.SamplingStrategyType_PROBABILISTIC,
		ProbabilisticSampling: &sampling.ProbabilisticSamplingStrategy{
			SamplingRate: 0.01,
		}}, nil
}

// AddSamplingStrategy registers a sampling strategy for a service
func (s *samplingManager) AddSamplingStrategy(service string, strategy *sampling.SamplingStrategyResponse) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.sampling[service] = strategy
}

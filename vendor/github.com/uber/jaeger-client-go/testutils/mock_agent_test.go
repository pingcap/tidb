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

package testutils

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/uber/jaeger-client-go/thrift-gen/jaeger"
	"github.com/uber/jaeger-client-go/thrift-gen/sampling"
	"github.com/uber/jaeger-client-go/utils"
)

func TestMockAgentSpanServer(t *testing.T) {
	mockAgent, err := StartMockAgent()
	require.NoError(t, err)
	defer mockAgent.Close()

	client, err := mockAgent.SpanServerClient()
	require.NoError(t, err)

	for i := 1; i < 5; i++ {
		batch := &jaeger.Batch{Process: &jaeger.Process{ServiceName: "svc"}}
		spans := make([]*jaeger.Span, i, i)
		for j := 0; j < i; j++ {
			spans[j] = jaeger.NewSpan()
			spans[j].OperationName = fmt.Sprintf("span-%d", j)
		}
		batch.Spans = spans

		err = client.EmitBatch(batch)
		assert.NoError(t, err)

		for k := 0; k < 100; k++ {
			time.Sleep(time.Millisecond)
			batches := mockAgent.GetJaegerBatches()
			if len(batches) > 0 && len(batches[0].Spans) == i {
				break
			}
		}
		batches := mockAgent.GetJaegerBatches()
		require.NotEmpty(t, len(batches))
		require.Equal(t, i, len(batches[0].Spans))
		for j := 0; j < i; j++ {
			assert.Equal(t, fmt.Sprintf("span-%d", j), batches[0].Spans[j].OperationName)
		}
		mockAgent.ResetJaegerBatches()
	}
}

func TestMockAgentSamplingManager(t *testing.T) {
	mockAgent, err := StartMockAgent()
	require.NoError(t, err)
	defer mockAgent.Close()

	err = utils.GetJSON("http://"+mockAgent.SamplingServerAddr()+"/", nil)
	require.Error(t, err, "no 'service' parameter")
	err = utils.GetJSON("http://"+mockAgent.SamplingServerAddr()+"/?service=a&service=b", nil)
	require.Error(t, err, "Too many 'service' parameters")

	var resp sampling.SamplingStrategyResponse
	err = utils.GetJSON("http://"+mockAgent.SamplingServerAddr()+"/?service=something", &resp)
	require.NoError(t, err)
	assert.Equal(t, sampling.SamplingStrategyType_PROBABILISTIC, resp.StrategyType)

	mockAgent.AddSamplingStrategy("service123", &sampling.SamplingStrategyResponse{
		StrategyType: sampling.SamplingStrategyType_RATE_LIMITING,
		RateLimitingSampling: &sampling.RateLimitingSamplingStrategy{
			MaxTracesPerSecond: 123,
		},
	})
	err = utils.GetJSON("http://"+mockAgent.SamplingServerAddr()+"/?service=service123", &resp)
	require.NoError(t, err)
	assert.Equal(t, sampling.SamplingStrategyType_RATE_LIMITING, resp.StrategyType)
	require.NotNil(t, resp.RateLimitingSampling)
	assert.EqualValues(t, 123, resp.RateLimitingSampling.MaxTracesPerSecond)
}

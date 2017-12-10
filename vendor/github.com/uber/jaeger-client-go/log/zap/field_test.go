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

package zap

import (
	"context"
	"testing"

	jaeger "github.com/uber/jaeger-client-go"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestTraceField(t *testing.T) {
	assert.Equal(t, zap.Skip(), Trace(nil), "Expected Trace of a nil context to be a no-op.")

	withTracedContext(func(ctx context.Context) {
		enc := zapcore.NewMapObjectEncoder()
		Trace(ctx).AddTo(enc)

		logged, ok := enc.Fields["trace"].(map[string]interface{})
		require.True(t, ok, "Expected trace to be a map.")

		// We could extract the span from the context and assert specific IDs,
		// but that just copies the production code. Instead, just assert that
		// the keys we expect are present.
		keys := make(map[string]struct{}, len(logged))
		for k := range logged {
			keys[k] = struct{}{}
		}
		assert.Equal(
			t,
			map[string]struct{}{"span": {}, "trace": {}},
			keys,
			"Expected to log span and trace IDs.",
		)
	})
}

func withTracedContext(f func(ctx context.Context)) {
	tracer, closer := jaeger.NewTracer(
		"serviceName", jaeger.NewConstSampler(true), jaeger.NewNullReporter(),
	)
	defer closer.Close()

	ctx := opentracing.ContextWithSpan(context.Background(), tracer.StartSpan("test"))
	f(ctx)
}

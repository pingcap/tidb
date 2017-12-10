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
	"fmt"

	jaeger "github.com/uber/jaeger-client-go"

	opentracing "github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Trace creates a field that extracts tracing information from a context and
// includes it under the "trace" key.
//
// Because the opentracing APIs don't expose this information, the returned
// zap.Field is a no-op for contexts that don't contain a span or contain a
// non-Jaeger span.
func Trace(ctx context.Context) zapcore.Field {
	if ctx == nil {
		return zap.Skip()
	}
	return zap.Object("trace", trace{ctx})
}

type trace struct {
	ctx context.Context
}

func (t trace) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	span := opentracing.SpanFromContext(t.ctx)
	if span == nil {
		return nil
	}
	j, ok := span.Context().(jaeger.SpanContext)
	if !ok {
		return nil
	}
	if !j.IsValid() {
		return fmt.Errorf("invalid span: %v", j.SpanID())
	}
	enc.AddString("span", j.SpanID().String())
	enc.AddString("trace", j.TraceID().String())
	return nil
}

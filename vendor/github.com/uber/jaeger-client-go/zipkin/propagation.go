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

package zipkin

import (
	"strconv"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"

	"github.com/uber/jaeger-client-go"
)

// Propagator is an Injector and Extractor
type Propagator struct{}

// NewZipkinB3HTTPHeaderPropagator creates a Propagator for extracting and injecting
// Zipkin HTTP B3 headers into SpanContexts.
func NewZipkinB3HTTPHeaderPropagator() Propagator {
	return Propagator{}
}

// Inject conforms to the Injector interface for decoding Zipkin HTTP B3 headers
func (p Propagator) Inject(
	sc jaeger.SpanContext,
	abstractCarrier interface{},
) error {
	textMapWriter, ok := abstractCarrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}

	// TODO this needs to change to support 128bit IDs
	textMapWriter.Set("x-b3-traceid", strconv.FormatUint(sc.TraceID().Low, 16))
	if sc.ParentID() != 0 {
		textMapWriter.Set("x-b3-parentspanid", strconv.FormatUint(uint64(sc.ParentID()), 16))
	}
	textMapWriter.Set("x-b3-spanid", strconv.FormatUint(uint64(sc.SpanID()), 16))
	if sc.IsSampled() {
		textMapWriter.Set("x-b3-sampled", "1")
	} else {
		textMapWriter.Set("x-b3-sampled", "0")
	}
	return nil
}

// Extract conforms to the Extractor interface for encoding Zipkin HTTP B3 headers
func (p Propagator) Extract(abstractCarrier interface{}) (jaeger.SpanContext, error) {
	textMapReader, ok := abstractCarrier.(opentracing.TextMapReader)
	if !ok {
		return jaeger.SpanContext{}, opentracing.ErrInvalidCarrier
	}
	var traceID uint64
	var spanID uint64
	var parentID uint64
	sampled := false
	err := textMapReader.ForeachKey(func(rawKey, value string) error {
		key := strings.ToLower(rawKey) // TODO not necessary for plain TextMap
		var err error
		if key == "x-b3-traceid" {
			traceID, err = strconv.ParseUint(value, 16, 64)
		} else if key == "x-b3-parentspanid" {
			parentID, err = strconv.ParseUint(value, 16, 64)
		} else if key == "x-b3-spanid" {
			spanID, err = strconv.ParseUint(value, 16, 64)
		} else if key == "x-b3-sampled" && value == "1" {
			sampled = true
		}
		return err
	})

	if err != nil {
		return jaeger.SpanContext{}, err
	}
	if traceID == 0 {
		return jaeger.SpanContext{}, opentracing.ErrSpanContextNotFound
	}
	return jaeger.NewSpanContext(
		jaeger.TraceID{Low: traceID},
		jaeger.SpanID(spanID),
		jaeger.SpanID(parentID),
		sampled, nil), nil
}

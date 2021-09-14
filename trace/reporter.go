// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package trace

import (
	"bytes"
	"sort"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/minitrace-go"
	"github.com/tikv/minitrace-go/datadog"
	"github.com/tikv/minitrace-go/jaeger"
	"go.uber.org/atomic"
)

var (
	// JaegerAgent to report tracing results.
	JaegerAgent = atomic.NewString("")
	// DatadogAgent to report tracing results.
	DatadogAgent = atomic.NewString("")
	// MaxSpansLength is the maximum length of spans produced by TiDB to report per SQL.
	MaxSpansLength = atomic.NewUint64(2000)
)

// Report tracing results to Jaeger and Datadog.
func Report(handle minitrace.TraceHandle) {
	trace, c := handle.Collect()
	ctx := c.(*Context)

	jaegerAgent := JaegerAgent.Load()
	datadogAgent := DatadogAgent.Load()

	shouldReportToJaeger := len(jaegerAgent) != 0
	shouldReportToDatadog := len(datadogAgent) != 0

	if (!shouldReportToJaeger && !shouldReportToDatadog) || !ctx.ShouldReport {
		return
	}

	go func() {
		trace.Spans = truncateSpans(trace.Spans, MaxSpansLength.Load())
		spanSet := minitraceToPbSpanSet(trace)

		traceDetail := ctx.TraceDetail
		traceDetail.SpanSets = append(traceDetail.SpanSets, &spanSet)

		spanSetLen, spanLen := traceDetailLen(traceDetail)
		if spanLen == 0 {
			return
		}

		var jgTraces *[]jaeger.Trace
		if shouldReportToJaeger {
			jgTraces = initJGTraces(spanSetLen)
		}
		var ddSpanList *datadog.SpanList
		if shouldReportToDatadog {
			ddSpanList = initDDSpanList(spanLen)
		}

		converter := newConverter(jgTraces, ddSpanList)
		converter.convert(traceDetail)

		reporter := newReporter(jaegerAgent, datadogAgent)
		reporter.reportToJaeger(jgTraces)
		reporter.reportToDatadog(ddSpanList)
	}()
}

func truncateSpans(spans []minitrace.Span, maxSpansLen uint64) []minitrace.Span {
	if len(spans) <= int(maxSpansLen) {
		return spans
	}

	sort.Sort(byBeginUnixTimeNs(spans))
	return spans[:maxSpansLen]
}

func traceDetailLen(traceDetail kvrpcpb.TraceDetail) (spanSetLen int, spanLen int) {
	spanSetLen = len(traceDetail.SpanSets)

	for _, set := range traceDetail.SpanSets {
		spanLen += len(set.Spans)
	}

	return
}

func initJGTraces(traceDetailSpanSetLen int) *[]jaeger.Trace {
	jTraces := make([]jaeger.Trace, 0, traceDetailSpanSetLen)
	return &jTraces
}

func initDDSpanList(traceDetailSpanLen int) *datadog.SpanList {
	s := make([]*datadog.Span, 0, traceDetailSpanLen)
	return (*datadog.SpanList)(&s)
}

func minitraceToPbSpanSet(trace minitrace.Trace) kvrpcpb.TraceDetail_SpanSet {
	ss := kvrpcpb.TraceDetail_SpanSet{
		ServiceName: "TiDB",
		TraceId: trace.TraceID,
		Spans:   make([]*kvrpcpb.TraceDetail_Span, 0, len(trace.Spans)),
	}

	for _, span := range trace.Spans {
		pps := make([]*kvrpcpb.TraceDetail_Span_Property, 0, len(span.Properties))
		for _, property := range span.Properties {
			pps = append(pps, &kvrpcpb.TraceDetail_Span_Property{
				Key:   property.Key,
				Value: property.Value,
			})
		}
		ss.Spans = append(ss.Spans, &kvrpcpb.TraceDetail_Span{
			Id:              span.ID,
			ParentId:        span.ParentID,
			BeginUnixTimeNs: span.BeginUnixTimeNs,
			DurationNs:      span.DurationNs,
			Event:           span.Event,
			Properties:      pps,
		})
	}

	return ss
}

type converter struct {
	jTraces   *[]jaeger.Trace
	dSpanList *datadog.SpanList

	curTraceID     uint64
	curServiceName string
	curJTrace      *jaeger.Trace

	tagBuf  []kv
	metaBuf map[string]string
}

func newConverter(jTraces *[]jaeger.Trace, dSpanList *datadog.SpanList) converter {
	return converter{
		jTraces:   jTraces,
		dSpanList: dSpanList,
		tagBuf:    make([]kv, 0, 1024),
	}
}

func (c *converter) convert(traceDetail kvrpcpb.TraceDetail) {
	for _, spanSet := range traceDetail.SpanSets {
		c.nextSpanSet(
			spanSet.ServiceName,
			len(spanSet.Spans),
			spanSet.TraceId,
		)

		for _, span := range spanSet.Spans {
			c.appendSpan(span)
		}
	}
}

func (c *converter) nextSpanSet(
	serviceName string,
	spanLen int,
	traceID uint64,
) {
	if c.jTraces != nil {
		*c.jTraces = append(*c.jTraces, jaeger.Trace{
			TraceIDLow:  int64(traceID),
			ServiceName: serviceName,
			Spans:       make([]jaeger.Span, 0, spanLen),
		})
		c.curJTrace = &(*c.jTraces)[len(*c.jTraces)-1]
	}

	c.curTraceID = traceID
	c.curServiceName = serviceName
}

func (c *converter) appendSpan(span *kvrpcpb.TraceDetail_Span) {
	c.updateProperties(span.Properties)

	if c.curJTrace != nil {
		c.curJTrace.Spans = append(c.curJTrace.Spans, jaeger.Span{
			SpanID:          int64(span.Id),
			ParentID:        int64(span.ParentId),
			StartUnixTimeUs: int64(span.BeginUnixTimeNs / 1000),
			DurationUs:      int64(span.DurationNs / 1000),
			OperationName:   span.Event,
			Tags:            c.tagBuf,
		})
	}

	if c.dSpanList != nil {
		*c.dSpanList = append(*c.dSpanList, &datadog.Span{
			Name:     span.Event,
			Service:  c.curServiceName,
			Start:    int64(span.BeginUnixTimeNs),
			Duration: int64(span.DurationNs),
			Meta:     c.metaBuf,
			SpanID:   span.Id,
			TraceID:  c.curTraceID,
			ParentID: span.ParentId,
		})
	}
}

func (c *converter) updateProperties(properties []*kvrpcpb.TraceDetail_Span_Property) {
	if len(properties) != 0 {
		c.tagBuf = c.tagBuf[len(c.tagBuf):]
		c.metaBuf = make(map[string]string, len(properties))
		for _, property := range properties {
			c.metaBuf[property.Key] = property.Value
			c.tagBuf = append(c.tagBuf, kv{
				Key:   property.Key,
				Value: property.Value,
			})
		}
	}
}

type reporter struct {
	jaegerAgent  string
	datadogAgent string

	buffer   *bytes.Buffer
	traceBuf []jaeger.Trace
}

func newReporter(jaegerAgent, datadogAgent string) reporter {
	return reporter{
		jaegerAgent:  jaegerAgent,
		datadogAgent: datadogAgent,

		buffer: bytes.NewBuffer(make([]byte, 0, 65535)),
	}
}

func (r *reporter) reportToJaeger(jTraces *[]jaeger.Trace) {
	if jTraces != nil {
		for _, trace := range *jTraces {
			r.splitTrace(trace)
			r.reportTraceBuf()
		}
	}
}

func (r *reporter) reportToDatadog(dSpanList *datadog.SpanList) {
	if dSpanList != nil {
		r.buffer.Truncate(0)
		err := datadog.MessagePackEncode(r.buffer, *dSpanList)
		if err != nil {
			return
		}

		err = datadog.Send(r.buffer, r.datadogAgent)
		if err != nil {
			return
		}
	}
}

func (r *reporter) splitTrace(trace jaeger.Trace) {
	if len(trace.Spans) == 0 {
		return
	}
	r.traceBuf = r.traceBuf[:0]

	LIMIT := 50
	totalTraces := (len(trace.Spans) + LIMIT - 1) / LIMIT
	for i := 0; i < totalTraces; i++ {
		r.traceBuf = append(r.traceBuf, jaeger.Trace{
			TraceIDLow:  trace.TraceIDLow,
			TraceIDHigh: trace.TraceIDHigh,
			ServiceName: trace.ServiceName,
			Spans:       trace.Spans[i*LIMIT : min((i+1)*LIMIT, len(trace.Spans))],
		})
	}

	return
}

func (r *reporter) reportTraceBuf() {
	for _, trace := range r.traceBuf {
		r.buffer.Reset()
		err := jaeger.ThriftCompactEncode(r.buffer, trace)
		if err != nil {
			break
		}

		err = jaeger.Send(r.buffer.Bytes(), r.jaegerAgent)
		if err != nil {
			break
		}
	}
}

type byBeginUnixTimeNs []minitrace.Span

func (a byBeginUnixTimeNs) Len() int           { return len(a) }
func (a byBeginUnixTimeNs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byBeginUnixTimeNs) Less(i, j int) bool { return a[i].BeginUnixTimeNs < a[j].BeginUnixTimeNs }

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

type kv = struct {
	Key   string
	Value string
}

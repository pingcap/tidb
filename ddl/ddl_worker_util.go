// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	minitrace "github.com/tikv/minitrace-go"
	"github.com/tikv/minitrace-go/jaeger"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var timeDetails = generic.NewSyncMap[int64, *spanCtx](10)

type spanCtx struct {
	ctx  context.Context
	root minitrace.TraceHandle
}

func injectSpan(jobID int64, event string) func() {
	if sctx, ok := timeDetails.Load(jobID); ok {
		hd := minitrace.StartSpan(sctx.ctx, event)
		return func() {
			hd.Finish()
		}
	}
	return func() {}
}

func initializeTrace(jobID int64) {
	ctx, root := minitrace.StartRootSpan(context.Background(),
		"add-index-worker", uint64(jobID), 0, nil)
	timeDetails.Store(jobID, &spanCtx{
		ctx:  ctx,
		root: root,
	})
}

func collectTrace(jobID int64) string {
	if sctx, ok := timeDetails.Load(jobID); ok {
		rootTrace, _ := sctx.root.Collect()
		analyzed := analyzeTrace(rootTrace)
		if len(rootTrace.Spans) < 1000 {
			reportTrace(rootTrace)
		}
		timeDetails.Delete(jobID)
		return analyzed
	}
	return ""
}

const batchSize = 512

func reportTrace(rootTrace minitrace.Trace) {
	buf := bytes.NewBuffer(make([]uint8, 0, 4096))
	for _, subTrace := range splitTraces(rootTrace) {
		buf.Reset()
		trace := jaeger.MiniSpansToJaegerTrace("add-index", subTrace)
		err := jaeger.ThriftCompactEncode(buf, trace)
		if err != nil {
			logutil.BgLogger().Warn("cannot collectTrace", zap.Error(err))
			return
		}
		err = jaeger.Send(buf.Bytes(), "127.0.0.1:6831")
		if err != nil {
			logutil.BgLogger().Warn("cannot collectTrace", zap.Error(err))
			return
		}
	}
}

func splitTraces(trace minitrace.Trace) []minitrace.Trace {
	var traces []minitrace.Trace
	for len(trace.Spans) > batchSize {
		traces = append(traces, minitrace.Trace{
			TraceID: trace.TraceID,
			Spans:   trace.Spans[:batchSize],
		})
		trace.Spans = trace.Spans[batchSize:]
	}
	traces = append(traces, minitrace.Trace{
		TraceID: trace.TraceID,
		Spans:   trace.Spans,
	})
	return traces
}

func analyzeTrace(trace minitrace.Trace) string {
	groupByEvent := make(map[string][]*minitrace.Span, 16)
	for i, span := range trace.Spans {
		spans := groupByEvent[span.Event]
		if len(spans) == 0 {
			groupByEvent[span.Event] = []*minitrace.Span{&trace.Spans[i]}
		} else {
			groupByEvent[span.Event] = append(spans, &trace.Spans[i])
		}
	}
	orderedEvents := make([]string, 0, len(groupByEvent))
	for event := range groupByEvent {
		orderedEvents = append(orderedEvents, event)
	}
	slices.Sort(orderedEvents)
	var sb strings.Builder
	sb.WriteString("{")
	for i := 0; i < len(orderedEvents); i++ {
		spans := groupByEvent[orderedEvents[i]]
		sum := uint64(0)
		min := uint64(math.MaxUint64)
		max := uint64(0)
		for _, span := range spans {
			dur := span.DurationNs
			sum += dur
			if dur < min {
				min = dur
			}
			if dur > max {
				max = dur
			}
		}
		sb.WriteString(orderedEvents[i])
		sb.WriteString(":")
		if len(spans) < 20 {
			sb.WriteString(fmt.Sprintf("%f", time.Duration(sum).Seconds()))
		} else {
			sb.WriteString(fmt.Sprintf(`{sum: %f, min: %f, max: %f, cnt: %d}`,
				time.Duration(sum).Seconds(), time.Duration(min).Seconds(),
				time.Duration(max).Seconds(), len(spans)))
		}
		if i != len(orderedEvents)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString("}")
	return sb.String()
}

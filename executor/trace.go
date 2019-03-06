// Copyright 2018 PingCAP, Inc.
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

package executor

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"sourcegraph.com/sourcegraph/appdash"
	traceImpl "sourcegraph.com/sourcegraph/appdash/opentracing"
)

// TraceExec represents a root executor of trace query.
type TraceExec struct {
	baseExecutor
	// CollectedSpans collects all span during execution. Span is appended via
	// callback method which passes into tracer implementation.
	CollectedSpans []basictracer.RawSpan
	// exhausted being true means there is no more result.
	exhausted bool
	// stmtNode is the real query ast tree and it is used for building real query's plan.
	stmtNode ast.StmtNode
	// rootTrace represents root span which is father of all other span.
	rootTrace opentracing.Span

	builder *executorBuilder
	format  string
}

// Next executes real query and collects span later.
func (e *TraceExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	req.Reset()
	if e.exhausted {
		return nil
	}
	se, ok := e.ctx.(sqlexec.SQLExecutor)
	if !ok {
		e.exhausted = true
		return nil
	}

	store := appdash.NewMemoryStore()
	tracer := traceImpl.NewTracer(store)
	span := tracer.StartSpan("trace")
	defer span.Finish()
	ctx = opentracing.ContextWithSpan(ctx, span)
	recordSets, err := se.Execute(ctx, e.stmtNode.Text())
	if err != nil {
		return errors.Trace(err)
	}

	for _, rs := range recordSets {
		_, err = drainRecordSet(ctx, e.ctx, rs)
		if err != nil {
			return errors.Trace(err)
		}
		if err = rs.Close(); err != nil {
			return errors.Trace(err)
		}
	}

	traces, err := store.Traces(appdash.TracesOpts{})
	if err != nil {
		return errors.Trace(err)
	}

	// Row format.
	if e.format != "json" {
		if len(traces) < 1 {
			e.exhausted = true
			return nil
		}
		trace := traces[0]
		sortTraceByStartTime(trace)
		dfsTree(trace, "", false, req.Chunk)
		e.exhausted = true
		return nil
	}

	// Json format.
	data, err := json.Marshal(traces)
	if err != nil {
		return errors.Trace(err)
	}

	// Split json data into rows to avoid the max packet size limitation.
	const maxRowLen = 4096
	for len(data) > maxRowLen {
		req.AppendString(0, string(data[:maxRowLen]))
		data = data[maxRowLen:]
	}
	req.AppendString(0, string(data))
	e.exhausted = true
	return nil
}

func drainRecordSet(ctx context.Context, sctx sessionctx.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	var rows []chunk.Row
	req := rs.NewRecordBatch()

	for {
		err := rs.Next(ctx, req)
		if err != nil || req.NumRows() == 0 {
			return rows, errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(req.Chunk)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		req.Chunk = chunk.Renew(req.Chunk, sctx.GetSessionVars().MaxChunkSize)
	}
}

type sortByStartTime []*appdash.Trace

func (t sortByStartTime) Len() int { return len(t) }
func (t sortByStartTime) Less(i, j int) bool {
	return getStartTime(t[j]).After(getStartTime(t[i]))
}
func (t sortByStartTime) Swap(i, j int) { t[i], t[j] = t[j], t[i] }

func getStartTime(trace *appdash.Trace) (t time.Time) {
	if e, err := trace.TimespanEvent(); err == nil {
		t = e.Start()
	}
	return
}

func sortTraceByStartTime(trace *appdash.Trace) {
	sort.Sort(sortByStartTime(trace.Sub))
	for _, t := range trace.Sub {
		sortTraceByStartTime(t)
	}
}

func dfsTree(t *appdash.Trace, prefix string, isLast bool, chk *chunk.Chunk) {
	var newPrefix, suffix string
	if len(prefix) == 0 {
		newPrefix = prefix + "  "
	} else {
		if !isLast {
			suffix = "├─"
			newPrefix = prefix + "│ "
		} else {
			suffix = "└─"
			newPrefix = prefix + "  "
		}
	}

	var start time.Time
	var duration time.Duration
	if e, err := t.TimespanEvent(); err == nil {
		start = e.Start()
		end := e.End()
		duration = end.Sub(start)
	}

	chk.AppendString(0, prefix+suffix+t.Span.Name())
	chk.AppendString(1, start.Format("15:04:05.000000"))
	chk.AppendString(2, duration.String())

	for i, sp := range t.Sub {
		dfsTree(sp, newPrefix, i == (len(t.Sub))-1 /*last element of array*/, chk)
	}
}

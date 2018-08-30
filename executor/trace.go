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
	"time"

	"github.com/juju/errors"
	"github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/tracing"
	"golang.org/x/net/context"
)

// TraceExec represents a root executor of trace query.
type TraceExec struct {
	baseExecutor
	// CollectedSpans collects all span during execution. Span is appended via
	// callback method which passes into tracer implementation.
	CollectedSpans []basictracer.RawSpan
	// exhausted being true means there is no more result.
	exhausted bool
	// plan is the real query plan and it is used for building real query's executor.
	plan plan.Plan
	// rootTrace represents root span which is father of all other span.
	rootTrace opentracing.Span

	childrenResults []*chunk.Chunk
}

// buildTrace builds a TraceExec for future executing. This method will be called
// at build().
func (b *executorBuilder) buildTrace(v *plan.Trace) Executor {
	e := &TraceExec{
		baseExecutor: newBaseExecutor(b.ctx, v.Schema(), v.ExplainID()),
	}

	pp, _ := v.StmtPlan.(plan.PhysicalPlan)
	e.children = make([]Executor, 0, len(pp.Children()))
	for _, child := range pp.Children() {
		switch p := child.(type) {
		case *plan.PhysicalTableReader, *plan.PhysicalIndexReader, *plan.PhysicalIndexLookUpReader, *plan.PhysicalHashAgg, *plan.PhysicalProjection, *plan.PhysicalStreamAgg, *plan.PhysicalSort:
			e.children = append(e.children, b.build(p))
		default:
			b.err = errors.Errorf("%v is not supported", child)
		}
	}

	return e
}

// Open opens a trace executor and it will create a root trace span which will be
// used for the following span in a relationship of `ChildOf` or `FollowFrom`.
// for more details, you could refer to http://opentracing.io
func (e *TraceExec) Open(ctx context.Context) error {
	e.rootTrace = tracing.NewRecordedTrace("trace_exec", func(sp basictracer.RawSpan) {
		e.CollectedSpans = append(e.CollectedSpans, sp)
	})
	// we actually don't care when underlying executor started. We only care how
	// much time was spent
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.childrenResults = make([]*chunk.Chunk, 0, len(e.children))
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, child.newChunk())
	}

	return nil
}

// Next executes real query and collects span later.
func (e *TraceExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.exhausted {
		return nil
	}

	// store span into context
	ctx = opentracing.ContextWithSpan(ctx, e.rootTrace)
	if len(e.children) > 0 {
		for {
			if err := e.children[0].Next(ctx, e.childrenResults[0]); err != nil {
				return errors.Trace(err)
			}
			if e.childrenResults[0].NumRows() != 0 {
				break
			}
		}
	}

	e.rootTrace.LogKV("event", "tracing completed")
	e.rootTrace.Finish()
	var rootSpan basictracer.RawSpan

	treeSpans := make(map[uint64][]basictracer.RawSpan)
	for _, sp := range e.CollectedSpans {
		treeSpans[sp.ParentSpanID] = append(treeSpans[sp.ParentSpanID], sp)
		// if a span's parentSpanID is 0, then it is root span
		// this is by design
		if sp.ParentSpanID == 0 {
			rootSpan = sp
		}
	}

	dfsTree(rootSpan, treeSpans, "", false, chk)
	e.exhausted = true
	return nil
}

func dfsTree(span basictracer.RawSpan, tree map[uint64][]basictracer.RawSpan, prefix string, isLast bool, chk *chunk.Chunk) {
	suffix := ""
	spans := tree[span.Context.SpanID]
	var newPrefix string
	if span.ParentSpanID == 0 {
		newPrefix = prefix
	} else {
		if len(tree[span.ParentSpanID]) > 0 && !isLast {
			suffix = "├─"
			newPrefix = prefix + "│ "
		} else {
			suffix = "└─"
			newPrefix = prefix + "  "
		}
	}

	chk.AppendString(0, prefix+suffix+span.Operation)
	chk.AppendString(1, span.Start.Format(time.StampNano))
	chk.AppendString(2, span.Duration.String())

	for i, sp := range spans {
		dfsTree(sp, tree, newPrefix, i == (len(spans))-1 /*last element of array*/, chk)
	}
}

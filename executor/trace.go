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
	"fmt"

	"bytes"
	"github.com/juju/errors"
	"github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/tracing"
	"golang.org/x/net/context"
)

var traceColumns = append([]*types.FieldType{})

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
		case *plan.PhysicalTableReader, *plan.PhysicalIndexReader, *plan.PhysicalIndexLookUpReader, *plan.PhysicalHashAgg, *plan.PhysicalProjection, *plan.PhysicalStreamAgg:
			e.children = append(e.children, b.build(p))
		default:
			panic(fmt.Sprintf("%v is not supported", child))
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
	ctx = opentracing.ContextWithSpan(ctx, e.rootTrace)
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

func getPrefix(idx int, suffix string, opName string) string {
	var buf bytes.Buffer
	for i := 0; i < 2*idx; i++ {
		buf.WriteString(" ")
	}

	buf.WriteString(suffix)
	buf.WriteString(opName)
	return buf.String()
}

// Next executes real query and collects span later.
func (e *TraceExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.exhausted {
		return nil
	}

	ctx = opentracing.ContextWithSpan(ctx, e.rootTrace)
	if len(e.children) > 0 {
		if err := e.children[0].Next(ctx, e.childrenResults[0]); err != nil {
			return errors.Trace(err)
		}
	}

	e.rootTrace.LogKV("event", "tracing completed")
	e.rootTrace.Finish()
	var rootSpan basictracer.RawSpan
	treeSpans := make(map[uint64][]basictracer.RawSpan)
	for _, sp := range e.CollectedSpans {
		if spans, ok := treeSpans[sp.ParentSpanID]; ok {
			treeSpans[sp.ParentSpanID] = append(spans, sp)
		} else {
			treeSpans[sp.ParentSpanID] = make([]basictracer.RawSpan, 0)
		}
		if sp.Context.SpanID == 0 {
			rootSpan = sp
		}
	}

	// add root span here
	dfsTree(rootSpan, treeSpans, "", chk)
	e.exhausted = true
	return nil
}

func dfsTree(span basictracer.RawSpan, tree map[uint64][]basictracer.RawSpan, prefix string, chk *chunk.Chunk) {
	// each span has a operation name, start time and duration.
	// add two empty string to prefix
	suffix := ""
	hasChild := false
	spans := tree[span.Context.SpanID]
	if len(spans) > 0 {
		hasChild = true
		// prefix += "| "
		suffix = "├─"
	} else {
		suffix = "└─"
	}
	fmt.Println("len of spans is ", len(spans))
	for _, sp := range spans {
		chk.AppendString(0, prefix+suffix+sp.Operation)
		chk.AppendString(1, sp.Duration.String())
		chk.AppendInt64(2, int64(sp.Context.SpanID))
		if hasChild {
			dfsTree(sp, tree, prefix+"| ", chk)
		} else {
			dfsTree(sp, tree, prefix+"  ", chk)
		}
	}
}

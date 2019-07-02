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

	"github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
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
	// stmtNode is the real query ast tree and it is used for building real query's plan.
	stmtNode ast.StmtNode
	// rootTrace represents root span which is father of all other span.
	rootTrace opentracing.Span

	builder *executorBuilder
}

// Next executes real query and collects span later.
func (e *TraceExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
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

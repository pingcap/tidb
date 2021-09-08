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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracing_test

import (
	"context"
	"testing"

	basictracer "github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/stretchr/testify/require"
)

func TestSpanFromContext(t *testing.T) {
	ctx := context.TODO()
	noopSp := tracing.SpanFromContext(ctx)
	// test noop span
	_, ok := noopSp.Tracer().(opentracing.NoopTracer)
	require.True(t, ok)

	// test tidb tracing
	collectedSpan := make([]basictracer.RawSpan, 1)
	sp := tracing.NewRecordedTrace("test", func(sp basictracer.RawSpan) {
		collectedSpan = append(collectedSpan, sp)
	})
	sp.Finish()
	opentracing.ContextWithSpan(ctx, sp)
	child := tracing.SpanFromContext(ctx)
	child.Finish()

	// verify second span's operation is not nil, this way we can ensure
	// callback logic works.
	require.NotNil(t, collectedSpan[0].Operation)
}

func TestChildSpanFromContext(t *testing.T) {
	ctx := context.TODO()
	noopSp, _ := tracing.ChildSpanFromContxt(ctx, "")
	_, ok := noopSp.Tracer().(opentracing.NoopTracer)
	require.True(t, ok)

	// test tidb tracing
	collectedSpan := make([]basictracer.RawSpan, 1)
	sp := tracing.NewRecordedTrace("test", func(sp basictracer.RawSpan) {
		collectedSpan = append(collectedSpan, sp)
	})
	sp.Finish()
	opentracing.ContextWithSpan(ctx, sp)
	child, _ := tracing.ChildSpanFromContxt(ctx, "test_child")
	child.Finish()

	// verify second span's operation is not nil, this way we can ensure
	// callback logic works.
	require.NotNil(t, collectedSpan[1].Operation)

}

func TestFollowFrom(t *testing.T) {
	var collectedSpans []basictracer.RawSpan
	// first start a root span
	sp1 := tracing.NewRecordedTrace("test", func(sp basictracer.RawSpan) {
		collectedSpans = append(collectedSpans, sp)
	})
	sp2 := sp1.Tracer().StartSpan("follow_from", opentracing.FollowsFrom(sp1.Context()))
	sp1.Finish()
	sp2.Finish()
	require.Equal(t, "follow_from", collectedSpans[1].Operation)
	require.NotEqual(t, uint64(0), collectedSpans[1].ParentSpanID)
	// only root span has 0 parent id
	require.Equal(t, uint64(0), collectedSpans[0].ParentSpanID)
}

func TestCreateSapnBeforeSetupGlobalTracer(t *testing.T) {
	var collectedSpans []basictracer.RawSpan
	sp := opentracing.StartSpan("before")
	sp.Finish()

	// first start a root span
	sp1 := tracing.NewRecordedTrace("test", func(sp basictracer.RawSpan) {
		collectedSpans = append(collectedSpans, sp)
	})
	sp1.Finish()

	// sp is a span started before we setup global tracer; hence such span will be
	// droped.
	require.Len(t, collectedSpans, 1)
}

func TestTreeRelationship(t *testing.T) {
	var collectedSpans []basictracer.RawSpan
	ctx := context.TODO()
	// first start a root span
	sp1 := tracing.NewRecordedTrace("test", func(sp basictracer.RawSpan) {
		collectedSpans = append(collectedSpans, sp)
	})

	// then store such root span into context
	ctx = opentracing.ContextWithSpan(ctx, sp1)

	// create children span from context
	sp2, ctx := tracing.ChildSpanFromContxt(ctx, "parent")
	sp3, _ := tracing.ChildSpanFromContxt(ctx, "child")

	// notify span that we are about to reach end of journey.
	sp1.Finish()
	sp2.Finish()
	sp3.Finish()

	// ensure length of collectedSpans is greather than 0
	require.Greater(t, len(collectedSpans), 0)
	if len(collectedSpans) > 0 {
		require.Equal(t, "test", collectedSpans[0].Operation)
		require.Equal(t, "parent", collectedSpans[1].Operation)
		require.Equal(t, "child", collectedSpans[2].Operation)
		// check tree relationship
		// sp1 is parent of sp2. And sp2 is parent of sp3.
		require.Equal(t, collectedSpans[0].Context.SpanID, collectedSpans[1].ParentSpanID)
		require.Equal(t, collectedSpans[1].Context.SpanID, collectedSpans[2].ParentSpanID)
	}
}

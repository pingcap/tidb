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

package tracing_test

import (
	"testing"

	. "github.com/pingcap/check"

	basictracer "github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/util/tracing"
	"golang.org/x/net/context"
)

var _ = Suite(&testTraceSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

type testTraceSuite struct {
}

func (s *testTraceSuite) TestSpanFromContext(c *C) {
	ctx := context.TODO()
	noopSp := tracing.SpanFromContext(ctx)
	// test noop span
	_, ok := noopSp.Tracer().(opentracing.NoopTracer)
	c.Assert(ok, IsTrue)

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
	c.Assert(collectedSpan[0].Operation, NotNil)
}

func (s *testTraceSuite) TestChildSpanFromContext(c *C) {
	ctx := context.TODO()
	noopSp, _ := tracing.ChildSpanFromContxt(ctx, "")
	_, ok := noopSp.Tracer().(opentracing.NoopTracer)
	c.Assert(ok, IsTrue)

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
	c.Assert(collectedSpan[1].Operation, NotNil)

}

func (s *testTraceSuite) TestTreeRelationship(c *C) {
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
	c.Assert(len(collectedSpans), Greater, 0)
	if len(collectedSpans) > 0 {
		c.Assert(collectedSpans[0].Operation, Equals, "test")
		c.Assert(collectedSpans[1].Operation, Equals, "parent")
		c.Assert(collectedSpans[2].Operation, Equals, "child")
	}
}

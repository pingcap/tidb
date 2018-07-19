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
	ctx := context.Background()
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
	ctx := context.Background()
	noopSp := tracing.ChildSpanFromContxt(ctx, "")
	_, ok := noopSp.Tracer().(opentracing.NoopTracer)
	c.Assert(ok, IsTrue)

	// test tidb tracing
	collectedSpan := make([]basictracer.RawSpan, 1)
	sp := tracing.NewRecordedTrace("test", func(sp basictracer.RawSpan) {
		collectedSpan = append(collectedSpan, sp)
	})
	sp.Finish()
	opentracing.ContextWithSpan(ctx, sp)
	child := tracing.ChildSpanFromContxt(ctx, "test_child")
	child.Finish()

	// verify second span's operation is not nil, this way we can ensure
	// callback logic works.
	c.Assert(collectedSpan[1].Operation, NotNil)

}

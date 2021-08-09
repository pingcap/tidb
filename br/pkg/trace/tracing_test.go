// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package trace

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	. "github.com/pingcap/check"
)

type testTracingSuite struct{}

var _ = Suite(&testTracingSuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func jobA(ctx context.Context) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("jobA", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	jobB(ctx)
	time.Sleep(time.Second)
}

func jobB(ctx context.Context) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("jobB", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	time.Sleep(time.Second)
}

func (t *testTracingSuite) TestSpan(c *C) {
	filename := path.Join(c.MkDir(), "br.trace")
	getTraceFileName = func() string {
		return filename
	}
	defer func() {
		getTraceFileName = timestampTraceFileName
	}()
	ctx, store := TracerStartSpan(context.Background())
	jobA(ctx)
	TracerFinishSpan(ctx, store)
	content, err := os.ReadFile(filename)
	c.Assert(err, IsNil)
	s := string(content)
	// possible result:
	// "jobA      18:00:30.745723  2.009301253s\n"
	// "  └─jobB  18:00:30.745724  1.003791041s\n"
	c.Assert(s, Matches, `jobA.*2\.[0-9]+s\n  └─jobB.*1\.[0-9]+s\n`)
}

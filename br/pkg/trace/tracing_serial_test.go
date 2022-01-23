// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package trace

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/require"
)

func jobA(ctx context.Context) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("jobA", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}
	jobB(ctx)
	time.Sleep(100 * time.Millisecond)
}

func jobB(ctx context.Context) {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("jobB", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
	}
	time.Sleep(100 * time.Millisecond)
}

func TestSpan(t *testing.T) {
	filename := path.Join(t.TempDir(), "br.trace")
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
	require.NoError(t, err)
	s := string(content)
	// possible result:
	// "jobA      22:02:02.545296  200.621764ms\n"
	// "  └─jobB  22:02:02.545297  100.293444ms\n"
	require.Regexp(t, `^jobA.*2[0-9][0-9]\.[0-9]+ms\n  └─jobB.*1[0-9][0-9]\.[0-9]+ms\n$`, s)
}

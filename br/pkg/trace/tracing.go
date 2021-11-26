// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package trace

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/cheynewallace/tabby"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"sourcegraph.com/sourcegraph/appdash"
	traceImpl "sourcegraph.com/sourcegraph/appdash/opentracing"
)

var getTraceFileName = timestampTraceFileName

func timestampTraceFileName() string {
	return filepath.Join(os.TempDir(), time.Now().Format("br.trace.2006-01-02T15.04.05Z0700"))
}

// TracerStartSpan starts the tracer for BR
func TracerStartSpan(ctx context.Context) (context.Context, *appdash.MemoryStore) {
	store := appdash.NewMemoryStore()
	tracer := traceImpl.NewTracer(store)
	span := tracer.StartSpan("trace")
	ctx = opentracing.ContextWithSpan(ctx, span)
	return ctx, store
}

// TracerFinishSpan finishes the tracer for BR
func TracerFinishSpan(ctx context.Context, store appdash.Queryer) {
	span := opentracing.SpanFromContext(ctx)
	traces, err := store.Traces(appdash.TracesOpts{})
	if err != nil {
		log.Error("fail to get traces", zap.Error(err))
		return
	}
	span.Finish()
	if len(traces) < 1 {
		return
	}
	trace := traces[0]

	filename := getTraceFileName()
	file, err := os.Create(filename)
	if err != nil {
		log.Error("fail to open trace file", zap.Error(err))
		return
	}
	defer file.Close()
	fmt.Fprintf(os.Stderr, "Detail BR trace in %s \n", filename)
	log.Info("Detail BR trace", zap.String("filename", filename))
	tub := tabby.NewCustom(tabwriter.NewWriter(file, 0, 0, 2, ' ', 0))

	dfsTree(trace, "", false, tub)
	tub.Print()
}

func dfsTree(t *appdash.Trace, prefix string, isLast bool, tub *tabby.Tabby) {
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

	tub.AddLine(prefix+suffix+t.Span.Name(), start.Format("15:04:05.000000"), duration.String())

	// Sort events by their start time
	sort.Slice(t.Sub, func(i, j int) bool {
		var istart, jstart time.Time
		if ievent, err := t.Sub[i].TimespanEvent(); err == nil {
			istart = ievent.Start()
		}
		if jevent, err := t.Sub[j].TimespanEvent(); err == nil {
			jstart = jevent.Start()
		}
		return istart.Before(jstart)
	})

	for i, sp := range t.Sub {
		dfsTree(sp, newPrefix, i == (len(t.Sub))-1 /*last element of array*/, tub)
	}
}

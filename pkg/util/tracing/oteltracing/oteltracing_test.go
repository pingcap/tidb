// Copyright 2026 PingCAP, Inc.
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

package oteltracing

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type warnings struct {
	mu   sync.Mutex
	msgs []string
}

func (w *warnings) handle(msg string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, msg)
}

func (w *warnings) get() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.msgs...)
}

func newRecordingTracer(t *testing.T, cfg Config) (*Tracer, *tracetest.SpanRecorder) {
	recorder := tracetest.NewSpanRecorder()
	tr, err := newTracer(context.Background(), cfg, sdktrace.WithSpanProcessor(recorder))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tr.Shutdown(context.Background())) })
	return tr, recorder
}

// useGlobalTracer installs tr as the OpenTracing global tracer, the same way
// tidb-server does, and restores the previous one when the test ends.
func useGlobalTracer(t *testing.T, tr opentracing.Tracer) {
	prev := opentracing.GlobalTracer()
	opentracing.SetGlobalTracer(tr)
	t.Cleanup(func() { opentracing.SetGlobalTracer(prev) })
}

func resourceAttr(attrs []attribute.KeyValue, key string) string {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	return ""
}

func TestTiDBSpansAreRecordedThroughBridge(t *testing.T) {
	warn := &warnings{}
	tr, recorder := newRecordingTracer(t, Config{
		SampleRatio:       1,
		ServiceName:       "TiDB",
		ServiceVersion:    "v9.0.0",
		ServiceInstanceID: "127.0.0.1:4000",
		WarningHandler:    warn.handle,
	})
	useGlobalTracer(t, tr)

	// Mirror what clientConn.dispatch and the executor do.
	region, ctx := tracing.StartRegionWithNewRootSpan(context.Background(), "server.dispatch")
	child, ctx := tracing.ChildSpanFromContxt(ctx, "session.ExecuteStmt")
	child.SetTag("sql", "select 1")
	grandchild, _ := tracing.ChildSpanFromContxt(ctx, "executor.Next")
	grandchild.Finish()
	child.Finish()
	region.End()

	spans := recorder.Ended()
	require.Len(t, spans, 3)
	byName := make(map[string]sdktrace.ReadOnlySpan, len(spans))
	for _, s := range spans {
		byName[s.Name()] = s
	}
	root := byName["server.dispatch"]
	stmt := byName["session.ExecuteStmt"]
	next := byName["executor.Next"]
	require.NotNil(t, root)
	require.NotNil(t, stmt)
	require.NotNil(t, next)

	require.False(t, root.Parent().IsValid())
	require.Equal(t, root.SpanContext().TraceID(), stmt.SpanContext().TraceID())
	require.Equal(t, root.SpanContext().TraceID(), next.SpanContext().TraceID())
	require.Equal(t, root.SpanContext().SpanID(), stmt.Parent().SpanID())
	require.Equal(t, stmt.SpanContext().SpanID(), next.Parent().SpanID())
	require.Contains(t, stmt.Attributes(), attribute.String("sql", "select 1"))
	require.Equal(t, instrumentationName, root.InstrumentationScope().Name)

	resAttrs := root.Resource().Attributes()
	require.Equal(t, "TiDB", resourceAttr(resAttrs, "service.name"))
	require.Equal(t, "v9.0.0", resourceAttr(resAttrs, "service.version"))
	require.Equal(t, "127.0.0.1:4000", resourceAttr(resAttrs, "service.instance.id"))
	require.Equal(t, "opentelemetry", resourceAttr(resAttrs, "telemetry.sdk.name"))

	require.Empty(t, warn.get())
}

func TestSampleRatio(t *testing.T) {
	tr, recorder := newRecordingTracer(t, Config{SampleRatio: 0})
	useGlobalTracer(t, tr)

	region, ctx := tracing.StartRegionWithNewRootSpan(context.Background(), "server.dispatch")
	child, _ := tracing.ChildSpanFromContxt(ctx, "session.ExecuteStmt")
	child.Finish()
	region.End()
	// Unsampled roots and their children are never recorded.
	require.Empty(t, recorder.Ended())

	for _, ratio := range []float64{-0.1, 1.5} {
		_, err := newTracer(context.Background(), Config{SampleRatio: ratio}, sdktrace.WithSpanProcessor(tracetest.NewSpanRecorder()))
		require.ErrorContains(t, err, "sample ratio must be in [0, 1]")
	}
}

func TestInjectExtractUsesW3CTraceContext(t *testing.T) {
	tr, recorder := newRecordingTracer(t, Config{SampleRatio: 1})

	parent := tr.StartSpan("client")
	carrier := opentracing.HTTPHeadersCarrier(http.Header{})
	require.NoError(t, tr.Inject(parent.Context(), opentracing.HTTPHeaders, carrier))
	traceparent := http.Header(carrier).Get("traceparent")
	require.NotEmpty(t, traceparent)

	remote, err := tr.Extract(opentracing.HTTPHeaders, carrier)
	require.NoError(t, err)
	server := tr.StartSpan("server", opentracing.ChildOf(remote))
	server.Finish()
	parent.Finish()

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	require.Equal(t, spans[0].SpanContext().TraceID(), spans[1].SpanContext().TraceID())
	require.Contains(t, traceparent, spans[1].SpanContext().TraceID().String())
}

func TestEnvironmentOverridesServiceName(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "tidb-prod")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=staging")
	tr, recorder := newRecordingTracer(t, Config{SampleRatio: 1, ServiceName: "TiDB"})

	tr.StartSpan("server.dispatch").Finish()

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	resAttrs := spans[0].Resource().Attributes()
	require.Equal(t, "tidb-prod", resourceAttr(resAttrs, "service.name"))
	require.Equal(t, "staging", resourceAttr(resAttrs, "deployment.environment"))
}

// fakeCollector is a minimal OTLP/gRPC trace receiver.
type fakeCollector struct {
	coltracepb.UnimplementedTraceServiceServer
	mu       sync.Mutex
	requests []*coltracepb.ExportTraceServiceRequest
}

func (c *fakeCollector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, req)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type exportedSpan struct {
	name, serviceName, traceID, parentSpanID, spanID string
}

func (c *fakeCollector) spans() []exportedSpan {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []exportedSpan
	for _, req := range c.requests {
		for _, rs := range req.ResourceSpans {
			service := ""
			for _, kv := range rs.Resource.Attributes {
				if kv.Key == "service.name" {
					service = kv.Value.GetStringValue()
				}
			}
			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					out = append(out, exportedSpan{
						name:         s.Name,
						serviceName:  service,
						traceID:      string(s.TraceId),
						parentSpanID: string(s.ParentSpanId),
						spanID:       string(s.SpanId),
					})
				}
			}
		}
	}
	return out
}

func startFakeCollector(t *testing.T) (string, *fakeCollector) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	collector := &fakeCollector{}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, collector)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(lis)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return lis.Addr().String(), collector
}

func TestExportOverOTLPGRPC(t *testing.T) {
	addr, collector := startFakeCollector(t)

	for _, endpoint := range []string{addr, "http://" + addr} {
		before := len(collector.spans())
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		tr, err := New(ctx, Config{
			Endpoint:    endpoint,
			Insecure:    endpoint == addr,
			SampleRatio: 1,
			ServiceName: "TiDB",
		})
		require.NoError(t, err)

		root := tr.StartSpan("server.dispatch")
		child := tr.StartSpan("session.ExecuteStmt", opentracing.ChildOf(root.Context()))
		child.Finish()
		root.Finish()
		// Shutdown must flush the batched spans before returning.
		require.NoError(t, tr.Shutdown(ctx))
		cancel()

		spans := collector.spans()[before:]
		require.Len(t, spans, 2, "endpoint %s", endpoint)
		byName := map[string]exportedSpan{}
		for _, s := range spans {
			require.Equal(t, "TiDB", s.serviceName)
			byName[s.name] = s
		}
		require.Equal(t, byName["server.dispatch"].traceID, byName["session.ExecuteStmt"].traceID)
		require.Equal(t, byName["server.dispatch"].spanID, byName["session.ExecuteStmt"].parentSpanID)
		require.Empty(t, byName["server.dispatch"].parentSpanID)
	}
}

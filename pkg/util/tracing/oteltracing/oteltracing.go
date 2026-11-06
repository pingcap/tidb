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

// Package oteltracing exports TiDB's tracing spans over OTLP.
//
// TiDB and client-go are instrumented with the OpenTracing API. Instead of
// rewriting every call site, this package builds an OpenTracing tracer that is
// backed by the OpenTelemetry SDK through the official OpenTracing bridge
// (go.opentelemetry.io/otel/bridge/opentracing). Once it is installed as the
// OpenTracing global tracer, every existing span is recorded by the
// OpenTelemetry SDK and exported to an OTLP/gRPC endpoint such as an
// OpenTelemetry Collector, Jaeger, or Tempo.
package oteltracing

import (
	"context"
	goerrors "errors"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/errors"
	"go.opentelemetry.io/otel/attribute"
	otbridge "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// instrumentationName is the instrumentation scope reported with every span.
const instrumentationName = "github.com/pingcap/tidb"

// Config describes how spans are sampled and where they are exported.
type Config struct {
	// Endpoint is the OTLP/gRPC endpoint. It accepts either "host:port" or a
	// URL such as "http://collector:4317". When it is empty, the standard
	// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT / OTEL_EXPORTER_OTLP_ENDPOINT
	// environment variables are used, falling back to "localhost:4317".
	Endpoint string
	// Insecure disables TLS for the exporter connection.
	Insecure bool
	// SampleRatio is the fraction of root spans to sample, in [0, 1]. Child
	// spans follow their parent's sampling decision.
	SampleRatio float64
	// ServiceName is reported as the service.name resource attribute.
	// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES take precedence.
	ServiceName string
	// ServiceVersion is reported as the service.version resource attribute.
	ServiceVersion string
	// ServiceInstanceID is reported as the service.instance.id resource
	// attribute, so spans from different TiDB instances can be told apart.
	ServiceInstanceID string
	// WarningHandler receives warnings from the OpenTracing bridge, for
	// example when a span context from another tracer is used as a parent.
	WarningHandler func(msg string)
}

// Tracer is an OpenTracing tracer whose spans are recorded by the
// OpenTelemetry SDK. Call Shutdown before the process exits to flush
// buffered spans.
type Tracer struct {
	opentracing.Tracer
	provider *sdktrace.TracerProvider
}

// New creates a Tracer that exports spans over OTLP/gRPC. Creating it does
// not wait for the endpoint to be reachable; export failures are reported by
// the OpenTelemetry SDK's error handler.
func New(ctx context.Context, cfg Config) (*Tracer, error) {
	exporter, err := otlptracegrpc.New(ctx, exporterOptions(cfg)...)
	if err != nil {
		return nil, errors.Annotate(err, "create OTLP trace exporter")
	}
	tracer, err := newTracer(ctx, cfg, sdktrace.WithBatcher(exporter))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return tracer, nil
}

func exporterOptions(cfg Config) []otlptracegrpc.Option {
	var opts []otlptracegrpc.Option
	if cfg.Endpoint != "" {
		if strings.Contains(cfg.Endpoint, "://") {
			opts = append(opts, otlptracegrpc.WithEndpointURL(cfg.Endpoint))
		} else {
			opts = append(opts, otlptracegrpc.WithEndpoint(cfg.Endpoint))
		}
	}
	// Only force plaintext when asked. Otherwise the exporter keeps its own
	// defaults, which also honor OTEL_EXPORTER_OTLP_INSECURE and an
	// "http://" endpoint scheme.
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	return opts
}

// newTracer wires the OpenTelemetry SDK and the OpenTracing bridge together.
// It is separate from New so tests can use an in-memory span processor.
func newTracer(ctx context.Context, cfg Config, processorOpt sdktrace.TracerProviderOption) (*Tracer, error) {
	if cfg.SampleRatio < 0 || cfg.SampleRatio > 1 {
		return nil, errors.Errorf("sample ratio must be in [0, 1], but got %v", cfg.SampleRatio)
	}
	res, err := newResource(ctx, cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	provider := sdktrace.NewTracerProvider(
		processorOpt,
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)

	bridge := otbridge.NewBridgeTracer()
	// Used by Inject/Extract, for example by the gRPC OpenTracing interceptors
	// in client-go, so that W3C trace context is propagated on the wire.
	bridge.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	if cfg.WarningHandler != nil {
		bridge.SetWarningHandler(func(msg string) { cfg.WarningHandler(msg) })
	}
	wrapperProvider := otbridge.NewTracerProvider(bridge, provider)
	bridge.SetOpenTelemetryTracer(wrapperProvider.Tracer(instrumentationName))

	return &Tracer{Tracer: bridge, provider: provider}, nil
}

func newResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	attrs := make([]attribute.KeyValue, 0, 3)
	if cfg.ServiceName != "" {
		attrs = append(attrs, attribute.String("service.name", cfg.ServiceName))
	}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, attribute.String("service.version", cfg.ServiceVersion))
	}
	if cfg.ServiceInstanceID != "" {
		attrs = append(attrs, attribute.String("service.instance.id", cfg.ServiceInstanceID))
	}
	// Later options win, so the standard OTEL_* environment variables can
	// override the values derived from the TiDB config.
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(),
	)
	// A malformed OTEL_RESOURCE_ATTRIBUTES only yields a partial resource;
	// the valid attributes are still worth exporting.
	if err != nil {
		if !goerrors.Is(err, resource.ErrPartialResource) {
			return nil, errors.Annotate(err, "create OpenTelemetry resource")
		}
		if cfg.WarningHandler != nil {
			cfg.WarningHandler(err.Error())
		}
	}
	return res, nil
}

// ForceFlush exports all spans that have ended but are still buffered.
func (t *Tracer) ForceFlush(ctx context.Context) error {
	return t.provider.ForceFlush(ctx)
}

// Shutdown flushes buffered spans and stops the exporter. Spans started after
// Shutdown are dropped.
func (t *Tracer) Shutdown(ctx context.Context) error {
	return t.provider.Shutdown(ctx)
}

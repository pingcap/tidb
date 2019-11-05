// Copyright 2019 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package ocagent adds the ability to export all telemetry to an ocagent.
// This keeps the compile time dependencies to zero and allows the agent to
// have the exporters needed for telemetry aggregation and viewing systems.
package ocagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/tools/internal/telemetry"
	"golang.org/x/tools/internal/telemetry/export"
	"golang.org/x/tools/internal/telemetry/export/ocagent/wire"
	"golang.org/x/tools/internal/telemetry/tag"
)

type Config struct {
	Start   time.Time
	Host    string
	Process uint32
	Client  *http.Client
	Service string
	Address string
	Rate    time.Duration
}

// Discover finds the local agent to export to, it will return nil if there
// is not one running.
// TODO: Actually implement a discovery protocol rather than a hard coded address
func Discover() *Config {
	return &Config{
		Address: "http://localhost:55678",
	}
}

type exporter struct {
	mu      sync.Mutex
	config  Config
	node    *wire.Node
	spans   []*wire.Span
	metrics []*wire.Metric
}

// Connect creates a process specific exporter with the specified
// serviceName and the address of the ocagent to which it will upload
// its telemetry.
func Connect(config *Config) export.Exporter {
	if config == nil || config.Address == "off" {
		return nil
	}
	exporter := &exporter{config: *config}
	if exporter.config.Start.IsZero() {
		exporter.config.Start = time.Now()
	}
	if exporter.config.Host == "" {
		hostname, _ := os.Hostname()
		exporter.config.Host = hostname
	}
	if exporter.config.Process == 0 {
		exporter.config.Process = uint32(os.Getpid())
	}
	if exporter.config.Client == nil {
		exporter.config.Client = http.DefaultClient
	}
	if exporter.config.Service == "" {
		exporter.config.Service = filepath.Base(os.Args[0])
	}
	if exporter.config.Rate == 0 {
		exporter.config.Rate = 2 * time.Second
	}
	exporter.node = &wire.Node{
		Identifier: &wire.ProcessIdentifier{
			HostName:       exporter.config.Host,
			Pid:            exporter.config.Process,
			StartTimestamp: convertTimestamp(exporter.config.Start),
		},
		LibraryInfo: &wire.LibraryInfo{
			Language:           wire.LanguageGo,
			ExporterVersion:    "0.0.1",
			CoreLibraryVersion: "x/tools",
		},
		ServiceInfo: &wire.ServiceInfo{
			Name: exporter.config.Service,
		},
	}
	go func() {
		for _ = range time.Tick(exporter.config.Rate) {
			exporter.Flush()
		}
	}()
	return exporter
}

func (e *exporter) StartSpan(ctx context.Context, span *telemetry.Span) {}

func (e *exporter) FinishSpan(ctx context.Context, span *telemetry.Span) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, convertSpan(span))
}

func (e *exporter) Log(context.Context, telemetry.Event) {}

func (e *exporter) Metric(ctx context.Context, data telemetry.MetricData) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.metrics = append(e.metrics, convertMetric(data))
}

func (e *exporter) Flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	spans := e.spans
	e.spans = nil
	metrics := e.metrics
	e.metrics = nil

	if len(spans) > 0 {
		e.send("/v1/trace", &wire.ExportTraceServiceRequest{
			Node:  e.node,
			Spans: spans,
			//TODO: Resource?
		})
	}
	if len(metrics) > 0 {
		e.send("/v1/metrics", &wire.ExportMetricsServiceRequest{
			Node:    e.node,
			Metrics: metrics,
			//TODO: Resource?
		})
	}
}

func (e *exporter) send(endpoint string, message interface{}) {
	blob, err := json.Marshal(message)
	if err != nil {
		errorInExport("ocagent failed to marshal message for %v: %v", endpoint, err)
		return
	}
	uri := e.config.Address + endpoint
	req, err := http.NewRequest("POST", uri, bytes.NewReader(blob))
	if err != nil {
		errorInExport("ocagent failed to build request for %v: %v", uri, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := e.config.Client.Do(req)
	if err != nil {
		errorInExport("ocagent failed to send message: %v \n", err)
		return
	}
	if res.Body != nil {
		res.Body.Close()
	}
	return
}

func errorInExport(message string, args ...interface{}) {
	// This function is useful when debugging the exporter, but in general we
	// want to just drop any export
}

func convertTimestamp(t time.Time) wire.Timestamp {
	return t.Format(time.RFC3339Nano)
}

func toTruncatableString(s string) *wire.TruncatableString {
	if s == "" {
		return nil
	}
	return &wire.TruncatableString{Value: s}
}

func convertSpan(span *telemetry.Span) *wire.Span {
	result := &wire.Span{
		TraceId:                 span.ID.TraceID[:],
		SpanId:                  span.ID.SpanID[:],
		TraceState:              nil, //TODO?
		ParentSpanId:            span.ParentID[:],
		Name:                    toTruncatableString(span.Name),
		Kind:                    wire.UnspecifiedSpanKind,
		StartTime:               convertTimestamp(span.Start),
		EndTime:                 convertTimestamp(span.Finish),
		Attributes:              convertAttributes(span.Tags),
		TimeEvents:              convertEvents(span.Events),
		SameProcessAsParentSpan: true,
		//TODO: StackTrace?
		//TODO: Links?
		//TODO: Status?
		//TODO: Resource?
	}
	return result
}

func convertMetric(data telemetry.MetricData) *wire.Metric {
	return nil //TODO:
}

func convertAttributes(tags telemetry.TagList) *wire.Attributes {
	if len(tags) == 0 {
		return nil
	}
	attributes := make(map[string]wire.Attribute)
	for _, tag := range tags {
		attributes[fmt.Sprint(tag.Key)] = convertAttribute(tag.Value)
	}
	return &wire.Attributes{AttributeMap: attributes}
}

func convertAttribute(v interface{}) wire.Attribute {
	switch v := v.(type) {
	case int8:
		return wire.IntAttribute{IntValue: int64(v)}
	case int16:
		return wire.IntAttribute{IntValue: int64(v)}
	case int32:
		return wire.IntAttribute{IntValue: int64(v)}
	case int64:
		return wire.IntAttribute{IntValue: v}
	case int:
		return wire.IntAttribute{IntValue: int64(v)}
	case uint8:
		return wire.IntAttribute{IntValue: int64(v)}
	case uint16:
		return wire.IntAttribute{IntValue: int64(v)}
	case uint32:
		return wire.IntAttribute{IntValue: int64(v)}
	case uint64:
		return wire.IntAttribute{IntValue: int64(v)}
	case uint:
		return wire.IntAttribute{IntValue: int64(v)}
	case float32:
		return wire.DoubleAttribute{DoubleValue: float64(v)}
	case float64:
		return wire.DoubleAttribute{DoubleValue: v}
	case bool:
		return wire.BoolAttribute{BoolValue: v}
	case string:
		return wire.StringAttribute{StringValue: toTruncatableString(v)}
	default:
		return wire.StringAttribute{StringValue: toTruncatableString(fmt.Sprint(v))}
	}
}

func convertEvents(events []telemetry.Event) *wire.TimeEvents {
	//TODO: MessageEvents?
	result := make([]wire.TimeEvent, len(events))
	for i, event := range events {
		result[i] = convertEvent(event)
	}
	return &wire.TimeEvents{TimeEvent: result}
}

func convertEvent(event telemetry.Event) wire.TimeEvent {
	return wire.TimeEvent{
		Time:       convertTimestamp(event.At),
		Annotation: convertAnnotation(event),
	}
}

func convertAnnotation(event telemetry.Event) *wire.Annotation {
	description := event.Message
	if description == "" && event.Error != nil {
		description = event.Error.Error()
		event.Error = nil
	}
	tags := event.Tags
	if event.Error != nil {
		tags = append(tags, tag.Of("Error", event.Error))
	}
	if description == "" && len(tags) == 0 {
		return nil
	}
	return &wire.Annotation{
		Description: toTruncatableString(description),
		Attributes:  convertAttributes(tags),
	}
}

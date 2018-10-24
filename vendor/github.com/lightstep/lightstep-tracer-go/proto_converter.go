package lightstep

import (
	"fmt"
	"reflect"
	"time"

	google_protobuf "github.com/golang/protobuf/ptypes/timestamp"
	cpb "github.com/lightstep/lightstep-tracer-go/collectorpb"
	ot "github.com/opentracing/opentracing-go"
)

type protoConverter struct {
	verbose        bool
	maxLogKeyLen   int // see GrpcOptions.MaxLogKeyLen
	maxLogValueLen int // see GrpcOptions.MaxLogValueLen
}

func newProtoConverter(options Options) *protoConverter {
	return &protoConverter{
		verbose:        options.Verbose,
		maxLogKeyLen:   options.MaxLogKeyLen,
		maxLogValueLen: options.MaxLogValueLen,
	}
}

func (converter *protoConverter) toReportRequest(
	reporterId uint64,
	attributes map[string]string,
	accessToken string,
	buffer *reportBuffer,
) *cpb.ReportRequest {
	return &cpb.ReportRequest{
		Reporter:        converter.toReporter(reporterId, attributes),
		Auth:            converter.toAuth(accessToken),
		Spans:           converter.toSpans(buffer),
		InternalMetrics: converter.toInternalMetrics(buffer),
	}

}

func (converter *protoConverter) toReporter(reporterId uint64, attributes map[string]string) *cpb.Reporter {
	return &cpb.Reporter{
		ReporterId: reporterId,
		Tags:       converter.toFields(attributes),
	}
}

func (converter *protoConverter) toAuth(accessToken string) *cpb.Auth {
	return &cpb.Auth{
		AccessToken: accessToken,
	}
}

func (converter *protoConverter) toSpans(buffer *reportBuffer) []*cpb.Span {
	spans := make([]*cpb.Span, len(buffer.rawSpans))
	for i, span := range buffer.rawSpans {
		spans[i] = converter.toSpan(span, buffer)
	}
	return spans
}

func (converter *protoConverter) toSpan(span RawSpan, buffer *reportBuffer) *cpb.Span {
	return &cpb.Span{
		SpanContext:    converter.toSpanContext(&span.Context),
		OperationName:  span.Operation,
		References:     converter.toReference(span.ParentSpanID),
		StartTimestamp: converter.toTimestamp(span.Start),
		DurationMicros: converter.fromDuration(span.Duration),
		Tags:           converter.fromTags(span.Tags),
		Logs:           converter.toLogs(span.Logs, buffer),
	}
}

func (converter *protoConverter) toInternalMetrics(buffer *reportBuffer) *cpb.InternalMetrics {
	return &cpb.InternalMetrics{
		StartTimestamp: converter.toTimestamp(buffer.reportStart),
		DurationMicros: converter.fromTimeRange(buffer.reportStart, buffer.reportEnd),
		Counts:         converter.toMetricsSample(buffer),
	}
}

func (converter *protoConverter) toMetricsSample(buffer *reportBuffer) []*cpb.MetricsSample {
	return []*cpb.MetricsSample{
		{
			Name:  spansDropped,
			Value: &cpb.MetricsSample_IntValue{IntValue: buffer.droppedSpanCount},
		},
		{
			Name:  logEncoderErrors,
			Value: &cpb.MetricsSample_IntValue{IntValue: buffer.logEncoderErrorCount},
		},
	}
}

func (converter *protoConverter) fromTags(tags ot.Tags) []*cpb.KeyValue {
	fields := make([]*cpb.KeyValue, 0, len(tags))
	for key, tag := range tags {
		fields = append(fields, converter.toField(key, tag))
	}
	return fields
}

func (converter *protoConverter) toField(key string, value interface{}) *cpb.KeyValue {
	field := cpb.KeyValue{Key: key}
	reflectedValue := reflect.ValueOf(value)
	switch reflectedValue.Kind() {
	case reflect.String:
		field.Value = &cpb.KeyValue_StringValue{StringValue: reflectedValue.String()}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		field.Value = &cpb.KeyValue_IntValue{IntValue: reflectedValue.Convert(intType).Int()}
	case reflect.Float32, reflect.Float64:
		field.Value = &cpb.KeyValue_DoubleValue{DoubleValue: reflectedValue.Float()}
	case reflect.Bool:
		field.Value = &cpb.KeyValue_BoolValue{BoolValue: reflectedValue.Bool()}
	default:
		var s string
		switch value := value.(type) {
		case fmt.Stringer:
			s = value.String()
		case error:
			s = value.Error()
		default:
			s = fmt.Sprintf("%#v", value)
			emitEvent(newEventUnsupportedValue(key, value, nil))
		}
		field.Value = &cpb.KeyValue_StringValue{StringValue: s}
	}
	return &field
}

func (converter *protoConverter) toLogs(records []ot.LogRecord, buffer *reportBuffer) []*cpb.Log {
	logs := make([]*cpb.Log, len(records))
	for i, record := range records {
		logs[i] = converter.toLog(record, buffer)
	}
	return logs
}

func (converter *protoConverter) toLog(record ot.LogRecord, buffer *reportBuffer) *cpb.Log {
	log := &cpb.Log{
		Timestamp: converter.toTimestamp(record.Timestamp),
	}
	marshalFields(converter, log, record.Fields, buffer)
	return log
}

func (converter *protoConverter) toFields(attributes map[string]string) []*cpb.KeyValue {
	tags := make([]*cpb.KeyValue, 0, len(attributes))
	for key, value := range attributes {
		tags = append(tags, converter.toField(key, value))
	}
	return tags
}

func (converter *protoConverter) toSpanContext(sc *SpanContext) *cpb.SpanContext {
	return &cpb.SpanContext{
		TraceId: sc.TraceID,
		SpanId:  sc.SpanID,
		Baggage: sc.Baggage,
	}
}

func (converter *protoConverter) toReference(parentSpanId uint64) []*cpb.Reference {
	if parentSpanId == 0 {
		return nil
	}
	return []*cpb.Reference{
		{
			Relationship: cpb.Reference_CHILD_OF,
			SpanContext: &cpb.SpanContext{
				SpanId: parentSpanId,
			},
		},
	}
}

func (converter *protoConverter) toTimestamp(t time.Time) *google_protobuf.Timestamp {
	return &google_protobuf.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.Nanosecond()),
	}
}

func (converter *protoConverter) fromDuration(d time.Duration) uint64 {
	return uint64(d / time.Microsecond)
}

func (converter *protoConverter) fromTimeRange(oldestTime time.Time, youngestTime time.Time) uint64 {
	return converter.fromDuration(youngestTime.Sub(oldestTime))
}

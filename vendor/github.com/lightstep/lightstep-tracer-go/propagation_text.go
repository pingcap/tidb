package lightstep

import (
	"strconv"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
)

const (
	prefixTracerState = "ot-tracer-"
	prefixBaggage     = "ot-baggage-"

	tracerStateFieldCount = 3
	fieldNameTraceID      = prefixTracerState + "traceid"
	fieldNameSpanID       = prefixTracerState + "spanid"
	fieldNameSampled      = prefixTracerState + "sampled"
)

var theTextMapPropagator textMapPropagator

type textMapPropagator struct{}

func (textMapPropagator) Inject(
	spanContext opentracing.SpanContext,
	opaqueCarrier interface{},
) error {
	sc, ok := spanContext.(SpanContext)
	if !ok {
		return opentracing.ErrInvalidSpanContext
	}
	carrier, ok := opaqueCarrier.(opentracing.TextMapWriter)
	if !ok {
		return opentracing.ErrInvalidCarrier
	}
	carrier.Set(fieldNameTraceID, strconv.FormatUint(sc.TraceID, 16))
	carrier.Set(fieldNameSpanID, strconv.FormatUint(sc.SpanID, 16))
	carrier.Set(fieldNameSampled, "true")

	for k, v := range sc.Baggage {
		carrier.Set(prefixBaggage+k, v)
	}
	return nil
}

func (textMapPropagator) Extract(
	opaqueCarrier interface{},
) (opentracing.SpanContext, error) {
	carrier, ok := opaqueCarrier.(opentracing.TextMapReader)
	if !ok {
		return nil, opentracing.ErrInvalidCarrier
	}

	requiredFieldCount := 0
	var traceID, spanID uint64
	var err error
	decodedBaggage := map[string]string{}
	err = carrier.ForeachKey(func(k, v string) error {
		switch strings.ToLower(k) {
		case fieldNameTraceID:
			traceID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return opentracing.ErrSpanContextCorrupted
			}
			requiredFieldCount++
		case fieldNameSpanID:
			spanID, err = strconv.ParseUint(v, 16, 64)
			if err != nil {
				return opentracing.ErrSpanContextCorrupted
			}
			requiredFieldCount++
		case fieldNameSampled:
			requiredFieldCount++
		default:
			lowercaseK := strings.ToLower(k)
			if strings.HasPrefix(lowercaseK, prefixBaggage) {
				decodedBaggage[strings.TrimPrefix(lowercaseK, prefixBaggage)] = v
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if requiredFieldCount < tracerStateFieldCount {
		if requiredFieldCount == 0 {
			return nil, opentracing.ErrSpanContextNotFound
		}
		return nil, opentracing.ErrSpanContextCorrupted
	}

	return SpanContext{
		TraceID: traceID,
		SpanID:  spanID,
		Baggage: decodedBaggage,
	}, nil
}

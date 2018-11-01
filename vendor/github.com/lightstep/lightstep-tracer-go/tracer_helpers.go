package lightstep

import (
	"golang.org/x/net/context"

	opentracing "github.com/opentracing/opentracing-go"
)

// Flush forces a synchronous Flush.
func Flush(ctx context.Context, tracer opentracing.Tracer) {
	switch lsTracer := tracer.(type) {
	case Tracer:
		lsTracer.Flush(ctx)
	case *tracerv0_14:
		Flush(ctx, lsTracer.Tracer)
	default:
		emitEvent(newEventUnsupportedTracer(tracer))
	}
}

// CloseTracer synchronously flushes the tracer, then terminates it.
func Close(ctx context.Context, tracer opentracing.Tracer) {
	switch lsTracer := tracer.(type) {
	case Tracer:
		lsTracer.Close(ctx)
	case *tracerv0_14:
		Close(ctx, lsTracer.Tracer)
	default:
		emitEvent(newEventUnsupportedTracer(tracer))
	}
}

// GetLightStepAccessToken returns the currently configured AccessToken.
func GetLightStepAccessToken(tracer opentracing.Tracer) (string, error) {
	switch lsTracer := tracer.(type) {
	case Tracer:
		return lsTracer.Options().AccessToken, nil
	case *tracerv0_14:
		return GetLightStepAccessToken(lsTracer.Tracer)
	default:
		return "", newEventUnsupportedTracer(tracer)
	}
}

// DEPRECATED: use Flush instead.
func FlushLightStepTracer(tracer opentracing.Tracer) error {
	switch lsTracer := tracer.(type) {
	case Tracer:
		lsTracer.Flush(context.Background())
		return nil
	case *tracerv0_14:
		return FlushLightStepTracer(lsTracer.Tracer)
	default:
		return newEventUnsupportedTracer(tracer)
	}
}

// DEPRECATED: use Close instead.
func CloseTracer(tracer opentracing.Tracer) error {
	switch lsTracer := tracer.(type) {
	case Tracer:
		lsTracer.Close(context.Background())
		return nil
	case *tracerv0_14:
		return CloseTracer(lsTracer.Tracer)
	default:
		return newEventUnsupportedTracer(tracer)
	}
}

// GetLightStepReporterID returns the currently configured Reporter ID.
func GetLightStepReporterID(tracer opentracing.Tracer) (uint64, error) {
	switch lsTracer := tracer.(type) {
	case *tracerImpl:
		return lsTracer.reporterID, nil
	case *tracerv0_14:
		return GetLightStepReporterID(lsTracer.Tracer)
	default:
		return 0, newEventUnsupportedTracer(tracer)
	}
}

package lightstep

import (
	"context"

	opentracing "github.com/opentracing/opentracing-go"
)

// DEPRECATED: Tracerv0_14 matches the Tracer interface from v0.14.0
type Tracerv0_14 interface {
	opentracing.Tracer

	// DEPRECATED: error is always nil. Equivalent to Tracer.Close(context.Background())
	Close() error
	// DEPRECATED: error is always nil. Equivalent to Tracer.Flush(context.Background())
	Flush() error
	// Options gets the Options used in New().
	Options() Options
	// Disable prevents the tracer from recording spans or flushing.
	Disable()
}

type tracerv0_14 struct {
	Tracer
}

// DEPRECATED: For backwards compatibility, NewTracerv0_14 returns a tracer which
// conforms to the Tracer interface from v0.14.0.
func NewTracerv0_14(opts Options) Tracerv0_14 {
	return &tracerv0_14{
		Tracer: NewTracer(opts),
	}
}

func (t *tracerv0_14) Close() error {
	t.Tracer.Close(context.Background())
	return nil
}

func (t *tracerv0_14) Flush() error {
	t.Tracer.Flush(context.Background())
	return nil
}

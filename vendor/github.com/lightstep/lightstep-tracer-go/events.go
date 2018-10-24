package lightstep

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
)

// Events are emitted by the LightStep tracer as a reporting mechanism. They are
// handled by registering an EventHandler callback via SetGlobalEventHandler. The
// emitted events may be cast to specific event types in order access additional
// information.
//
// NOTE: To ensure that events can be accurately identified, each event type contains
// a sentinel method matching the name of the type. This method is a no-op, it is
// only used for type coersion.
type Event interface {
	Event()
	String() string
}

// The ErrorEvent type can be used to filter events. The `Err` method
// retuns the underlying error.
type ErrorEvent interface {
	Event
	error
	Err() error
}

// EventStartError occurs if the Options passed to NewTracer are invalid, and
// the Tracer has failed to start.
type EventStartError interface {
	ErrorEvent
	EventStartError()
}

type eventStartError struct {
	err error
}

func newEventStartError(err error) *eventStartError {
	return &eventStartError{err: err}
}

func (*eventStartError) Event()           {}
func (*eventStartError) EventStartError() {}

func (e *eventStartError) String() string {
	return e.err.Error()
}

func (e *eventStartError) Error() string {
	return e.err.Error()
}

func (e *eventStartError) Err() error {
	return e.err
}

// EventFlushErrorState lists the possible causes for a flush to fail.
type EventFlushErrorState string

const (
	FlushErrorTracerClosed   EventFlushErrorState = "flush failed, the tracer is closed."
	FlushErrorTracerDisabled EventFlushErrorState = "flush failed, the tracer is disabled."
	FlushErrorTransport      EventFlushErrorState = "flush failed, could not send report to Collector"
	FlushErrorReport         EventFlushErrorState = "flush failed, report contained errors"
)

var (
	flushErrorTracerClosed   = errors.New(string(FlushErrorTracerClosed))
	flushErrorTracerDisabled = errors.New(string(FlushErrorTracerDisabled))
)

// EventFlushError occurs when a flush fails to send. Call the `State` method to
// determine the type of error.
type EventFlushError interface {
	ErrorEvent
	EventFlushError()
	State() EventFlushErrorState
}

type eventFlushError struct {
	err   error
	state EventFlushErrorState
}

func newEventFlushError(err error, state EventFlushErrorState) *eventFlushError {
	return &eventFlushError{err: err, state: state}
}

func (*eventFlushError) Event()           {}
func (*eventFlushError) EventFlushError() {}

func (e *eventFlushError) State() EventFlushErrorState {
	return e.state
}

func (e *eventFlushError) String() string {
	return e.err.Error()
}

func (e *eventFlushError) Error() string {
	return e.err.Error()
}

func (e *eventFlushError) Err() error {
	return e.err
}

// EventConnectionError occurs when the tracer fails to maintain it's connection
// with the Collector.
type EventConnectionError interface {
	ErrorEvent
	EventConnectionError()
}

type eventConnectionError struct {
	err error
}

func newEventConnectionError(err error) *eventConnectionError {
	return &eventConnectionError{err: err}
}

func (*eventConnectionError) Event()                {}
func (*eventConnectionError) EventConnectionError() {}

func (e *eventConnectionError) String() string {
	return e.err.Error()
}

func (e *eventConnectionError) Error() string {
	return e.err.Error()
}

func (e *eventConnectionError) Err() error {
	return e.err
}

// EventStatusReport occurs on every successful flush. It contains all metrics
// collected since the previous succesful flush.
type EventStatusReport interface {
	Event
	EventStatusReport()
	StartTime() time.Time
	FinishTime() time.Time
	Duration() time.Duration
	SentSpans() int
	DroppedSpans() int
	EncodingErrors() int
}

type eventStatusReport struct {
	startTime      time.Time
	finishTime     time.Time
	sentSpans      int
	droppedSpans   int
	encodingErrors int
}

func newEventStatusReport(
	startTime, finishTime time.Time,
	sentSpans, droppedSpans, encodingErrors int,
) *eventStatusReport {
	return &eventStatusReport{
		startTime:      startTime,
		finishTime:     finishTime,
		sentSpans:      sentSpans,
		droppedSpans:   droppedSpans,
		encodingErrors: encodingErrors,
	}
}

func (*eventStatusReport) Event() {}

func (*eventStatusReport) EventStatusReport() {}

func (s *eventStatusReport) SetSentSpans(sent int) {
	s.sentSpans = sent
}

func (s *eventStatusReport) StartTime() time.Time {
	return s.startTime
}

func (s *eventStatusReport) FinishTime() time.Time {
	return s.finishTime
}

func (s *eventStatusReport) Duration() time.Duration {
	return s.finishTime.Sub(s.startTime)
}

func (s *eventStatusReport) SentSpans() int {
	return s.sentSpans
}

func (s *eventStatusReport) DroppedSpans() int {
	return s.droppedSpans
}

func (s *eventStatusReport) EncodingErrors() int {
	return s.encodingErrors
}

func (s *eventStatusReport) String() string {
	return fmt.Sprint(
		"STATUS REPORT start: ", s.startTime,
		", end: ", s.finishTime,
		", dropped spans: ", s.droppedSpans,
		", encoding errors: ", s.encodingErrors,
	)
}

// EventUnsupportedTracer occurs when a tracer being passed to a helper function
// fails to typecast as a LightStep tracer.
type EventUnsupportedTracer interface {
	ErrorEvent
	EventUnsupportedTracer()
	Tracer() opentracing.Tracer
}

type eventUnsupportedTracer struct {
	tracer opentracing.Tracer
	err    error
}

func newEventUnsupportedTracer(tracer opentracing.Tracer) EventUnsupportedTracer {
	return &eventUnsupportedTracer{
		tracer: tracer,
		err:    fmt.Errorf("unsupported tracer type: %v", reflect.TypeOf(tracer)),
	}
}

func (e *eventUnsupportedTracer) Event()                  {}
func (e *eventUnsupportedTracer) EventUnsupportedTracer() {}

func (e *eventUnsupportedTracer) Tracer() opentracing.Tracer {
	return e.tracer
}

func (e *eventUnsupportedTracer) String() string {
	return e.err.Error()
}

func (e *eventUnsupportedTracer) Error() string {
	return e.err.Error()
}

func (e *eventUnsupportedTracer) Err() error {
	return e.err
}

// EventUnsupportedValue occurs when a tracer encounters an unserializable tag
// or log field.
type EventUnsupportedValue interface {
	ErrorEvent
	EventUnsupportedValue()
	Key() string
	Value() interface{}
}

type eventUnsupportedValue struct {
	key   string
	value interface{}
	err   error
}

func newEventUnsupportedValue(key string, value interface{}, err error) EventUnsupportedValue {
	if err == nil {
		err = fmt.Errorf(
			"value `%v` of type `%T` for key `%s` is an unsupported type",
			value, value, key,
		)
	}
	return &eventUnsupportedValue{
		key:   key,
		value: value,
		err:   err,
	}
}

func (e *eventUnsupportedValue) Event()                 {}
func (e *eventUnsupportedValue) EventUnsupportedValue() {}

func (e *eventUnsupportedValue) Key() string {
	return e.key
}

func (e *eventUnsupportedValue) Value() interface{} {
	return e.value
}

func (e *eventUnsupportedValue) String() string {
	return e.err.Error()
}

func (e *eventUnsupportedValue) Error() string {
	return e.err.Error()
}

func (e *eventUnsupportedValue) Err() error {
	return e.err
}

const tracerDisabled = "the tracer has been disabled"

// EventTracerDisabled occurs when a tracer is disabled by either the user or
// the collector.
type EventTracerDisabled interface {
	Event
	EventTracerDisabled()
}

type eventTracerDisabled struct{}

func newEventTracerDisabled() EventTracerDisabled {
	return eventTracerDisabled{}
}

func (eventTracerDisabled) Event()               {}
func (eventTracerDisabled) EventTracerDisabled() {}
func (eventTracerDisabled) String() string {
	return tracerDisabled
}

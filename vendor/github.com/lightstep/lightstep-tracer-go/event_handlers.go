package lightstep

import (
	"log"
	"sync"
	"sync/atomic"
)

func init() {
	SetGlobalEventHandler(NewEventLogOneError())
}

var eventHandler atomic.Value

// An OnEventHandler can be registered with SetGlobalEventHandler to
type EventHandler func(Event)

// emitEvent is a thread-safe function for emiting tracer events.
func emitEvent(event Event) {
	handler := eventHandler.Load().(EventHandler)
	handler(event)
}

// SetGlobalEventHandler sets a global handler to receive tracer events as they occur. Events
// may be emitted by the tracer, or by calls to static functions in this package.
// It is suggested that you set your EventHandler before starting your tracer,
// but it is safe to set a new handler at any time. Setting a new handler removes
// the previous one.
//
// The EventHandler is called synchronously – do not block in your handler as it
// may interfere with the tracer. If no EventHandler handler is set, a
// LogOnceOnError handler is set by default.
//
// NOTE: Event handling is for reporting purposes only. It is not intended as a
// mechanism for controling or restarting the tracer. Connection issues, retry
// logic, and other transient errors are handled internally by the tracer.
func SetGlobalEventHandler(handler EventHandler) {
	eventHandler.Store(handler)
}

/*
	SetGlobalEventHandler Handlers
*/

// NewOnEventLogger logs events using the standard go logger.
func NewEventLogger() EventHandler {
	return logOnEvent
}

func logOnEvent(event Event) {
	switch event := event.(type) {
	case ErrorEvent:
		log.Println("LS Tracer error: ", event)
	default:
		log.Println("LS Tracer event: ", event)
	}
}

// NewOnEventLogOneError logs the first error event that occurs.
func NewEventLogOneError() EventHandler {
	logger := logOneError{}
	return logger.OnEvent
}

type logOneError struct {
	sync.Once
}

func (l *logOneError) OnEvent(event Event) {
	switch event := event.(type) {
	case ErrorEvent:
		l.Once.Do(func() {
			log.Printf("LS Tracer error: (%s). NOTE: Set the SetGlobalEventHandler handler to log events.\n", event.Error())
		})
	}
}

// NewChannelOnEvent returns an SetGlobalEventHandler callback handler, and a channel that
// produces the errors. When the channel buffer is full, subsequent errors will
// be dropped. A buffer size of less than one is incorrect, and will be adjusted
// to a buffer size of one.
func NewEventChannel(buffer int) (EventHandler, <-chan Event) {
	if buffer < 1 {
		buffer = 1
	}

	eventChan := make(chan Event, buffer)

	handler := func(event Event) {
		select {
		case eventChan <- event:
		default:
		}
	}

	return handler, eventChan
}

// package lightstep implements the LightStep OpenTracing client for Go.
package lightstep

import (
	"fmt"
	"time"

	"golang.org/x/net/context"

	"runtime"
	"sync"

	ot "github.com/opentracing/opentracing-go"
)

// Tracer extends the `opentracing.Tracer` interface with methods for manual
// flushing and closing. To access these methods, you can take the global
// tracer and typecast it to a `lightstep.Tracer`. As a convenience, the
// lightstep package provides static functions which perform the typecasting.
type Tracer interface {
	ot.Tracer

	// Close flushes and then terminates the LightStep collector
	Close(context.Context)
	// Flush sends all spans currently in the buffer to the LighStep collector
	Flush(context.Context)
	// Options gets the Options used in New() or NewWithOptions().
	Options() Options
	// Disable prevents the tracer from recording spans or flushing
	Disable()
}

// Implements the `Tracer` interface. Buffers spans and forwards the to a Lightstep collector.
type tracerImpl struct {
	//////////////////////////////////////////////////////////////
	// IMMUTABLE IMMUTABLE IMMUTABLE IMMUTABLE IMMUTABLE IMMUTABLE
	//////////////////////////////////////////////////////////////

	// Note: there may be a desire to update some of these fields
	// at runtime, in which case suitable changes may be needed
	// for variables accessed during Flush.

	reporterID uint64 // the LightStep tracer guid
	opts       Options

	// report loop management
	closeOnce               sync.Once
	closeReportLoopChannel  chan struct{}
	reportLoopClosedChannel chan struct{}

	//////////////////////////////////////////////////////////
	// MUTABLE MUTABLE MUTABLE MUTABLE MUTABLE MUTABLE MUTABLE
	//////////////////////////////////////////////////////////

	// the following fields are modified under `lock`.
	lock sync.Mutex

	// Remote service that will receive reports.
	client     collectorClient
	connection Connection

	// Two buffers of data.
	buffer   reportBuffer
	flushing reportBuffer

	// Flush state.
	flushingLock      sync.Mutex
	reportInFlight    bool
	lastReportAttempt time.Time

	// We allow our remote peer to disable this instrumentation at any
	// time, turning all potentially costly runtime operations into
	// no-ops.
	//
	// TODO this should use atomic load/store to test disabled
	// prior to taking the lock, do please.
	disabled bool
}

// NewTracer creates and starts a new Lightstep Tracer.
func NewTracer(opts Options) Tracer {
	err := opts.Initialize()
	if err != nil {
		emitEvent(newEventStartError(err))
		return nil
	}

	attributes := map[string]string{}
	for k, v := range opts.Tags {
		attributes[k] = fmt.Sprint(v)
	}
	// Don't let the GrpcOptions override these values. That would be confusing.
	attributes[TracerPlatformKey] = TracerPlatformValue
	attributes[TracerPlatformVersionKey] = runtime.Version()
	attributes[TracerVersionKey] = TracerVersionValue

	now := time.Now()
	impl := &tracerImpl{
		opts:                    opts,
		reporterID:              genSeededGUID(),
		buffer:                  newSpansBuffer(opts.MaxBufferedSpans),
		flushing:                newSpansBuffer(opts.MaxBufferedSpans),
		closeReportLoopChannel:  make(chan struct{}),
		reportLoopClosedChannel: make(chan struct{}),
	}

	impl.buffer.setCurrent(now)

	impl.client, err = newCollectorClient(opts, impl.reporterID, attributes)
	if err != nil {
		fmt.Println("Failed to create to Collector client!", err)
		return nil
	}

	conn, err := impl.client.ConnectClient()
	if err != nil {
		emitEvent(newEventStartError(err))
		return nil
	}
	impl.connection = conn

	go impl.reportLoop()

	return impl
}

func (tracer *tracerImpl) Options() Options {
	return tracer.opts
}

func (tracer *tracerImpl) StartSpan(
	operationName string,
	sso ...ot.StartSpanOption,
) ot.Span {
	return newSpan(operationName, tracer, sso)
}

func (tracer *tracerImpl) Inject(sc ot.SpanContext, format interface{}, carrier interface{}) error {
	switch format {
	case ot.TextMap, ot.HTTPHeaders:
		return theTextMapPropagator.Inject(sc, carrier)
	case BinaryCarrier:
		return theBinaryPropagator.Inject(sc, carrier)
	}
	return ot.ErrUnsupportedFormat
}

func (tracer *tracerImpl) Extract(format interface{}, carrier interface{}) (ot.SpanContext, error) {
	switch format {
	case ot.TextMap, ot.HTTPHeaders:
		return theTextMapPropagator.Extract(carrier)
	case BinaryCarrier:
		return theBinaryPropagator.Extract(carrier)
	}
	return nil, ot.ErrUnsupportedFormat
}

func (tracer *tracerImpl) reconnectClient(now time.Time) {
	conn, err := tracer.client.ConnectClient()
	if err != nil {
		emitEvent(newEventConnectionError(err))
	} else {
		tracer.lock.Lock()
		oldConn := tracer.connection
		tracer.connection = conn
		tracer.lock.Unlock()

		oldConn.Close()
	}
}

// Close flushes and then terminates the LightStep collector. Close may only be
// called once; subsequent calls to Close are no-ops.
func (tracer *tracerImpl) Close(ctx context.Context) {
	tracer.closeOnce.Do(func() {
		// notify report loop that we are closing
		close(tracer.closeReportLoopChannel)
		select {
		case <-tracer.reportLoopClosedChannel:
			tracer.Flush(ctx)
		case <-ctx.Done():
			return
		}

		// now its safe to close the connection
		tracer.lock.Lock()
		conn := tracer.connection
		tracer.connection = nil
		tracer.lock.Unlock()

		if conn != nil {
			err := conn.Close()
			if err != nil {
				emitEvent(newEventConnectionError(err))
			}
		}
	})
}

// RecordSpan records a finished Span.
func (tracer *tracerImpl) RecordSpan(raw RawSpan) {
	tracer.lock.Lock()

	// Early-out for disabled runtimes
	if tracer.disabled {
		tracer.lock.Unlock()
		return
	}

	tracer.buffer.addSpan(raw)
	tracer.lock.Unlock()

	if tracer.opts.Recorder != nil {
		tracer.opts.Recorder.RecordSpan(raw)
	}
}

// Flush sends all buffered data to the collector.
func (tracer *tracerImpl) Flush(ctx context.Context) {
	tracer.flushingLock.Lock()
	defer tracer.flushingLock.Unlock()
	var flushErrorEvent *eventFlushError

	flushErrorEvent = tracer.preFlush()
	if flushErrorEvent != nil {
		emitEvent(flushErrorEvent)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, tracer.opts.ReportTimeout)
	defer cancel()

	req, flushErr := tracer.client.Translate(ctx, &tracer.flushing)
	resp, flushErr := tracer.client.Report(ctx, req)

	if flushErr != nil {
		flushErrorEvent = newEventFlushError(flushErr, FlushErrorTransport)
	} else if len(resp.GetErrors()) > 0 {
		flushErrorEvent = newEventFlushError(fmt.Errorf(resp.GetErrors()[0]), FlushErrorReport)
	}

	statusReportEvent := tracer.postFlush(flushErrorEvent)

	if flushErrorEvent != nil {
		emitEvent(flushErrorEvent)
	}

	emitEvent(statusReportEvent)

	if flushErr == nil && resp.Disable() {
		tracer.Disable()
	}
}

// preFlush handles lock-protected data manipulation before flushing
func (tracer *tracerImpl) preFlush() *eventFlushError {
	tracer.lock.Lock()
	defer tracer.lock.Unlock()

	if tracer.disabled {
		return newEventFlushError(flushErrorTracerClosed, FlushErrorTracerDisabled)
	}

	if tracer.connection == nil {
		return newEventFlushError(flushErrorTracerClosed, FlushErrorTracerClosed)
	}

	now := time.Now()
	tracer.buffer, tracer.flushing = tracer.flushing, tracer.buffer
	tracer.reportInFlight = true
	tracer.flushing.setFlushing(now)
	tracer.buffer.setCurrent(now)
	tracer.lastReportAttempt = now
	return nil
}

// postFlush handles lock-protected data manipulation after flushing
func (tracer *tracerImpl) postFlush(flushEventError *eventFlushError) *eventStatusReport {
	tracer.lock.Lock()
	defer tracer.lock.Unlock()

	tracer.reportInFlight = false

	statusReportEvent := newEventStatusReport(
		tracer.flushing.reportStart,
		tracer.flushing.reportEnd,
		len(tracer.flushing.rawSpans),
		int(tracer.flushing.droppedSpanCount+tracer.buffer.droppedSpanCount),
		int(tracer.flushing.logEncoderErrorCount+tracer.buffer.logEncoderErrorCount),
	)

	if flushEventError != nil {
		// Restore the records that did not get sent correctly
		tracer.buffer.mergeFrom(&tracer.flushing)
		statusReportEvent.SetSentSpans(0)
	} else {
		tracer.flushing.clear()
	}

	return statusReportEvent
}

func (tracer *tracerImpl) Disable() {
	tracer.lock.Lock()
	if tracer.disabled {
		tracer.lock.Unlock()
		return
	}
	tracer.disabled = true
	tracer.buffer.clear()
	tracer.lock.Unlock()

	emitEvent(newEventTracerDisabled())
}

// Every MinReportingPeriod the reporting loop wakes up and checks to see if
// either (a) the Runtime's max reporting period is about to expire (see
// maxReportingPeriod()), (b) the number of buffered log records is
// approaching kMaxBufferedLogs, or if (c) the number of buffered span records
// is approaching kMaxBufferedSpans. If any of those conditions are true,
// pending data is flushed to the remote peer. If not, the reporting loop waits
// until the next cycle. See Runtime.maybeFlush() for details.
//
// This could alternatively be implemented using flush channels and so forth,
// but that would introduce opportunities for client code to block on the
// runtime library, and we want to avoid that at all costs (even dropping data,
// which can certainly happen with high data rates and/or unresponsive remote
// peers).

func (tracer *tracerImpl) shouldFlushLocked(now time.Time) bool {
	if now.Add(tracer.opts.MinReportingPeriod).Sub(tracer.lastReportAttempt) > tracer.opts.ReportingPeriod {
		return true
	} else if tracer.buffer.isHalfFull() {
		return true
	}
	return false
}

func (tracer *tracerImpl) reportLoop() {
	tickerChan := time.Tick(tracer.opts.MinReportingPeriod)
	for {
		select {
		case <-tickerChan:
			now := time.Now()

			tracer.lock.Lock()
			disabled := tracer.disabled
			reconnect := !tracer.reportInFlight && tracer.client.ShouldReconnect()
			shouldFlush := tracer.shouldFlushLocked(now)
			tracer.lock.Unlock()

			if disabled {
				return
			}
			if shouldFlush {
				tracer.Flush(context.Background())
			}
			if reconnect {
				tracer.reconnectClient(now)
			}
		case <-tracer.closeReportLoopChannel:
			close(tracer.reportLoopClosedChannel)
			return
		}
	}
}

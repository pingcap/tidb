package server

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/tracing"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// Tracing holds the state used by SET TRACING {ON,OFF, executor} statements in
// the context of one SQL session.
// It holds the current trace being collected (or the last trace collected, if
// tracing is not currently ongoing).
//
// Tracing and its interactions with the clientConn are thread-safe;
// tracing can be turned on at any time.
type sessionTracing struct {
	// enabled is set at times when "session enabled" is active - i.e. when
	// transactions are being recorded.
	enabled bool

	// kvTracingEnabled is set at times when KV tracing is active. When
	// KV tracning is enabled, the SQL/KV interface logs individual K/V
	// operators to the current context.
	kvTracingEnabled bool

	// showResults, when set, indicates that the result rows produced by
	// the execution statement must be reported in the
	// trace. showResults can be set manually by SET TRACING = ...,
	// results
	showResults bool

	// If recording==true, recordingType indicates the type of the current
	// recording.
	recordingType tracing.RecordingType

	// Cc is the clientConn to which this SessionTracing is tied.
	Cc *clientConn

	// firstTxnSpan is the span of the first txn that was active when session
	// tracing was enabled.
	firstTxnSpan opentracing.Span

	// connSpan is the connection's span. This is recording.
	connSpan opentracing.Span

	// lastRecording will collect the recording when stopping tracing.
	lastRecording *chunk.Chunk
}

// GetSessionTrace returns the session trace. If we're not currently tracing,
// this will be the last recorded trace. If we are currently tracing, we'll
// return whatever was recorded so far.
func (st *sessionTracing) GetSessionTrace() (*chunk.Chunk, error) {
	if !st.enabled {
		return st.lastRecording, nil
	}

	return generateSessionTraceVTable(st.getRecording())
}

// getRecording returns the recorded spans of the current trace.
func (st *sessionTracing) getRecording() []tracing.RecordedSpan {
	var spans []tracing.RecordedSpan
	if st.firstTxnSpan != nil {
		spans = append(spans, tracing.GetRecording(st.firstTxnSpan)...)
	}
	return append(spans, tracing.GetRecording(st.connSpan)...)
}

// StartTracing starts "session tracing". From this moment on, everything
// happening on both the connection's context and the current txn's context (if
// any) will be traced.
// StopTracing() needs to be called to finish this trace.
//
// There's two contexts on which we must record:
// 1) If we're inside a txn, we start recording on the txn's span. We assume
// that the txn's ctx has a recordable span on it.
// 2) Regardless of whether we're in a txn or not, we need to record the
// connection's context. This context generally does not have a span, so we
// "hijack" it with one that does. Whatever happens on that context, plus
// whatever happens in future derived txn contexts, will be recorded.
//
// Args:
// kvTracingEnabled: If set, the traces will also include "KV trace" messages -
//   verbose messages around the interaction of SQL with KV. Some of the messages
//   are per-row.
// showResults: If set, result rows are reported in the trace.
func (st *sessionTracing) StartTracing(
	recType tracing.RecordingType, kvTracingEnabled, showResults bool,
) error {
	if st.enabled {
		// We're already tracing. Only treat as no-op if the same options
		// are requested.
		if kvTracingEnabled != st.kvTracingEnabled ||
			showResults != st.showResults ||
			recType != st.recordingType {
			var desiredOptions bytes.Buffer
			comma := ""
			if kvTracingEnabled {
				desiredOptions.WriteString("kv")
				comma = ", "
			}
			if showResults {
				fmt.Fprintf(&desiredOptions, "%sresults", comma)
				comma = ", "
			}
			recOption := "cluster"
			if recType == tracing.SingleNodeRecording {
				recOption = "local"
			}
			fmt.Fprintf(&desiredOptions, "%s%s", comma, recOption)

			return errors.New("tracing is already started with different options")
		}

		return nil
	}

	// If we're inside a transaction, start recording on the txn span.
	if st.Cc.ctx.GetSessionVars().InTxn() {
		// we need support form like the following in session and figure out
		// a way to pass ctx into txn.
		sp := opentracing.SpanFromContext(st.Cc.ctx.session.TxnState().Ctx)
		if sp == nil {
			return errors.Errorf("no txn span for SessionTracing")
		}
		tracing.StartRecording(sp, recType)
		st.firstTxnSpan = sp
	}

	st.enabled = true
	st.kvTracingEnabled = kvTracingEnabled
	st.showResults = showResults
	st.recordingType = recType

	// Now hijack the conn's ctx with one that has a recording span.

	opName := "session recording"
	var sp opentracing.Span
	if parentSp := opentracing.SpanFromContext(st.Cc.ctxHolder.connCtx); parentSp != nil {
		// Create a child span while recording.
		sp = parentSp.Tracer().StartSpan(
			opName, opentracing.ChildOf(parentSp.Context()), tracing.Recordable)
	} else {
		// Create a root span while recording.
		sp = st.Cc.server.Tracer.StartSpan(opName, tracing.Recordable)
	}
	tracing.StartRecording(sp, recType)
	st.connSpan = sp

	// Hijack the connections context.
	newConnCtx := opentracing.ContextWithSpan(st.Cc.ctxHolder.connCtx, sp)
	st.Cc.ctxHolder.hijack(newConnCtx)

	return nil
}

func (st *sessionTracing) GetSession() session.Session {
	return st.Cc.ctx.session
}

// TODO remove this method once we merge server, session and executor package.
func (st *sessionTracing) Ctx() context.Context {
	return st.Cc.Ctx()
}

// StopTracing stops the trace that was started with StartTracing().
// An error is returned if tracing was not active.
func (st *sessionTracing) StopTracing() error {
	if !st.enabled {
		// We're not currently tracing. No-op.
		return nil
	}
	st.enabled = false
	st.kvTracingEnabled = false
	st.showResults = false
	st.recordingType = tracing.NoRecording

	var spans []tracing.RecordedSpan

	if st.firstTxnSpan != nil {
		spans = append(spans, tracing.GetRecording(st.firstTxnSpan)...)
		tracing.StopRecording(st.firstTxnSpan)
	}
	st.connSpan.Finish()
	spans = append(spans, tracing.GetRecording(st.connSpan)...)
	// NOTE: We're stopping recording on the connection's ctx only; the stopping
	// is not inherited by children. If we are inside of a txn, that span will
	// continue recording, even though nobody will collect its recording again.
	tracing.StopRecording(st.connSpan)
	st.Cc.ctxHolder.unhijack()

	var err error
	st.lastRecording, err = generateSessionTraceVTable(spans)
	return err
}

// RecordingType returns which type of tracing is currently being done.
func (st *sessionTracing) RecordingType() tracing.RecordingType {
	return st.recordingType
}

// KVTracingEnabled checks whether KV tracing is currently enabled.
func (st *sessionTracing) KVTracingEnabled() bool {
	return st.kvTracingEnabled
}

// Enabled checks whether session tracing is currently enabled.
func (st *sessionTracing) Enabled() bool {
	return st.enabled
}

func (st *sessionTracing) TraceParseStart(ctx context.Context, stmtTag string) {
	if st.enabled {
		log.Eventf(ctx, "parsing starts: %s", stmtTag)
	}
}

func (st *sessionTracing) TraceParseEnd(ctx context.Context, err error) {
	log.Eventf(ctx, "parsing ends")
	if err != nil {
		log.Eventf(ctx, "parsing error: %v", err)
	}
}

// TracePlanStart conditionally emits a trace message at the moment
// logical planning starts.
func (st *sessionTracing) TracePlanStart(ctx context.Context, stmtTag string) {
	if st.enabled {
		log.Eventf(ctx, "planning starts: %s", stmtTag)
	}
}

// TracePlanEnd conditionally emits a trace message at the moment
// logical planning ends.
func (st *sessionTracing) TracePlanEnd(ctx context.Context, err error) {
	log.Eventf(ctx, "planning ends")
	if err != nil {
		log.Eventf(ctx, "planning error: %v", err)
	}
}

// TraceExecStart conditionally emits a trace message at the moment
// plan execution starts.
func (st *sessionTracing) TraceExecStart(ctx context.Context, engine string) {
	log.Eventf(ctx, "execution starts: %s", engine)
}

// TraceExecConsume creates a context for TraceExecRowsResult below.
func (st *sessionTracing) TraceExecConsume(ctx context.Context) (context.Context, func()) {
	if st.enabled {
		consumeCtx, sp := tracing.ChildSpan(ctx, "consuming rows")
		return consumeCtx, sp.Finish
	}
	return ctx, func() {}
}

// traceExecRowsResultHelper converts datum values into string.
func traceExecRowsResultHelper(values []types.Datum) string {
	var buf bytes.Buffer
	for _, v := range values {
		str, _ := v.ToString()
		buf.WriteString(str)
	}
	return buf.String()
}

// TraceExecRowsResult conditionally emits a trace message for a single output row.
func (st *sessionTracing) TraceExecRowsResult(ctx context.Context, values []types.Datum) {
	if st.showResults {
		log.Eventf(ctx, "output row: %s", traceExecRowsResultHelper(values))
	}
}

// TraceExecEnd conditionally emits a trace message at the moment
// plan execution completes.
// TODO(zhexuahy): we may need log message too.
func (st *sessionTracing) TraceExecEnd(ctx context.Context, err error, count int) {
	log.Event(ctx, "execution ends")
	if err != nil {
		log.Eventf(ctx, "execution failed after %d rows: %v", count, err)
	} else {
		log.Eventf(ctx, "rows affected: %d", count)
	}
}

// extractMsgFromRecord extracts the message of the event, which is either in an
// "event" or "error" field.
func extractMsgFromRecord(rec opentracing.LogRecord) string {
	for _, f := range rec.Fields {
		key := f.Key()
		if key == "event" {
			return f.Value().(string)
		}
		if key == "error" {
			return fmt.Sprint("error:", f.Value().(string))
		}
	}
	return "<event missing in trace message>"
}

// A regular expression to split log messages.
// It has three parts:
// - the (optional) code location, with at least one forward slash and a period
//   in the file name:
//   ((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?)
// - the (optional) tag: ((?:\[(?:[^][]|\[[^]]*\])*\])?)
// - the message itself: the rest.
var logMessageRE = regexp.MustCompile(
	`(?s:^((?:[^][ :]+/[^][ :]+\.[^][ :]+:[0-9]+)?) *((?:\[(?:[^][]|\[[^]]*\])*\])?) *(.*))`)

func generateSessionTraceVTable(spans []tracing.RecordedSpan) (*chunk.Chunk, error) {
	// Get all the log messages, in the right order.
	var allLogs []logRecordRow

	// NOTE: The spans are recorded in the order in which they are started.
	seenSpans := make(map[uint64]struct{})
	for spanIdx, span := range spans {
		if _, ok := seenSpans[span.SpanID]; ok {
			continue
		}
		spanWithIndex := spanWithIndex{
			RecordedSpan: &spans[spanIdx],
			index:        spanIdx,
		}
		msgs, err := getMessagesForSubtrace(spanWithIndex, spans, seenSpans)
		if err != nil {
			return nil, err
		}
		allLogs = append(allLogs, msgs...)
	}

	// Transform the log messages into table rows.
	// We need to populate "operation" later because it is only
	// set for the first row in each span.
	opMap := make(map[int64]*string)
	durMap := make(map[int64]*types.Duration)
	var res = chunk.NewChunkWithCapacity(sessionctx.TraceFieldTps, 1024)
	var minTimestamp, zeroTime time.Time
	for _, lrr := range allLogs {
		// The "operation" column is only set for the first row in span.
		// We'll populate the rest below.
		if lrr.index == 0 {
			spanIdx := int64(lrr.span.index)
			opMap[spanIdx] = &lrr.span.Operation
			if lrr.span.Duration != 0 {
				durMap[spanIdx] = &types.Duration{
					Duration: lrr.span.Duration,
					Fsp:      types.MaxFsp,
				}
			}
		}

		// We'll need the lowest timestamp to compute ages below.
		if minTimestamp == zeroTime || lrr.timestamp.Before(minTimestamp) {
			minTimestamp = lrr.timestamp
		}

		// Split the message into component parts.
		//
		// The result of FindStringSubmatchIndex is a 1D array of pairs
		// [start, end) of positions in the input string.  The first pair
		// identifies the entire match; the 2nd pair corresponds to the
		// 1st parenthetized expression in the regexp, and so on.
		loc := logMessageRE.FindStringSubmatchIndex(lrr.msg)
		if loc == nil {
			return nil, fmt.Errorf("unable to split trace message: %q", lrr.msg)
		}

		// span_idx
		res.AppendInt64(sessionctx.TraceSpanIdxCol, int64(lrr.span.index))
		// message_idx
		res.AppendInt64(sessionctx.TraceMsgIdxCol, int64(lrr.index))
		// timestamp
		res.AppendTime(sessionctx.TraceTimestampCol, types.Time{Time: types.FromGoTime(lrr.timestamp)})
		// location
		res.AppendString(sessionctx.TraceLocCol, lrr.msg[loc[2]:loc[3]])
		// tag
		res.AppendString(sessionctx.TraceTagCol, lrr.msg[loc[4]:loc[5]])
		// message
		res.AppendString(sessionctx.TraceMsgCol, lrr.msg[loc[6]:loc[7]])
	}

	if res.NumRows() == 0 {
		// Nothing to do below. Shortcut.
		return res, nil
	}

	chkLen := res.NumRows()
	// Populate the operation and age columns.
	for i := 0; i < chkLen; i++ {
		spanIdx := res.GetRow(i).GetInt64(sessionctx.TraceSpanIdxCol)

		// operation
		if opStr, ok := opMap[spanIdx]; ok {
			res.AppendString(sessionctx.TraceOpCol, *opStr)
		}

		if dur, ok := durMap[spanIdx]; ok {
			res.AppendDuration(sessionctx.TraceDurationCol, *dur)
		}

		age := res.GetRow(i).GetTime(sessionctx.TraceTimestampCol)
		goAge, err := age.Time.GoTime(time.Local)
		if err != nil {
			return nil, err
		}
		// age
		res.AppendString(sessionctx.TraceAgeCol, goAge.Sub(minTimestamp).String())
	}

	return res, nil
}

// getOrderedChildSpans returns all the spans in allSpans that are children of
// spanID. It assumes the input is ordered by start time, in which case the
// output is also ordered.
func getOrderedChildSpans(spanID uint64, allSpans []tracing.RecordedSpan) []spanWithIndex {
	children := make([]spanWithIndex, 0)
	for i := range allSpans {
		if allSpans[i].ParentSpanID == spanID {
			children = append(
				children,
				spanWithIndex{
					RecordedSpan: &allSpans[i],
					index:        i,
				})
		}
	}
	return children
}

// getMessagesForSubtrace takes a span and interleaves its log messages with
// those from its children (recursively). The order is the one defined in the
// comment on generateSessionTraceVTable().
//
// seenSpans is modified to record all the spans that are part of the subtrace
// rooted at span.
func getMessagesForSubtrace(
	span spanWithIndex, allSpans []tracing.RecordedSpan, seenSpans map[uint64]struct{},
) ([]logRecordRow, error) {
	if _, ok := seenSpans[span.SpanID]; ok {
		return nil, errors.Errorf("duplicate span %d", span.SpanID)
	}
	var allLogs []logRecordRow
	const spanStartMsgTemplate = "=== SPAN START: %s ==="

	// spanStartMsgs are metadata about the span, e.g. the operation name and tags
	// contained in the span. They are added as one log message.
	spanStartMsgs := make([]string, 0, len(span.Tags)+1)

	spanStartMsgs = append(spanStartMsgs, fmt.Sprintf(spanStartMsgTemplate, span.Operation))

	// Add recognized tags to the output.
	for name, value := range span.Tags {
		if !strings.HasPrefix(name, tracing.TagPrefix) {
			// Not a tag to be output.
			continue
		}
		spanStartMsgs = append(spanStartMsgs, fmt.Sprintf("%s: %s", name, value))
	}

	// This message holds all the spanStartMsgs and marks the beginning of the
	// span, to indicate the start time and duration of the span.
	allLogs = append(
		allLogs,
		logRecordRow{
			timestamp: span.StartTime,
			msg:       strings.Join(spanStartMsgs, "\n"),
			span:      span,
			index:     0,
		},
	)

	seenSpans[span.SpanID] = struct{}{}
	childSpans := getOrderedChildSpans(span.SpanID, allSpans)
	var i, j int
	// Sentinel value - year 6000.
	maxTime := time.Date(6000, 0, 0, 0, 0, 0, 0, time.UTC)
	// Merge the logs with the child spans.
	for i < len(span.Logs) || j < len(childSpans) {
		logTime := maxTime
		childTime := maxTime
		if i < len(span.Logs) {
			logTime = span.Logs[i].Timestamp
		}
		if j < len(childSpans) {
			childTime = childSpans[j].StartTime
		}

		if logTime.Before(childTime) {
			allLogs = append(allLogs,
				logRecordRow{
					timestamp: logTime,
					msg:       extractMsgFromRecord(span.Logs[i]),
					span:      span,
					// Add 1 to the index to account for the first dummy message in a
					// span.
					index: i + 1,
				})
			i++
		} else {
			// Recursively append messages from the trace rooted at the child.
			childMsgs, err := getMessagesForSubtrace(childSpans[j], allSpans, seenSpans)
			if err != nil {
				return nil, err
			}
			allLogs = append(allLogs, childMsgs...)
			j++
		}
	}
	return allLogs, nil
}

// logRecordRow is used to temporarily hold on to log messages and their
// metadata while flattening a trace.
type logRecordRow struct {
	timestamp time.Time
	msg       string
	span      spanWithIndex
	// index of the log message within its span.
	index int
}

type spanWithIndex struct {
	*tracing.RecordedSpan
	index int
}

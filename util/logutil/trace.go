// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"fmt"
	"sync"

	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pingcap/tidb/util/tracing"
	"golang.org/x/net/context"
	"golang.org/x/net/trace"
)

// Event looks for an opentracing.Trace in the context and logs the given
// message to it. If no Trace is found, it looks for an EventLog in the context
// and logs the message to it. If neither is found, does nothing.
func Event(ctx context.Context, msg string) {
	eventInternal(ctx, false, true, msg)
}

// Eventf first checks is there an opentracing.Trace available in the context.
// if yes, it formats and logs the given message to it. If no, it does nothing.
// TODO(zhexuany: add eventLog in short future)
func Eventf(ctx context.Context, format string, args ...interface{}) {
	eventInternal(ctx, false, true, format, args...)
}

// eventInternal is the common code for logging an event. If no args are given,
// the format is treated as a pre-formatted string.
func eventInternal(ctx context.Context, isErr, withTags bool, format string, args ...interface{}) {
	if sp, el, ok := getSpanOrEventLog(ctx); ok {
		var buf msgBuf
		if withTags {
			withTags = formatTags(ctx, &buf)
		}

		var msg string
		if !withTags && len(args) == 0 {
			// Fast path for pre-formatted messages.
			msg = format
		} else {
			if len(args) == 0 {
				buf.WriteString(format)
			} else {
				fmt.Fprintf(&buf, format, args...)
			}
			msg = buf.String()
		}

		if sp != nil {
			sp.LogFields(otlog.String("event", msg))
		} else {
			el.Lock()
			if el.eventLog != nil {
				if isErr {
					el.eventLog.Errorf("%s", msg)
				} else {
					el.eventLog.Printf("%s", msg)
				}
			}
			el.Unlock()
		}
	}
}

// ctxEventLogKey is an empty type for the handle associated with the
// ctxEventLog value (see context.Value).
type ctxEventLogKey struct{}

// ctxEventLog value (see context.Value).
type ctxEventLog struct {
	sync.Mutex
	eventLog trace.EventLog
}

func (el *ctxEventLog) finish() {
	el.Lock()
	if el.eventLog != nil {
		el.eventLog.Finish()
		el.eventLog = nil
	}
	el.Unlock()
}

func embedCtxEventLog(ctx context.Context, el *ctxEventLog) context.Context {
	return context.WithValue(ctx, ctxEventLogKey{}, el)
}

func withEventLogInternal(ctx context.Context, eventLog trace.EventLog) context.Context {
	if opentracing.SpanFromContext(ctx) != nil {
		panic("event log under span")
	}
	return embedCtxEventLog(ctx, &ctxEventLog{eventLog: eventLog})
}

// WithEventLog creates and embeds a trace.EventLog in the context, causing
// future logging and event calls to go to the EventLog. The current context
// must not have an existing open span.
func WithEventLog(ctx context.Context, family, title string) context.Context {
	return withEventLogInternal(ctx, trace.NewEventLog(family, title))
}

var _ = WithEventLog

// WithNoEventLog creates a context which no longer has an embedded event log.
func WithNoEventLog(ctx context.Context) context.Context {
	return withEventLogInternal(ctx, nil)
}

func eventLogFromCtx(ctx context.Context) *ctxEventLog {
	if val := ctx.Value(ctxEventLogKey{}); val != nil {
		return val.(*ctxEventLog)
	}
	return nil
}

func getSpanOrEventLog(ctx context.Context) (opentracing.Span, *ctxEventLog, bool) {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		if tracing.IsBlackHoleSpan(sp) {
			return nil, nil, false
		}
		return sp, nil, true
	}
	if el := eventLogFromCtx(ctx); el != nil {
		return nil, el, true
	}
	return nil, nil, false
}

// HasSpanOrEvent returns true if the context has a span or event that should
// be logged to.
func HasSpanOrEvent(ctx context.Context) bool {
	_, _, ok := getSpanOrEventLog(ctx)
	return ok
}

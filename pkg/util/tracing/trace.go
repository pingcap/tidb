// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tracing

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Constants used in event fields.
// See https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
// for more details.

type Phase string

const (
	PhaseBegin      Phase = "B"
	PhaseEnd        Phase = "E"
	PhaseAsyncBegin Phase = "b"
	PhaseAsyncEnd   Phase = "e"
	PhaseFlowBegin  Phase = "s"
	PhaseFlowEnd    Phase = "f"
	PhaseInstant    Phase = "i"
)

func getTraceContext(ctx context.Context) (traceContext, bool) {
	v := ctx.Value(traceKey{})
	if v == nil {
		return traceContext{}, false
	}
	return v.(traceContext), true
}

type Event struct {
	Name     string `json:"name"`
	Phase    Phase  `json:"ph"`
	Ts       int64  `json:"ts"` // microsecond
	PID      string `json:"pid"`
	TID      uint64 `json:"tid"`
	ID       uint64 `json:"id,omitempty"` // used by async / flow
	Category string `json:"cat,omitempty"`
}

type Span struct {
	traceContext
	name string
}

// StartSpan starts a trace event with the given name. The Span ends when its Done method is called.
func StartSpan(ctx context.Context, name string) Span {
	tc, ok := getTraceContext(ctx)
	if !ok || tc.t == nil {
		return Span{}
	}

	event := Event{
		Name:  name,
		Phase: PhaseBegin,
		Ts:    time.Now().UnixMicro(),
		PID:   tc.t.pid,
		TID:   tc.tid,
	}
	tc.t.record(event)
	return Span{traceContext: tc, name: name}
}

func Log(ctx context.Context, name string) {
	tc, ok := getTraceContext(ctx)
	if !ok || tc.t == nil {
		return
	}
	event := Event{
		Name:  name,
		Phase: PhaseInstant,
		Ts:    time.Now().UnixMicro(),
		PID:   tc.t.pid,
		TID:   tc.tid,
	}
	tc.t.record(event)
}

func (s *Span) Done() {
	if s == nil || s.t == nil {
		return
	}
	event := Event{
		Name:  s.name,
		Phase: PhaseEnd,
		Ts:    time.Now().UnixMicro(),
		PID:   s.t.pid,
		TID:   s.tid,
	}
	s.t.record(event)
}

type AsyncSpan struct {
	t    *Trace
	name string
	id   uint64
}

func StartAsyncSpan(ctx context.Context, name string) AsyncSpan {
	tc, ok := getTraceContext(ctx)
	if !ok {
		return AsyncSpan{}
	}

	id := getNextEventID()
	now := time.Now().UnixMicro()
	tc.t.record(Event{
		Name:  name,
		Phase: PhaseAsyncBegin,
		Ts:    now,
		PID:   tc.t.pid,
		TID:   tc.tid,
		ID:    id,
	})
	return AsyncSpan{tc.t, name, id}
}

func (s *AsyncSpan) GetTrace() *Trace {
	return s.t
}

func (s *AsyncSpan) Done(ctx context.Context) {
	tc, ok := getTraceContext(ctx)
	if !ok {
		return
	}

	now := time.Now().UnixMicro()
	s.t.record(Event{
		Name:  s.name,
		Phase: PhaseAsyncEnd,
		Ts:    now,
		PID:   s.t.pid,
		TID:   tc.tid,
		ID:    s.id,
	})
}

type Flow struct {
	t    *Trace
	name string
	id   uint64
}

func StartFlow(ctx context.Context, name string) Flow {
	tc, ok := getTraceContext(ctx)
	if !ok || tc.t == nil {
		return Flow{}
	}

	id := getNextEventID()
	tc.t.record(Event{
		Name:     name,
		Phase:    PhaseFlowBegin,
		PID:      tc.t.pid,
		TID:      tc.tid,
		Ts:       time.Now().UnixMicro(),
		ID:       id,
		Category: "flow",
	})
	return Flow{tc.t, name, id}
}

func (flow *Flow) Done(ctx context.Context) {
	tc, ok := getTraceContext(ctx)
	if !ok {
		return
	}

	flow.t.record(Event{
		Name:     flow.name,
		Phase:    PhaseFlowEnd,
		PID:      flow.t.pid,
		TID:      tc.tid,
		Ts:       time.Now().UnixMicro(),
		ID:       flow.id,
		Category: "flow",
	})
}

var (
	nextTID atomic.Uint64
	nextID  atomic.Uint64
)

func GetNextTID() uint64 {
	return nextTID.Add(1)
}

func getNextEventID() uint64 {
	return nextID.Add(1)
}

// traceKey is the context key for tracing information. It is unexported to prevent collisions with context keys defined in
// other packages.
type traceKey struct{}

type traceContext struct {
	t   *Trace
	tid uint64
}

type Trace struct {
	Keep   bool
	pid    string
	mu     sync.Mutex
	events []Event
}

// NewTrace starts a trace which writes to the given buffer.
func NewTrace(ctx context.Context, buf []Event) (context.Context, *Trace) {
	t := &Trace{
		events: buf,
	}
	ctx = context.WithValue(ctx, traceKey{}, traceContext{t, GetNextTID()})
	return ctx, t
}

func WithTraceContext(ctx context.Context, t *Trace, tid uint64) context.Context {
	if t == nil {
		tc, ok := getTraceContext(ctx)
		if ok {
			t = tc.t
		}
	}
	if tid == 0 {
		tc, ok := getTraceContext(ctx)
		if ok {
			tid = tc.tid
		}
	}
	return context.WithValue(ctx, traceKey{}, traceContext{t, tid})
}

func (t *Trace) record(ev Event) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, ev)
}

// MarkDump marks the task need to be dump.
func MarkDump(ctx context.Context) {
	tc, ok := getTraceContext(ctx)
	if !ok {
		return
	}
	tc.t.Keep = true
}

func (t *Trace) Close() []Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.events
}

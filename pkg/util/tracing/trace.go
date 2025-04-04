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
//
// This file is taken from the Go project originally, but largely revisited for our use
// https://github.com/golang/go/blob/dceb77a33676c8a4efb9c63267c351268848de6f/src/cmd/go/internal/trace/trace.go
//
// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tracing

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type phaseType byte

// Constants used in event fields.
// See https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
// for more details.
const (
	phaseDurationBegin phaseType = 'B'
	phaseDurationEnd   phaseType = 'E'
	phaseFlowStart     phaseType = 's'
	phaseFlowEnd       phaseType = 'f'

	bindEnclosingSlice phaseType = 'e'
)

func getTraceContext(ctx context.Context) (traceContext, bool) {
	v := ctx.Value(traceKey{})
	if v == nil {
		return traceContext{}, false
	}
	return v.(traceContext), true
}

// StartSpan starts a trace event with the given name. The Span ends when its Done method is called.
func StartSpan(ctx context.Context, name string) Span {
	tc, ok := getTraceContext(ctx)
	if !ok {
		return Span{}
	}
	childSpan := Span{t: tc.t, name: name, tid: tc.tid, start: time.Now()}
	tc.t.writeDurationBeginEvent(childSpan.name, float64(childSpan.start.UnixNano())/float64(time.Microsecond), childSpan.tid)
	return childSpan
}

// StartGoroutine associates the context with a new Thread ID. The Chrome trace viewer associates each
// trace event with a thread, and doesn't expect events with the same thread id to happen at the
// same time.
func StartGoroutine(ctx context.Context) context.Context {
	tc, ok := getTraceContext(ctx)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, traceContext{tc.t, getNextTID()})
}

// Flow marks a flow indicating that the 'to' span depends on the 'from' span.
// Flow should be called while the 'to' span is in progress.
func Flow(ctx context.Context, from *Span, to *Span) {
	tc, ok := getTraceContext(ctx)
	if !ok || from == nil || to == nil {
		return
	}

	id := getNextFlowID()
	name := from.name + "->" + to.name
	tc.t.writeFlowStartEvent(name, id, float64(from.end.UnixNano())/float64(time.Microsecond), from.tid)
	tc.t.writeFlowEndEvent(name, id, float64(to.start.UnixNano())/float64(time.Microsecond), to.tid)
}

type Span struct {
	t *Task

	name  string
	tid   uint64
	start time.Time
	end   time.Time
}

func (s *Span) Done() {
	if s == nil || s.t == nil {
		return
	}
	s.end = time.Now()
	s.t.writeDurationEndEvent(s.name, float64(s.end.UnixNano())/float64(time.Microsecond), s.tid)
}

var (
	nextTID    atomic.Uint64
	nextFlowID atomic.Uint64
)

type fieldType byte

const (
	fieldTypeUint64 fieldType = iota
	fieldTypeString
	fieldTypeByte
	fieldTypeFloat64
)

type field struct {
	Key       string
	Type      fieldType
	Integer   int64
	String    string
	Interface interface{}
}

func String(key string, val string) field {
	return field{Key: key, Type: fieldTypeString, String: val}
}

func Uint64(key string, val uint64) field {
	return field{Key: key, Type: fieldTypeUint64, Integer: int64(val)}
}

func Byte(key string, val byte) field {
	return field{Key: key, Type: fieldTypeByte, Integer: int64(val)}
}

func Float64(key string, val float64) field {
	return field{Key: key, Type: fieldTypeFloat64, Integer: int64(math.Float64bits(val))}
}

func (t *Task) writeEvent(tp phaseType, fields ...field) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.buf.WriteByte(byte(tp))
	for _, fl := range fields {
		switch fl.Type {
		case fieldTypeUint64:
			fallthrough
		case fieldTypeFloat64:
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], uint64(fl.Integer))
			t.buf.Write(buf[:])
		case fieldTypeString:
			t.buf.WriteString(fl.String)
			t.buf.WriteByte(0)
		case fieldTypeByte:
			t.buf.WriteByte(byte(fl.Integer))
		default:
			panic("unsupported")
		}
	}
	t.entries++
	return
}

func (t *Task) writeDurationBeginEvent(name string, dur float64, tid uint64) {
	t.writeEvent(phaseDurationBegin, String("Name", name), Float64("Time", dur), Uint64("TID", tid))
}

func (t *Task) writeDurationEndEvent(name string, dur float64, tid uint64) {
	t.writeEvent(phaseDurationEnd, String("Name", name), Float64("Time", dur), Uint64("TID", tid))
}

func (t *Task) writeFlowStartEvent(name string, id uint64, dur float64, tid uint64) {
	t.writeEvent(phaseFlowStart,
		String("Name", name),
		String("Catagory", "flow"),
		Uint64("ID", id),
		Float64("Time", dur),
		Uint64("TID", tid))
}

func (t *Task) writeFlowEndEvent(name string, id uint64, dur float64, tid uint64) {
	t.writeEvent(phaseFlowEnd,
		String("Name", name),
		String("Category", "flow"),
		Uint64("ID", id),
		Float64("Time", dur),
		Uint64("TID", tid),
		Byte("BindPoint", byte(bindEnclosingSlice)))
}

func getNextTID() uint64 {
	return nextTID.Add(1)
}

func getNextFlowID() uint64 {
	return nextFlowID.Add(1)
}

// traceKey is the context key for tracing information. It is unexported to prevent collisions with context keys defined in
// other packages.
type traceKey struct{}

type traceContext struct {
	t   *Task
	tid uint64
}

const version uint32 = 1
const magicHead = "tidb-trace"

type Task struct {
	mu      sync.Mutex
	buf     *bytes.Buffer
	entries int64
	Keep bool
}

// NewTask starts a trace which writes to the given buffer.
func NewTask(ctx context.Context, buf *bytes.Buffer) (context.Context, *Task) {
	t := &Task{
		buf: buf,
	}
	// The trace format is using internal representation!
	// Compatibility is guaranteed by tools.
	// The tool should be able to understand the format and convert it to some standard one.
	// Just one magic number for the version.
	buf.WriteString(magicHead)
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], version)
	buf.Write(tmp[:])
	tc := traceContext{t: t, tid: getNextTID()}
	ctx = context.WithValue(ctx, traceKey{}, tc)
	return ctx, t
}

// MarkDump marks the task need to be dump.
func MarkDump(ctx context.Context) {
	tc, ok := getTraceContext(ctx)
	if !ok {
		return
	}
	tc.t.Keep = true
}

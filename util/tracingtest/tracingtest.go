// Copyright 2021 PingCAP, Inc.
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

package tracingtest

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maruel/panicparse/stack"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/v2pro/plz/gls"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	sandboxBreakpoint = "sandboxBreakpoint"
	sandboxPkg        = "github.com/pingcap/tidb/util/tracingtest"
)

type routineCtxKey struct{}

// Sandbox manages test `Routine`s.
// it also support collect history trace log to assert execution path.
// see a demo in `tikv.TestCommitSecondarySlow`
type Sandbox struct {
	routines map[*routine]struct{}
	idAlloc  int
}

// routine defines a test routine.
// test script can be abstracted as several routines
// and test target is arrange them in different execution order or path.
type routine struct {
	Ctx  context.Context
	Name string
	ID   int

	tracer    opentracing.Tracer
	recorder  *basictracer.InMemorySpanRecorder
	span      opentracing.Span
	ctxCancel context.CancelFunc

	sandbox  *Sandbox
	finished *atomic.Bool

	muBp struct {
		sync.Mutex
		breakpoints map[string]*breakpoint
	}

	hook routineHook
}

// breakpoint defines "debug breakpoint like" breakpoints in a routine.
// with breakpoint in routine, test script can schedule routines in different execution order.
type breakpoint struct {
	c   chan struct{}
	hit bool
	gid *atomic.Int64
}

type routineHook struct {
	sync.Mutex
	enable bool
	fps    map[string]struct{}
}

func (h *routineHook) hook(ctx context.Context, fpname string) bool {
	if !h.enable {
		return true
	}
	if len(h.fps) == 0 {
		return false
	}
	_, exists := h.fps[fpname]
	return exists
}

// NewSandbox creates a sandbox.
func NewSandbox() *Sandbox {
	supportBreakpoint()
	return &Sandbox{routines: make(map[*routine]struct{})}
}

// StartRoutine starts routine inside sandbox.
// it return `context.Context` that should be used for single test routine.
func (m *Sandbox) StartRoutine(ctx context.Context, opName string) context.Context {
	recorder := basictracer.NewInMemoryRecorder()
	tracer := basictracer.New(recorder)
	span := tracer.StartSpan(opName)
	ctx = opentracing.ContextWithSpan(ctx, span)
	ctx, cancel := context.WithCancel(ctx)
	m.idAlloc++
	routine := &routine{
		Ctx:  ctx,
		Name: opName,
		ID:   m.idAlloc,

		tracer:    tracer,
		recorder:  recorder,
		span:      span,
		ctxCancel: cancel,

		sandbox:  m,
		finished: atomic.NewBool(false),

		hook: routineHook{
			enable: true,
			fps:    make(map[string]struct{}),
		},
	}
	routine.muBp.breakpoints = make(map[string]*breakpoint)
	m.routines[routine] = struct{}{}
	ctx = context.WithValue(ctx, routineCtxKey{}, routine)
	ctx = failpoint.WithHook(ctx, routine.hook.hook)
	return ctx
}

// CollectLogs collects tracing log for routines inside sandbox.
func (m *Sandbox) CollectLogs() string {
	var logs []*logRecord
	for routine := range m.routines {
		logs = append(logs, routine.collectLogs()...)
		if routine.finished.Load() {
			delete(m.routines, routine)
		}
	}
	sort.SliceStable(logs, func(i, j int) bool {
		return logs[i].rid < logs[j].rid
	})
	var builder strings.Builder
	for _, log := range logs {
		if builder.Len() != 0 {
			fmt.Fprint(&builder, "\n")
		}
		id := "-"
		if log.rid > 0 {
			id = strconv.Itoa(log.rid)
		}
		fmt.Fprintf(&builder, "[%s] %s: %s", id, log.op, log.val)
	}
	return builder.String()
}

// FinishRoutine finishes started routine inside context and release resources.
func FinishRoutine(ctx context.Context) {
	rr := ctx.Value(routineCtxKey{})
	if rr != nil {
		r := rr.(*routine)
		r.ctxCancel()
		r.span.Finish()
		r.finished.Store(true)
	}
}

func supportBreakpoint() {
	failpoint.Enable(sandboxPkg+"/"+sandboxBreakpoint, "return(true)")
}

// CheckBreakpoint checks breakpoint and wait if need.
// this method need called in code and only be enable after `make failpoint-enable`.
// then test script can enable it by using `EnableBreakpoint`
func CheckBreakpoint(ctx context.Context, bp string) {
	failpoint.Inject(sandboxBreakpoint, func() {
		rr := ctx.Value(routineCtxKey{})
		if rr == nil {
			return
		}
		r := rr.(*routine)
		r.muBp.Lock()
		b, ok := r.muBp.breakpoints[bp]
		firstHit := ok && !b.hit
		if firstHit {
			b.hit = true
		}
		r.muBp.Unlock()
		if firstHit {
			b.gid.Store(gls.GoID())
			<-b.c
		}
	})
}

// EnableBreakpoint enables breakpoint that be defined by `CheckBreakpoint` for context's routine.
func EnableBreakpoint(ctx context.Context, bp string) {
	failpoint.Inject(sandboxBreakpoint, func() {
		rr := ctx.Value(routineCtxKey{})
		if rr == nil {
			return
		}
		r := rr.(*routine)
		r.muBp.Lock()
		r.muBp.breakpoints[bp] = &breakpoint{c: make(chan struct{}), gid: atomic.NewInt64(-1)}
		r.muBp.Unlock()
	})
}

const maxWaitDuration = 10 * time.Second

// EnsureWaitOnBreakpoint ensures context's routine waiting on breakpoint.
func EnsureWaitOnBreakpoint(ctx context.Context, bp string) {
	failpoint.Inject(sandboxBreakpoint, func() {
		rr := ctx.Value(routineCtxKey{})
		if rr == nil {
			return
		}
		r := rr.(*routine)

		r.muBp.Lock()
		b, ok := r.muBp.breakpoints[bp]
		if !ok {
			r.muBp.Unlock()
			return
		}
		r.muBp.Unlock()

		waitCond(func() bool {
			return b.gid.Load() > 0
		})
		gid := b.gid.Load()

		buf := make([]byte, 1<<16)
		waitCond(func() bool {
			g := goroutineStack(gid, &buf)
			var waitOnBreakpoint bool
			if g.State == "chan receive" {
				for _, call := range g.Stack.Calls {
					if strings.Contains(call.Func.Raw, "CheckBreakpoint") {
						waitOnBreakpoint = true
						break
					}
				}
			}
			return waitOnBreakpoint
		})
	})
}

func waitCond(cond func() bool) {
	wait, start := 1*time.Millisecond, time.Now()
	for {
		if time.Since(start) > maxWaitDuration {
			panic("can not reach desired stats after retry..")
		}
		if cond() {
			return
		}
		time.Sleep(wait)
		wait *= 2
		if wait > time.Second {
			wait = time.Second
		}
	}
}

// ContinueBreakpoint continues context's routine that waiting on breakpoint and return `gid`.
func ContinueBreakpoint(ctx context.Context, bp string) (gid int64) {
	gid = -1
	failpoint.Inject(sandboxBreakpoint, func() {
		rr := ctx.Value(routineCtxKey{})
		if rr == nil {
			return
		}
		r := rr.(*routine)
		var waitRun bool
		r.muBp.Lock()
		b, ok := r.muBp.breakpoints[bp]
		if ok && b.hit {
			waitRun = true
			delete(r.muBp.breakpoints, bp)
		}
		r.muBp.Unlock()
		if waitRun {
			gid = b.gid.Load()
			b.c <- struct{}{}
			b.gid.Store(-1)
		}
	})
	return
}

// EnsureGoroutineFinished ensure `gid`'s goroutine has be finished.
func EnsureGoroutineFinished(gid int64) {
	buf := make([]byte, 1<<16)
	waitCond(func() bool {
		return goroutineStack(gid, &buf) == nil
	})
}

func goroutineStack(gid int64, buf *[]byte) *stack.Goroutine {
	if buf == nil {
		*buf = make([]byte, 1<<16)
	}
	var usedBuf []byte
	for {
		n := runtime.Stack(*buf, true)
		if n < len(*buf) {
			usedBuf = (*buf)[:n]
			break
		}
		*buf = make([]byte, 2*len(*buf))
	}

	ctx, err := stack.ParseDump(bytes.NewBuffer(usedBuf), ioutil.Discard, true)
	if err != nil {
		panic("get stack error: " + err.Error())
	}
	for _, g := range ctx.Goroutines {
		if g.ID == int(gid) {
			return g
		}
	}
	return nil
}

// EnableFailpoint enables failpoint in context's routine.
func EnableFailpoint(ctx context.Context, fp string) {
	rr := ctx.Value(routineCtxKey{})
	if rr == nil {
		return
	}
	r := rr.(*routine)
	r.hook.Lock()
	r.hook.fps[fp] = struct{}{}
	r.hook.Unlock()
}

// DisableFailpoint disables failpoint in context's routine.
func DisableFailpoint(ctx context.Context, fp string) {
	rr := ctx.Value(routineCtxKey{})
	if rr == nil {
		return
	}
	r := rr.(*routine)
	r.hook.Lock()
	delete(r.hook.fps, fp)
	r.hook.Unlock()
}

type logRecord struct {
	rid int
	op  string
	val string
	ts  time.Time
}

func (r *routine) collectLogs() (logs []*logRecord) {
	for _, sp := range r.recorder.GetSpans() {
		for _, l := range sp.Logs {
			for _, f := range l.Fields {
				logs = append(logs, &logRecord{
					rid: r.ID,
					op:  r.Name,
					val: fmt.Sprintf("%s", f.Value()),
					ts:  l.Timestamp,
				})
			}
		}
	}
	sort.SliceStable(logs, func(i, j int) bool {
		return logs[i].ts.Before(logs[j].ts)
	})
	return
}

// traceLogEqualsChecker is a checker for trace logs.
type traceLogEqualsChecker struct {
	*check.CheckerInfo
}

// LogsEquals is a helper checker for trace logs.
var LogsEquals check.Checker = &traceLogEqualsChecker{
	&check.CheckerInfo{Name: "LogsEquals", Params: []string{"obtained", "expected"}},
}

func (checker *traceLogEqualsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	defer func() {
		if v := recover(); v != nil {
			result = false
			error = fmt.Sprint(v)
			logutil.BgLogger().Error("panic in logsEqualsChecker.Check",
				zap.Reflect("r", v),
				zap.Stack("stack trace"))
		}
	}()
	paramFirst, ok := params[0].(string)
	if !ok {
		panic("the first param should be logs")
	}
	paramSecond, ok := params[1].(string)
	if !ok {
		panic("the second param should be logs")
	}
	first, second := strings.Split(paramFirst, "\n"), strings.Split(paramSecond, "\n")
	if len(first) != len(second) {
		panic(fmt.Sprintf("logs length not match %d:%d", len(first), len(second)))
	}
	for i := 0; i < len(second); i++ {
		if strings.TrimSpace(first[i]) != strings.TrimSpace(second[i]) {
			panic(fmt.Sprintf("logs not match [%s]:[%s]", strings.TrimSpace(first[i]), strings.TrimSpace(second[i])))
		}
	}
	return true, ""
}

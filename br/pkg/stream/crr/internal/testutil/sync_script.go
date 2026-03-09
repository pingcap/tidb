// Copyright 2026 PingCAP, Inc.
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

package testutil

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

// InjectContext coordinates failpoint handlers in tests.
type InjectContext struct {
	script      *SyncScript
	name        string
	releaseThis func()
}

// Wait blocks until the named failpoint has completed at least once.
func (ctx InjectContext) Wait(name string) {
	ctx.script.WaitUntil(name, 1)
}

// Release lets one blocked invocation of the named failpoint continue.
func (ctx InjectContext) Release(name string) {
	ctx.script.Release(name)
}

// ReleaseThis marks the current failpoint invocation as completed.
func (ctx InjectContext) ReleaseThis() {
	ctx.releaseThis()
}

// SyncScript coordinates InjectCall handlers inside tests.
type SyncScript struct {
	tc       *TestContext
	basePath string

	mu     sync.Mutex
	points map[string]*syncPoint
	rng    *deterministicRNG
}

type syncPoint struct {
	mu        sync.Mutex
	cond      *sync.Cond
	blocked   bool
	permits   int
	completed int

	handlerType reflect.Type
	handlers    []reflect.Value
	enabled     bool
}

// NewSyncScript creates a test-only failpoint script bound to one package path.
func NewSyncScript(t testing.TB, basePath string) *SyncScript {
	return NewSyncScriptWithTestContext(NewTestContext(t), basePath)
}

func NewSyncScriptWithSeed(t testing.TB, basePath string, seed int64) *SyncScript {
	return NewSyncScriptWithTestContext(NewTestContextWithSeed(t, seed), basePath)
}

func NewSyncScriptWithTestContext(tc *TestContext, basePath string) *SyncScript {
	tc.T.Helper()
	return &SyncScript{
		tc:       tc,
		basePath: basePath,
		points:   make(map[string]*syncPoint),
		rng:      tc.RNG("sync-script"),
	}
}

// On registers a handler for a named InjectCall failpoint.
//
// The handler must have the form:
//
//	func(ctx InjectContext)
//	func(ctx InjectContext, arg1 T1, arg2 T2, ...)
func (s *SyncScript) On(name string, fn any) {
	s.tc.T.Helper()

	fnValue := reflect.ValueOf(fn)
	fnType := fnValue.Type()
	if fnType.Kind() != reflect.Func {
		s.tc.T.Fatalf("sync script handler for %s must be a function", name)
	}
	if fnType.NumIn() < 1 || fnType.In(0) != reflect.TypeFor[InjectContext]() {
		s.tc.T.Fatalf("sync script handler for %s must accept InjectContext as the first argument", name)
	}
	if fnType.NumOut() != 0 {
		s.tc.T.Fatalf("sync script handler for %s must not return values", name)
	}

	point := s.getPoint(name)
	point.mu.Lock()
	defer point.mu.Unlock()

	if point.handlerType == nil {
		point.handlerType = fnType
	} else if !sameHandlerSignature(point.handlerType, fnType) {
		s.tc.T.Fatalf("sync script handler for %s has inconsistent argument types", name)
	}
	point.handlers = append(point.handlers, fnValue)

	if point.enabled {
		return
	}

	wrapper := reflect.MakeFunc(stripContextFuncType(fnType), func(args []reflect.Value) []reflect.Value {
		point.waitForPermit()

		handlers := point.snapshotHandlers()
		ctx := s.newContext(name)
		defer ctx.ReleaseThis()

		callArgs := make([]reflect.Value, 0, len(args)+1)
		callArgs = append(callArgs, reflect.ValueOf(ctx))
		callArgs = append(callArgs, args...)
		for _, handler := range handlers {
			handler.Call(callArgs)
		}
		return nil
	})

	testfailpoint.EnableCall(s.tc.T, s.fullPath(name), wrapper.Interface())
	point.enabled = true
}

// BlockAll blocks all future invocations of the named failpoints until released.
func (s *SyncScript) BlockAll(names ...string) {
	for _, name := range names {
		point := s.getPoint(name)
		point.mu.Lock()
		point.blocked = true
		point.mu.Unlock()
	}
}

// Release lets one blocked invocation of the named failpoint continue.
func (s *SyncScript) Release(name string) {
	point := s.getPoint(name)
	point.mu.Lock()
	point.permits++
	point.cond.Broadcast()
	point.mu.Unlock()
}

// WaitUntil blocks until the named failpoint has completed at least n times.
func (s *SyncScript) WaitUntil(name string, n int) {
	if n <= 0 {
		return
	}
	point := s.getPoint(name)
	point.mu.Lock()
	defer point.mu.Unlock()
	for point.completed < n {
		point.cond.Wait()
	}
}

// RequireSeq configures a simple sequential schedule A -> B -> C.
func (s *SyncScript) RequireSeq(names ...string) {
	if len(names) <= 1 {
		return
	}
	s.BlockAll(names[1:]...)
	for i := 1; i < len(names); i++ {
		prev := names[i-1]
		s.On(names[i], func(ctx InjectContext) {
			ctx.Wait(prev)
		})
	}
	for i := range len(names) - 1 {
		next := names[i+1]
		s.On(names[i], func(ctx InjectContext) {
			ctx.Release(next)
		})
	}
}

// Interleave configures a random merged schedule of two ordered sequences.
//
// The relative order inside each sequence is preserved, but the two sequences
// may interleave arbitrarily. For example:
//
//	Interleave([]string{"A1", "A2", "A3"}, []string{"B1", "B2"})
//
// may require either:
//
//	"A1" -> "B1" -> "A2" -> "B2" -> "A3"
//
// or:
//
//	"B1" -> "A1" -> "A2" -> "B2" -> "A3"
func (s *SyncScript) Interleave(left, right []string) {
	merged := interleaveSeqs(s.rng, left, right)
	s.RequireSeq(merged...)
}

func interleaveSeqs(rng *deterministicRNG, left, right []string) []string {
	if len(left) == 0 {
		return append([]string(nil), right...)
	}
	if len(right) == 0 {
		return append([]string(nil), left...)
	}

	merged := make([]string, 0, len(left)+len(right))
	i, j := 0, 0
	for i < len(left) && j < len(right) {
		if rng.IntN(len(left)-i+len(right)-j) < len(left)-i {
			merged = append(merged, left[i])
			i++
			continue
		}
		merged = append(merged, right[j])
		j++
	}
	merged = append(merged, left[i:]...)
	merged = append(merged, right[j:]...)
	return merged
}

func (s *SyncScript) newContext(name string) InjectContext {
	var once sync.Once
	return InjectContext{
		script: s,
		name:   name,
		releaseThis: func() {
			once.Do(func() {
				point := s.getPoint(name)
				point.mu.Lock()
				point.completed++
				point.cond.Broadcast()
				point.mu.Unlock()
			})
		},
	}
}

func (s *SyncScript) getPoint(name string) *syncPoint {
	s.mu.Lock()
	defer s.mu.Unlock()

	point := s.points[name]
	if point != nil {
		return point
	}

	point = &syncPoint{}
	point.cond = sync.NewCond(&point.mu)
	s.points[name] = point
	return point
}

func (s *SyncScript) fullPath(name string) string {
	if s.basePath == "" {
		return name
	}
	return fmt.Sprintf("%s/%s", s.basePath, name)
}

func (p *syncPoint) waitForPermit() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for p.blocked && p.permits == 0 {
		p.cond.Wait()
	}
	if p.blocked {
		p.permits--
	}
}

func (p *syncPoint) snapshotHandlers() []reflect.Value {
	p.mu.Lock()
	defer p.mu.Unlock()

	handlers := make([]reflect.Value, len(p.handlers))
	copy(handlers, p.handlers)
	return handlers
}

func sameHandlerSignature(a, b reflect.Type) bool {
	if a.NumIn() != b.NumIn() {
		return false
	}
	for i := 1; i < a.NumIn(); i++ {
		if a.In(i) != b.In(i) {
			return false
		}
	}
	return true
}

func stripContextFuncType(fnType reflect.Type) reflect.Type {
	args := make([]reflect.Type, 0, fnType.NumIn()-1)
	for i := 1; i < fnType.NumIn(); i++ {
		args = append(args, fnType.In(i))
	}
	return reflect.FuncOf(args, nil, false)
}

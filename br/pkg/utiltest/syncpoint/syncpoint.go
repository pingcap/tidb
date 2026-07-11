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

package syncpoint

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
)

// StepDecl declares one failpoint participation in an explicit sequence.
type StepDecl struct {
	name string
	fn   any
}

type activeStep struct {
	name string
	fn   reflect.Value
}

type registeredStep struct {
	fnType reflect.Type
}

// Step builds one sequence step.
//
// The name must be the full failpoint path accepted by EnableCall. The fn must
// be a non-nil function with no return values and a signature compatible with
// the failpoint arguments.
func Step(name string, fn any) StepDecl {
	return StepDecl{name: name, fn: fn}
}

// Script coordinates failpoint ordering inside tests.
type Script struct {
	t          testing.TB
	state      *state
	registered map[string]registeredStep
}

type state struct {
	mu sync.Mutex

	cond *sync.Cond

	seq       []activeStep
	next      int
	err       error
	stopWatch func() bool
}

func New(t testing.TB) *Script {
	t.Helper()

	st := &state{}
	st.cond = sync.NewCond(&st.mu)
	return &Script{
		t:          t,
		state:      st,
		registered: make(map[string]registeredStep),
	}
}

// BeginSeq starts an explicit ordered sequence for subsequent failpoint hits.
func (s *Script) BeginSeq(ctx context.Context, steps ...StepDecl) {
	s.t.Helper()

	if ctx == nil {
		s.t.Fatalf("syncpoint sequence context must not be nil")
	}
	if len(steps) == 0 {
		s.t.Fatalf("syncpoint sequence must not be empty")
	}

	s.state.mu.Lock()
	alreadyActive := len(s.state.seq) != 0
	s.state.mu.Unlock()
	if alreadyActive {
		s.t.Fatalf("syncpoint sequence already active")
	}

	active := make([]activeStep, 0, len(steps))
	for _, step := range steps {
		active = append(active, s.prepareStep(step))
	}

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	if len(s.state.seq) != 0 {
		s.t.Fatalf("syncpoint sequence already active")
	}
	if s.state.stopWatch != nil {
		s.state.stopWatch()
	}

	s.state.seq = append(s.state.seq[:0], active...)
	s.state.next = 0
	s.state.err = nil
	s.state.stopWatch = context.AfterFunc(ctx, func() {
		s.state.mu.Lock()
		defer s.state.mu.Unlock()
		if len(s.state.seq) == 0 || s.state.next >= len(s.state.seq) || s.state.err != nil {
			return
		}
		s.state.err = fmt.Errorf(
			"sequence canceled while waiting for step %d (%s): %w",
			s.state.next,
			s.state.seq[s.state.next].name,
			ctx.Err(),
		)
		s.state.cond.Broadcast()
	})
}

// EndSeq waits for the active sequence to complete and validates it.
func (s *Script) EndSeq() {
	s.t.Helper()

	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	if len(s.state.seq) == 0 {
		s.t.Fatalf("syncpoint sequence underflow")
	}
	if s.state.stopWatch != nil {
		s.state.stopWatch()
	}
	require.NoError(s.t, s.state.err)
	require.Equal(s.t, len(s.state.seq), s.state.next)

	s.state.seq = s.state.seq[:0]
	s.state.next = 0
	s.state.err = nil
	s.state.stopWatch = nil
}

func (s *Script) prepareStep(step StepDecl) activeStep {
	s.t.Helper()

	if step.name == "" {
		s.t.Fatalf("syncpoint step name must not be empty")
	}

	fnValue := reflect.ValueOf(step.fn)
	if !fnValue.IsValid() {
		s.t.Fatalf("syncpoint step %s must provide a function", step.name)
	}
	fnType := fnValue.Type()
	if fnType.Kind() != reflect.Func {
		s.t.Fatalf("syncpoint step %s must provide a function", step.name)
	}
	if fnType.NumOut() != 0 {
		s.t.Fatalf("syncpoint step %s callback must not return values", step.name)
	}

	s.register(step.name, fnType)

	return activeStep{
		name: step.name,
		fn:   fnValue,
	}
}

func (s *Script) register(name string, fnType reflect.Type) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	if registered, ok := s.registered[name]; ok {
		if registered.fnType != fnType {
			s.t.Fatalf("syncpoint step %s registered with inconsistent signature", name)
		}
		return
	}

	wrapper := reflect.MakeFunc(fnType, func(args []reflect.Value) []reflect.Value {
		callback, ok := s.advance(name)
		if ok {
			callback.Call(args)
		}
		return nil
	})

	testfailpoint.EnableCall(s.t, name, wrapper.Interface())
	s.registered[name] = registeredStep{fnType: fnType}
}

func (s *Script) advance(name string) (reflect.Value, bool) {
	s.state.mu.Lock()
	defer s.state.mu.Unlock()

	if len(s.state.seq) == 0 {
		return reflect.Value{}, false
	}

	for s.state.err == nil {
		if s.state.next >= len(s.state.seq) {
			s.state.err = fmt.Errorf("unexpected step %s after sequence completed", name)
			s.state.cond.Broadcast()
			return reflect.Value{}, false
		}
		current := s.state.seq[s.state.next]
		if current.name == name {
			s.state.next++
			if s.state.next == len(s.state.seq) && s.state.stopWatch != nil {
				s.state.stopWatch()
			}
			s.state.cond.Broadcast()
			return current.fn, true
		}
		s.state.cond.Wait()
	}
	return reflect.Value{}, false
}

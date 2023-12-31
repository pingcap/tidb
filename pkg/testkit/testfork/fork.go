// Copyright 2022 PingCAP, Inc.
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

package testfork

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
)

type pickStack struct {
	stack [][]any
	pos   int
	valid bool
}

func newPickStack() *pickStack {
	return &pickStack{
		valid: true,
	}
}

func (s *pickStack) NextStack() {
	for len(s.stack) > 0 {
		lastIndex := len(s.stack) - 1
		s.stack[lastIndex] = s.stack[lastIndex][1:]
		if len(s.stack[lastIndex]) > 0 {
			break
		}
		s.stack = s.stack[:lastIndex]
	}

	s.pos = 0
	s.valid = len(s.stack) > 0
}

func (s *pickStack) PickValue(values []any) (any, error) {
	if len(values) == 0 {
		return nil, errors.New("values should not be empty")
	}

	stackLen := len(s.stack)
	if s.pos > stackLen {
		return nil, errors.Errorf("illegal state %d > %d", s.pos, stackLen)
	}

	defer func() {
		s.pos++
	}()

	if s.pos == stackLen {
		s.stack = append(s.stack, values)
	}
	return s.stack[s.pos][0], nil
}

func (s *pickStack) Values() []any {
	values := make([]any, 0)
	for _, v := range s.stack {
		values = append(values, v[0])
	}
	return values
}

func (s *pickStack) ValuesText() string {
	values := s.Values()
	strValues := make([]string, len(values))
	for i, value := range values {
		switch v := value.(type) {
		case string:
			strValues[i] = fmt.Sprintf(`"%s"`, v)
		default:
			strValues[i] = fmt.Sprintf("%v", v)
		}
	}
	return "[" + strings.Join(strValues, " ") + "]"
}

func (s *pickStack) Valid() bool {
	return s.valid
}

// T is used by for test
type T struct {
	*testing.T
	stack *pickStack
}

// RunTest runs the test function `f` multiple times util all the values in `Pick` are tested.
func RunTest(t *testing.T, f func(t *T)) {
	idx := 0
	runFunc := func(stack *pickStack, f func(t *T)) func(t *testing.T) {
		return func(t *testing.T) {
			f(&T{T: t, stack: stack})
		}
	}
	for stack := newPickStack(); stack.Valid(); stack.NextStack() {
		success := t.Run("", runFunc(stack, f))

		if !success {
			_, err := fmt.Fprintf(os.Stderr, "SubTest #%v failed, failed values: %s\n", idx, stack.ValuesText())
			require.NoError(t, err)
		}
		idx++
	}
}

// Pick returns a value from the values list
func Pick[E any](t *T, values []E) E {
	slice := make([]any, len(values))
	for i, item := range values {
		slice[i] = item
	}
	value, err := t.stack.PickValue(slice)
	require.NoError(t, err)
	return value.(E)
}

// PickEnum returns a value from the value enums
func PickEnum[E any](t *T, item E, other ...E) E {
	return Pick(t, append([]E{item}, other...))
}

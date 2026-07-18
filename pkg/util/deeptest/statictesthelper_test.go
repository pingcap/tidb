// Copyright 2024 PingCAP, Inc.
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

package deeptest

import (
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ require.TestingT = &shouldFailT{}

type shouldFailT struct {
	fail atomic.Bool
}

// Errorf implements require.TestingT.
func (s *shouldFailT) Errorf(format string, args ...any) {
	s.fail.Store(true)
	panic("panic now")
}

// FailNow implements require.TestingT.
func (s *shouldFailT) FailNow() {
	s.fail.Store(true)
	panic("panic now")
}

func (s *shouldFailT) failed() bool {
	return s.fail.Load()
}

// shouldFail asserts that the given function fails.
func shouldFail(t *testing.T, f func(t require.TestingT)) {
	st := &shouldFailT{}
	defer func() {
		_ = recover()

		require.True(t, st.failed(), "test should have failed")
	}()

	f(st)
}

type testInterface interface {
	testFn()
}

type testInterfaceImplA struct {
	_ int
}

func (*testInterfaceImplA) testFn() {}

type testInterfaceImplB struct{}

func (*testInterfaceImplB) testFn() {}

func TestAssertRecursivelyNotEqual(t *testing.T) {
	shouldFail(t, func(t require.TestingT) {
		AssertRecursivelyNotEqual(t, 1, 1)
	})
	AssertRecursivelyNotEqual(t, 1, 2)

	// nil are considered equal
	shouldFail(t, func(t require.TestingT) {
		AssertRecursivelyNotEqual[*int](t, nil, nil)
	})

	// different types are considered not equal
	h := &staticTestHelper{}
	h.assertRecursivelyNotEqual(t, reflect.ValueOf(1.0), reflect.ValueOf(1), "$")

	// one field equal is considered equal
	type testStructA struct {
		a int
		b int
	}
	AssertRecursivelyNotEqual(t, testStructA{1, 2}, testStructA{2, 3})
	shouldFail(t, func(t require.TestingT) {
		AssertRecursivelyNotEqual(t, testStructA{1, 2}, testStructA{1, 3})
	})

	// the common part of slice is also compared
	AssertRecursivelyNotEqual(t, []int{1, 2, 3}, []int{2, 3, 4})
	shouldFail(t, func(t require.TestingT) {
		AssertRecursivelyNotEqual(t, []int{1, 2, 3}, []int{1, 2, 4})
	})

	// the common part of map is also compared
	AssertRecursivelyNotEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{2: 4, 3: 4})
	shouldFail(t, func(t require.TestingT) {
		AssertRecursivelyNotEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 3: 4})
	})

	var a, b testInterface
	a = &testInterfaceImplA{}
	b = &testInterfaceImplB{}
	AssertRecursivelyNotEqual(t, a, b)

	// every function should be compared by pointer or ignored
	shouldFail(t, func(t require.TestingT) {
		AssertRecursivelyNotEqual(t, func() {}, func() {})
	})
}

func TestAssertRecursivelyNotEqualAndComparePointer(t *testing.T) {
	// for function
	// compare by pointer
	AssertRecursivelyNotEqual(t, func() {}, func() {}, WithPointerComparePath([]string{"$"}))
	// ignore path
	AssertRecursivelyNotEqual(t, func() {}, func() {}, WithIgnorePath([]string{"$"}))
	shouldFail(t, func(t require.TestingT) {
		a := func() {}
		AssertRecursivelyNotEqual(t, a, a, WithPointerComparePath([]string{"$"}))
	})

	// for ptr
	type structA struct{ a int }
	AssertRecursivelyNotEqual(t, &structA{1}, &structA{1}, WithPointerComparePath([]string{"$"}))

	// for slice
	AssertRecursivelyNotEqual(t, []int{1, 2, 3}, []int{1, 2, 3}, WithPointerComparePath([]string{"$"}))

	// for map
	AssertRecursivelyNotEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 2: 3}, WithPointerComparePath([]string{"$"}))
}

func TestAssertDeepClonedEqual(t *testing.T) {
	type structA struct{ a, b int }
	AssertDeepClonedEqual(t, structA{1, 2}, structA{1, 2})

	// For pointer
	var a, b *structA
	a = &structA{1, 2}
	b = &structA{1, 2}
	AssertDeepClonedEqual(t, a, b)
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, a, a)
	})
	AssertDeepClonedEqual(t, a, a, WithPointerComparePath([]string{"$"}))

	// For slice
	AssertDeepClonedEqual(t, []int(nil), []int(nil))
	AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2, 3})
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2, 4})
	})
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2, 3, 4})
	})
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2})
	})
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2, 3}, WithPointerComparePath([]string{"$"}))
	})
	s := []int{1, 2, 3}
	AssertDeepClonedEqual(t, s[:2], s[:2], WithPointerComparePath([]string{"$"}))
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, s[:2], s[:3], WithPointerComparePath([]string{"$"}))
	})

	// For map
	AssertDeepClonedEqual(t, map[int]int(nil), map[int]int(nil))
	AssertDeepClonedEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 2: 3})
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 3: 4})
	})
	m := map[int]int{1: 2, 2: 3}
	AssertDeepClonedEqual(t, m, m, WithPointerComparePath([]string{"$"}))
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 2: 3}, WithPointerComparePath([]string{"$"}))
	})

	// For interface
	var a1, b1 testInterface
	a1 = &testInterfaceImplA{}
	b1 = &testInterfaceImplA{}
	AssertDeepClonedEqual(t, a1, b1)
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, a1, a1)
	})
	AssertDeepClonedEqual(t, a1, a1, WithPointerComparePath([]string{"$"}))

	// For function
	var nilFunc1 func()
	var nilFunc2 func()
	AssertDeepClonedEqual(t, nilFunc1, nilFunc2)
	AssertDeepClonedEqual(t, TestAssertDeepClonedEqual, TestAssertDeepClonedEqual, WithPointerComparePath([]string{"$"}))
	shouldFail(t, func(t require.TestingT) {
		AssertDeepClonedEqual(t, func() {}, func() {})
	})
}

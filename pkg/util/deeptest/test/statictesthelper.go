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

package deeptest_test

import (
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/util/deeptest"
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

func RunAssertRecursivelyNotEqual(t *testing.T) {
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertRecursivelyNotEqual(t, 1, 1)
	})
	deeptest.AssertRecursivelyNotEqual(t, 1, 2)

	// nil are considered equal
	shouldFail(t, func(t require.TestingT) {
		AssertRecursivelyNotEqual[*int](t, nil, nil)
	})

	// different types are considered not equal
	h := deeptest.ExportNewStaticTestHelper()
	deeptest.ExportAssertRecursivelyNotEqual(h, t, reflect.ValueOf(1.0), reflect.ValueOf(1), "$")

	// one field equal is considered equal
	type testStructA struct {
		a int
		b int
	}
	deeptest.AssertRecursivelyNotEqual(t, testStructA{1, 2}, testStructA{2, 3})
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertRecursivelyNotEqual(t, testStructA{1, 2}, testStructA{1, 3})
	})

	// the common part of slice is also compared
	deeptest.AssertRecursivelyNotEqual(t, []int{1, 2, 3}, []int{2, 3, 4})
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertRecursivelyNotEqual(t, []int{1, 2, 3}, []int{1, 2, 4})
	})

	// the common part of map is also compared
	deeptest.AssertRecursivelyNotEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{2: 4, 3: 4})
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertRecursivelyNotEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 3: 4})
	})

	var a, b testInterface
	a = &testInterfaceImplA{}
	b = &testInterfaceImplB{}
	deeptest.AssertRecursivelyNotEqual(t, a, b)

	// every function should be compared by pointer or ignored
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertRecursivelyNotEqual(t, func() {}, func() {})
	})
}

func RunAssertRecursivelyNotEqualAndComparePointer(t *testing.T) {
	// for function
	// compare by pointer
	deeptest.AssertRecursivelyNotEqual(t, func() {}, func() {}, deeptest.WithPointerComparePath([]string{"$"}))
	// ignore path
	deeptest.AssertRecursivelyNotEqual(t, func() {}, func() {}, deeptest.WithIgnorePath([]string{"$"}))
	shouldFail(t, func(t require.TestingT) {
		a := func() {}
		deeptest.AssertRecursivelyNotEqual(t, a, a, deeptest.WithPointerComparePath([]string{"$"}))
	})

	// for ptr
	type structA struct{ a int }
	deeptest.AssertRecursivelyNotEqual(t, &structA{1}, &structA{1}, deeptest.WithPointerComparePath([]string{"$"}))

	// for slice
	deeptest.AssertRecursivelyNotEqual(t, []int{1, 2, 3}, []int{1, 2, 3}, deeptest.WithPointerComparePath([]string{"$"}))

	// for map
	deeptest.AssertRecursivelyNotEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 2: 3}, deeptest.WithPointerComparePath([]string{"$"}))
}

func RunAssertDeepClonedEqual(t *testing.T) {
	type structA struct{ a, b int }
	deeptest.AssertDeepClonedEqual(t, structA{1, 2}, structA{1, 2})

	// For pointer
	var a, b *structA
	a = &structA{1, 2}
	b = &structA{1, 2}
	deeptest.AssertDeepClonedEqual(t, a, b)
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, a, a)
	})
	deeptest.AssertDeepClonedEqual(t, a, a, deeptest.WithPointerComparePath([]string{"$"}))

	// For slice
	deeptest.AssertDeepClonedEqual(t, []int(nil), []int(nil))
	deeptest.AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2, 3})
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2, 4})
	})
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2, 3, 4})
	})
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2})
	})
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, []int{1, 2, 3}, []int{1, 2, 3}, deeptest.WithPointerComparePath([]string{"$"}))
	})
	s := []int{1, 2, 3}
	deeptest.AssertDeepClonedEqual(t, s[:2], s[:2], deeptest.WithPointerComparePath([]string{"$"}))
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, s[:2], s[:3], deeptest.WithPointerComparePath([]string{"$"}))
	})

	// For map
	deeptest.AssertDeepClonedEqual(t, map[int]int(nil), map[int]int(nil))
	deeptest.AssertDeepClonedEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 2: 3})
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 3: 4})
	})
	m := map[int]int{1: 2, 2: 3}
	deeptest.AssertDeepClonedEqual(t, m, m, deeptest.WithPointerComparePath([]string{"$"}))
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, map[int]int{1: 2, 2: 3}, map[int]int{1: 2, 2: 3}, deeptest.WithPointerComparePath([]string{"$"}))
	})

	// For interface
	var a1, b1 testInterface
	a1 = &testInterfaceImplA{}
	b1 = &testInterfaceImplA{}
	deeptest.AssertDeepClonedEqual(t, a1, b1)
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, a1, a1)
	})
	deeptest.AssertDeepClonedEqual(t, a1, a1, deeptest.WithPointerComparePath([]string{"$"}))

	// For function
	var nilFunc1 func()
	var nilFunc2 func()
	deeptest.AssertDeepClonedEqual(t, nilFunc1, nilFunc2)
	deeptest.AssertDeepClonedEqual(t, TestAssertDeepClonedEqual, TestAssertDeepClonedEqual, deeptest.WithPointerComparePath([]string{"$"}))
	shouldFail(t, func(t require.TestingT) {
		deeptest.AssertDeepClonedEqual(t, func() {}, func() {})
	})
}

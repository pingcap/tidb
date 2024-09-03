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
	"fmt"
	"reflect"

	"github.com/gobwas/glob"
	"github.com/stretchr/testify/require"
)

type staticTestHelper struct {
	ignorePath         []glob.Glob
	pointerComparePath []glob.Glob
}

func (h *staticTestHelper) applyOptions(opts ...Option) {
	for _, opt := range opts {
		opt.apply(h)
	}
}

func (h *staticTestHelper) shouldIgnore(path string) bool {
	for _, g := range h.ignorePath {
		if g.Match(path) {
			return true
		}
	}
	return false
}

func (h *staticTestHelper) shouldComparePointer(path string) bool {
	for _, g := range h.pointerComparePath {
		if g.Match(path) {
			return true
		}
	}
	return false
}

func (h *staticTestHelper) assertRecursivelyNotEqual(t require.TestingT, valA, valB reflect.Value, path string) {
	if h.shouldIgnore(path) {
		return
	}

	if !valA.IsValid() || !valB.IsValid() {
		require.False(t, !valA.IsValid() && !valB.IsValid(), path+" should not be zero value at the same time")
		return
	}

	if valA.Type() != valB.Type() {
		return
	}

	// This function assumes that `a` and `b` are the same type
	switch valA.Type().Kind() {
	case reflect.Struct:
		for i := 0; i < valA.NumField(); i++ {
			h.assertRecursivelyNotEqual(t, valA.Field(i), valB.Field(i), path+"."+valA.Type().Field(i).Name)
		}
	case reflect.Ptr:
		require.NotEqual(t, valA.Pointer(), valB.Pointer(), path+" should not be the same")

		if !h.shouldComparePointer(path) {
			h.assertRecursivelyNotEqual(t, valA.Elem(), valB.Elem(), path)
		}
	case reflect.Slice:
		require.NotEqual(t, valA.Pointer(), valB.Pointer(), path+" should not be the same")

		if !h.shouldComparePointer(path) {
			minLen := min(valA.Len(), valB.Len())
			for i := 0; i < minLen; i++ {
				h.assertRecursivelyNotEqual(t, valA.Index(i), valB.Index(i), path+fmt.Sprintf("[%d]", i))
			}
		}
	case reflect.Array:
		minLen := min(valA.Len(), valB.Len())
		for i := 0; i < minLen; i++ {
			h.assertRecursivelyNotEqual(t, valA.Index(i), valB.Index(i), path+fmt.Sprintf("[%d]", i))
		}
	case reflect.Bool:
		require.NotEqual(t, valA.Bool(), valB.Bool(), path+" should not be the same")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		require.NotEqual(t, valA.Int(), valB.Int(), path+" should not be the same")
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		require.NotEqual(t, valA.Uint(), valB.Uint(), path+" should not be the same")
	case reflect.Float32, reflect.Float64:
		require.NotEqual(t, valA.Float(), valB.Float(), path+" should not be the same")
	case reflect.String:
		require.NotEqual(t, valA.String(), valB.String(), path+" should not be the same")
	case reflect.Map:
		require.NotEqual(t, valA.Pointer(), valB.Pointer(), path+" should not be the same")
		if !h.shouldComparePointer(path) {
			for _, key := range valA.MapKeys() {
				if valB.MapIndex(key).IsValid() {
					h.assertRecursivelyNotEqual(t, valA.MapIndex(key), valB.MapIndex(key), path+fmt.Sprintf("[%v]", key))
				}
			}
		}
	case reflect.Interface:
		if valA.IsNil() || valB.IsNil() {
			require.False(t, valA.IsNil() && valB.IsNil(), path+" should not be nil at the same time")
			return
		}
		h.assertRecursivelyNotEqual(t, valA.Elem(), valB.Elem(), path)
	case reflect.Func:
		if h.shouldComparePointer(path) {
			require.NotEqual(t, valA.Pointer(), valB.Pointer(), path+" should be different")
		} else {
			require.Fail(t, "a function should be compared by pointer or ignored, because there's no way to compare the content of a function", path)
		}
	default:
		require.Fail(t, "unsupported type", path)
	}
}

func (h *staticTestHelper) assertDeepClonedEqual(t require.TestingT, valA, valB reflect.Value, path string) {
	if h.shouldIgnore(path) {
		return
	}
	if valA.IsValid() != valB.IsValid() {
		require.Fail(t, "one of them is invalid value", path)
	}
	require.Equal(t, valA.Type(), valB.Type(), path+" should have the same type")

	// This function assumes that `a` and `b` are the same type
	switch valA.Type().Kind() {
	case reflect.Struct:
		for i := 0; i < valA.NumField(); i++ {
			h.assertDeepClonedEqual(t, valA.Field(i), valB.Field(i), path+"."+valA.Type().Field(i).Name)
		}
	case reflect.Ptr:
		if valA.IsNil() && valB.IsNil() {
			return
		}
		// both of them are not nil
		require.NotEqual(t, 0, valA.Pointer(), path+" should not be nil")
		require.NotEqual(t, 0, valB.Pointer(), path+" should not be nil")

		if h.shouldComparePointer(path) {
			require.Equal(t, valA.Pointer(), valB.Pointer(), path+" should be the same")
		} else {
			require.NotEqual(t, valA.Pointer(), valB.Pointer(), path+" should be different")
			h.assertDeepClonedEqual(t, valA.Elem(), valB.Elem(), path)
		}
	case reflect.Slice:
		require.Equal(t, valA.Len(), valB.Len(), path+" should have the same length")

		if h.shouldComparePointer(path) {
			require.Equal(t, valA.Pointer(), valB.Pointer(), path+" should be the same")
		} else {
			require.NotEqual(t, valA.Pointer(), valB.Pointer(), path+" should not be the same")
			for i := 0; i < valA.Len(); i++ {
				h.assertDeepClonedEqual(t, valA.Index(i), valB.Index(i), path+fmt.Sprintf("[%d]", i))
			}
		}
	case reflect.Array:
		require.Equal(t, valA.Len(), valB.Len(), path+" should have the same length")
		for i := 0; i < valA.Len(); i++ {
			h.assertDeepClonedEqual(t, valA.Index(i), valB.Index(i), path+fmt.Sprintf("[%d]", i))
		}
	case reflect.Bool:
		require.Equal(t, valA.Bool(), valB.Bool(), path+" should be the same")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		require.Equal(t, valA.Int(), valB.Int(), path+" should be the same")
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		require.Equal(t, valA.Uint(), valB.Uint(), path+" should be the same")
	case reflect.Float32, reflect.Float64:
		require.Equal(t, valA.Float(), valB.Float(), path+" should be the same")
	case reflect.String:
		require.Equal(t, valA.String(), valB.String(), path+" should be the same")
	case reflect.Map:
		require.Equal(t, valA.Len(), valB.Len(), path+" should have the same length")

		if h.shouldComparePointer(path) {
			require.Equal(t, valA.Pointer(), valB.Pointer(), path+" should be the same")
		} else {
			require.NotEqual(t, valA.Pointer(), valB.Pointer(), path+" should not be the same")
			for _, key := range valA.MapKeys() {
				h.assertDeepClonedEqual(t, valA.MapIndex(key), valB.MapIndex(key), path+fmt.Sprintf("[%v]", key))
			}
		}
	case reflect.Interface:
		if valA.IsNil() && valB.IsNil() {
			return
		}
		h.assertDeepClonedEqual(t, valA.Elem(), valB.Elem(), path)
	case reflect.Func:
		if h.shouldComparePointer(path) {
			require.Equal(t, valA.Pointer(), valB.Pointer(), path+" should be the same")
		} else {
			require.Fail(t, "a function should be compared by pointer or ignored, because there's no way to compare the content of a function", path)
		}
	default:
		require.Fail(t, "unsupported type: "+valA.Type().String(), path)
	}
}

// Option is the option for the deep test.
type Option interface {
	apply(*staticTestHelper)
}

type ignorePathOption struct {
	ignorePath []string
}

func (o ignorePathOption) apply(h *staticTestHelper) {
	h.ignorePath = make([]glob.Glob, 0, len(o.ignorePath))
	for _, path := range o.ignorePath {
		h.ignorePath = append(h.ignorePath, glob.MustCompile(path))
	}
}

// WithIgnorePath specifies the paths that should be ignored during the deep test.
func WithIgnorePath(ignorePath []string) Option {
	return ignorePathOption{ignorePath}
}

type pointerComparePathOption struct {
	pointerComparePath []string
}

func (o pointerComparePathOption) apply(h *staticTestHelper) {
	h.pointerComparePath = make([]glob.Glob, 0, len(o.pointerComparePath))
	for _, path := range o.pointerComparePath {
		h.pointerComparePath = append(h.pointerComparePath, glob.MustCompile(path))
	}
}

// WithPointerComparePath specifies the paths that should be compared by pointer during the deep test.
func WithPointerComparePath(pointerComparePath []string) Option {
	return pointerComparePathOption{pointerComparePath}
}

// AssertRecursivelyNotEqual asserts that every field of `a` is not equal to the corresponding field of `b` deeply.
func AssertRecursivelyNotEqual[T any](t require.TestingT, valA, valB T, opts ...Option) {
	h := &staticTestHelper{}
	h.applyOptions(opts...)

	h.assertRecursivelyNotEqual(t, reflect.ValueOf(valA), reflect.ValueOf(valB), "$")
}

// AssertDeepClonedEqual tells whether a and b are deeply equal.
func AssertDeepClonedEqual[T any](t require.TestingT, valA, valB T, opts ...Option) {
	h := &staticTestHelper{}
	h.applyOptions(opts...)

	h.assertDeepClonedEqual(t, reflect.ValueOf(valA), reflect.ValueOf(valB), "$")
}

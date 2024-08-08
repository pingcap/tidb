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
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
)

type staticTestHelper struct {
	ignorePath []regexp.Regexp
}

// NewStaticTestHelper creates a new helper to help testing the deep equality of two objects.
func NewStaticTestHelper(ignorePath []string) *staticTestHelper {
	pathRegexp := make([]regexp.Regexp, 0, len(ignorePath))
	for _, path := range ignorePath {
		pathRegexp = append(pathRegexp, *regexp.MustCompile(path))
	}
	return &staticTestHelper{ignorePath: pathRegexp}
}

func (h *staticTestHelper) shouldIgnore(path string) bool {
	for _, reg := range h.ignorePath {
		if reg.MatchString(path) {
			return true
		}
	}
	return false
}

// AssertNotEmpty asserts that the value and its every fields are not empty.
func (h *staticTestHelper) AssertNotEmpty(t *testing.T, valObj reflect.Value, path string) {
	if h.shouldIgnore(path) {
		return
	}

	require.False(t, valObj.IsZero(), path+" should not be empty")

	switch valObj.Type().Kind() {
	case reflect.Struct:
		for i := 0; i < valObj.NumField(); i++ {
			h.AssertNotEmpty(t, valObj.Field(i), path+"."+valObj.Type().Field(i).Name)
		}
	case reflect.Ptr:
		h.AssertNotEmpty(t, valObj.Elem(), path)
	case reflect.Slice, reflect.Array:
		require.NotEqual(t, 0, valObj.Len(), path+" should not be empty")
		for i := 0; i < valObj.Len(); i++ {
			h.AssertNotEmpty(t, valObj.Index(i), path+fmt.Sprintf("[%d]", i))
		}
	case reflect.Bool:
		require.Equal(t, true, valObj.Bool(), path+" should be true")
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		require.NotEqual(t, 0, valObj.Int(), path+" should not be 0")
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		require.NotEqual(t, 0, valObj.Uint(), path+" should not be 0")
	case reflect.Float32, reflect.Float64:
		require.NotEqual(t, 0.0, valObj.Float(), path+" should not be 0")
	case reflect.String:
		require.NotEqual(t, "", valObj.String(), path+" should not be empty")
	case reflect.Map:
		require.NotEqual(t, 0, valObj.Len(), path+" should not be empty")
		for _, key := range valObj.MapKeys() {
			h.AssertNotEmpty(t, valObj.MapIndex(key), path+fmt.Sprintf("[%v]", key))
		}
	case reflect.Interface:
		h.AssertNotEmpty(t, valObj.Elem(), path)
	case reflect.Func:
		return
	default:
		require.Fail(t, "unsupported type", path)
	}
}

// Equal tells whether a and b are deeply equal.
func (h *staticTestHelper) Equal(t *testing.T, valA, valB reflect.Value, path string) {
	if h.shouldIgnore(path) {
		return
	}

	// This function assumes that `a` and `b` are the same type
	switch valA.Type().Kind() {
	case reflect.Struct:
		for i := 0; i < valA.NumField(); i++ {
			h.Equal(t, valA.Field(i), valB.Field(i), path+"."+valA.Type().Field(i).Name)
		}
	case reflect.Ptr:
		// both of them are not nil
		require.NotEqual(t, 0, valA.Pointer(), path+" should not be nil")
		require.NotEqual(t, 0, valB.Pointer(), path+" should not be nil")
		// pointer type should be different
		require.NotSame(t, valA.Pointer(), valB.Pointer(), path+" should be different")

		h.Equal(t, valA.Elem(), valB.Elem(), path)
	case reflect.Slice, reflect.Array:
		require.Equal(t, valA.Len(), valB.Len(), path+" should have the same length")
		for i := 0; i < valA.Len(); i++ {
			h.Equal(t, valA.Index(i), valB.Index(i), path+fmt.Sprintf("[%d]", i))
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
		for _, key := range valA.MapKeys() {
			h.Equal(t, valA.MapIndex(key), valB.MapIndex(key), path+fmt.Sprintf("[%v]", key))
		}
	case reflect.Interface:
		h.Equal(t, valA.Elem(), valB.Elem(), path)
	case reflect.Func:
		require.NotEqual(t, 0, valA.Pointer(), path+" should not be nil")
		require.NotEqual(t, 0, valB.Pointer(), path+" should not be nil")
		require.NotSame(t, valA.Pointer(), valB.Pointer(), path+" should be different")
		return
	default:
		require.Fail(t, "unsupported type", path)
	}
}

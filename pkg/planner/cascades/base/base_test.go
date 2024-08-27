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

package base

import (
	"testing"
)

type testcase struct {
	a int
	b int
	c string
}

type TestEquals[T any] interface {
	EqualsT(other T) bool
}

type TestEqualAny interface {
	EqualsAny(other any) bool
}

type TestSuperEquals interface {
	TestEquals[TestSuperEquals]
}

func (tc *testcase) EqualsT(other *testcase) bool {
	return tc.a == other.a && tc.b == other.b && tc.c == other.c
}

func (tc *testcase) EqualsAny(other any) bool {
	tc1, ok := other.(*testcase)
	if !ok {
		return false
	}
	return tc.a == tc1.a && tc.b == tc1.b && tc.c == tc1.c
}

// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tidb/pkg/planner/cascades/memo
// cpu: 12th Gen Intel(R) Core(TM) i7-12700KF
// BenchmarkEqualsT
// BenchmarkEqualsT-20    	1000000000	         0.8981 ns/op
func BenchmarkEqualsT(b *testing.B) {
	tc1 := &testcase{a: 1, b: 2, c: "3"}
	var tc2 TestEquals[*testcase] = &testcase{a: 1, b: 2, c: "3"}
	for i := 0; i < b.N; i++ {
		if tc3, ok := tc2.(*testcase); ok {
			tc1.EqualsT(tc3)
		}
	}
}

// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tidb/pkg/planner/cascades/memo
// cpu: 12th Gen Intel(R) Core(TM) i7-12700KF
// BenchmarkEqualsAny
// BenchmarkEqualsAny-20    	1000000000	         0.4837 ns/op
func BenchmarkEqualsAny(b *testing.B) {
	tc1 := &testcase{a: 1, b: 2, c: "3"}
	var tc2 TestEqualAny = &testcase{a: 1, b: 2, c: "3"}
	for i := 0; i < b.N; i++ {
		tc1.EqualsAny(tc2)
	}
}

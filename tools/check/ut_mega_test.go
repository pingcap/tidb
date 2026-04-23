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

package main

import "testing"

func TestDefaultMegaParallelism(t *testing.T) {
	cases := []struct {
		name     string
		maxProcs int
		expected int
	}{
		{name: "zero", maxProcs: 0, expected: 1},
		{name: "one", maxProcs: 1, expected: 1},
		{name: "two", maxProcs: 2, expected: 1},
		{name: "five", maxProcs: 5, expected: 4},
		{name: "ten", maxProcs: 10, expected: 9},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := defaultMegaParallelism(tc.maxProcs)
			if got != tc.expected {
				t.Fatalf("defaultMegaParallelism(%d) = %d, want %d", tc.maxProcs, got, tc.expected)
			}
		})
	}
}

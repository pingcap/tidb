// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestDefaultTestParallelism covers the downward-API budget available in CI.
// Supply a host-sized runtime default, check request/fallback cases, then
// verify that an explicit caller setting wins over the container request.
func TestDefaultTestParallelism(t *testing.T) {
	for _, tc := range []struct {
		name       string
		request    string
		missing    bool
		gomaxprocs string
		want       int
	}{
		{name: "shared host", request: "16\n", want: 16},
		{name: "request above runtime", request: "256", want: 128},
		{name: "equal runtime", request: "128", want: 128},
		{name: "native run", missing: true, want: 128},
		{name: "empty request", request: "", want: 128},
		{name: "invalid request", request: "unknown", want: 128},
		{name: "zero request", request: "0", want: 128},
		{name: "negative request", request: "-1", want: 128},
		{name: "explicit caller", request: "16", gomaxprocs: "128", want: 128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GOMAXPROCS", tc.gomaxprocs)
			path := filepath.Join(t.TempDir(), "cpu_request")
			if !tc.missing {
				if err := os.WriteFile(path, []byte(tc.request), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if got := defaultTestParallelism(128, path); got != tc.want {
				t.Fatalf("parallelism = %d, want %d", got, tc.want)
			}
		})
	}
}

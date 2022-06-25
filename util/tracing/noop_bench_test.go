// Copyright 2018 PingCAP, Inc.
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

package tracing

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkNoopLogKV benchs the cost of noop's `LogKV`.
func BenchmarkNoopLogKV(b *testing.B) {
	sp := noopSpan()
	for i := 0; i < b.N; i++ {
		sp.LogKV("event", "noop is finished")
	}
}

// BenchmarkNoopLogKVWithF benchs the the cosst of noop's `LogKV` when
// used with `fmt.Sprintf`
func BenchmarkNoopLogKVWithF(b *testing.B) {
	sp := noopSpan()
	for i := 0; i < b.N; i++ {
		sp.LogKV("event", fmt.Sprintf("this is format %s", "noop is finished"))
	}
}

// BenchmarkSpanFromContext benchs the cost of `SpanFromContext`.
func BenchmarkSpanFromContext(b *testing.B) {
	ctx := context.TODO()
	for i := 0; i < b.N; i++ {
		SpanFromContext(ctx)
	}
}

// BenchmarkChildFromContext benchs the cost of `ChildSpanFromContxt`.
func BenchmarkChildFromContext(b *testing.B) {
	ctx := context.TODO()
	for i := 0; i < b.N; i++ {
		ChildSpanFromContxt(ctx, "child")
	}
}

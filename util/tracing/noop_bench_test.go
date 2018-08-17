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
// See the License for the specific language governing permissions and
// limitations under the License.


package tracing

import (
	"testing"
	"context"
)


func BenchmarkNoopLogKV(b *testing.B) {
	sp := noopSpan()
	for i := 0; i <  b.N; i++ {
		sp.LogKV("event", "noop is finished")
	}
}

func BenchmarkNoopLogKVWithF(b *testing.B) {
	sp := noopSpan()
	for i := 0; i <  b.N; i++ {
		sp.LogKV("event", "this is format %s", "noop is finished")
	}
}


func BenchmarkSpanFromContext(b *testing.B) {
	ctx := context.TODO()
	for i := 0 ; i < b.N; i++ {
		SpanFromContext(ctx)
	}
}

func BenchmarkChildFromContext(b *testing.B) {
	ctx := context.TODO()
	for i := 0 ; i < b.N; i++ {
		ChildSpanFromContxt(ctx, "child")
	}
}

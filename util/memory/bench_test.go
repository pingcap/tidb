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

package memory

import (
	"testing"
)

func BenchmarkMemTotal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = MemTotal()
	}
}

func BenchmarkMemUsed(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = MemUsed()
	}
}

func BenchmarkConsume(b *testing.B) {
	tracker := NewTracker(1, -1)
	b.RunParallel(func(pb *testing.PB) {
		childTracker := NewTracker(2, -1)
		childTracker.AttachTo(tracker)
		for pb.Next() {
			childTracker.Consume(256 << 20)
		}
	})
}

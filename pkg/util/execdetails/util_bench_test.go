// Copyright 2025 PingCAP, Inc.
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

package execdetails

import (
	"testing"
	"time"
)

func BenchmarkPercentileAdd(b *testing.B) {
	b.ReportAllocs()
	var p Percentile[Duration]
	for i := 0; i < b.N; i++ {
		p.Add(Duration(time.Duration(i) * time.Microsecond))
	}
}

func BenchmarkPercentileGetPercentile(b *testing.B) {
	b.ReportAllocs()
	var p Percentile[Duration]
	// Pre-populate with 2000 samples (forces TDigest path)
	for i := 0; i < 2000; i++ {
		p.Add(Duration(time.Duration(i) * time.Microsecond))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.GetPercentile(0.95)
	}
}

func BenchmarkPercentileAddContended(b *testing.B) {
	b.ReportAllocs()
	var p Percentile[Duration]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			p.Add(Duration(time.Duration(i) * time.Microsecond))
			i++
		}
	})
}

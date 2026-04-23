// Copyright 2020-present PingCAP, Inc.
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

package fastrand_test

import (
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/util/fastrand"
	"github.com/stretchr/testify/require"
)

func RunRand(t *testing.T) {
	x := fastrand.ExportUint32N(1024)
	require.Less(t, x, uint32(1024))
	y := fastrand.ExportUint64N(1 << 63)
	require.Less(t, y, uint64(1<<63))

	_ = fastrand.ExportBuf(20)
	var arr [256]bool
	for range 1024 {
		idx := fastrand.ExportUint32N(256)
		arr[idx] = true
	}
	sum := 0
	for i := range 256 {
		if !arr[i] {
			sum++
		}
	}
	require.Less(t, sum, 24)
}

func BenchmarkFastRandBuf(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fastrand.ExportBuf(20)
		}
	})
}

func BenchmarkFastRandUint32N(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fastrand.ExportUint32N(127)
		}
	})
}

func BenchmarkFastRand(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fastrand.ExportUint32()
		}
	})
	b.Log(fastrand.ExportUint32())
}

func BenchmarkGlobalRand(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rand.Int() //nolint:gosec // benchmarking weak RNG is acceptable
		}
	})
	b.Log(rand.Int()) //nolint:gosec // benchmarking weak RNG is acceptable
}

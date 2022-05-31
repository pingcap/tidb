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

package fastrand

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRand(t *testing.T) {
	x := Uint32N(1024)
	require.Less(t, x, uint32(1024))
	y := Uint64N(1 << 63)
	require.Less(t, y, uint64(1<<63))

	_ = Buf(20)
	var arr [256]bool
	for i := 0; i < 1024; i++ {
		idx := Uint32N(256)
		arr[idx] = true
	}
	sum := 0
	for i := 0; i < 256; i++ {
		if arr[i] == false {
			sum++
		}
	}
	require.Less(t, sum, 24)
}

func BenchmarkFastRandBuf(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Buf(20)
		}
	})
}

func BenchmarkFastRandUint32N(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Uint32N(127)
		}
	})
}

func BenchmarkFastRand(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Uint32()
		}
	})
	b.Log(Uint32())
}

func BenchmarkGlobalRand(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rand.Int()
		}
	})
	b.Log(rand.Int())
}

// Copyright 2019 PingCAP, Inc.
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

package types

import (
	"math/rand"
	"testing"

	"github.com/pingcap/parser/mysql"
)

func BenchmarkDefaultTypeForValue(b *testing.B) {
	lenNums := 1000000
	numsFull := make([]uint64, lenNums)
	nums64k := make([]uint64, lenNums)
	nums512 := make([]uint64, lenNums)

	for i := range numsFull {
		r := rand.Uint64()
		numsFull[i] = r
		nums64k[i] = r % 64000
		nums512[i] = r % 512
	}

	b.Run("LenOfUint64_input full range", func(b *testing.B) {
		b.StartTimer()
		var ft FieldType
		for i := 0; i < b.N; i++ {
			DefaultTypeForValue(numsFull[i%lenNums], &ft, mysql.DefaultCharset, mysql.DefaultCollationName)
		}
	})

	b.Run("LenOfUint64_input 0 to 64K  ", func(b *testing.B) {
		b.StartTimer()
		var ft FieldType
		for i := 0; i < b.N; i++ {
			DefaultTypeForValue(nums64k[i%lenNums], &ft, mysql.DefaultCharset, mysql.DefaultCollationName)
		}
	})

	b.Run("LenOfUint64_input 0 to 512  ", func(b *testing.B) {
		b.StartTimer()
		var ft FieldType
		for i := 0; i < b.N; i++ {
			DefaultTypeForValue(nums512[i%lenNums], &ft, mysql.DefaultCharset, mysql.DefaultCollationName)
		}
	})
}

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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mathutil

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStrLenOfUint64Fast(t *testing.T) {
	t.Run("RandomInput", func(t *testing.T) {
		for i := 0; i < 1000000; i++ {
			num := rand.Uint64()
			expected := len(strconv.FormatUint(num, 10))
			actual := StrLenOfUint64Fast(num)
			require.Equal(t, expected, actual)
		}
	})

	t.Run("ManualInput", func(t *testing.T) {
		nums := [22]uint64{0,
			1, 12, 123, 1234, 12345,
			123456, 1234567, 12345678, 123456789, 1234567890,
			1234567891, 12345678912, 123456789123, 1234567891234, 12345678912345,
			123456789123456, 1234567891234567, 12345678912345678, 123456789123456789,
			123456789123457890,
			^uint64(0),
		}
		for _, num := range nums {
			expected := len(strconv.FormatUint(num, 10))
			actual := StrLenOfUint64Fast(num)
			require.Equal(t, expected, actual)
		}
	})
}

func TestClamp(t *testing.T) {
	require.Equal(t, 3, Clamp(100, 1, 3))
	require.Equal(t, 2.0, Clamp(float64(2), 1.0, 3.0))
	require.Equal(t, float32(1.0), Clamp(float32(0), 1.0, 3.0))
	require.Equal(t, 1, Clamp(0, 1, 1))
	require.Equal(t, 1, Clamp(100, 1, 1))
	require.Equal(t, "ab", Clamp("aa", "ab", "xy"))
	require.Equal(t, "xy", Clamp("yy", "ab", "xy"))
	require.Equal(t, "ab", Clamp("ab", "ab", "ab"))
}

func TestNextPowerOfTwo(t *testing.T) {
	require.Equal(t, int64(1), NextPowerOfTwo(1))
	require.Equal(t, int64(4), NextPowerOfTwo(3))
	require.Equal(t, int64(256), NextPowerOfTwo(255))
	require.Equal(t, int64(1024), NextPowerOfTwo(1024))
	require.Equal(t, int64(0x100000000), NextPowerOfTwo(0xabcd1234))
}

func TestDivide2Batches(t *testing.T) {
	require.EqualValues(t, []int{}, Divide2Batches(0, 1))
	require.EqualValues(t, []int{1}, Divide2Batches(1, 1))
	require.EqualValues(t, []int{1}, Divide2Batches(1, 3))
	require.EqualValues(t, []int{1, 1}, Divide2Batches(2, 2))
	require.EqualValues(t, []int{1, 1}, Divide2Batches(2, 10))
	require.EqualValues(t, []int{10}, Divide2Batches(10, 1))
	require.EqualValues(t, []int{5, 5}, Divide2Batches(10, 2))
	require.EqualValues(t, []int{4, 3, 3}, Divide2Batches(10, 3))
	require.EqualValues(t, []int{3, 3, 2, 2}, Divide2Batches(10, 4))
	require.EqualValues(t, []int{2, 2, 2, 2, 2}, Divide2Batches(10, 5))
}

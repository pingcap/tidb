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

package mathutil_test

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/stretchr/testify/require"
)

func RunStrLenOfUint64Fast(t *testing.T) {
	t.Run("RandomInput", func(t *testing.T) {
		for range 1000000 {
			num := rand.Uint64()
			expected := len(strconv.FormatUint(num, 10))
			actual := mathutil.StrLenOfUint64Fast(num)
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
			actual := mathutil.StrLenOfUint64Fast(num)
			require.Equal(t, expected, actual)
		}
	})
}

func RunClamp(t *testing.T) {
	require.Equal(t, 3, mathutil.Clamp(100, 1, 3))
	require.Equal(t, 2.0, mathutil.Clamp(float64(2), 1.0, 3.0))
	require.Equal(t, float32(1.0), mathutil.Clamp(float32(0), 1.0, 3.0))
	require.Equal(t, 1, mathutil.Clamp(0, 1, 1))
	require.Equal(t, 1, mathutil.Clamp(100, 1, 1))
	require.Equal(t, "ab", mathutil.Clamp("aa", "ab", "xy"))
	require.Equal(t, "xy", mathutil.Clamp("yy", "ab", "xy"))
	require.Equal(t, "ab", mathutil.Clamp("ab", "ab", "ab"))
}

func RunNextPowerOfTwo(t *testing.T) {
	require.Equal(t, int64(1), mathutil.NextPowerOfTwo(1))
	require.Equal(t, int64(4), mathutil.NextPowerOfTwo(3))
	require.Equal(t, int64(256), mathutil.NextPowerOfTwo(255))
	require.Equal(t, int64(1024), mathutil.NextPowerOfTwo(1024))
	require.Equal(t, int64(0x100000000), mathutil.NextPowerOfTwo(0xabcd1234))
}

func RunDivide2Batches(t *testing.T) {
	require.EqualValues(t, []int{}, mathutil.Divide2Batches(0, 1))
	require.EqualValues(t, []int{1}, mathutil.Divide2Batches(1, 1))
	require.EqualValues(t, []int{1}, mathutil.Divide2Batches(1, 3))
	require.EqualValues(t, []int{1, 1}, mathutil.Divide2Batches(2, 2))
	require.EqualValues(t, []int{1, 1}, mathutil.Divide2Batches(2, 10))
	require.EqualValues(t, []int{10}, mathutil.Divide2Batches(10, 1))
	require.EqualValues(t, []int{5, 5}, mathutil.Divide2Batches(10, 2))
	require.EqualValues(t, []int{4, 3, 3}, mathutil.Divide2Batches(10, 3))
	require.EqualValues(t, []int{3, 3, 2, 2}, mathutil.Divide2Batches(10, 4))
	require.EqualValues(t, []int{2, 2, 2, 2, 2}, mathutil.Divide2Batches(10, 5))
}

// Copyright 2015 PingCAP, Inc.
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

package types

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	tblUint64 := []struct {
		lsh      uint64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, 1, 0, true},
		{math.MaxUint64, 0, math.MaxUint64, false},
		{1, 1, 2, false},
	}

	for _, tt := range tblUint64 {
		ret, err := AddUint64(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}

	tblInt64 := []struct {
		lsh      int64
		rsh      int64
		ret      int64
		overflow bool
	}{
		{math.MaxInt64, 1, 0, true},
		{math.MaxInt64, 0, math.MaxInt64, false},
		{0, math.MinInt64, math.MinInt64, false},
		{-1, math.MinInt64, 0, true},
		{math.MaxInt64, math.MinInt64, -1, false},
		{1, 1, 2, false},
		{1, -1, 0, false},
	}

	for _, tt := range tblInt64 {
		ret, err := AddInt64(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
		ret2, err := AddDuration(time.Duration(tt.lsh), time.Duration(tt.rsh))
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, time.Duration(tt.ret), ret2)
		}
	}

	tblInt := []struct {
		lsh      uint64
		rsh      int64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, math.MinInt64, math.MaxUint64 + math.MinInt64, false},
		{math.MaxInt64, math.MinInt64, 0, true},
		{0, -1, 0, true},
		{1, -1, 0, false},
		{0, 1, 1, false},
		{1, 1, 2, false},
	}

	for _, tt := range tblInt {
		ret, err := AddInteger(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}
}

func TestSub(t *testing.T) {
	tblUint64 := []struct {
		lsh      uint64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, 1, math.MaxUint64 - 1, false},
		{math.MaxUint64, 0, math.MaxUint64, false},
		{0, math.MaxUint64, 0, true},
		{0, 1, 0, true},
		{1, math.MaxUint64, 0, true},
		{1, 1, 0, false},
	}

	for _, tt := range tblUint64 {
		ret, err := SubUint64(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}

	tblInt64 := []struct {
		lsh      int64
		rsh      int64
		ret      int64
		overflow bool
	}{
		{math.MinInt64, 0, math.MinInt64, false},
		{math.MinInt64, 1, 0, true},
		{math.MaxInt64, -1, 0, true},
		{0, math.MinInt64, 0, true},
		{-1, math.MinInt64, math.MaxInt64, false},
		{math.MinInt64, math.MaxInt64, 0, true},
		{math.MinInt64, math.MinInt64, 0, false},
		{math.MinInt64, -math.MaxInt64, -1, false},
		{1, 1, 0, false},
	}

	for _, tt := range tblInt64 {
		ret, err := SubInt64(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}

	tblInt := []struct {
		lsh      uint64
		rsh      int64
		ret      uint64
		overflow bool
	}{
		{0, math.MinInt64, -math.MinInt64, false},
		{0, 1, 0, true},
		{math.MaxUint64, math.MinInt64, 0, true},
		{math.MaxInt64, math.MinInt64, 2*math.MaxInt64 + 1, false},
		{math.MaxUint64, -1, 0, true},
		{0, -1, 1, false},
		{1, 1, 0, false},
	}

	for _, tt := range tblInt {
		ret, err := SubUintWithInt(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}

	tblInt2 := []struct {
		lsh      int64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MinInt64, 0, 0, true},
		{math.MaxInt64, 0, math.MaxInt64, false},
		{math.MaxInt64, math.MaxUint64, 0, true},
		{math.MaxInt64, -math.MinInt64, 0, true},
		{-1, 0, 0, true},
		{1, 1, 0, false},
	}

	for _, tt := range tblInt2 {
		ret, err := SubIntWithUint(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}
}

func TestMul(t *testing.T) {
	tblUint64 := []struct {
		lsh      uint64
		rsh      uint64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, 1, math.MaxUint64, false},
		{math.MaxUint64, 0, 0, false},
		{math.MaxUint64, 2, 0, true},
		{1, 1, 1, false},
	}

	for _, tt := range tblUint64 {
		ret, err := MulUint64(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}

	tblInt64 := []struct {
		lsh      int64
		rsh      int64
		ret      int64
		overflow bool
	}{
		{math.MaxInt64, 1, math.MaxInt64, false},
		{math.MinInt64, 1, math.MinInt64, false},
		{math.MaxInt64, -1, -math.MaxInt64, false},
		{math.MinInt64, -1, 0, true},
		{math.MinInt64, 0, 0, false},
		{math.MaxInt64, 0, 0, false},
		{math.MaxInt64, math.MaxInt64, 0, true},
		{math.MaxInt64, math.MinInt64, 0, true},
		{math.MinInt64 / 10, 11, 0, true},
		{1, 1, 1, false},
	}

	for _, tt := range tblInt64 {
		ret, err := MulInt64(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}

	tblInt := []struct {
		lsh      uint64
		rsh      int64
		ret      uint64
		overflow bool
	}{
		{math.MaxUint64, 0, 0, false},
		{0, -1, 0, false},
		{1, -1, 0, true},
		{math.MaxUint64, -1, 0, true},
		{math.MaxUint64, 10, 0, true},
		{1, 1, 1, false},
	}

	for _, tt := range tblInt {
		ret, err := MulInteger(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}
}

func TestDiv(t *testing.T) {
	tblInt64 := []struct {
		lsh      int64
		rsh      int64
		ret      int64
		overflow bool
	}{
		{math.MaxInt64, 1, math.MaxInt64, false},
		{math.MinInt64, 1, math.MinInt64, false},
		{math.MinInt64, -1, 0, true},
		{math.MaxInt64, -1, -math.MaxInt64, false},
		{1, -1, -1, false},
		{-1, 1, -1, false},
		{-1, 2, 0, false},
		{math.MinInt64, 2, math.MinInt64 / 2, false},
	}

	for _, tt := range tblInt64 {
		ret, err := DivInt64(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}

	tblInt := []struct {
		lsh      uint64
		rsh      int64
		ret      uint64
		overflow bool
	}{
		{0, -1, 0, false},
		{1, -1, 0, true},
		{math.MaxInt64, math.MinInt64, 0, false},
		{math.MaxInt64, -1, 0, true},
		{100, 20, 5, false},
	}

	for _, tt := range tblInt {
		ret, err := DivUintWithInt(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}

	tblInt2 := []struct {
		lsh      int64
		rsh      uint64
		ret      uint64
		overflow bool
		err      string
	}{
		{math.MinInt64, math.MaxInt64, 0, true, "^*BIGINT UNSIGNED value is out of range in '\\(-9223372036854775808, 9223372036854775807\\)'$"},
		{0, 1, 0, false, ""},
		{-1, math.MaxInt64, 0, false, ""},
	}

	for _, tt := range tblInt2 {
		ret, err := DivIntWithUint(tt.lsh, tt.rsh)
		if tt.overflow {
			require.Error(t, err)
			require.Regexp(t, tt.err, err.Error())
		} else {
			require.Equal(t, tt.ret, ret)
		}
	}
}

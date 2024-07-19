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

package dump

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestDumpBinaryTime(t *testing.T) {
	typeCtx := types.DefaultStmtNoWarningContext
	parsedTime, err := types.ParseTimestamp(typeCtx, "0000-00-00 00:00:00.000000")
	require.NoError(t, err)
	d := BinaryDateTime(nil, parsedTime)
	require.Equal(t, []byte{0}, d)

	parsedTime, err = types.ParseTimestamp(typeCtx.WithLocation(time.UTC), "1991-05-01 01:01:01.100001")
	require.NoError(t, err)
	d = BinaryDateTime(nil, parsedTime)
	// 199 & 7 composed to uint16 1991 (litter-endian)
	// 160 & 134 & 1 & 0 composed to uint32 1000001 (litter-endian)
	require.Equal(t, []byte{11, 199, 7, 5, 1, 1, 1, 1, 161, 134, 1, 0}, d)

	parsedTime, err = types.ParseDatetime(typeCtx, "0000-00-00 00:00:00.000000")
	require.NoError(t, err)
	d = BinaryDateTime(nil, parsedTime)
	require.Equal(t, []byte{0}, d)

	parsedTime, err = types.ParseDatetime(typeCtx, "1993-07-13 01:01:01.000000")
	require.NoError(t, err)
	d = BinaryDateTime(nil, parsedTime)
	// 201 & 7 composed to uint16 1993 (litter-endian)
	require.Equal(t, []byte{7, 201, 7, 7, 13, 1, 1, 1}, d)

	parsedTime, err = types.ParseDate(typeCtx, "0000-00-00")
	require.NoError(t, err)
	d = BinaryDateTime(nil, parsedTime)
	require.Equal(t, []byte{0}, d)
	parsedTime, err = types.ParseDate(typeCtx, "1992-06-01")
	require.NoError(t, err)
	d = BinaryDateTime(nil, parsedTime)
	// 200 & 7 composed to uint16 1992 (litter-endian)
	require.Equal(t, []byte{4, 200, 7, 6, 1}, d)

	parsedTime, err = types.ParseDate(typeCtx, "0000-00-00")
	require.NoError(t, err)
	d = BinaryDateTime(nil, parsedTime)
	require.Equal(t, []byte{0}, d)

	myDuration, _, err := types.ParseDuration(typeCtx, "0000-00-00 00:00:00.000000", 6)
	require.NoError(t, err)
	d = BinaryTime(myDuration.Duration)
	require.Equal(t, []byte{0}, d)

	d = BinaryTime(0)
	require.Equal(t, []byte{0}, d)

	d = BinaryTime(-1)
	require.Equal(t, []byte{12, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, d)

	d = BinaryTime(time.Nanosecond + 86400*1000*time.Microsecond)
	require.Equal(t, []byte{12, 0, 0, 0, 0, 0, 0, 1, 26, 128, 26, 6, 0}, d)
}

func TestDumpLengthEncodedInt(t *testing.T) {
	testCases := []struct {
		num    uint64
		buffer []byte
	}{
		{
			uint64(0),
			[]byte{0x00},
		},
		{
			uint64(513),
			[]byte{'\xfc', '\x01', '\x02'},
		},
		{
			uint64(197121),
			[]byte{'\xfd', '\x01', '\x02', '\x03'},
		},
		{
			uint64(578437695752307201),
			[]byte{'\xfe', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', '\x08'},
		},
	}
	for _, tc := range testCases {
		b := LengthEncodedInt(nil, tc.num)
		require.Equal(t, tc.buffer, b)
	}
}

func TestDumpUint(t *testing.T) {
	testCases := []uint64{
		0,
		1,
		1<<64 - 1,
	}
	parseUint64 := func(b []byte) uint64 {
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 |
			uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40 |
			uint64(b[6])<<48 | uint64(b[7])<<56
	}
	for _, tc := range testCases {
		b := Uint64(nil, tc)
		require.Len(t, b, 8)
		require.Equal(t, tc, parseUint64(b))
	}
}

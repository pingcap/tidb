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

package kv_test

import (
	"bytes"
	"errors"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	. "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartialNext(t *testing.T) {
	sc := stmtctx.NewStmtCtxWithTimeZone(time.Local)
	// keyA represents a multi column index.
	keyA, err := codec.EncodeValue(sc.TimeZone(), nil, types.NewDatum("abc"), types.NewDatum("def"))
	require.NoError(t, err)
	keyB, err := codec.EncodeValue(sc.TimeZone(), nil, types.NewDatum("abca"), types.NewDatum("def"))
	require.NoError(t, err)

	// We only use first column value to seek.
	seekKey, err := codec.EncodeValue(sc.TimeZone(), nil, types.NewDatum("abc"))
	require.NoError(t, err)

	nextKey := Key(seekKey).Next()
	cmp := bytes.Compare(nextKey, keyA)
	assert.Equal(t, -1, cmp)

	// Use next partial key, we can skip all index keys with first column value equal to "abc".
	nextPartialKey := Key(seekKey).PrefixNext()
	cmp = bytes.Compare(nextPartialKey, keyA)
	assert.Equal(t, 1, cmp)

	cmp = bytes.Compare(nextPartialKey, keyB)
	assert.Equal(t, -1, cmp)
}

func TestIsPoint(t *testing.T) {
	tests := []struct {
		start   []byte
		end     []byte
		isPoint bool
	}{
		{
			start:   Key("rowkey1"),
			end:     Key("rowkey2"),
			isPoint: true,
		},
		{
			start:   Key("rowkey1"),
			end:     Key("rowkey3"),
			isPoint: false,
		},
		{
			start:   Key(""),
			end:     []byte{0},
			isPoint: true,
		},
		{
			start:   []byte{123, 123, 255, 255},
			end:     []byte{123, 124, 0, 0},
			isPoint: true,
		},
		{
			start:   []byte{123, 123, 255, 255},
			end:     []byte{123, 124, 0, 1},
			isPoint: false,
		},
		{
			start:   []byte{123, 123},
			end:     []byte{123, 123, 0},
			isPoint: true,
		},
		{
			start:   []byte{255},
			end:     []byte{0},
			isPoint: false,
		},
	}

	for _, tt := range tests {
		kr := KeyRange{
			StartKey: tt.start,
			EndKey:   tt.end,
		}
		assert.Equal(t, tt.isPoint, kr.IsPoint())
	}
}

func TestBasicFunc(t *testing.T) {
	assert.False(t, IsTxnRetryableError(nil))
	assert.True(t, IsTxnRetryableError(ErrTxnRetryable))
	assert.False(t, IsTxnRetryableError(errors.New("test")))
}

func TestHandle(t *testing.T) {
	ih := IntHandle(100)
	assert.True(t, ih.IsInt())

	_, iv, _ := codec.DecodeInt(ih.Encoded())
	assert.Equal(t, ih.IntValue(), iv)

	ih2 := ih.Next()
	assert.Equal(t, int64(101), ih2.IntValue())
	assert.False(t, ih.Equal(ih2))
	assert.Equal(t, -1, ih.Compare(ih2))
	assert.Equal(t, "100", ih.String())

	ch := testutil.MustNewCommonHandle(t, 100, "abc")
	assert.False(t, ch.IsInt())

	ch2 := ch.Next()
	assert.False(t, ch.Equal(ch2))
	assert.Equal(t, -1, ch.Compare(ch2))
	assert.Len(t, ch2.Encoded(), len(ch.Encoded()))
	assert.Equal(t, 2, ch.NumCols())

	_, d, err := codec.DecodeOne(ch.EncodedCol(0))
	assert.Nil(t, err)
	assert.Equal(t, int64(100), d.GetInt64())

	_, d, err = codec.DecodeOne(ch.EncodedCol(1))
	assert.Nil(t, err)
	assert.Equal(t, "abc", d.GetString())
	assert.Equal(t, "{100, abc}", ch.String())
}

func TestPaddingHandle(t *testing.T) {
	dec := types.NewDecFromInt(1)
	encoded, err := codec.EncodeKey(stmtctx.NewStmtCtx().TimeZone(), nil, types.NewDecimalDatum(dec))
	assert.Nil(t, err)
	assert.Less(t, len(encoded), 9)

	handle, err := NewCommonHandle(encoded)
	assert.Nil(t, err)
	assert.Len(t, handle.Encoded(), 9)
	assert.Equal(t, encoded, handle.EncodedCol(0))

	newHandle, err := NewCommonHandle(handle.Encoded())
	assert.Nil(t, err)
	assert.Equal(t, handle.EncodedCol(0), newHandle.EncodedCol(0))
}

func TestHandleMap(t *testing.T) {
	m := NewHandleMap()
	h := IntHandle(1)

	assert.Equal(t, SizeofHandleMap, m.MemUsage())

	m.Set(h, 1)
	v, ok := m.Get(h)
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	assert.Equal(t, SizeofHandleMap+size.SizeOfInt64+size.SizeOfInterface, m.MemUsage())

	m.Delete(h)
	v, ok = m.Get(h)
	assert.False(t, ok)
	assert.Nil(t, v)

	assert.Equal(t, SizeofHandleMap, m.MemUsage())

	ch := testutil.MustNewCommonHandle(t, 100, "abc")
	m.Set(ch, "a")
	v, ok = m.Get(ch)
	assert.True(t, ok)
	assert.Equal(t, "a", v)

	{
		key := string(ch.Encoded())
		sz := size.SizeOfString + int64(len(key)) + SizeofStrHandleVal
		assert.Equal(t, SizeofHandleMap+sz, m.MemUsage())
	}

	m.Delete(ch)
	v, ok = m.Get(ch)
	assert.False(t, ok)
	assert.Nil(t, v)

	m.Set(ch, "a")
	ch2 := testutil.MustNewCommonHandle(t, 101, "abc")
	m.Set(ch2, "b")
	ch3 := testutil.MustNewCommonHandle(t, 99, "def")
	m.Set(ch3, "c")
	assert.Equal(t, 3, m.Len())

	cnt := 0
	m.Range(func(h Handle, val any) bool {
		cnt++
		if h.Equal(ch) {
			assert.Equal(t, "a", val)
		} else if h.Equal(ch2) {
			assert.Equal(t, "b", val)
		} else {
			assert.Equal(t, "c", val)
		}
		if cnt == 2 {
			return false
		}
		return true
	})

	assert.Equal(t, 2, cnt)
}

func TestHandleMapWithPartialHandle(t *testing.T) {
	m := NewHandleMap()
	ph1 := NewPartitionHandle(1, IntHandle(1))
	m.Set(ph1, 1)

	ph2 := NewPartitionHandle(2, IntHandle(1))
	m.Set(ph2, 2)

	ph3 := NewPartitionHandle(1, IntHandle(3))
	m.Set(ph3, 5)

	ih := IntHandle(1)
	m.Set(ih, 3)

	dec := types.NewDecFromInt(1)
	encoded, err := codec.EncodeKey(stmtctx.NewStmtCtx().TimeZone(), nil, types.NewDecimalDatum(dec))
	assert.Nil(t, err)
	assert.Less(t, len(encoded), 9)

	ch, err := NewCommonHandle(encoded)
	assert.NoError(t, err)
	m.Set(ch, 4)

	v, ok := m.Get(ph1)
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	v, ok = m.Get(ph2)
	assert.True(t, ok)
	assert.Equal(t, 2, v)

	v, ok = m.Get(ph3)
	assert.True(t, ok)
	assert.Equal(t, 5, v)

	v, ok = m.Get(ih)
	assert.True(t, ok)
	assert.Equal(t, 3, v)

	v, ok = m.Get(ch)
	assert.True(t, ok)
	assert.Equal(t, 4, v)

	assert.Equal(t, 5, m.Len())

	m.Delete(ph1)
	v, ok = m.Get(ph1)
	assert.False(t, ok)
	assert.Nil(t, v)
	assert.Equal(t, 4, m.Len())

	// not panic for delete non exsits handle
	m.Delete(NewPartitionHandle(3, IntHandle(1)))
	assert.Equal(t, 4, m.Len())
}

func TestMemAwareHandleMapWithPartialHandle(t *testing.T) {
	m := NewMemAwareHandleMap[int]()
	ph1 := NewPartitionHandle(1, IntHandle(1))
	m.Set(ph1, 1)

	ph2 := NewPartitionHandle(2, IntHandle(1))
	m.Set(ph2, 2)

	ph3 := NewPartitionHandle(1, IntHandle(3))
	m.Set(ph3, 5)

	ih := IntHandle(1)
	m.Set(ih, 3)

	dec := types.NewDecFromInt(1)
	encoded, err := codec.EncodeKey(stmtctx.NewStmtCtx().TimeZone(), nil, types.NewDecimalDatum(dec))
	assert.Nil(t, err)
	assert.Less(t, len(encoded), 9)

	ch, err := NewCommonHandle(encoded)
	assert.NoError(t, err)
	m.Set(ch, 4)

	v, ok := m.Get(ph1)
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	v, ok = m.Get(ph2)
	assert.True(t, ok)
	assert.Equal(t, 2, v)

	v, ok = m.Get(ph3)
	assert.True(t, ok)
	assert.Equal(t, 5, v)

	v, ok = m.Get(ih)
	assert.True(t, ok)
	assert.Equal(t, 3, v)

	v, ok = m.Get(ch)
	assert.True(t, ok)
	assert.Equal(t, 4, v)
}

func TestKeyRangeDefinition(t *testing.T) {
	// The struct layout for kv.KeyRange and coprocessor.KeyRange should be exactly the same.
	// This allow us to use unsafe pointer to convert them and reduce allocation.
	var r1 KeyRange
	var r2 coprocessor.KeyRange
	// Same size.
	require.Equal(t, unsafe.Sizeof(r1), unsafe.Sizeof(r2))
	// And same default value.
	require.Equal(t, (*coprocessor.KeyRange)(unsafe.Pointer(&r1)), &r2)
	require.Equal(t, &r1, (*KeyRange)(unsafe.Pointer(&r2)))

	s := []KeyRange{{
		StartKey: []byte("s1"),
		EndKey:   []byte("e1"),
	}, {
		StartKey: []byte("s2"),
		EndKey:   []byte("e2"),
	}}
	require.Equal(t, int64(168), KeyRangeSliceMemUsage(s))
}

func BenchmarkIsPoint(b *testing.B) {
	b.ReportAllocs()
	kr := KeyRange{
		StartKey: []byte("rowkey1"),
		EndKey:   []byte("rowkey2"),
	}
	for i := 0; i < b.N; i++ {
		kr.IsPoint()
	}
}

var result int

var inputs = []struct {
	input int
}{
	{input: 1},
	{input: 100},
	{input: 10000},
	{input: 1000000},
}

func memAwareIntMap(size int, handles []Handle) int {
	var x int
	m := NewMemAwareHandleMap[int]()
	for j := 0; j < size; j++ {
		m.Set(handles[j], j)
	}
	for j := 0; j < size; j++ {
		x, _ = m.Get(handles[j])
	}
	return x
}

func nativeIntMap(size int, handles []Handle) int {
	var x int
	m := make(map[Handle]int)
	for j := 0; j < size; j++ {
		m[handles[j]] = j
	}

	for j := 0; j < size; j++ {
		x = m[handles[j]]
	}
	return x
}

func BenchmarkMemAwareHandleMap(b *testing.B) {
	var sc stmtctx.StatementContext
	for _, s := range inputs {
		handles := make([]Handle, s.input)
		for i := 0; i < s.input; i++ {
			if i%2 == 0 {
				handles[i] = IntHandle(i)
			} else {
				handleBytes, _ := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(int64(i)))
				handles[i], _ = NewCommonHandle(handleBytes)
			}
		}
		b.Run("MemAwareIntMap_"+strconv.Itoa(s.input), func(b *testing.B) {
			var x int
			for i := 0; i < b.N; i++ {
				x = memAwareIntMap(s.input, handles)
			}
			result = x
		})
	}
}

func BenchmarkNativeHandleMap(b *testing.B) {
	var sc stmtctx.StatementContext
	for _, s := range inputs {
		handles := make([]Handle, s.input)
		for i := 0; i < s.input; i++ {
			if i%2 == 0 {
				handles[i] = IntHandle(i)
			} else {
				handleBytes, _ := codec.EncodeKey(sc.TimeZone(), nil, types.NewIntDatum(int64(i)))
				handles[i], _ = NewCommonHandle(handleBytes)
			}
		}
		b.Run("NativeIntMap_"+strconv.Itoa(s.input), func(b *testing.B) {
			var x int
			for i := 0; i < b.N; i++ {
				x = nativeIntMap(s.input, handles)
			}
			result = x
		})
	}
}

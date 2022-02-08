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
	"testing"
	"time"

	. "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartialNext(t *testing.T) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	// keyA represents a multi column index.
	keyA, err := codec.EncodeValue(sc, nil, types.NewDatum("abc"), types.NewDatum("def"))
	require.NoError(t, err)
	keyB, err := codec.EncodeValue(sc, nil, types.NewDatum("abca"), types.NewDatum("def"))
	require.NoError(t, err)

	// We only use first column value to seek.
	seekKey, err := codec.EncodeValue(sc, nil, types.NewDatum("abc"))
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

	ch := testkit.MustNewCommonHandle(t, 100, "abc")
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
	encoded, err := codec.EncodeKey(new(stmtctx.StatementContext), nil, types.NewDecimalDatum(dec))
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

	m.Set(h, 1)
	v, ok := m.Get(h)
	assert.True(t, ok)
	assert.Equal(t, 1, v)

	m.Delete(h)
	v, ok = m.Get(h)
	assert.False(t, ok)
	assert.Nil(t, v)

	ch := testkit.MustNewCommonHandle(t, 100, "abc")
	m.Set(ch, "a")
	v, ok = m.Get(ch)
	assert.True(t, ok)
	assert.Equal(t, "a", v)

	m.Delete(ch)
	v, ok = m.Get(ch)
	assert.False(t, ok)
	assert.Nil(t, v)

	m.Set(ch, "a")
	ch2 := testkit.MustNewCommonHandle(t, 101, "abc")
	m.Set(ch2, "b")
	ch3 := testkit.MustNewCommonHandle(t, 99, "def")
	m.Set(ch3, "c")
	assert.Equal(t, 3, m.Len())

	cnt := 0
	m.Range(func(h Handle, val interface{}) bool {
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

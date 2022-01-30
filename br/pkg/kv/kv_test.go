// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"bytes"
	"strings"
	"testing"

	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMarshal(t *testing.T) {
	dats := make([]types.Datum, 4)
	dats[0].SetInt64(1)
	dats[1].SetNull()
	dats[2] = types.MaxValueDatum()
	dats[3] = types.MinNotNullDatum()

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{})
	out, err := encoder.EncodeEntry(zapcore.Entry{}, []zap.Field{zapRow("row", dats)})
	require.NoError(t, err)
	require.Equal(t,
		`{"row": ["kind: int64, val: 1", "kind: null, val: NULL", "kind: max, val: +inf", "kind: min, val: -inf"]}`,
		strings.TrimRight(out.String(), "\n"))
}

func TestSimplePairIter(t *testing.T) {
	pairs := []Pair{
		{Key: []byte("1"), Val: []byte("a")},
		{Key: []byte("2"), Val: []byte("b")},
		{Key: []byte("3"), Val: []byte("c")},
		{Key: []byte("5"), Val: []byte("d")},
	}
	expectCount := 4
	iter := newSimpleKVIter(pairs)
	count := 0
	for iter.Next() {
		count++
	}
	require.Equal(t, expectCount, count)

	require.True(t, iter.First())
	require.True(t, iter.Last())

	require.True(t, iter.Seek([]byte("1")))
	require.True(t, bytes.Equal(iter.Key(), []byte("1")))
	require.True(t, bytes.Equal(iter.Value(), []byte("a")))
	require.True(t, iter.Valid())

	require.True(t, iter.Seek([]byte("2")))
	require.True(t, bytes.Equal(iter.Key(), []byte("2")))
	require.True(t, bytes.Equal(iter.Value(), []byte("b")))
	require.True(t, iter.Valid())

	require.True(t, iter.Seek([]byte("3")))
	require.True(t, bytes.Equal(iter.Key(), []byte("3")))
	require.True(t, bytes.Equal(iter.Value(), []byte("c")))
	require.True(t, iter.Valid())

	// 4 not exists, so seek position will move to 5.
	require.True(t, iter.Seek([]byte("4")))
	require.True(t, bytes.Equal(iter.Key(), []byte("5")))
	require.True(t, bytes.Equal(iter.Value(), []byte("d")))
	require.True(t, iter.Valid())

	// 6 not exists, so seek position will not valid.
	require.False(t, iter.Seek([]byte("6")))
	require.False(t, iter.Valid())
}

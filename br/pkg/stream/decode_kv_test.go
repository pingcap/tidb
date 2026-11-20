// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream_test

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/stretchr/testify/require"
)

func TestDecodeKVEntry(t *testing.T) {
	var (
		pairs = map[string]string{
			"db":       "tidb",
			"kv":       "tikv",
			"company":  "PingCAP",
			"employee": "Zak",
		}
		buff = make([]byte, 0)
	)

	for k, v := range pairs {
		buff = append(buff, stream.EncodeKVEntry([]byte(k), []byte(v))...)
	}

	ei := stream.NewEventIterator(buff)
	for ei.Valid() {
		ei.Next()
		err := ei.GetError()
		require.NoError(t, err)

		key := ei.Key()
		value := ei.Value()
		v, exist := pairs[string(key)]
		require.True(t, exist)
		require.Equal(t, v, string(value))
	}
}

func TestDecodeKVEntryError(t *testing.T) {
	var (
		k    = []byte("db")
		v    = []byte("tidb")
		buff = make([]byte, 0)
	)

	buff = append(buff, stream.EncodeKVEntry(k, v)...)
	buff = append(buff, 'x')

	ei := stream.NewEventIterator(buff)
	ei.Next()
	require.Equal(t, k, ei.Key())
	require.Equal(t, v, ei.Value())
	require.True(t, ei.Valid())

	ei.Next()
	require.Error(t, ei.GetError())
}

func TestDecodeKVEntryInvalidLengths(t *testing.T) {
	for _, tc := range []struct {
		name             string
		keyLen, valueLen uint32
	}{
		{"key overflow", math.MaxUint32, 0},
		{"key header overflow", math.MaxUint32 - 7, 0},
		{"value overflow", 0, math.MaxUint32},
		{"value header overflow", 0, math.MaxUint32 - 7},
		{"truncated key", 1, 0},
		{"truncated value", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buff := make([]byte, 8)
			binary.LittleEndian.PutUint32(buff, tc.keyLen)
			binary.LittleEndian.PutUint32(buff[4:], tc.valueLen)
			require.NotPanics(t, func() {
				k, v, n, err := stream.DecodeKVEntry(buff)
				require.Error(t, err)
				require.Nil(t, k)
				require.Nil(t, v)
				require.Zero(t, n)
			})
		})
	}
}

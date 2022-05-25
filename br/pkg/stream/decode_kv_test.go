// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream_test

import (
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

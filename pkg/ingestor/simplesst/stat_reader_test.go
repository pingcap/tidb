// Copyright 2026 PingCAP, Inc.
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

package simplesst

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
)

func buildStatBlobV0(props []*RangeProperty) []byte {
	return encodeMultiProps(nil, props)
}

func buildStatBlobV1(props []*RangeProperty) []byte {
	var buf bytes.Buffer
	buf.Write(fileHeaderV1Zstd)
	for _, p := range props {
		body := encodePropV1(nil, p)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(body)))
		buf.Write(lenBuf[:])
		buf.Write(body)
	}
	return buf.Bytes()
}

func TestStatsReader_DispatchV0(t *testing.T) {
	props := []*RangeProperty{
		{FirstKey: []byte("a"), LastKey: []byte("b"), Size: 10, Keys: 2, Offset: 0},
	}
	blob := buildStatBlobV0(props)

	ctx := context.Background()
	store := objstore.NewMemStorage()
	require.NoError(t, store.WriteFile(ctx, "fake", blob))

	r, err := NewStatsReader(ctx, store, "fake", 1024)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	require.EqualValues(t, FileFormatV0, r.version())
	got, err := r.NextProp()
	require.NoError(t, err)
	require.Equal(t, props[0], got)
}

func TestStatsReader_DispatchV1(t *testing.T) {
	props := []*RangeProperty{
		{FirstKey: []byte("a"), LastKey: []byte("b"), Size: 10, Keys: 2, Offset: 6, CompressedSize: 9},
	}
	blob := buildStatBlobV1(props)

	ctx := context.Background()
	store := objstore.NewMemStorage()
	require.NoError(t, store.WriteFile(ctx, "fake", blob))

	r, err := NewStatsReader(ctx, store, "fake", 1024)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	require.EqualValues(t, FileFormatV1, r.version())
	got, err := r.NextProp()
	require.NoError(t, err)
	require.Equal(t, props[0], got)
}

func TestUnsupportedCompressedHeaders(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()
	for _, header := range [][]byte{[]byte("TGSC"), []byte("TGSC\x01"), []byte("TGSC\x02\x01"), []byte("TGSC\x01\xff")} {
		require.NoError(t, store.WriteFile(ctx, "invalid", header))
		_, err := DetectDataFileFormat(ctx, store, "invalid")
		require.Error(t, err)
		reader, err := NewStatsReader(ctx, store, "invalid", 1024)
		if reader != nil {
			require.NoError(t, reader.Close())
		}
		require.Error(t, err)
	}
}

func TestCompressedStatPropertyLengths(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()
	for _, body := range [][]byte{{}, {1}, make([]byte, propertyLengthExceptKeysV1)} {
		// The last body has a key length that exceeds its available bytes.
		if len(body) == propertyLengthExceptKeysV1 {
			binary.BigEndian.PutUint32(body, ^uint32(0))
		}
		blob := append([]byte(nil), fileHeaderV1Zstd...)
		blob = binary.BigEndian.AppendUint32(blob, uint32(len(body)))
		blob = append(blob, body...)
		require.NoError(t, store.WriteFile(ctx, "bad-prop", blob))
		r, err := NewStatsReader(ctx, store, "bad-prop", 1024)
		require.NoError(t, err)
		require.NotPanics(t, func() {
			_, err = r.NextProp()
			require.Error(t, err)
		})
		require.NoError(t, r.Close())
	}
}

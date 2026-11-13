// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/stretchr/testify/require"
)

func TestSegmentKVReader_RoundTrip(t *testing.T) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	require.NoError(t, err)
	defer enc.Close()

	encodeKV := func(k, v []byte) []byte {
		buf := make([]byte, 2*LengthBytes+len(k)+len(v))
		binary.BigEndian.PutUint64(buf[0:LengthBytes], uint64(len(k)))
		binary.BigEndian.PutUint64(buf[LengthBytes:2*LengthBytes], uint64(len(v)))
		copy(buf[2*LengthBytes:], k)
		copy(buf[2*LengthBytes+len(k):], v)
		return buf
	}

	// Two segments, two KVs each.
	seg1Raw := append(encodeKV([]byte("aaa"), []byte("v1")), encodeKV([]byte("bbb"), []byte("v2"))...)
	seg2Raw := append(encodeKV([]byte("ccc"), []byte("v3")), encodeKV([]byte("ddd"), []byte("v4"))...)

	frame1 := enc.EncodeAll(seg1Raw, nil)
	frame2 := enc.EncodeAll(seg2Raw, nil)

	// Build the v1 file blob: 6-byte header followed by two zstd frames. The
	// header bytes mirror what a real v1 writer emits, so SegmentKVReader sees
	// the same offsets it would in production.
	var blob bytes.Buffer
	blob.Write(fileHeaderV1Zstd)
	frame1Off := uint64(blob.Len())
	blob.Write(frame1)
	frame2Off := uint64(blob.Len())
	blob.Write(frame2)

	props := []*RangeProperty{
		{FirstKey: []byte("aaa"), LastKey: []byte("bbb"), Offset: frame1Off, CompressedSize: uint64(len(frame1)), Size: uint64(len(seg1Raw))},
		{FirstKey: []byte("ccc"), LastKey: []byte("ddd"), Offset: frame2Off, CompressedSize: uint64(len(frame2)), Size: uint64(len(seg2Raw))},
	}

	ctx := context.Background()
	store := objstore.NewMemStorage()
	require.NoError(t, store.WriteFile(ctx, "fake", blob.Bytes()))

	r := newSegmentKVReader(ctx, store, "fake", props)
	defer func() { require.NoError(t, r.Close()) }()

	want := []struct{ k, v string }{
		{"aaa", "v1"}, {"bbb", "v2"}, {"ccc", "v3"}, {"ddd", "v4"},
	}
	for _, w := range want {
		k, v, err := r.NextKV()
		require.NoError(t, err)
		require.Equal(t, w.k, string(k))
		require.Equal(t, w.v, string(v))
	}
	_, _, err = r.NextKV()
	require.ErrorIs(t, err, io.EOF)

	// Close is idempotent: calling it twice must not panic / double-pool the
	// decoder.
	require.NoError(t, r.Close())
}

func TestSegmentKVReader_ZeroCompressedSizeRejected(t *testing.T) {
	// In v1, a prop with compressedSize == 0 is the v0 sentinel and must not
	// be silently treated as raw bytes. SegmentKVReader must surface this as
	// an error rather than corrupting the merge stream.
	ctx := context.Background()
	store := objstore.NewMemStorage()
	require.NoError(t, store.WriteFile(ctx, "fake", fileHeaderV1Zstd))

	props := []*RangeProperty{
		{Offset: uint64(FileHeaderLen), CompressedSize: 0, Size: 16},
	}
	r := newSegmentKVReader(ctx, store, "fake", props)
	defer func() { require.NoError(t, r.Close()) }()

	_, _, err := r.NextKV()
	require.Error(t, err)
	require.Contains(t, err.Error(), "zero compressedSize")
}

func TestSegmentKVReaderLengthOverflow(t *testing.T) {
	raw := make([]byte, 2*LengthBytes)
	binary.BigEndian.PutUint64(raw, ^uint64(0))
	r := &SegmentKVReader{segBuf: raw}
	require.NotPanics(t, func() {
		_, _, err := r.NextKV()
		require.Error(t, err)
	})
}

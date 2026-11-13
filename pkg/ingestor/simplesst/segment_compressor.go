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
	"context"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
)

// zstdEncoderPool holds level-1 (zstd.SpeedFastest) encoders for reuse across
// segmentCompressor instances. EncodeAll resets internal encoder state on each
// call, so encoders are safe to reuse.
var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
		return enc
	},
}

// segmentCompressor stages raw KV bytes for one in-flight stat segment and
// flushes the whole segment as a single zstd frame when the segment boundary
// fires. It is owned by KeyValueStore when compression is enabled and is nil
// otherwise.
type segmentCompressor struct {
	encoder *zstd.Encoder // borrowed from zstdEncoderPool

	// rawBuf is the staging buffer for the in-flight segment. KeyValueStore
	// appends raw <keyLen><valLen><key><value> bytes here as KVs arrive, and
	// flushSegment consumes the leading propRawBytes when a boundary fires.
	rawBuf []byte
	// compressed is reusable scratch for the zstd frame produced by EncodeAll.
	compressed []byte
	// physOffset tracks bytes already written to dataWriter, INCLUDING the
	// 6-byte file header. flushSegment writes physOffset onto the segment's
	// RangeProperty before bumping it by len(compressedFrame).
	physOffset uint64
}

// newSegmentCompressor returns a segmentCompressor with an encoder borrowed
// from the pool. Callers must call release() when done so the encoder goes
// back to the pool.
func newSegmentCompressor() *segmentCompressor {
	return &segmentCompressor{
		encoder: zstdEncoderPool.Get().(*zstd.Encoder),
	}
}

// release returns the borrowed encoder to the pool. Safe to call multiple
// times.
func (c *segmentCompressor) release() {
	if c.encoder != nil {
		zstdEncoderPool.Put(c.encoder)
		c.encoder = nil
	}
}

// flushSegment compresses the leading propRawBytes of c.rawBuf as a single
// zstd frame, writes the frame to dataWriter, fills the just-finalized
// finalizedProp's compressedSize and physical offset, advances c.physOffset,
// and trims the consumed bytes from c.rawBuf.
//
// propRawBytes is the segment's full byte count, INCLUDING the per-KV length
// headers. The caller (KeyValueStore via the RangePropertiesCollector
// onBoundary callback) computes this as finalizedProp.Size +
// finalizedProp.Keys * 2 * LengthBytes, matching RangeProperty.totalSize().
func (c *segmentCompressor) flushSegment(
	ctx context.Context,
	dataWriter objectio.Writer,
	finalizedProp *RangeProperty,
	propRawBytes uint64,
) error {
	if propRawBytes == 0 {
		// Empty segment: do not write a zero-byte frame. Leave compressedSize
		// at zero, but in v1 the writer is expected never to produce a prop
		// with raw size 0 in the first place. The boundary detector only
		// emits a prop after it has accumulated keys.
		return errors.New("segmentCompressor: refusing to compress empty segment")
	}
	if uint64(len(c.rawBuf)) < propRawBytes {
		return errors.Errorf(
			"segmentCompressor: rawBuf has %d bytes but segment needs %d",
			len(c.rawBuf), propRawBytes,
		)
	}
	frame := c.encoder.EncodeAll(c.rawBuf[:propRawBytes], c.compressed[:0])
	if _, err := dataWriter.Write(ctx, frame); err != nil {
		return errors.Trace(err)
	}
	finalizedProp.Offset = c.physOffset
	finalizedProp.CompressedSize = uint64(len(frame))
	c.physOffset += uint64(len(frame))
	c.compressed = frame[:0]
	// Trim the consumed leading bytes from rawBuf without reallocating.
	remaining := uint64(len(c.rawBuf)) - propRawBytes
	copy(c.rawBuf, c.rawBuf[propRawBytes:])
	c.rawBuf = c.rawBuf[:remaining]
	return nil
}

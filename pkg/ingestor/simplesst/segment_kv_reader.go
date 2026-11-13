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
	"encoding/binary"
	"io"
	"math"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// zstdDecoderPool holds *zstd.Decoder instances for one-shot DecodeAll use.
// Each SegmentKVReader borrows one decoder for its lifetime; we never share
// decoders across goroutines.
var zstdDecoderPool = sync.Pool{
	New: func() any {
		dec, _ := zstd.NewReader(nil)
		return dec
	},
}

// SegmentKVReader yields key/value pairs from a v1 (zstd-compressed)
// global-sort intermediate data file by ranged-GETing one segment at a time
// and decompressing the entire zstd frame in memory before iterating KVs.
//
// The reader implements KVStream at merge call sites and takes a slice of
// *RangeProperty already filtered for one file, and assumes each prop's
// (offset, compressedSize) describes one self-contained zstd frame.
type SegmentKVReader struct {
	ctx     context.Context
	store   storeapi.Storage
	name    string
	props   []*RangeProperty
	decoder *zstd.Decoder

	segIdx int
	segBuf []byte
	segPos int
}

// newSegmentKVReader returns a reader for the given file and pre-filtered
// props. Caller MUST call Close() so the borrowed decoder is returned to the
// pool.
func newSegmentKVReader(
	ctx context.Context,
	store storeapi.Storage,
	name string,
	props []*RangeProperty,
) *SegmentKVReader {
	return &SegmentKVReader{
		ctx:     ctx,
		store:   store,
		name:    name,
		props:   props,
		decoder: zstdDecoderPool.Get().(*zstd.Decoder),
	}
}

// NextKV yields the next (key, value) pair. Returns (nil, nil, io.EOF) when
// the reader has consumed every prop. The returned key/value slices alias
// into the reader's internal segment buffer and remain valid only until the
// next NextKV call (since the next call may overwrite segBuf when loading a
// new segment); callers that need to retain them must copy.
func (r *SegmentKVReader) NextKV() (key, val []byte, err error) {
	for r.segPos >= len(r.segBuf) {
		if err := r.loadNextSegment(); err != nil {
			return nil, nil, err
		}
	}
	if len(r.segBuf)-r.segPos < 2*LengthBytes {
		return nil, nil, errors.New("SegmentKVReader: truncated KV header")
	}

	keySize := binary.BigEndian.Uint64(r.segBuf[r.segPos : r.segPos+LengthBytes])
	valSize := binary.BigEndian.Uint64(r.segBuf[r.segPos+LengthBytes : r.segPos+2*LengthBytes])
	r.segPos += 2 * LengthBytes
	remaining := uint64(len(r.segBuf) - r.segPos)
	if keySize > remaining || valSize > remaining-keySize {
		return nil, nil, errors.New("SegmentKVReader: truncated KV body")
	}
	keyLen, valLen := int(keySize), int(valSize)

	key = r.segBuf[r.segPos : r.segPos+keyLen]
	val = r.segBuf[r.segPos+keyLen : r.segPos+keyLen+valLen]
	r.segPos += keyLen + valLen
	return key, val, nil
}

// loadNextSegment ranged-GETs the next segment, decompresses it into segBuf,
// and resets segPos. Returns io.EOF when there are no more props.
func (r *SegmentKVReader) loadNextSegment() error {
	if r.segIdx >= len(r.props) {
		return io.EOF
	}
	p := r.props[r.segIdx]
	r.segIdx++
	// In v1, every prop MUST carry a non-zero compressedSize. Zero is the v0
	// sentinel and must never be silently treated as raw here.
	if p.CompressedSize == 0 {
		return errors.Errorf("SegmentKVReader: v1 prop with zero compressedSize at offset %d", p.Offset)
	}

	if p.Offset < FileHeaderLen || p.Offset > math.MaxInt64 || p.CompressedSize > math.MaxInt64-p.Offset {
		return errors.New("SegmentKVReader: invalid compressed segment range")
	}
	start := int64(p.Offset)
	end := int64(p.Offset + p.CompressedSize)
	rd, err := r.store.Open(r.ctx, r.name, &storeapi.ReaderOption{
		StartOffset: aws.Int64(start),
		EndOffset:   aws.Int64(end),
	})
	if err != nil {
		return errors.Trace(err)
	}
	frame, err := io.ReadAll(rd)
	_ = rd.Close()
	if err != nil {
		return errors.Annotatef(err, "SegmentKVReader: read segment offset=%d size=%d", p.Offset, p.CompressedSize)
	}
	r.segBuf, err = r.decoder.DecodeAll(frame, r.segBuf[:0])
	if err != nil {
		return errors.Annotatef(err, "SegmentKVReader: decode segment offset=%d size=%d", p.Offset, p.CompressedSize)
	}
	r.segPos = 0
	return nil
}

// Close returns the borrowed decoder to the pool. Safe to call multiple times.
func (r *SegmentKVReader) Close() error {
	if r.decoder != nil {
		zstdDecoderPool.Put(r.decoder)
		r.decoder = nil
	}
	return nil
}

// StreamingV1KVReader reads KV pairs from a v1 (zstd-compressed) data file
// sequentially from beginning to end. Unlike SegmentKVReader (which seeks to
// per-segment byte ranges based on a pre-computed []*RangeProperty),
// StreamingV1KVReader requires only the data file path: it opens the file,
// skips the 6-byte header, and wraps the rest in a streaming zstd decoder.
// zstd frames are self-delimiting, so a concatenation of segment frames
// decodes into the same byte sequence as reading each segment individually.
//
// This reader is used where the caller wants every KV in the file in order
// and does NOT have the companion stat file (or its props) in scope — e.g.
// the merge step (NewMergeKVIter) and the conflict-resolution file reader
// (ReadKVFilesAsync). It has no concurrent-prefetch mode; parallelism across
// files is the caller's responsibility.
type StreamingV1KVReader struct {
	source  objectio.Reader // underlying ranged storage reader
	decoder *zstd.Decoder   // owns its own Decoder; not pooled because it wraps a persistent Reader
	kvBuf   []byte          // reusable scratch for parsing a single KV body
}

// NewStreamingV1KVReader opens `name` in `store`, skips the 6-byte v1 header,
// and returns a reader that iterates KVs via a streaming zstd decoder over the
// remaining bytes. Callers MUST call Close(). It is the caller's responsibility
// to confirm the file is v1 (e.g. via DetectDataFileFormat) before calling.
func NewStreamingV1KVReader(ctx context.Context, store storeapi.Storage, name string) (*StreamingV1KVReader, error) {
	// Open the file from offset 0 to EOF. Explicit StartOffset = 0 to match
	// how other readers call Open in this package.
	startOffset := int64(0)
	sr, err := store.Open(ctx, name, &storeapi.ReaderOption{
		StartOffset: &startOffset,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Skip the 6-byte file header. We trust the caller verified the format,
	// so a short read here is a hard error.
	hdr := make([]byte, FileHeaderLen)
	if _, err := io.ReadFull(sr, hdr); err != nil {
		_ = sr.Close()
		return nil, errors.Annotatef(err, "StreamingV1KVReader: read file header for %s", name)
	}
	if version, _, ok := parseFileHeader(hdr); !ok || version != FileFormatV1 {
		_ = sr.Close()
		return nil, errors.Errorf("StreamingV1KVReader: file %s is not v1 (bad or missing header)", name)
	}
	dec, err := zstd.NewReader(sr)
	if err != nil {
		_ = sr.Close()
		return nil, errors.Trace(err)
	}
	return &StreamingV1KVReader{source: sr, decoder: dec}, nil
}

// NextKV reads the next (key, value) pair. Returns (nil, nil, io.EOF) at the
// end of the file. Slices are backed by an internal buffer that is reused on
// each call, so callers that need to retain the bytes MUST copy them.
func (r *StreamingV1KVReader) NextKV() (key, val []byte, err error) {
	var lenBuf [2 * LengthBytes]byte
	if _, err := io.ReadFull(r.decoder, lenBuf[:]); err != nil {
		// io.EOF at a record boundary is the clean end-of-stream.
		return nil, nil, err
	}

	keySize := binary.BigEndian.Uint64(lenBuf[0:LengthBytes])
	valSize := binary.BigEndian.Uint64(lenBuf[LengthBytes : 2*LengthBytes])
	if keySize > uint64(math.MaxInt) || valSize > uint64(math.MaxInt)-keySize {
		return nil, nil, errors.New("StreamingV1KVReader: invalid KV lengths")
	}
	keyLen := int(keySize)
	need := int(keySize + valSize)

	if cap(r.kvBuf) < need {
		r.kvBuf = make([]byte, need)
	} else {
		r.kvBuf = r.kvBuf[:need]
	}
	if _, err := io.ReadFull(r.decoder, r.kvBuf); err != nil {
		return nil, nil, noEOF(err)
	}
	return r.kvBuf[:keyLen], r.kvBuf[keyLen:], nil
}

// Close tears down the streaming decoder and the underlying storage reader.
// Safe to call multiple times.
func (r *StreamingV1KVReader) Close() error {
	if r.decoder != nil {
		r.decoder.Close()
		r.decoder = nil
	}
	if r.source != nil {
		err := r.source.Close()
		r.source = nil
		return err
	}
	return nil
}

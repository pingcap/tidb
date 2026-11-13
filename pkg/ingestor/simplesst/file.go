// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
)

const (
	statSuffix = "_stat"
	dupSuffix  = "_dup"
	// LengthBytes we use uint64 to store the length of key and value.
	LengthBytes = 8
)

// KeyValueStore stores key-value pairs and maintains the range properties.
type KeyValueStore struct {
	dataWriter objectio.Writer

	rc         *RangePropertiesCollector
	ctx        context.Context
	offset     uint64
	compressor *segmentCompressor // optional; nil for v0 (uncompressed) writers
}

// NewKeyValueStore creates a new KeyValueStore. The data will be written to the
// given dataWriter and range properties will be maintained in the given
// RangePropertiesCollector.
func NewKeyValueStore(
	ctx context.Context,
	dataWriter objectio.Writer,
	rangePropertiesCollector *RangePropertiesCollector,
) *KeyValueStore {
	kvStore := &KeyValueStore{
		dataWriter: dataWriter,
		ctx:        ctx,
		rc:         rangePropertiesCollector,
	}
	return kvStore
}

// WithCompressor enables per-segment zstd compression on this KeyValueStore.
// When set, addEncodedData stages bytes in compressor.rawBuf instead of
// writing them directly to dataWriter; the actual compressed-frame writes
// happen in the rc.onBoundary callback (which the caller is responsible for
// wiring to compressor.flushSegment).
//
// Finish flushes any trailing partial segment via flushSegment.
//
// Ownership: KeyValueStore does NOT own the compressor's lifetime. The
// caller MUST call compressor.release() after this store is finished so the
// borrowed encoder is returned to the pool.
func (s *KeyValueStore) WithCompressor(c *segmentCompressor) {
	s.compressor = c
}

// addEncodedData saves encoded key-value pairs to the KeyValueStore.
// data layout: keyLen + valueLen + key + value. If the accumulated
// size or key count exceeds the given distance, a new range property will be
// appended to the RangePropertiesCollector with current status.
// `key` must be in strictly ascending order for invocations of a KeyValueStore.
func (s *KeyValueStore) addEncodedData(data []byte) error {
	if s.compressor != nil {
		// Stage raw bytes; the actual write happens in rc.onBoundary via
		// segmentCompressor.flushSegment when a segment boundary fires.
		s.compressor.rawBuf = append(s.compressor.rawBuf, data...)
	} else {
		if _, err := s.dataWriter.Write(s.ctx, data); err != nil {
			return err
		}
	}

	s.offset += uint64(len(data))
	if s.rc != nil {
		if err := s.rc.onNextEncodedData(data, s.offset); err != nil {
			return err
		}
	}

	return nil
}

// Offset returns the current offset of the KeyValueStore.
func (s *KeyValueStore) Offset() uint64 {
	return s.offset
}

// AddRawKV add a raw key-value pair to the KeyValueStore.
func (s *KeyValueStore) AddRawKV(key, val []byte) error {
	length := len(key) + len(val) + LengthBytes*2
	buf := make([]byte, length)
	encodeToBuf(buf, key, val)
	return s.addEncodedData(buf[:length])
}

// Finish finalizes the KeyValueStore and appends the last range property.
// When compression is enabled, Finish also flushes any trailing partial
// segment (bytes that did not reach a propSizeDist boundary) as one final
// compressed frame, so the last prop in rc.props always has compressedSize
// set.
func (s *KeyValueStore) Finish() error {
	if s.rc == nil {
		return nil
	}
	s.rc.onFileEnd()
	if s.compressor == nil || len(s.compressor.rawBuf) == 0 {
		return nil
	}
	// onFileEnd appended the trailing partial prop. Flush the corresponding
	// rawBuf bytes as one final zstd frame so the last prop in rc.props has
	// compressedSize set.
	if len(s.rc.props) == 0 {
		return errors.New("KeyValueStore.finish: trailing bytes but no rc.props")
	}
	trailing := s.rc.props[len(s.rc.props)-1]
	// Use the actual rawBuf length as the source of truth: it is what we will
	// be compressing. Cross-check against trailing.totalSize() so any future
	// drift between the two is loud rather than silently losing bytes.
	propRaw := uint64(len(s.compressor.rawBuf))
	if propRaw != trailing.totalSize() {
		return errors.Errorf(
			"KeyValueStore.finish: rawBuf has %d trailing bytes but trailing prop totalSize is %d",
			propRaw, trailing.totalSize())
	}
	if propRaw == 0 {
		return errors.New("KeyValueStore.finish: trailing prop has zero raw bytes")
	}
	return s.compressor.flushSegment(s.ctx, s.dataWriter, trailing, propRaw)
}

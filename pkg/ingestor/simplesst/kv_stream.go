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
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// KVStream is a tiny iterator interface satisfied by both KVReader (v0) and
// SegmentKVReader (v1). The merge step only needs NextKV/Close, so this
// interface lets format dispatch happen at construction time and the iterator
// internals stay format-specific.
type KVStream interface {
	NextKV() (key, val []byte, err error)
	Close() error
}

// DetectDataFileFormat probes the first FileHeaderLen bytes of a data file
// and reports whether it is a v1 (zstd-compressed) file or a v0 (raw) file.
// It is safe to call on either format because a raw v0 data file begins with
// an 8-byte big-endian key length, whose high 4 bytes can never collide with
// the ASCII "TGSC" magic for any realistic key size (< ~6 PB). Callers use
// this to dispatch to the right KVStream implementation when no companion
// StatsReader is in scope.
//
// Known compressed magic with an unsupported or truncated header is an error.
func DetectDataFileFormat(ctx context.Context, store storeapi.Storage, name string) (uint8, error) {
	startOffset := int64(0)
	endOffset := int64(FileHeaderLen)
	rd, err := store.Open(ctx, name, &storeapi.ReaderOption{
		StartOffset: &startOffset,
		EndOffset:   &endOffset,
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer func() { _ = rd.Close() }()
	buf := make([]byte, FileHeaderLen)

	n, readErr := io.ReadFull(rd, buf)
	if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
		return 0, errors.Trace(readErr)
	}
	if n >= len(fileMagic) && bytes.Equal(buf[:len(fileMagic)], fileMagic[:]) {
		if version, _, ok := parseFileHeader(buf[:n]); ok {
			return version, nil
		}
		return 0, errors.New("unsupported or truncated compressed file header")
	}

	return FileFormatV0, nil
}

// openKVStream returns a KVStream for the given data file. fileVersion comes
// from the corresponding StatsReader (which already inspected the stat file's
// magic header in NewStatsReader).
//
// For v1 files, props MUST be the pre-filtered list of segments to read in
// order; SegmentKVReader ranged-GETs each prop's [offset, offset+compressedSize)
// region as one zstd frame. For v0 files, the existing initFileOffset/bufSize
// parameters are passed through to NewKVReader and props is ignored.
func openKVStream(
	ctx context.Context,
	store storeapi.Storage,
	name string,
	fileVersion uint8,
	props []*RangeProperty,
	initFileOffset uint64,
	bufSize int,
) (KVStream, error) {
	switch fileVersion {
	case FileFormatV0:
		return NewKVReader(ctx, name, store, initFileOffset, bufSize)
	case FileFormatV1:
		return newSegmentKVReader(ctx, store, name, props), nil
	default:
		return nil, errors.Errorf("openKVStream: unknown file version %d", fileVersion)
	}
}

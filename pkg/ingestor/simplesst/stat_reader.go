// Copyright 2023 PingCAP, Inc.
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
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// StatsReader is used to read RangeProperty from the file.
type StatsReader struct {
	byteReader  *byteReader
	fileVersion uint8
	// pendingHeader holds the first magic-length bytes of a v0 stat file
	// after the v1-magic probe consumed them. They are virtually replayed
	// before any further byteReader reads so the v0 decoder still sees the
	// original byte stream starting at offset 0. nil for v1 files (where the
	// header is not part of the prop stream).
	pendingHeader []byte
}

// NewStatsReader creates a new StatsReader to read RangeProperty.
func NewStatsReader(ctx context.Context, store storeapi.Storage, name string, bufSize int) (*StatsReader, error) {
	sr, err := openStoreReaderAndSeek(ctx, store, name, 0, 250*1024)
	if err != nil {
		return nil, err
	}
	br, err := newByteReader(ctx, sr, bufSize)
	if err != nil {
		return nil, err
	}
	r := &StatsReader{
		byteReader:  br,
		fileVersion: FileFormatV0,
	}

	// Probe only the magic first. Replay it for raw files without reopening storage.
	magic, err := br.readNBytes(len(fileMagic))
	if err != nil {
		if err == io.EOF {
			return r, nil
		}
		_ = br.Close()
		return nil, err
	}
	if !bytes.Equal(magic, fileMagic[:]) {
		r.pendingHeader = append([]byte(nil), magic...)
		return r, nil
	}
	header := append([]byte(nil), magic...)
	suffix, err := br.readNBytes(FileHeaderLen - len(fileMagic))
	if err == nil {
		header = append(header, suffix...)
	}
	if _, _, ok := parseFileHeader(header); !ok || err != nil {
		_ = br.Close()
		return nil, errors.New("unsupported or truncated compressed stat header")
	}
	r.fileVersion = FileFormatV1

	return r, nil
}

// version returns the on-disk format detected at open time. Callers pair the
// stat-file format with the corresponding data-file reader (KVReader for v0,
// SegmentKVReader for v1).
func (r *StatsReader) version() uint8 { return r.fileVersion }

// readNBytes reads n bytes, draining pendingHeader (the bytes consumed by the
// magic probe) before falling through to byteReader. This keeps the v0 path
// byte-identical to opening the file at offset 0.
func (r *StatsReader) readNBytes(n int) ([]byte, error) {
	if len(r.pendingHeader) == 0 {
		return r.byteReader.readNBytes(n)
	}
	if n <= len(r.pendingHeader) {
		out := r.pendingHeader[:n:n]
		r.pendingHeader = r.pendingHeader[n:]
		return out, nil
	}
	// Need bytes from both pendingHeader and byteReader. Allocate a fresh
	// buffer so the returned slice is not invalidated by the next byteReader
	// read.
	out := make([]byte, n)
	copy(out, r.pendingHeader)
	rest, err := r.byteReader.readNBytes(n - len(r.pendingHeader))
	if err != nil {
		return nil, err
	}
	copy(out[len(r.pendingHeader):], rest)
	r.pendingHeader = nil
	return out, nil
}

// NextProp returns the next RangeProperty.
func (r *StatsReader) NextProp() (*RangeProperty, error) {
	lenBytes, err := r.readNBytes(4)
	if err != nil {
		return nil, err
	}
	propLen := int(binary.BigEndian.Uint32(lenBytes))
	propBytes, err := r.readNBytes(propLen)
	if err != nil {
		return nil, noEOF(err)
	}
	if r.fileVersion == FileFormatV1 {
		// Validate variable-length keys before the unchecked property decoder slices them.
		if len(propBytes) < propertyLengthExceptKeysV1 {
			return nil, errors.New("truncated compressed range property")
		}
		firstLen := uint64(binary.BigEndian.Uint32(propBytes[:4]))
		maxKeys := uint64(len(propBytes) - propertyLengthExceptKeysV1)
		if firstLen > maxKeys {
			return nil, errors.New("invalid compressed range property first key length")
		}
		lastLen := uint64(binary.BigEndian.Uint32(propBytes[4+firstLen : 8+firstLen]))
		if lastLen != maxKeys-firstLen {
			return nil, errors.New("invalid compressed range property last key length")
		}
		return decodePropV1(propBytes), nil
	}
	return decodeProp(propBytes), nil
}

// Close the StatsReader.
func (r *StatsReader) Close() error {
	return r.byteReader.Close()
}

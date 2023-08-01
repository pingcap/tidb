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

package external

import (
	"context"
	"io"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

type byteReader struct {
	ctx           context.Context
	storageReader storage.ReadSeekCloser

	buf       []byte
	bufOffset int

	auxBuf   []byte
	cowSlice [][]byte // copy-on-write slices
	isEOF    bool
}

func openStoreReaderAndSeek(
	ctx context.Context,
	store storage.ExternalStorage,
	name string,
	initFileOffset uint64,
) (storage.ReadSeekCloser, error) {
	storageReader, err := store.Open(ctx, name)
	if err != nil {
		return nil, err
	}
	_, err = storageReader.Seek(int64(initFileOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	return storageReader, nil
}

func newByteReader(ctx context.Context, storageReader storage.ReadSeekCloser, bufSize int) *byteReader {
	return &byteReader{
		ctx:           ctx,
		storageReader: storageReader,
		buf:           make([]byte, bufSize),
		bufOffset:     0,
	}
}

type slice interface {
	get() []byte
}

type commonSlice []byte

func (c commonSlice) get() []byte {
	return c
}

type lazySlice struct {
	cowIdx     int
	byteReader *byteReader
}

func (s lazySlice) get() []byte {
	return s.byteReader.cowSlice[s.cowIdx]
}

// sliceNext reads the next n bytes from the reader and returns a buffer slice containing those bytes.
// If the reader has fewer than n bytes remaining in current buffer, `auxBuf` is used as a container instead.
func (r *byteReader) sliceNext(n int) (slice, error) {
	b := r.next(n)
	readLen := len(b)
	if readLen == n {
		return mkLazySlice(r, b), nil
	}
	if cap(r.auxBuf) < n {
		r.auxBuf = make([]byte, n)
	}
	r.auxBuf = r.auxBuf[:n]
	copy(r.auxBuf, b)
	for readLen < n {
		r.cloneSlices()
		err := r.reload()
		if err != nil {
			return nil, err
		}
		b = r.next(n - readLen)
		copy(r.auxBuf[readLen:], b)
		readLen += len(b)
	}
	return commonSlice(r.auxBuf), nil
}

func mkLazySlice(r *byteReader, s []byte) lazySlice {
	r.cowSlice = append(r.cowSlice, s)
	return lazySlice{
		cowIdx:     len(r.cowSlice) - 1,
		byteReader: r,
	}
}

func (r *byteReader) reset() {
	r.cowSlice = r.cowSlice[:0]
}

func (r *byteReader) cloneSlices() {
	for i := range r.cowSlice {
		r.cowSlice[i] = append([]byte(nil), r.cowSlice[i]...)
	}
}

func (r *byteReader) eof() bool {
	return r.isEOF && len(r.buf) == r.bufOffset
}

func (r *byteReader) next(n int) []byte {
	end := mathutil.Min(r.bufOffset+n, len(r.buf))
	ret := r.buf[r.bufOffset:end]
	r.bufOffset += len(ret)
	return ret
}

func (r *byteReader) reload() error {
	nBytes, err := io.ReadFull(r.storageReader, r.buf[0:])
	if err == io.EOF {
		logutil.Logger(r.ctx).Error("unexpected EOF")
		r.isEOF = true
		return err
	} else if err != nil && err == io.ErrUnexpectedEOF {
		r.isEOF = true
	} else if err != nil {
		logutil.Logger(r.ctx).Warn("other error during reading from external storage", zap.Error(err))
		return err
	}
	r.bufOffset = 0
	if nBytes < len(r.buf) {
		// The last batch.
		r.buf = r.buf[:nBytes]
	}
	return nil
}

func (r *byteReader) Close() error {
	return r.storageReader.Close()
}

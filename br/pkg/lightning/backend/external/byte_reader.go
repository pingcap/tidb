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
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

// ConcurrentReaderBufferSize is the buffer size for concurrent reader.
var ConcurrentReaderBufferSize = 4 * 1024 * 1024

// byteReader provides structured reading on a byte stream of external storage.
type byteReader struct {
	ctx           context.Context
	storageReader storage.ReadSeekCloser

	buf       []byte
	bufOffset int

	retPointers []*[]byte

	useConcurrentReaderCurrent bool
	useConcurrentReader        atomic.Bool

	currFileOffset int64
	conReader      *singeFileReader
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

// newByteReader wraps readNBytes functionality to storageReader.
func newByteReader(
	ctx context.Context,
	storageReader storage.ExternalFileReader,
	bufSize int,
	st storage.ExternalStorage,
	name string,
	defaultUseConcurrency bool,
) (r *byteReader, err error) {
	defer func() {
		if err != nil && r != nil {
			_ = r.Close()
		}
	}()
	conReader, err := newSingeFileReader(ctx, st, name, 8, ConcurrentReaderBufferSize)
	if err != nil {
		return nil, err
	}
	r = &byteReader{
		ctx:           ctx,
		storageReader: storageReader,
		buf:           make([]byte, bufSize),
		bufOffset:     0,
		conReader:     conReader,
	}
	r.switchReaderMode(defaultUseConcurrency)
	return r, r.reload()
}

// switchReaderMode switches to concurrent reader.
func (r *byteReader) switchReaderMode(useConcurrent bool) {
	r.useConcurrentReader.Store(useConcurrent)
}

func (r *byteReader) switchToConcurrentReaderImpl() error {
	if r.conReader == nil {
		return errors.New("can't use the concurrent mode because reader is not initialized correctly")
	}
	currOffset, err := r.storageReader.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	r.currFileOffset = currOffset
	r.conReader.currentFileOffset = currOffset
	r.conReader.bufferReadOffset = 0

	r.conReader.buffer = make([]byte, r.conReader.concurrency*r.conReader.readBufferSize)
	r.useConcurrentReaderCurrent = true
	return nil
}

func (r *byteReader) switchToNormalReaderImpl() error {
	r.useConcurrentReaderCurrent = false
	r.currFileOffset = r.conReader.currentFileOffset
	r.conReader.buffer = nil
	_, err := r.storageReader.Seek(r.currFileOffset, io.SeekStart)
	return err
}

// readNBytes reads the next n bytes from the reader and returns a buffer slice containing those bytes.
// The returned slice (pointer) can not be used after r.reset. In the same interval of r.reset,
// byteReader guarantees that the returned slice (pointer) will point to the same content
// though the slice may be changed.
func (r *byteReader) readNBytes(n int) (*[]byte, error) {
	b := r.next(n)
	readLen := len(b)
	if readLen == n {
		ret := &b
		r.retPointers = append(r.retPointers, ret)
		return ret, nil
	}
	// If the reader has fewer than n bytes remaining in current buffer,
	// `auxBuf` is used as a container instead.
	auxBuf := make([]byte, n)
	copy(auxBuf, b)
	for readLen < n {
		r.cloneSlices()
		err := r.reload()
		switch err {
		case nil:
		case io.EOF:
			if readLen > 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		default:
			return nil, err
		}
		b = r.next(n - readLen)
		copy(auxBuf[readLen:], b)
		readLen += len(b)
	}
	return &auxBuf, nil
}

func (r *byteReader) reset() {
	for i := range r.retPointers {
		r.retPointers[i] = nil
	}
	r.retPointers = r.retPointers[:0]
}

func (r *byteReader) cloneSlices() {
	for i := range r.retPointers {
		copied := make([]byte, len(*r.retPointers[i]))
		copy(copied, *r.retPointers[i])
		*r.retPointers[i] = copied
		r.retPointers[i] = nil
	}
	r.retPointers = r.retPointers[:0]
}

func (r *byteReader) next(n int) []byte {
	if r.useConcurrentReaderCurrent {
		return r.conReader.next(n)
	}
	end := mathutil.Min(r.bufOffset+n, len(r.buf))
	ret := r.buf[r.bufOffset:end]
	r.bufOffset += len(ret)
	return ret
}

func (r *byteReader) reload() error {
	to := r.useConcurrentReader.Load()
	now := r.useConcurrentReaderCurrent
	if to != now {
		logutil.Logger(r.ctx).Info("switch reader mode", zap.Bool("use concurrent mode", to))
		if to {
			err := r.switchToConcurrentReaderImpl()
			if err != nil {
				return err
			}
		} else {
			err := r.switchToNormalReaderImpl()
			if err != nil {
				return err
			}
		}
	}
	if to {
		return r.conReader.reload()
	}
	nBytes, err := io.ReadFull(r.storageReader, r.buf[0:])
	if err != nil {
		switch err {
		case io.EOF:
			return err
		case io.ErrUnexpectedEOF:
			// The last batch.
			r.buf = r.buf[:nBytes]
		default:
			logutil.Logger(r.ctx).Warn("other error during reload", zap.Error(err))
			return err
		}
	}
	r.bufOffset = 0
	return nil
}

func (r *byteReader) Close() error {
	return r.storageReader.Close()
}

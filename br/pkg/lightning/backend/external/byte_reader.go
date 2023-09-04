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

	useConcurrentReader bool
	switchFunc          func() error

	currFileOffset int64
	conReader      *SingeFileReader
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

// newByteReader wraps readNBytes functionality to storageReader. It will not
// close storageReader when meet error.
func newByteReader(ctx context.Context, storageReader storage.ReadSeekCloser, bufSize int, st storage.ExternalStorage, name string, defaultUseConcurrency bool) (*byteReader, error) {
	conReader, err := NewSingeFileReader(ctx, st, name, 8, ConcurrentReaderBufferSize)
	if err != nil {
		return nil, err
	}
	r := &byteReader{
		ctx:           ctx,
		storageReader: storageReader,
		buf:           make([]byte, bufSize),
		bufOffset:     0,
		conReader:     conReader,
	}
	if defaultUseConcurrency {
		err = r.SwitchToConcurrentReader()
		if err != nil {
			return nil, err
		}
	}
	return r, r.reload()
}

// SwitchToConcurrentReader switches to concurrent reader. It mustn't be called if conReader is nil.
func (r *byteReader) SwitchToConcurrentReader() error {
	if r.useConcurrentReader {
		return nil
	}
	r.switchFunc = r.switchToConcurrentReaderImpl
	return nil
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

	r.useConcurrentReader = true
	r.conReader.buffer = make([]byte, r.conReader.concurrency*r.conReader.readBufferSize)
	return nil
}

// SwitchToNormalReader switches to normal reader.
func (r *byteReader) SwitchToNormalReader() error {
	if !r.useConcurrentReader {
		return nil
	}
	r.switchFunc = r.switchToNormalReaderImpl
	return nil
}

// UseConcurrentReader returns whether the reader is using concurrent reader.
func (r *byteReader) UseConcurrentReader() bool {
	return r.useConcurrentReader
}

func (r *byteReader) switchToNormalReaderImpl() error {
	r.useConcurrentReader = false
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
	if r.useConcurrentReader {
		return r.conReader.Next(n)
	}
	end := mathutil.Min(r.bufOffset+n, len(r.buf))
	ret := r.buf[r.bufOffset:end]
	r.bufOffset += len(ret)
	return ret
}

func (r *byteReader) reload() error {
	if r.switchFunc != nil {
		err := r.switchFunc()
		if err != nil {
			return err
		}
		r.switchFunc = nil
	}
	if r.useConcurrentReader {
		return r.conReader.Reload()
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

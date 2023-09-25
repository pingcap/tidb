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

	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

var (
	// ConcurrentReaderBufferSize is the buffer size for concurrent reader.
	ConcurrentReaderBufferSize  = 4 * 1024 * 1024
	ConcurrentReaderConcurrency = 8
)

// byteReader provides structured reading on a byte stream of external storage.
// It can also switch to concurrent reading mode and fetch a larger amount of
// data to improve throughput.
type byteReader struct {
	ctx           context.Context
	storageReader storage.ReadSeekCloser

	// curBuf is either smallBuf or concurrentReader.largeBuf.
	curBuf       []byte
	curBufOffset int
	smallBuf     []byte

	retPointers []*[]byte

	concurrentReader struct {
		now      bool
		expected bool

		largeBufferPool *membuf.Buffer
		largeBuf        []byte
	}
	//useConcurrentReaderCurrent bool
	//
	//currFileOffset int64
	//conReader      *singeFileReader
}

func openStoreReaderAndSeek(
	ctx context.Context,
	store storage.ExternalStorage,
	name string,
	initFileOffset uint64,
) (storage.ExternalFileReader, error) {
	storageReader, err := store.Open(ctx, name, nil)
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
	r = &byteReader{
		ctx:           ctx,
		storageReader: storageReader,
		smallBuf:      make([]byte, bufSize),
		curBufOffset:  0,
	}
	r.curBuf = r.smallBuf
	r.needLargePrefetch(defaultUseConcurrency)
	return r, r.reload()
}

// needLargePrefetch is used to help implement sortedReader.needLargePrefetch.
// See the comment of the interface.
func (r *byteReader) needLargePrefetch(useConcurrent bool) {
	here
	r.concurrentReader.expected = useConcurrent
}

func (r *byteReader) switchToConcurrentReader() error {
	// because it will be called only when buffered data of storageReader is used
	// up, we can use seek(0, io.SeekCurrent) to get the offset for concurrent
	// reader
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
	end := mathutil.Min(r.curBufOffset+n, len(r.curBuf))
	ret := r.curBuf[r.curBufOffset:end]
	r.curBufOffset += len(ret)
	return ret
}

func (r *byteReader) reload() error {
	to := r.concurrentReader.expected
	now := r.concurrentReader.now
	// in reload only false -> true is possible
	if !now && to {
		logutil.Logger(r.ctx).Info("switch reader mode", zap.Bool("use concurrent mode", true))
		err := r.switchToConcurrentReader()
		if err != nil {
			return err
		}
	}

	if to != now {
		if to {
			err := r.switchToConcurrentReader()
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

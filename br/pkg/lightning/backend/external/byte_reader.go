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
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
)

var (
	// ConcurrentReaderBufferSizePerConc is the buffer size for concurrent reader per
	// concurrency.
	ConcurrentReaderBufferSizePerConc = 4 * 1024 * 1024
	// ConcurrentReaderConcurrency is the concurrency for concurrent reader.
	ConcurrentReaderConcurrency = 8
)

// byteReader provides structured reading on a byte stream of external storage.
// It can also switch to concurrent reading mode and fetch a larger amount of
// data to improve throughput.
type byteReader struct {
	ctx           context.Context
	storageReader storage.ExternalFileReader

	// curBuf is either smallBuf or concurrentReader.largeBuf.
	curBuf       []byte
	curBufOffset int
	smallBuf     []byte

	retPointers []*[]byte

	concurrentReader struct {
		largeBufferPool *membuf.Buffer
		store           storage.ExternalStorage
		filename        string
		concurrency     int
		bufSizePerConc  int

		now       bool
		expected  bool
		largeBuf  []byte
		reader    *concurrentFileReader
		reloadCnt int
	}

	logger *zap.Logger
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

// newByteReader wraps readNBytes functionality to storageReader. If store and
// filename are also given, this reader can use switchConcurrentMode to switch to
// concurrent reading mode.
func newByteReader(
	ctx context.Context,
	storageReader storage.ExternalFileReader,
	bufSize int,
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
	r.logger = logutil.Logger(r.ctx)
	return r, r.reload()
}

func (r *byteReader) enableConcurrentRead(
	store storage.ExternalStorage,
	filename string,
	concurrency int,
	bufSizePerConc int,
	bufferPool *membuf.Buffer,
) {
	r.concurrentReader.store = store
	r.concurrentReader.filename = filename
	r.concurrentReader.concurrency = concurrency
	r.concurrentReader.bufSizePerConc = bufSizePerConc
	r.concurrentReader.largeBufferPool = bufferPool
}

// switchConcurrentMode is used to help implement sortedReader.switchConcurrentMode.
// See the comment of the interface.
func (r *byteReader) switchConcurrentMode(useConcurrent bool) error {
	readerFields := &r.concurrentReader
	if readerFields.store == nil {
		r.logger.Warn("concurrent reader is not enabled, skip switching")
		// caller don't need to care about it.
		return nil
	}
	// need to set it before reload()
	readerFields.expected = useConcurrent
	// concurrent reader will be lazily initialized when reload()
	if useConcurrent {
		return nil
	}

	// no change
	if !readerFields.now {
		return nil
	}

	// rest cases is caller want to turn off concurrent reader. We should turn off
	// immediately to release memory.
	reloadCnt, offsetInOldBuf := r.closeConcurrentReader()
	// here we can assume largeBuf is always fully loaded, because the only exception
	// is it's the end of file. When it's the end of the file, caller will see EOF
	// and no further switchConcurrentMode should be called.
	largeBufSize := readerFields.bufSizePerConc * readerFields.concurrency
	delta := int64(offsetInOldBuf + (reloadCnt-1)*largeBufSize)
	if _, err := r.storageReader.Seek(delta, io.SeekCurrent); err != nil {
		return err
	}
	err := r.reload()
	if err != nil && err == io.EOF {
		// ignore EOF error, let readNBytes handle it
		return nil
	}
	return err
}

func (r *byteReader) switchToConcurrentReader() error {
	// because it will be called only when buffered data of storageReader is used
	// up, we can use seek(0, io.SeekCurrent) to get the offset for concurrent
	// reader
	currOffset, err := r.storageReader.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	fileSize, err := r.storageReader.GetFileSize()
	if err != nil {
		return err
	}
	readerFields := &r.concurrentReader
	readerFields.reader, err = newConcurrentFileReader(
		r.ctx,
		readerFields.store,
		readerFields.filename,
		currOffset,
		fileSize,
		readerFields.concurrency,
		readerFields.bufSizePerConc,
	)
	if err != nil {
		return err
	}

	totalSize := readerFields.concurrency * readerFields.bufSizePerConc
	readerFields.largeBuf = readerFields.largeBufferPool.AllocBytes(totalSize)
	r.curBuf = readerFields.largeBuf
	r.curBufOffset = 0
	readerFields.now = true
	return nil
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
	// in read only false -> true is possible
	if !now && to {
		r.logger.Info("switch reader mode", zap.Bool("use concurrent mode", true))
		err := r.switchToConcurrentReader()
		if err != nil {
			return err
		}
	}

	if r.concurrentReader.now {
		r.concurrentReader.reloadCnt++
		n, err := r.concurrentReader.reader.read(r.concurrentReader.largeBuf)
		if err != nil {
			return err
		}
		r.curBuf = r.curBuf[:n]
		r.curBufOffset = 0
		return nil
	}
	n, err := io.ReadFull(r.storageReader, r.curBuf[0:])
	if err != nil {
		switch err {
		case io.EOF:
			return err
		case io.ErrUnexpectedEOF:
			// The last batch.
			r.curBuf = r.curBuf[:n]
		default:
			r.logger.Warn("other error during read", zap.Error(err))
			return err
		}
	}
	r.curBufOffset = 0
	return nil
}

func (r *byteReader) closeConcurrentReader() (reloadCnt, offsetInOldBuffer int) {
	r.logger.Info("drop data in closeConcurrentReader",
		zap.Int("reloadCnt", r.concurrentReader.reloadCnt),
		zap.Int("dropBytes", len(r.curBuf)-r.curBufOffset),
	)
	r.concurrentReader.largeBufferPool.Destroy()
	r.concurrentReader.largeBuf = nil
	r.concurrentReader.now = false
	reloadCnt = r.concurrentReader.reloadCnt
	r.concurrentReader.reloadCnt = 0
	r.curBuf = r.smallBuf
	offsetInOldBuffer = r.curBufOffset
	r.curBufOffset = 0
	return
}

func (r *byteReader) Close() error {
	if r.concurrentReader.now {
		r.closeConcurrentReader()
	}
	return r.storageReader.Close()
}

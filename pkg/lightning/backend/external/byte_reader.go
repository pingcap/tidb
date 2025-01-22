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
	"fmt"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

var (
	// ConcurrentReaderBufferSizePerConc is the buffer size for concurrent reader per
	// concurrency.
	ConcurrentReaderBufferSizePerConc = int(8 * size.MB)
	// in readAllData, expected concurrency less than this value will not use
	// concurrent reader.
	readAllDataConcThreshold = uint64(4)
)

// byteReader provides structured reading on a byte stream of external storage.
// It can also switch to concurrent reading mode and fetch a larger amount of
// data to improve throughput.
type byteReader struct {
	ctx           context.Context
	storageReader storage.ExternalFileReader

	// curBuf is either smallBuf or concurrentReader.largeBuf.
	curBuf       [][]byte
	curBufIdx    int // invariant: 0 <= curBufIdx < len(curBuf) when curBuf contains unread data
	curBufOffset int // invariant: 0 <= curBufOffset < len(curBuf[curBufIdx]) if curBufIdx < len(curBuf)
	smallBuf     []byte

	concurrentReader struct {
		largeBufferPool *membuf.Buffer
		store           storage.ExternalStorage
		filename        string
		concurrency     int
		bufSizePerConc  int

		now       bool
		expected  bool
		largeBuf  [][]byte
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
	prefetchSize int,
) (storage.ExternalFileReader, error) {
	storageReader, err := store.Open(ctx, name, &storage.ReaderOption{PrefetchSize: prefetchSize})
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
	r.curBuf = [][]byte{r.smallBuf}
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

	readerFields.largeBuf = make([][]byte, readerFields.concurrency)
	for i := range readerFields.largeBuf {
		readerFields.largeBuf[i] = readerFields.largeBufferPool.AllocBytes(readerFields.bufSizePerConc)
		if readerFields.largeBuf[i] == nil {
			return errors.Errorf("alloc large buffer failed, size %d", readerFields.bufSizePerConc)
		}
	}

	r.curBuf = readerFields.largeBuf
	r.curBufOffset = 0
	readerFields.now = true
	return nil
}

// readNBytes reads the next n bytes from the reader and returns a buffer slice
// containing those bytes. The content of returned slice may be changed after
// next call.
func (r *byteReader) readNBytes(n int) ([]byte, error) {
	if n <= 0 {
		return nil, errors.Errorf("illegal n (%d) when reading from external storage", n)
	}
	if n > int(size.GB) {
		return nil, errors.Errorf("read %d bytes from external storage, exceed max limit %d", n, size.GB)
	}

	readLen, bs := r.next(n)
	if readLen == n && len(bs) == 1 {
		return bs[0], nil
	}
	// need to flatten bs
	auxBuf := make([]byte, n)
	for _, b := range bs {
		copy(auxBuf[len(auxBuf)-n:], b)
		n -= len(b)
	}
	hasRead := readLen > 0
	for n > 0 {
		err := r.reload()
		switch err {
		case nil:
		case io.EOF:
			// EOF is only allowed when we have not read any data
			if hasRead {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		default:
			return nil, err
		}
		readLen, bs = r.next(n)
		hasRead = hasRead || readLen > 0
		for _, b := range bs {
			copy(auxBuf[len(auxBuf)-n:], b)
			n -= len(b)
		}
	}
	return auxBuf, nil
}

func (r *byteReader) next(n int) (int, [][]byte) {
	retCnt := 0
	// TODO(lance6716): heap escape performance?
	ret := make([][]byte, 0, len(r.curBuf)-r.curBufIdx+1)
	for r.curBufIdx < len(r.curBuf) && n > 0 {
		cur := r.curBuf[r.curBufIdx]
		if r.curBufOffset+n <= len(cur) {
			ret = append(ret, cur[r.curBufOffset:r.curBufOffset+n])
			retCnt += n
			r.curBufOffset += n
			if r.curBufOffset == len(cur) {
				r.curBufIdx++
				r.curBufOffset = 0
			}
			break
		}
		ret = append(ret, cur[r.curBufOffset:])
		retCnt += len(cur) - r.curBufOffset
		n -= len(cur) - r.curBufOffset
		r.curBufIdx++
		r.curBufOffset = 0
	}

	return retCnt, ret
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
		buffers, err := r.concurrentReader.reader.read(r.concurrentReader.largeBuf)
		if err != nil {
			return err
		}
		r.curBuf = buffers
		r.curBufIdx = 0
		r.curBufOffset = 0
		return nil
	}
	// when not using concurrentReader, len(curBuf) == 1
	n, err := io.ReadFull(r.storageReader, r.curBuf[0][0:])
	if err != nil {
		switch err {
		case io.EOF:
			// move curBufIdx so following read will also find EOF
			r.curBufIdx = len(r.curBuf)
			return err
		case io.ErrUnexpectedEOF:
			// The last batch.
			r.curBuf[0] = r.curBuf[0][:n]
		case context.Canceled:
			return err
		default:
			r.logger.Warn("other error during read", zap.Error(err))
			return err
		}
	}
	r.curBufIdx = 0
	r.curBufOffset = 0
	return nil
}

func (r *byteReader) closeConcurrentReader() (reloadCnt, offsetInOldBuffer int) {
	r.logger.Info("drop data in closeConcurrentReader",
		zap.Int("reloadCnt", r.concurrentReader.reloadCnt),
		zap.Int("dropBytes", r.concurrentReader.bufSizePerConc*(len(r.curBuf)-r.curBufIdx)-r.curBufOffset),
		zap.Int("curBufIdx", r.curBufIdx),
	)
	failpoint.Inject("assertReloadAtMostOnce", func() {
		if r.concurrentReader.reloadCnt > 1 {
			panic(fmt.Sprintf("reloadCnt is %d", r.concurrentReader.reloadCnt))
		}
	})
	r.concurrentReader.largeBufferPool.Destroy()
	r.concurrentReader.largeBuf = nil
	r.concurrentReader.now = false
	reloadCnt = r.concurrentReader.reloadCnt
	r.concurrentReader.reloadCnt = 0
	r.curBuf = [][]byte{r.smallBuf}
	offsetInOldBuffer = r.curBufOffset + r.curBufIdx*r.concurrentReader.bufSizePerConc
	r.curBufOffset = 0
	return
}

func (r *byteReader) Close() error {
	if r.concurrentReader.now {
		r.closeConcurrentReader()
	}
	return r.storageReader.Close()
}

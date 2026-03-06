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

package external

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	goerrors "errors"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/membuf"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func readAllData(
	ctx context.Context,
	store storeapi.Storage,
	dataFiles, statsFiles []string,
	startKey, endKey []byte,
	startOffsets, endOffsets []uint64,
	smallBlockBufPool *membuf.Pool,
	largeBlockBufPool *membuf.Pool,
	output *memKVsAndBuffers,
) (err error) {
	task := log.BeginTask(logutil.Logger(ctx), "read all data")
	task.Info("arguments",
		zap.Int("data-file-count", len(dataFiles)),
		zap.Int("stat-file-count", len(statsFiles)),
		zap.String("start-key", hex.EncodeToString(startKey)),
		zap.String("end-key", hex.EncodeToString(endKey)),
	)
	defer func() {
		if err != nil {
			output.kvsPerFile = nil
			for _, b := range output.memKVBuffers {
				b.Destroy()
			}
			output.memKVBuffers = nil
		} else {
			// try to fix a bug that the memory is retained in http2 package
			if gcs, ok := store.(*objstore.GCSStorage); ok {
				err = gcs.Reset(ctx)
			}
		}
		task.End(zap.ErrorLevel, err)
	}()

	cachedReaders := make([]cachedReader, len(dataFiles))

	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(ctx)
	readConn := 32
	readConn = min(readConn, len(dataFiles))
	taskCh := make(chan int)
	output.memKVBuffers = make([]*membuf.Buffer, readConn*2)
	for readIdx := range readConn {
		eg.Go(func() error {
			output.memKVBuffers[readIdx] = smallBlockBufPool.NewBuffer()
			output.memKVBuffers[readIdx+readConn] = largeBlockBufPool.NewBuffer()
			smallBlockBuf := output.memKVBuffers[readIdx]
			largeBlockBuf := output.memKVBuffers[readIdx+readConn]

			for {
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				case fileIdx, ok := <-taskCh:
					if !ok {
						return nil
					}
					if err := cachedReaders[fileIdx].open(
						egCtx,
						store,
						dataFiles[fileIdx],
						int64(startOffsets[fileIdx]),
						int64(endOffsets[fileIdx]),
						largeBlockBuf,
					); err != nil {
						return err
					}

					if err := cachedReaders[fileIdx].read(
						egCtx,
						startKey,
						endKey,
						smallBlockBuf,
						output,
					); err != nil {
						return errors.Annotatef(err, "failed to read file %s", dataFiles[fileIdx])
					}
				}
			}
		})
	}

	for fileIdx := range dataFiles {
		select {
		case <-egCtx.Done():
			return eg.Wait()
		case taskCh <- fileIdx:
		}
	}
	close(taskCh)
	return eg.Wait()
}

// ReadKVFilesAsync reads multiple KV files asynchronously and sends the KV pairs
// to the returned channel, the channel will be closed when finish read.
func ReadKVFilesAsync(ctx context.Context, eg *util.ErrorGroupWithRecover,
	store storeapi.Storage, files []string) chan *KVPair {
	pairCh := make(chan *KVPair)
	eg.Go(func() error {
		defer close(pairCh)
		for _, file := range files {
			if err := readOneKVFile2Ch(ctx, store, file, pairCh); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	return pairCh
}

func readOneKVFile2Ch(ctx context.Context, store storeapi.Storage, file string, outCh chan *KVPair) error {
	reader, err := NewKVReader(ctx, file, store, 0, 3*DefaultReadBufferSize)
	if err != nil {
		return err
	}
	// if we successfully read all data, it's ok to ignore the error of Close
	//nolint: errcheck
	defer reader.Close()
	for {
		key, val, err := reader.NextKV()
		if err != nil {
			if goerrors.Is(err, io.EOF) {
				break
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case outCh <- &KVPair{
			Key:   bytes.Clone(key),
			Value: bytes.Clone(val),
		}:
		}
	}
	return nil
}

type fileReader interface {
	// nextKV reads the next key-value pair from the file. The key and value
	// will be allocated from the provided membuf.Buffer if needed.
	nextKV(*membuf.Buffer) ([]byte, []byte, error)

	// reserve is used to reserve the last read key-value pair
	// When the key is out of range, we can cache it. So we can skip opening a
	// new reader and seeking when we need to read next time.
	reserve(key, value []byte)

	close() error
}

type sequentialReader struct {
	kvr *KVReader

	reserved    bool
	reservedKey []byte
	reservedVal []byte
}

func newSequentialReader(
	ctx context.Context,
	store storeapi.Storage,
	dataFile string,
	initFileOffset uint64,
) (*sequentialReader, error) {
	kr, err := NewKVReader(ctx, dataFile, store, initFileOffset, 3*DefaultReadBufferSize)
	if err != nil {
		return nil, err
	}

	return &sequentialReader{kvr: kr}, nil
}

func (r *sequentialReader) nextKV(smallBlockBuf *membuf.Buffer) (key, value []byte, err error) {
	if r.reserved {
		r.reserved = false
		return r.reservedKey, r.reservedVal, nil
	}

	k, v, err := r.kvr.NextKV()
	if err != nil {
		return nil, nil, err
	}

	return smallBlockBuf.AddBytes(k), smallBlockBuf.AddBytes(v), nil
}

func (r *sequentialReader) reserve(key, value []byte) {
	r.reserved = true
	r.reservedKey = key
	r.reservedVal = value
}

func (r *sequentialReader) close() (err error) {
	if r.kvr != nil {
		err = r.kvr.Close()
		r.kvr = nil
	}
	return err
}

// concurrentReader reads data from multiple concurrent file range read tasks.
type concurrentReader struct {
	ctx context.Context

	store    storeapi.Storage
	dataFile string

	largeBlockBuf *membuf.Buffer

	startOffset int64
	endOffset   int64
	buffers     [][]byte

	curIndex  int
	curOffset int
}

// newConcurrentReader creates a concurrentReader that reads the specified data file in parallel.
// It divides the file into multiple range read tasks and starts goroutines to read
// each range concurrently. The data is read into buffers allocated from blockBuf.
// It returns a concurrentReader that can be used to read KV pairs from the file.
func newConcurrentReader(
	ctx context.Context,
	store storeapi.Storage,
	dataFile string,
	startOffset, endOffset int64,
	largeBlockBuf *membuf.Buffer,
) (*concurrentReader, error) {
	reader := &concurrentReader{
		ctx:           ctx,
		largeBlockBuf: largeBlockBuf,
		startOffset:   startOffset,
		endOffset:     endOffset,
		store:         store,
		dataFile:      dataFile,
	}
	if err := reader.startOnce(); err != nil {
		return nil, err
	}
	return reader, nil
}

func (r *concurrentReader) startOnce() error {
	eg, egCtx := util.NewErrorGroupWithRecoverWithCtx(r.ctx)

	offset := r.startOffset
	for offset < r.endOffset {
		readSize := min(int64(ConcurrentReaderBufferSizePerConc), r.endOffset-offset)
		block := r.largeBlockBuf.AllocBytes(int(readSize))
		r.buffers = append(r.buffers, block)
		readOffset := offset
		offset += readSize

		eg.Go(func() error {
			_, err := objstore.ReadDataInRange(
				egCtx,
				r.store,
				r.dataFile,
				readOffset,
				block,
			)
			return err
		})
	}

	return eg.Wait()
}

func (r *concurrentReader) close() error {
	return nil
}

// readNBytes reads exactly n bytes from the reader.
// If the requested data is within a single task's buffer, it returns a direct
// slice reference without copying, as the task's buffer is already allocated
// from blockBuf. When the data spans multiple tasks, it allocates a new buffer
// from blockBuf and copies the data from the involved tasks.
func (r *concurrentReader) readNBytes(blockBuf *membuf.Buffer, n int) ([]byte, error) {
	if r.curIndex >= len(r.buffers) {
		return nil, io.EOF
	}

	// First, check if all data is available in the current task
	currentBuffer := r.buffers[r.curIndex]
	availableInCurrent := len(currentBuffer) - r.curOffset
	if availableInCurrent >= n {
		start := r.curOffset
		r.curOffset += n
		if r.curOffset >= len(currentBuffer) {
			r.curIndex++
			r.curOffset = 0
		}
		return currentBuffer[start : start+n], nil
	}

	// Data spans multiple buffers, need to copy to a new buffer
	result := blockBuf.AllocBytes(n)
	copied := 0

	// Copy remaining data from current buffer
	bytesToCopy := availableInCurrent
	copy(result[copied:], currentBuffer[r.curOffset:r.curOffset+bytesToCopy])
	copied += bytesToCopy
	r.curIndex++
	r.curOffset = 0

	// Copy from subsequent buffers
	for copied < n && r.curIndex < len(r.buffers) {
		buffer := r.buffers[r.curIndex]

		availableInBuffer := len(buffer)
		bytesToCopy := min(n-copied, availableInBuffer)
		copy(result[copied:], buffer[0:bytesToCopy])

		copied += bytesToCopy
		r.curOffset = bytesToCopy

		// Move to next buffer
		if r.curOffset >= len(buffer) {
			r.curIndex++
			r.curOffset = 0
		}
	}

	if copied < n {
		return result[:copied], io.EOF
	}

	return result, nil
}

func (r *concurrentReader) nextKV(blockBuf *membuf.Buffer) ([]byte, []byte, error) {
	lenBytes, err := r.readNBytes(blockBuf, 8)
	if err != nil {
		return nil, nil, err
	}
	keyLen := int(binary.BigEndian.Uint64(lenBytes))
	lenBytes, err = r.readNBytes(blockBuf, 8)
	if err != nil {
		return nil, nil, noEOF(err)
	}
	valLen := int(binary.BigEndian.Uint64(lenBytes))
	keyAndValue, err := r.readNBytes(blockBuf, keyLen+valLen)
	if err != nil {
		return nil, nil, noEOF(err)
	}
	return keyAndValue[:keyLen], keyAndValue[keyLen:], nil
}

func (r *concurrentReader) reserve([]byte, []byte) {}

type cachedReader struct {
	ctx              context.Context
	r                fileReader
	lastIsConcurrent bool
}

func (cr *cachedReader) canReuse(isConcurrent bool) bool {
	if isConcurrent || cr.lastIsConcurrent || cr.r == nil {
		return false
	}

	select {
	case <-cr.ctx.Done():
		return false
	default:
		return true
	}
}

func (cr *cachedReader) open(
	ctx context.Context,
	store storeapi.Storage,
	dataFile string,
	startOffset, endOffset int64,
	largeBlockBuf *membuf.Buffer,
) error {
	concurrentRead := false
	bufSize := int64(ConcurrentReaderBufferSizePerConc)
	concurrency := (endOffset - startOffset + bufSize - 1) / bufSize
	if concurrency >= int64(readAllDataConcThreshold) {
		concurrentRead = true
	}

	// only log for files with expected concurrency > 1, to avoid too many logs
	if concurrency > 1 {
		logutil.Logger(ctx).Info("found hotspot file in readOneFile",
			zap.String("filename", dataFile),
			zap.Int64("startOffset", startOffset),
			zap.Int64("endOffset", endOffset),
			zap.Int64("concurrency", concurrency),
		)
	}

	if cr.canReuse(concurrentRead) {
		return nil
	}

	var err error
	if err = cr.close(); err != nil {
		return err
	}

	if concurrentRead {
		cr.r, err = newConcurrentReader(
			ctx,
			store,
			dataFile,
			startOffset,
			endOffset,
			largeBlockBuf,
		)
	} else {
		cr.r, err = newSequentialReader(
			ctx,
			store,
			dataFile,
			uint64(startOffset),
		)
	}

	if err != nil {
		return err
	}

	cr.ctx = ctx
	cr.lastIsConcurrent = concurrentRead
	return nil
}

func (cr *cachedReader) close() (err error) {
	if cr.r != nil {
		err = cr.r.close()
		cr.r = nil
	}
	return err
}

func (cr *cachedReader) read(
	ctx context.Context,
	startKey, endKey []byte,
	smallBlockBuf *membuf.Buffer,
	output *memKVsAndBuffers,
) error {
	readAndSortDurHist := metrics.GlobalSortReadFromCloudStorageDuration.WithLabelValues("read_one_file")
	ts := time.Now()

	kvs := make([]KVPair, 0, 1024)
	size := 0
	droppedSize := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		k, v, err := cr.r.nextKV(smallBlockBuf)
		if err != nil {
			if goerrors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if bytes.Compare(k, startKey) < 0 {
			droppedSize += len(k) + len(v)
			continue
		}
		if bytes.Compare(k, endKey) >= 0 {
			cr.r.reserve(k, v)
			break
		}
		kvs = append(kvs, KVPair{Key: k, Value: v})
		size += len(k) + len(v)
	}
	readAndSortDurHist.Observe(time.Since(ts).Seconds())
	output.mu.Lock()
	output.kvsPerFile = append(output.kvsPerFile, kvs)
	output.size += size
	output.droppedSizePerFile = append(output.droppedSizePerFile, droppedSize)
	output.mu.Unlock()
	return nil
}

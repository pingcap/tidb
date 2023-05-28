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

package extsort

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
	"go.uber.org/atomic"
)

const (
	rawKVSorterStateWriting = iota
	rawKVSorterStateSorted
)

var errRawKVPutSorterPanic = errors.New("rawkv put sorter panic, please retry or switch to disk sort")

// RawKVPutSorter is an external sorter that sorts data in TiKV. It will use
// KV put API to write data to TiKV.
type RawKVPutSorter struct {
	taskName string

	pdAddr   []string
	security config.Security
	keyspace string

	opts  *RawKVPutSorterOptions
	state atomic.Int32
}

// RawKVPutSorterOptions holds the optional parameters for RawKVPutSorter.
type RawKVPutSorterOptions struct {
	// WriterBufferSize is the size of the buffer used by the writer.
	// RawKVPutSorter will buffer the data in memory and write it to TiKV
	// asynchronously, when the WriterBufferSize is reached and there is
	// buffered data, it will block the Writer until the buffered data is
	// flushed to TiKV. If there is no buffered data and even one KV is too
	// large, it will increase the buffer size to fit the KV.
	//
	// The default value is 128MB.
	WriterBufferSize int

	// WriterBufferKeyCount acts like WriterBufferSize, but it is the number of
	// keys instead of the size of the buffer.
	//
	// The default value is 1_000_000.
	WriterBufferKeyCount int

	// AsyncFlushInterval is the interval of flushing buffered data to TiKV at
	// background
	//
	// The default value is 1s
	AsyncFlushInterval time.Duration
}

func (o *RawKVPutSorterOptions) ensureDefaults() {
	if o.WriterBufferSize == 0 {
		o.WriterBufferSize = 128 << 20
	}
	if o.WriterBufferKeyCount == 0 {
		o.WriterBufferKeyCount = 1_000_000
	}
	if o.AsyncFlushInterval == 0 {
		o.AsyncFlushInterval = time.Second
	}
}

// OpenRawKVPutSorter creates a new RawKVPutSorter.
func OpenRawKVPutSorter(
	taskName string,
	pdAddr []string,
	security config.Security,
	keyspace string,
	opts *RawKVPutSorterOptions,
) *RawKVPutSorter {
	if opts == nil {
		opts = &RawKVPutSorterOptions{}
	}
	opts.ensureDefaults()

	return &RawKVPutSorter{
		taskName: taskName,
		pdAddr:   pdAddr,
		security: security,
		keyspace: keyspace,
		opts:     opts,
	}
}

// NewWriter implements the ExternalSorter interface.
func (r *RawKVPutSorter) NewWriter(ctx context.Context) (Writer, error) {
	if r.state.Load() > rawKVSorterStateWriting {
		return nil, errors.Errorf("prohibited to call NewWriter after Sort for RawKVPutSorter")
	}
	cli, err := rawkv.NewClientWithOpts(
		ctx,
		r.pdAddr,
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithSecurity(r.security),
		rawkv.WithKeyspace(r.keyspace))
	if err != nil {
		return nil, errors.Trace(err)
	}

	w := newRawKVPutWriter(ctx, cli, r.taskName, r.opts)

	w.wg.Add(1)
	go w.asyncFlushLoop()
	return w, nil
}

func newRawKVPutWriter(
	ctx context.Context,
	cli rawKVClient,
	taskName string,
	opts *RawKVPutSorterOptions,
) *rawKVPutWriter {
	ret := &rawKVPutWriter{
		ctx:      ctx,
		cli:      cli,
		taskName: taskName,
		opts:     opts,
		done:     make(chan struct{}),
	}
	ret.kvsBuf.kvs = make([]keyValueWithEnd, opts.WriterBufferKeyCount)
	ret.dataBuf.data = make([]byte, opts.WriterBufferSize)
	return ret
}

type keyValueWithEnd struct {
	keyValue
	end int // exclusive, and dataBuf[end] must be a valid
}

type rawKVPutWriter struct {
	ctx      context.Context
	cli      rawKVClient
	taskName string
	opts     *RawKVPutSorterOptions

	kvsBuf struct {
		// kvs is a circular buffer
		kvs []keyValueWithEnd
		// start is inclusive and end is exclusive, which means
		//   start == end: empty
		//   end - start == capacity - 1 (mod capacity): full
		// note that the kvs[end] is always not used
		start, end atomic.Int64
	}
	dataBuf struct {
		// same as kvsBuf
		data       []byte
		start, end atomic.Int64
	}

	done chan struct{}
	wg   sync.WaitGroup
	// flushMu is used to protect flush function, it will be called by two
	// goroutines: asyncFlushLoop and Put. Put's path must flush all data and
	// asyncFlushLoop's path try to flush seen data.
	flushMu sync.Mutex
}

type rawKVClient interface {
	BatchPut(ctx context.Context, keys [][]byte, values [][]byte, options ...rawkv.RawOption) error
	Close() error
}

func (w *rawKVPutWriter) asyncFlushLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.opts.AsyncFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
		}
		locked := w.flushMu.TryLock()
		if !locked {
			// skip when Put triggers flush
			continue
		}
		// ignore the error because the error maybe transient
		_ = w.flush()
		w.flushMu.Unlock()
	}
}

// tryAllocate tries to allocate a continuous space from w.dataBuf.
func (w *rawKVPutWriter) tryAllocate(
	startOfFree int,
	allocLen int,
) (startOfAllocated int, ok bool) {
	startOfData := int(w.dataBuf.start.Load())
	// when allocating, we will meet startOfData first
	if startOfFree < startOfData {
		// allocate from [startOfFree:startOfData-2]. the "1" offset is
		// because we define dataBuf[end] must not be used.
		if startOfFree+allocLen < startOfData {
			return startOfFree, true
		}
		return 0, false
	}
	// when allocating, we will meet end of buffer first, and the remaining
	// free space is enough to store the data and ending sentinel
	if startOfFree+allocLen < len(w.dataBuf.data) {
		return startOfFree, true
	}
	// when allocating, we will meet end of buffer first, and the remaining
	// free space is only enough to store the data. The ending sentinel
	// is at the beginning of the buffer.
	if startOfFree+allocLen == len(w.dataBuf.data) {
		if startOfData == 0 {
			return 0, false
		}
		return startOfFree, true
	}
	// try to allocate from the beginning of the buffer
	if allocLen < startOfData {
		return 0, true
	}
	return 0, false
}

// tryStoreKV tries to copy the key and value to w.dataBuf and insert a
// keyValueWithEnd to w.kvsBuf whose members point to w.dataBuf. The key will be
// attached with w.taskName and '/' at rest.
// When it returns false, it means the w.kvsBuf is full and the caller should
// call flush to clear the w.kvsBuf.
func (w *rawKVPutWriter) tryStoreKeyValue(
	startOfKey int,
	key []byte,
	startOfValue int,
	value []byte,
) bool {
	startOfKvs, endOfKvs := int(w.kvsBuf.start.Load()), int(w.kvsBuf.end.Load())
	if (endOfKvs+1)%len(w.kvsBuf.kvs) == startOfKvs {
		return false
	}

	// stored key is "${taskName}/${key}"
	keyLen := len(w.taskName) + 1 + len(key)
	var kv keyValueWithEnd
	kv.key = w.dataBuf.data[startOfKey : startOfKey+keyLen]
	copy(kv.key, w.taskName)
	kv.key[len(w.taskName)] = '/'
	copy(kv.key[len(w.taskName)+1:], key)

	kv.value = w.dataBuf.data[startOfValue : startOfValue+len(value)]
	copy(kv.value, value)

	kvEndOffset := startOfValue + len(value)
	// dataBuf[kv.end] must be valid
	if kvEndOffset == len(w.dataBuf.data) {
		kvEndOffset = 0
	}
	kv.end = kvEndOffset
	w.kvsBuf.kvs[endOfKvs] = kv
	return true
}

// Put implements the Writer interface.
func (w *rawKVPutWriter) Put(key, value []byte) error {
	return w.put(key, value)
}

// put stores the "${taskName}/${key}" and "${value}" to the buffer.
// it is concurrency safe with asyncFlushLoop.
func (w *rawKVPutWriter) put(key, value []byte) error {
	// stored key is "${taskName}/${key}"
	keyLen := len(w.taskName) + 1 + len(key)

	endOfData := int(w.dataBuf.end.Load())

	var (
		startOfKey, startOfValue int
		ok                       bool
	)
	startOfKey, ok = w.tryAllocate(endOfData, keyLen)
	if ok {
		startOfValue, ok = w.tryAllocate(startOfKey+keyLen, len(value))
	}
	if ok {
		ok = w.tryStoreKeyValue(startOfKey, key, startOfValue, value)
	}
	if ok {
		goto commit
	}

	// need a force flush to make room for new data
	w.flushMu.Lock()
	if err := w.flush(); err != nil {
		w.flushMu.Unlock()
		return errors.Trace(err)
	}
	w.flushMu.Unlock()

	// reset start/end of dataBuf and kvsBuf to 0, in case the start/end is at
	// the middle of the buffer and only half of buffer can be allocated
	// continuously.
	w.kvsBuf.start.Store(0)
	w.kvsBuf.end.Store(0)
	w.dataBuf.start.Store(0)
	w.dataBuf.end.Store(0)

	if keyLen+len(value)+1 > len(w.dataBuf.data) {
		// because this function is the only writer to dataBuf, after above
		// flush, we can drop the old dataBuf.
		w.dataBuf.data = make([]byte, keyLen+len(value)+1)
	}
	if startOfKey, ok = w.tryAllocate(0, keyLen); !ok {
		return errors.Trace(errRawKVPutSorterPanic)
	}
	if startOfValue, ok = w.tryAllocate(startOfKey+keyLen, len(value)); !ok {
		return errors.Trace(errRawKVPutSorterPanic)
	}
	if !w.tryStoreKeyValue(startOfKey, key, startOfValue, value) {
		return errors.Trace(errRawKVPutSorterPanic)
	}

commit:
	// update the end of buffers. Update kvsBuf at last. OTHERWISE,
	// asyncFlushLoop will move the start of dataBuf larger than end of
	// dataBuf before updating dataBuf, which is error-prone.
	w.dataBuf.end.Store(int64(startOfValue + len(value)))
	endOfKvs := w.kvsBuf.end.Load()
	w.kvsBuf.end.Store((endOfKvs + 1) % int64(len(w.kvsBuf.kvs)))
	return nil
}

// flush is not concurrent safe with itself.
func (w *rawKVPutWriter) flush() error {
	start, end := w.kvsBuf.start.Load(), w.kvsBuf.end.Load()
	kvCnt := int(end - start)
	if kvCnt < 0 {
		kvCnt += len(w.kvsBuf.kvs)
	}
	if kvCnt == 0 {
		return nil
	}

	keys := make([][]byte, 0, kvCnt)
	values := make([][]byte, 0, kvCnt)
	nextStartOfData := 0

	for start != end {
		keys = append(keys, w.kvsBuf.kvs[start].key)
		values = append(values, w.kvsBuf.kvs[start].value)
		// if the value of w.kvsBuf.kvs[start] and key of w.kvsBuf.kvs[start+1]
		// is not stored adjacently because the trailing free space of circular
		// buffer is not enough, nextStartOfData is smaller than the real "next
		// start of data". But it should be ok.
		nextStartOfData = w.kvsBuf.kvs[start].end
		start = (start + 1) % int64(len(w.kvsBuf.kvs))
	}

	// TODO: check API size limit
	if err := w.cli.BatchPut(w.ctx, keys, values); err != nil {
		return errors.Trace(err)
	}

	// the order is not important, but since we first check dataBuf we also
	// release dataBuf first, in case w.put thinks there is no enough space
	w.dataBuf.start.Store(int64(nextStartOfData))
	w.kvsBuf.start.Store(end)
	return nil
}

// Flush implements the Writer interface.
func (w *rawKVPutWriter) Flush() error {
	w.flushMu.Lock()
	if err := w.flush(); err != nil {
		w.flushMu.Unlock()
		return errors.Trace(err)
	}
	w.flushMu.Unlock()
	return nil
}

// Close implements the Writer interface.
func (w *rawKVPutWriter) Close() error {
	close(w.done)
	w.wg.Wait()
	if err := w.flush(); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(w.cli.Close())
}

// Sort implements the ExternalSorter interface.
func (r *RawKVPutSorter) Sort(_ context.Context) error {
	// TiKV stores data in sorted order, so we don't need to sort it.
	r.state.Store(rawKVSorterStateSorted)
	return nil
}

// IsSorted implements the ExternalSorter interface.
func (r *RawKVPutSorter) IsSorted() bool {
	return r.state.Load() == rawKVSorterStateSorted
}

// NewIterator implements the ExternalSorter interface.
func (r *RawKVPutSorter) NewIterator(_ context.Context) (Iterator, error) {
	if r.state.Load() != rawKVSorterStateSorted {
		return nil, errors.Errorf("RawKVPutSorter is not sorted")
	}
	panic("implement me")
}

// Close implements the ExternalSorter interface.
func (_ *RawKVPutSorter) Close() error {
	//TODO implement me
	panic("implement me")
}

// CloseAndCleanup implements the ExternalSorter interface.
func (_ *RawKVPutSorter) CloseAndCleanup() error {
	//TODO implement me
	panic("implement me")
}

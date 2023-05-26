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

// RawKVPutSorter is an external sorter that sorts data in TiKV. It will use
// KV put API to write data to TiKV.
type RawKVPutSorter struct {
	taskName string

	pdAddr []string
	opts   *RawKVPutSorterOptions

	state atomic.Int32
}

// RawKVPutSorterOptions holds the optional parameters for RawKVPutSorter.
type RawKVPutSorterOptions struct {
	// Security configures the security settings for the client.
	Security config.Security

	// WriterBufferSize is the size of the buffer used by the writer.
	// RawKVPutSorter will buffer the data in memory and write it to TiKV
	// asynchronously, when the WriterBufferSize is reached and there is
	// buffered data, it will block the Writer until the buffered data is
	// flushed to TiKV. If there is no buffered data and even one KV is too
	// large, it will increase the buffer size to fit the KV.
	//
	// The default value is 128MB.
	WriterBufferSize int

	// Keyspace is the tenant keyspace name.
	Keyspace string
}

func (o *RawKVPutSorterOptions) ensureDefaults() {
	if o.WriterBufferSize == 0 {
		o.WriterBufferSize = 128 << 20
	}
}

// OpenRawKVPutSorter creates a new RawKVPutSorter.
func OpenRawKVPutSorter(
	taskName string,
	pdAddr []string,
	opts *RawKVPutSorterOptions,
) *RawKVPutSorter {
	if opts == nil {
		opts = &RawKVPutSorterOptions{}
	}
	opts.ensureDefaults()

	return &RawKVPutSorter{
		taskName: taskName,
		pdAddr:   pdAddr,
		opts:     opts,
	}
}

// NewWriter implements the ExternalSorter interface.
func (r *RawKVPutSorter) NewWriter(ctx context.Context) (Writer, error) {
	if r.state.Load() > rawKVSorterStateWriting {
		return nil, errors.Errorf("prohibited to call NewWriter after Sort for RawKVPutSorter")
	}
	cli, err := rawkv.NewClientWithOpts(
		ctx, r.pdAddr,
		rawkv.WithAPIVersion(kvrpcpb.APIVersion_V2),
		rawkv.WithSecurity(r.opts.Security),
		rawkv.WithKeyspace(r.opts.Keyspace))
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := &rawKVPutWriter{
		ctx:      ctx,
		cli:      cli,
		taskName: r.taskName,
		dataBuf:  make([]byte, 0, r.opts.WriterBufferSize),

		done:             make(chan struct{}),
		notifyAsyncFlush: make(chan struct{}, 1),
	}
	go ret.asyncFlushLoop()
	return ret, nil
}

type rawKVPutWriter struct {
	ctx      context.Context
	cli      *rawkv.Client
	taskName string

	kvsBuf struct {
		// mu is used to protect kvs and capacity
		mu sync.Mutex

		// kvs is circular buffer
		kvs      []keyValue
		capacity int
		// start is inclusive and end is exclusive, which means
		//   start == end: empty
		//   end - start == capacity - 1 (mod capacity): full
		// note that the kvs[end] is always not used
		start, end atomic.Int64
	}
	dataBuf []byte
	off     int

	done chan struct{}
	// flushMu is used to protect flush function, it will be called by two
	// goroutines: asyncFlushLoop and Put. Put's path must flush all data and
	// asyncFlushLoop's path try to flush seen data.
	flushMu          sync.Mutex
	notifyAsyncFlush chan struct{}
}

func (w *rawKVPutWriter) Put(key, value []byte) error {
	return w.put(key, value)
}

// put is concurrency safe with asyncFlushLoop
// TODO: this function has many duplicated code with diskSorterWriter
func (w *rawKVPutWriter) put(key, value []byte) error {
	// check if dataBuf is full
	if w.off+len(key)+len(value) > len(w.dataBuf) {
		w.flushMu.Lock()
		if err := w.flush(); err != nil {
			w.flushMu.Unlock()
			return errors.Trace(err)
		}
		w.flushMu.Unlock()

		// the only writer to dataBuf/kvsBuf is put, so we can say all data is
		// flushed
		w.off = 0

		// The default buffer is too small, enlarge it to fit the key and value.
		if w.off+len(key)+len(value) > len(w.dataBuf) {
			w.dataBuf = make([]byte, w.off+len(key)+len(value))
		}
	}

	// generate keyValue variable for this invocation
	var kv keyValue
	kv.key = w.dataBuf[w.off : w.off+len(key)]
	w.off += len(key)
	copy(kv.key, key)
	if len(value) > 0 {
		kv.value = w.dataBuf[w.off : w.off+len(value)]
		w.off += len(value)
		copy(kv.value, value)
	}

	// try to put kv into kvsBuf. If it's full, grow the kvsBuf
	start, end := w.kvsBuf.start.Load(), w.kvsBuf.end.Load()
	if (end+1)%int64(w.kvsBuf.capacity) == start {
		w.kvsBuf.mu.Lock()
		defer w.kvsBuf.mu.Unlock()
		// reacquire start and end because it may be changed by asyncFlushLoop
		start, end = w.kvsBuf.start.Load(), w.kvsBuf.end.Load()
		if (end+1)%int64(w.kvsBuf.capacity) == start {
			newKVs := make([]keyValue, 2*w.kvsBuf.capacity)
			copy(newKVs, w.kvsBuf.kvs[start:])
			copy(newKVs[w.kvsBuf.capacity-int(start):], w.kvsBuf.kvs[:end])

			w.kvsBuf.kvs = newKVs
			w.kvsBuf.start.Store(0)
			w.kvsBuf.end.Store(int64(w.kvsBuf.capacity - 1))
			start, end = 0, int64(w.kvsBuf.capacity-1)
			w.kvsBuf.capacity = len(newKVs)
		}
	}

	end = (end + 1) % int64(w.kvsBuf.capacity)
	w.kvsBuf.kvs[end] = kv
	w.kvsBuf.end.Store(end)
	return nil
}

// flush must be called with w.flushMu held.
func (w *rawKVPutWriter) flush() error {
	w.kvsBuf.mu.Lock()
	defer w.kvsBuf.mu.Unlock()

	start, end := w.kvsBuf.start.Load(), w.kvsBuf.end.Load()
	if start != end {
		size := int(end - start)
		if size < 0 {
			size += w.kvsBuf.capacity
		}
		keys := make([][]byte, 0, size)
		values := make([][]byte, 0, size)

		for start != end {
			kv := w.kvsBuf.kvs[start]

			key := make([]byte, len(w.taskName)+1+len(kv.key))
			copy(key, w.taskName)
			key[len(w.taskName)] = '/'
			copy(key[len(w.taskName)+1:], kv.key)
			keys = append(keys, key)

			values = append(values, kv.value)

			start = (start + 1) % int64(w.kvsBuf.capacity)
		}
		// TODO: check API size limit
		if err := w.cli.BatchPut(w.ctx, keys, values); err != nil {
			return errors.Trace(err)
		}
		w.kvsBuf.start.Store(end)
	}
	return nil
}

func (w *rawKVPutWriter) asyncFlushLoop() {
	for {
		select {
		case <-w.done:
			return
		case <-w.notifyAsyncFlush:
		}
		panic("implement me")
	}
}

func (w *rawKVPutWriter) Flush() error {
	//TODO implement me
	panic("implement me")
}

func (w *rawKVPutWriter) Close() error {
	//TODO implement me
	panic("implement me")
	return w.cli.Close()
}

func (r *RawKVPutSorter) Sort(_ context.Context) error {
	// TiKV stores data in sorted order, so we don't need to sort it.
	r.state.Store(rawKVSorterStateSorted)
	return nil
}

func (r *RawKVPutSorter) IsSorted() bool {
	return r.state.Load() == rawKVSorterStateSorted
}

func (r *RawKVPutSorter) NewIterator(ctx context.Context) (Iterator, error) {
	if r.state.Load() != rawKVSorterStateSorted {
		return nil, errors.Errorf("RawKVPutSorter is not sorted")
	}
	panic("implement me")
}

func (r *RawKVPutSorter) Close() error {
	//TODO implement me
	panic("implement me")
}

func (r *RawKVPutSorter) CloseAndCleanup() error {
	//TODO implement me
	panic("implement me")
}

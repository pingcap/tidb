// Copyright 2021 PingCAP, Inc.
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

package local

import (
	"bytes"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"go.uber.org/zap"
)

var errorUnorderedSSTInsertion = errors.New("inserting KVs into SST without order")

type sstWriter struct {
	*sstMeta
	writer *sstable.Writer

	// To dedup keys before write them into the SST file.
	// NOTE: keys should be sorted and deduped when construct one SST file.
	lastKey []byte

	logger log.Logger
}

func newSSTWriter(path string, blockSize int) (*sstable.Writer, error) {
	f, err := vfs.Default.Create(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Logic to ensure the default block size is set to 16KB.
	// If a smaller block size is used (e.g., 4KB, the default for Pebble),
	// a single large SST file may generate a disproportionately large index block,
	// potentially causing a memory spike and leading to an Out of Memory (OOM) scenario.
	// If the user specifies a smaller block size, respect their choice.
	if blockSize <= 0 {
		blockSize = config.DefaultBlockSize
	}

	writable := objstorageprovider.NewFileWritable(f)
	writer := sstable.NewWriter(writable, sstable.WriterOptions{
		TablePropertyCollectors: []func() pebble.TablePropertyCollector{
			newRangePropertiesCollector,
		},
		BlockSize: blockSize,
	})
	return writer, nil
}

func (sw *sstWriter) writeKVs(kvs []common.KvPair) error {
	if len(kvs) == 0 {
		return nil
	}
	if len(sw.minKey) == 0 {
		sw.minKey = slices.Clone(kvs[0].Key)
	}
	if bytes.Compare(kvs[0].Key, sw.maxKey) <= 0 {
		return errorUnorderedSSTInsertion
	}

	internalKey := sstable.InternalKey{
		Trailer: uint64(sstable.InternalKeyKindSet),
	}
	for _, p := range kvs {
		if sw.lastKey != nil && bytes.Equal(p.Key, sw.lastKey) {
			sw.logger.Warn("duplicated key found, skip write", logutil.Key("key", p.Key))
			continue
		}
		internalKey.UserKey = p.Key
		if err := sw.writer.Add(internalKey, p.Val); err != nil {
			return errors.Trace(err)
		}
		sw.totalSize += int64(len(p.Key)) + int64(len(p.Val))
		sw.lastKey = p.Key
	}
	sw.totalCount += int64(len(kvs))
	sw.maxKey = append(sw.maxKey[:0], sw.lastKey...)
	return nil
}

func (sw *sstWriter) close() (*sstMeta, error) {
	if err := sw.writer.Close(); err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := sw.writer.Metadata()
	if err != nil {
		return nil, errors.Trace(err)
	}
	sw.fileSize = int64(meta.Size)
	return sw.sstMeta, nil
}

type sstIter struct {
	name   string
	key    []byte
	val    []byte
	iter   sstable.Iterator
	reader *sstable.Reader
	valid  bool
}

// Close implements common.Iterator.
func (i *sstIter) Close() error {
	if err := i.iter.Close(); err != nil {
		return errors.Trace(err)
	}
	err := i.reader.Close()
	return errors.Trace(err)
}

type sstIterHeap struct {
	iters []*sstIter
}

// Len implements heap.Interface.
func (h *sstIterHeap) Len() int {
	return len(h.iters)
}

// Less implements heap.Interface.
func (h *sstIterHeap) Less(i, j int) bool {
	return bytes.Compare(h.iters[i].key, h.iters[j].key) < 0
}

// Swap implements heap.Interface.
func (h *sstIterHeap) Swap(i, j int) {
	h.iters[i], h.iters[j] = h.iters[j], h.iters[i]
}

// Push implements heap.Interface.
func (h *sstIterHeap) Push(x any) {
	h.iters = append(h.iters, x.(*sstIter))
}

// Pop implements heap.Interface.
func (h *sstIterHeap) Pop() any {
	item := h.iters[len(h.iters)-1]
	h.iters = h.iters[:len(h.iters)-1]
	return item
}

// Next implements common.Iterator.
func (h *sstIterHeap) Next() (key, val []byte, err error) {
	for {
		if len(h.iters) == 0 {
			return nil, nil, nil
		}

		iter := h.iters[0]
		if iter.valid {
			iter.valid = false
			return iter.key, iter.val, iter.iter.Error()
		}

		var k *pebble.InternalKey
		var v pebble.LazyValue
		k, v = iter.iter.Next()

		if k != nil {
			vBytes, _, err := v.Value(nil)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			iter.key = k.UserKey
			iter.val = vBytes
			iter.valid = true
			heap.Fix(h, 0)
		} else {
			err := iter.Close()
			heap.Remove(h, 0)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
		}
	}
}

// sstIngester is a interface used to merge and ingest SST files.
// it's a interface mainly used for test convenience
type sstIngester interface {
	mergeSSTs(metas []*sstMeta, dir string, blockSize int) (*sstMeta, error)
	ingest([]*sstMeta) error
}

type dbSSTIngester struct {
	e *Engine
}

func (i dbSSTIngester) mergeSSTs(metas []*sstMeta, dir string, blockSize int) (*sstMeta, error) {
	failpoint.InjectCall("beforeMergeSSTs")
	failpoint.Inject("mockErrInMergeSSTs", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mocked error in mergeSSTs"))
		}
	})

	if len(metas) == 0 {
		return nil, errors.New("sst metas is empty")
	} else if len(metas) == 1 {
		return metas[0], nil
	}

	start := time.Now()
	newMeta := &sstMeta{
		seq: metas[len(metas)-1].seq,
	}
	mergeIter := &sstIterHeap{
		iters: make([]*sstIter, 0, len(metas)),
	}

	for _, p := range metas {
		f, err := vfs.Default.Open(p.path)
		if err != nil {
			return nil, errors.Trace(err)
		}
		readable, err := sstable.NewSimpleReadable(f)
		if err != nil {
			return nil, errors.Trace(err)
		}
		reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
		if err != nil {
			return nil, errors.Trace(err)
		}
		iter, err := reader.NewIter(nil, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		key, val := iter.Next()
		if key == nil {
			continue
		}
		valBytes, _, err := val.Value(nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if iter.Error() != nil {
			return nil, errors.Trace(iter.Error())
		}
		mergeIter.iters = append(mergeIter.iters, &sstIter{
			name:   p.path,
			iter:   iter,
			key:    key.UserKey,
			val:    valBytes,
			reader: reader,
			valid:  true,
		})
		newMeta.totalSize += p.totalSize
		newMeta.totalCount += p.totalCount
	}
	heap.Init(mergeIter)

	name := filepath.Join(dir, fmt.Sprintf("%s.sst", uuid.New()))
	writer, err := newSSTWriter(name, blockSize)
	if err != nil {
		return nil, errors.Trace(err)
	}
	newMeta.path = name

	internalKey := sstable.InternalKey{
		Trailer: uint64(sstable.InternalKeyKindSet),
	}
	key, val, err := mergeIter.Next()
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, errors.New("all ssts are empty")
	}
	newMeta.minKey = append(newMeta.minKey[:0], key...)
	lastKey := make([]byte, 0)
	for {
		if bytes.Equal(lastKey, key) {
			i.e.logger.Warn("duplicated key found, skipped", zap.Binary("key", lastKey))
			newMeta.totalCount--
			newMeta.totalSize -= int64(len(key) + len(val))

			goto nextKey
		}
		internalKey.UserKey = key
		err = writer.Add(internalKey, val)
		if err != nil {
			return nil, err
		}
		lastKey = append(lastKey[:0], key...)
	nextKey:
		key, val, err = mergeIter.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
	}
	err = writer.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}
	meta, err := writer.Metadata()
	if err != nil {
		return nil, errors.Trace(err)
	}
	newMeta.maxKey = lastKey
	newMeta.fileSize = int64(meta.Size)

	dur := time.Since(start)
	i.e.logger.Info("compact sst", zap.Int("fileCount", len(metas)), zap.Int64("size", newMeta.totalSize),
		zap.Int64("count", newMeta.totalCount), zap.Duration("cost", dur), zap.String("file", name))

	// async clean raw SSTs.
	go func() {
		totalSize := int64(0)
		for _, m := range metas {
			totalSize += m.fileSize
			if err := os.Remove(m.path); err != nil {
				i.e.logger.Warn("async cleanup sst file failed", zap.Error(err))
			}
		}
		// decrease the pending size after clean up
		i.e.pendingFileSize.Sub(totalSize)
	}()

	return newMeta, err
}

func (i dbSSTIngester) ingest(metas []*sstMeta) error {
	if len(metas) == 0 {
		return nil
	}
	paths := make([]string, 0, len(metas))
	for _, m := range metas {
		paths = append(paths, m.path)
	}
	db := i.e.getDB()
	if db == nil {
		return errorEngineClosed
	}
	return db.Ingest(paths)
}

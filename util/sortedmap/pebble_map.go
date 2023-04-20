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

package sortedmap

import (
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/arena"
	"golang.org/x/exp/slices"
)

// PebbleMap is a SortedMap implementation based on pebble.
type PebbleMap struct {
	db     *pebble.DB
	opts   *pebble.Options
	tmpDir string        // temporary directory for sst files
	idGen  *atomic.Int64 // id generator for sst files
}

// OpenPebbleMap opens a PebbleMap in the given directory.
func OpenPebbleMap(dirname string, opts *pebble.Options) (*PebbleMap, error) {
	opts = opts.EnsureDefaults()

	dbDir := filepath.Join(dirname, "db")
	tmpDir := filepath.Join(dirname, "tmp")

	// Clean up the temporary directory.
	if err := opts.FS.RemoveAll(tmpDir); err != nil {
		return nil, errors.Trace(err)
	}
	if err := opts.FS.MkdirAll(tmpDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	db, err := pebble.Open(dbDir, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &PebbleMap{
		db:     db,
		opts:   opts,
		tmpDir: tmpDir,
		idGen:  new(atomic.Int64),
	}, nil
}

// NewIterator implements the SortedMap.NewIterator.
func (p *PebbleMap) NewIterator() Iterator {
	return &pebbleMapIterator{iter: p.db.NewIter(nil)}
}

// NewWriter implements the SortedMap.NewWriter.
func (p *PebbleMap) NewWriter() Writer {
	return &pebbleMapWriter{
		m:     p,
		alloc: arena.NewAllocator(32 * 1024),
	}
}

// Close implements the SortedMap.Close.
func (p *PebbleMap) Close() error {
	return errors.Trace(p.db.Close())
}

type pebbleMapIterator struct{ iter *pebble.Iterator }

func (p *pebbleMapIterator) Seek(key []byte) bool { return p.iter.SeekGE(key) }
func (p *pebbleMapIterator) First() bool          { return p.iter.First() }
func (p *pebbleMapIterator) Next() bool           { return p.iter.Next() }
func (p *pebbleMapIterator) Last() bool           { return p.iter.Last() }
func (p *pebbleMapIterator) Valid() bool          { return p.iter.Valid() }
func (p *pebbleMapIterator) Error() error         { return p.iter.Error() }
func (p *pebbleMapIterator) UnsafeKey() []byte    { return p.iter.Key() }
func (p *pebbleMapIterator) UnsafeValue() []byte  { return p.iter.Value() }
func (p *pebbleMapIterator) Close() error         { return p.iter.Close() }

type keyValue struct {
	key   []byte
	value []byte
}

type pebbleMapWriter struct {
	m     *PebbleMap
	kvs   []keyValue
	alloc arena.Allocator
	size  int64
}

func (w *pebbleMapWriter) Put(key, value []byte) error {
	kv := keyValue{}
	kv.key = w.alloc.AllocWithLen(len(key), len(key))
	copy(kv.key, key)
	kv.value = w.alloc.AllocWithLen(len(value), len(value))
	copy(kv.value, value)
	w.kvs = append(w.kvs, kv)
	w.size += int64(len(key) + len(value))
	return nil
}

func (w *pebbleMapWriter) Size() int64 {
	return w.size
}

func (w *pebbleMapWriter) Flush() error {
	if len(w.kvs) == 0 {
		return nil
	}
	fs := w.m.opts.FS
	comparer := w.m.opts.Comparer

	filename := fmt.Sprintf("%d.sst", w.m.idGen.Add(1))
	sstPath := filepath.Join(w.m.tmpDir, filename)

	defer func() {
		if _, err := fs.Stat(sstPath); err == nil {
			_ = fs.Remove(sstPath)
		}
	}()

	f, err := fs.Create(sstPath)
	if err != nil {
		return errors.Trace(err)
	}

	slices.SortFunc(w.kvs, func(a, b keyValue) bool {
		return comparer.Compare(a.key, b.key) < 0
	})

	sstWriter := sstable.NewWriter(f, sstable.WriterOptions{
		Comparer: comparer,
	})
	for _, kv := range w.kvs {
		if err := sstWriter.Set(kv.key, kv.value); err != nil {
			_ = sstWriter.Close()
			return errors.Trace(err)
		}
	}
	if err := sstWriter.Close(); err != nil {
		return errors.Trace(err)
	}

	if err := w.m.db.Ingest([]string{sstPath}); err != nil {
		return errors.Trace(err)
	}

	w.kvs = nil
	w.size = 0
	w.alloc.Reset()
	return nil
}

func (w *pebbleMapWriter) Close() error {
	return w.Flush()
}

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

package extsort

import (
	"context"
	goerrors "errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/arena"
	"golang.org/x/exp/slices"
)

// PebbleSorter is an external sorter based on pebble.
type PebbleSorter struct {
	db     *pebble.DB
	opts   *pebble.Options
	dbDir  string
	tmpDir string        // temporary directory for sst files
	idGen  *atomic.Int64 // id generator for sst files
	sorted bool
}

// DefaultPebbleOptions returns the default options for pebble sorter.
func DefaultPebbleOptions() *pebble.Options {
	return &pebble.Options{
		DisableWAL:               true,
		L0CompactionThreshold:    math.MaxInt,
		L0StopWritesThreshold:    math.MaxInt,
		MaxConcurrentCompactions: runtime.GOMAXPROCS(0),
	}
}

// OpenPebbleSorter opens a pebble sorter with the given directory and options.
func OpenPebbleSorter(dirname string, opts *pebble.Options) (*PebbleSorter, error) {
	if opts == nil {
		opts = DefaultPebbleOptions()
	}
	opts.EnsureDefaults()

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
	return &PebbleSorter{
		db:     db,
		opts:   opts,
		dbDir:  dbDir,
		tmpDir: tmpDir,
		idGen:  new(atomic.Int64),
	}, nil
}

// NewWriter implements the ExternalSorter.NewWriter.
func (p *PebbleSorter) NewWriter(_ context.Context) (Writer, error) {
	if p.sorted {
		return nil, errors.Trace(ErrSorted)
	}
	return &pebbleSorterWriter{
		m:     p,
		alloc: arena.NewAllocator(1 << 20),
	}, nil
}

// Sort implements the ExternalSorter.Sort.
func (p *PebbleSorter) Sort(_ context.Context) error {
	if p.sorted {
		return nil
	}

	iter := p.db.NewIter(nil)
	if !iter.Last() {
		// No keys or any error occurred.
		_ = iter.Close()
		return errors.Trace(iter.Error())
	}
	end := slices.Clone(iter.Key())
	if err := iter.Close(); err != nil {
		return errors.Trace(err)
	}

	// Compact treats end as an exclusive bound,
	// it doesn't matter since correctness is not affected.
	err := p.db.Compact(nil, end)
	if err != nil {
		return errors.Trace(err)
	}

	p.sorted = true
	return nil
}

// NewIterator implements the ExternalSorter.NewIterator.
func (p *PebbleSorter) NewIterator(_ context.Context) (Iterator, error) {
	if !p.sorted {
		return nil, errors.Trace(ErrNotSorted)
	}
	return &PebbleSorterIterator{iter: p.db.NewIter(nil)}, nil
}

// Close implements the ExternalSorter.Close.
func (p *PebbleSorter) Close() error {
	return errors.Trace(p.db.Close())
}

// CloseAndCleanup implements the ExternalSorter.CloseAndCleanup.
func (p *PebbleSorter) CloseAndCleanup() error {
	if err := p.Close(); err != nil {
		return errors.Trace(err)
	}
	fs := p.opts.FS
	err1 := fs.RemoveAll(p.dbDir)
	err2 := fs.RemoveAll(p.tmpDir)
	err := goerrors.Join(err1, err2)
	return errors.Trace(err)
}

type PebbleSorterIterator struct{ iter *pebble.Iterator }

func (p *PebbleSorterIterator) Seek(key []byte) bool { return p.iter.SeekGE(key) }
func (p *PebbleSorterIterator) First() bool          { return p.iter.First() }
func (p *PebbleSorterIterator) Next() bool           { return p.iter.Next() }
func (p *PebbleSorterIterator) Last() bool           { return p.iter.Last() }
func (p *PebbleSorterIterator) Valid() bool          { return p.iter.Valid() }
func (p *PebbleSorterIterator) Error() error         { return p.iter.Error() }
func (p *PebbleSorterIterator) UnsafeKey() []byte    { return p.iter.Key() }
func (p *PebbleSorterIterator) UnsafeValue() []byte  { return p.iter.Value() }
func (p *PebbleSorterIterator) Close() error         { return p.iter.Close() }

type keyValue struct {
	key   []byte
	value []byte
}

type pebbleSorterWriter struct {
	m     *PebbleSorter
	kvs   []keyValue
	alloc arena.Allocator
	size  int64
}

func (w *pebbleSorterWriter) Put(key, value []byte) error {
	kv := keyValue{}
	kv.key = w.alloc.AllocWithLen(len(key), len(key))
	copy(kv.key, key)
	if len(value) > 0 {
		kv.value = w.alloc.AllocWithLen(len(value), len(value))
		copy(kv.value, value)
	}
	w.kvs = append(w.kvs, kv)
	w.size += int64(len(key) + len(value))
	return nil
}

func (w *pebbleSorterWriter) Size() int64 {
	return w.size
}

func (w *pebbleSorterWriter) Flush() error {
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

func (w *pebbleSorterWriter) Close() error {
	return w.Flush()
}

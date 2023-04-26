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
	"golang.org/x/exp/slices"
)

// DiskSorter is an external sorter that sorts data on disk.
type DiskSorter struct {
	db     *pebble.DB
	dbOpts *pebble.Options

	opts   *DiskSorterOptions
	dbDir  string // directory for the pebble database
	tmpDir string // directory for temporary files

	idGen  *atomic.Int64 // id generator for sst files
	sorted bool
}

// DiskSorterOptions holds the optional parameters for DiskSorter.
type DiskSorterOptions struct {
	// Concurrency is the maximum number of goroutines that can be used to
	// sort data in parallel.
	//
	// The default value is runtime.GOMAXPROCS(0).
	Concurrency int

	// WriterBufferSize is the size of the buffer used by the writer.
	// Larger buffer size can improve the write and sort performance,
	// and reduce the number of disk operations.
	//
	// The default value is 128MB.
	WriterBufferSize int
}

func (o *DiskSorterOptions) ensureDefaults() {
	if o.Concurrency == 0 {
		o.Concurrency = runtime.GOMAXPROCS(0)
	}
	if o.WriterBufferSize == 0 {
		o.WriterBufferSize = 128 << 20
	}
}

// OpenDiskSorter opens a DiskSorter with the given directory.
func OpenDiskSorter(dirname string, opts *DiskSorterOptions) (*DiskSorter, error) {
	if opts == nil {
		opts = &DiskSorterOptions{}
	}
	opts.ensureDefaults()

	dbOpts := &pebble.Options{
		MaxConcurrentCompactions: opts.Concurrency,
		DisableWAL:               true,
		L0CompactionThreshold:    math.MaxInt,
		L0StopWritesThreshold:    math.MaxInt,
	}
	dbOpts = dbOpts.EnsureDefaults()

	dbDir := filepath.Join(dirname, "db")
	tmpDir := filepath.Join(dirname, "tmp")

	// Clean up the temporary directory.
	if err := dbOpts.FS.RemoveAll(tmpDir); err != nil {
		return nil, errors.Trace(err)
	}
	if err := dbOpts.FS.MkdirAll(tmpDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	db, err := pebble.Open(dbDir, dbOpts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &DiskSorter{
		db:     db,
		dbOpts: dbOpts,
		opts:   opts,
		dbDir:  dbDir,
		tmpDir: tmpDir,
		idGen:  new(atomic.Int64),
	}, nil
}

// NewWriter implements the ExternalSorter.NewWriter.
func (d *DiskSorter) NewWriter(_ context.Context) (Writer, error) {
	if d.sorted {
		return nil, errors.Trace(ErrSorted)
	}
	return &diskSorterWriter{
		d:   d,
		buf: make([]byte, d.opts.WriterBufferSize),
	}, nil
}

// Sort implements the ExternalSorter.Sort.
func (d *DiskSorter) Sort(_ context.Context) error {
	if d.sorted {
		return nil
	}

	iter := d.db.NewIter(nil)
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
	err := d.db.Compact(nil, end)
	if err != nil {
		return errors.Trace(err)
	}

	d.sorted = true
	return nil
}

// IsSorted implements the ExternalSorter.IsSorted.
func (d *DiskSorter) IsSorted() bool {
	return d.sorted
}

// NewIterator implements the ExternalSorter.NewIterator.
func (d *DiskSorter) NewIterator(_ context.Context) (Iterator, error) {
	if !d.sorted {
		return nil, errors.Trace(ErrNotSorted)
	}
	return &diskSorterIterator{iter: d.db.NewIter(nil)}, nil
}

// Close implements the ExternalSorter.Close.
func (d *DiskSorter) Close() error {
	return errors.Trace(d.db.Close())
}

// CloseAndCleanup implements the ExternalSorter.CloseAndCleanup.
func (d *DiskSorter) CloseAndCleanup() error {
	if err := d.Close(); err != nil {
		return errors.Trace(err)
	}
	fs := d.dbOpts.FS
	err1 := fs.RemoveAll(d.dbDir)
	err2 := fs.RemoveAll(d.tmpDir)
	err := goerrors.Join(err1, err2)
	return errors.Trace(err)
}

type diskSorterIterator struct{ iter *pebble.Iterator }

func (i *diskSorterIterator) Seek(key []byte) bool { return i.iter.SeekGE(key) }
func (i *diskSorterIterator) First() bool          { return i.iter.First() }
func (i *diskSorterIterator) Next() bool           { return i.iter.Next() }
func (i *diskSorterIterator) Last() bool           { return i.iter.Last() }
func (i *diskSorterIterator) Valid() bool          { return i.iter.Valid() }
func (i *diskSorterIterator) Error() error         { return i.iter.Error() }
func (i *diskSorterIterator) UnsafeKey() []byte    { return i.iter.Key() }
func (i *diskSorterIterator) UnsafeValue() []byte  { return i.iter.Value() }
func (i *diskSorterIterator) Close() error         { return i.iter.Close() }

type keyValue struct {
	key   []byte
	value []byte
}

type diskSorterWriter struct {
	d   *DiskSorter
	kvs []keyValue
	buf []byte
	off int
}

func (w *diskSorterWriter) Put(key, value []byte) error {
	if w.off+len(key)+len(value) > len(w.buf) {
		if err := w.flush(); err != nil {
			return errors.Trace(err)
		}
		// The default buffer is too small, enlarge it to fit the key and value.
		if w.off+len(key)+len(value) > len(w.buf) {
			w.buf = make([]byte, w.off+len(key)+len(value))
		}
	}

	var kv keyValue
	kv.key = w.buf[w.off : w.off+len(key)]
	w.off += len(key)
	copy(kv.key, key)
	if len(value) > 0 {
		kv.value = w.buf[w.off : w.off+len(value)]
		w.off += len(value)
		copy(kv.value, value)
	}
	w.kvs = append(w.kvs, kv)
	return nil
}

func (w *diskSorterWriter) Flush() error {
	if len(w.kvs) == 0 {
		return nil
	}
	return w.flush()
}

func (w *diskSorterWriter) flush() error {
	db := w.d.db
	fs := w.d.dbOpts.FS
	comparer := w.d.dbOpts.Comparer

	filename := fmt.Sprintf("%d.sst", w.d.idGen.Add(1))
	sstPath := filepath.Join(w.d.tmpDir, filename)

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

	if err := db.Ingest([]string{sstPath}); err != nil {
		return errors.Trace(err)
	}

	w.kvs = w.kvs[:0]
	w.off = 0
	return nil
}

func (w *diskSorterWriter) Close() error {
	return w.Flush()
}

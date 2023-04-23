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
	"bytes"
	"context"
	goerrors "errors"
	"fmt"
	"math"
	"path/filepath"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/pingcap/errors"
	"golang.org/x/exp/slices"
)

type DiskSorter[T Item[T]] struct {
	db     *pebble.DB
	codec  ItemCodec[T]
	opts   *pebble.Options
	dbDir  string
	tmpDir string
	idGen  *atomic.Int64
	sorted bool
}

// DiskSorterOptions holds optional parameters for NewDiskSorter.
type DiskSorterOptions struct {
	// Concurrency sets the number of concurrent workers that will be used
	// to sort items. If zero, a default value of 1 will be used.
	Concurrency int
}

func (opts *DiskSorterOptions) ensureDefaults() {
	if opts.Concurrency == 0 {
		opts.Concurrency = 1
	}
}

func NewDiskSorter[T Item[T]](
	dirname string,
	codec ItemCodec[T],
	opts *DiskSorterOptions,
) (*DiskSorter[T], error) {
	if opts == nil {
		opts = &DiskSorterOptions{}
	}
	opts.ensureDefaults()

	dbOpts := &pebble.Options{
		DisableWAL:               true,
		L0CompactionThreshold:    math.MaxInt,
		L0StopWritesThreshold:    math.MaxInt,
		MaxConcurrentCompactions: opts.Concurrency,
		Comparer:                 makeComparer[T](codec),
	}
	dbOpts = dbOpts.EnsureDefaults()
	fs := dbOpts.FS

	dbDir := filepath.Join(dirname, "db")
	tmpDir := filepath.Join(dirname, "tmp")

	// Clean up the temporary directory.
	if err := fs.RemoveAll(tmpDir); err != nil {
		return nil, errors.Trace(err)
	}
	if err := fs.MkdirAll(tmpDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	db, err := pebble.Open(dbDir, dbOpts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &DiskSorter[T]{
		db:     db,
		codec:  codec,
		opts:   dbOpts,
		dbDir:  dbDir,
		tmpDir: tmpDir,
		idGen:  new(atomic.Int64),
	}, nil
}

func makeComparer[T Item[T]](
	codec ItemCodec[T],
) *pebble.Comparer {
	return &pebble.Comparer{
		Compare: func(a, b []byte) int {
			var itemA, itemB T
			itemA, err1 := codec.Decode(a, itemA)
			itemB, err2 := codec.Decode(b, itemB)
			if err1 != nil || err2 != nil {
				return bytes.Compare(a, b)
			}
			return itemA.Compare(itemB)
		},
		Equal: func(a, b []byte) bool {
			var itemA, itemB T
			itemA, err1 := codec.Decode(a, itemA)
			itemB, err2 := codec.Decode(b, itemB)
			if err1 != nil || err2 != nil {
				return bytes.Equal(a, b)
			}
			return itemA.Compare(itemB) == 0
		},
		FormatKey:      pebble.DefaultComparer.FormatKey,
		FormatValue:    pebble.DefaultComparer.FormatValue,
		AbbreviatedKey: func(key []byte) uint64 { return 0 },
		Separator:      func(dst, a, b []byte) []byte { return append(dst, a...) },
		Split:          func(a []byte) int { return len(a) },
		Successor:      func(dst, a []byte) []byte { return append(dst, a...) },
		Name:           "extsort.ItemComparer",
	}
}

// NewWriter implements the ExternalSorter.NewWriter.
func (ds *DiskSorter[T]) NewWriter(_ context.Context) (Writer[T], error) {
	if ds.sorted {
		return nil, errors.Trace(ErrSorted)
	}
	return &diskSorterWriter[T]{ds: ds}, nil
}

// Sort implements the ExternalSorter.Sort.
func (ds *DiskSorter[T]) Sort(_ context.Context) error {
	if ds.sorted {
		return nil
	}

	iter := ds.db.NewIter(nil)

	if !iter.First() {
		// No keys or any error occurred.
		_ = iter.Close()
	}
	start := slices.Clone(iter.Key())

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
	err := ds.db.Compact(start, end)
	if err != nil {
		return errors.Trace(err)
	}

	ds.sorted = true
	return nil
}

// NewIterator implements the ExternalSorter.NewIterator.
func (ds *DiskSorter[T]) NewIterator(_ context.Context) (Iterator[T], error) {
	if !ds.sorted {
		return nil, errors.Trace(ErrNotSorted)
	}
	return &diskSorterIterator[T]{
		iter:  ds.db.NewIter(nil),
		codec: ds.codec,
	}, nil
}

// Close implements the ExternalSorter.Close.
func (ds *DiskSorter[T]) Close() error {
	if err := ds.db.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// CloseAndCleanup implements the ExternalSorter.CloseAndCleanup.
func (ds *DiskSorter[T]) CloseAndCleanup() error {
	if err := ds.Close(); err != nil {
		return errors.Trace(err)
	}
	err1 := ds.opts.FS.RemoveAll(ds.dbDir)
	err2 := ds.opts.FS.RemoveAll(ds.tmpDir)
	err := goerrors.Join(err1, err2)
	return errors.Trace(err)
}

type diskSorterWriter[T Item[T]] struct {
	ds    *DiskSorter[T]
	items []T
}

func (w *diskSorterWriter[T]) Put(item T) error {
	w.items = append(w.items, item)
	return nil
}

func (w *diskSorterWriter[T]) Flush() error {
	if len(w.items) == 0 {
		return nil
	}
	fs := w.ds.opts.FS
	comparer := w.ds.opts.Comparer
	codec := w.ds.codec

	filename := fmt.Sprintf("%d.sst", w.ds.idGen.Add(1))
	sstPath := filepath.Join(w.ds.tmpDir, filename)

	defer func() {
		if _, err := fs.Stat(sstPath); err == nil {
			_ = fs.Remove(sstPath)
		}
	}()

	f, err := fs.Create(sstPath)
	if err != nil {
		return errors.Trace(err)
	}

	slices.SortFunc(w.items, func(a, b T) bool {
		return a.Compare(b) < 0
	})

	sstWriter := sstable.NewWriter(f, sstable.WriterOptions{
		Comparer: comparer,
	})

	var encoded []byte
	for _, item := range w.items {
		encoded = codec.Encode(encoded[:0], item)
		if err := sstWriter.Set(encoded, nil); err != nil {
			_ = sstWriter.Close()
			return errors.Trace(err)
		}
	}
	if err := sstWriter.Close(); err != nil {
		return errors.Trace(err)
	}

	if err := w.ds.db.Ingest([]string{sstPath}); err != nil {
		return errors.Trace(err)
	}

	w.items = nil
	return nil
}

func (w *diskSorterWriter[T]) Close() error {
	return w.Flush()
}

type diskSorterIterator[T Item[T]] struct {
	iter  *pebble.Iterator
	codec ItemCodec[T]
	curr  T
	err   error
}

func (i *diskSorterIterator[T]) Seek(item T) bool {
	var encoded []byte
	encoded = i.codec.Encode(encoded[:0], item)
	return i.iter.SeekGE(encoded)
}

func (i *diskSorterIterator[T]) First() bool {
	if i.iter.First() {
		return i.decodeItem()
	}
	return false
}

func (i *diskSorterIterator[T]) Next() bool {
	if i.iter.Next() {
		return i.decodeItem()
	}
	return false
}

func (i *diskSorterIterator[T]) Last() bool {
	if i.iter.Last() {
		return i.decodeItem()
	}
	return false
}

func (i *diskSorterIterator[T]) decodeItem() bool {
	i.curr, i.err = i.codec.Decode(i.iter.Key(), i.curr)
	return i.err == nil
}

func (i *diskSorterIterator[T]) Valid() bool {
	return i.err == nil && i.iter.Valid()
}

func (i *diskSorterIterator[T]) Error() error {
	if i.err != nil {
		return i.err
	}
	return i.iter.Error()
}

func (i *diskSorterIterator[T]) Item() T {
	return i.curr
}

func (i *diskSorterIterator[T]) Close() error {
	return i.iter.Close()
}

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package filesort

import (
	"container/heap"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
)

type ComparableRow struct {
	key    []types.Datum
	val    []types.Datum
	handle int64
}

type Item struct {
	index int
	value *ComparableRow
}

// Min-heap of ComparableRows
type RowHeap struct {
	sc     *StatementContext
	items  []*Item
	byDesc []bool
}

func (rh *RowHeap) Len() int { return len(items) }

func (rh *RowHeap) Swap(i, j int) { rh.items[i], rh.items[j] = rh.items[j], rh.items[i] }

func (rh *RowHeap) Less(i, j int) bool {
	for k, desc := range rh.byDesc {
		v1 := rh.items[i].key[k]
		v2 := fs.items[j].key[k]

		ret, err := v1.CompareDatum(rh.sc, v2)
		if err != nil {
			panic(err)
		}

		if desc {
			ret = -ret
		}

		if ret < 0 {
			return true
		} else if ret > 0 {
			return false
		}
	}
	return false
}

func (rh *RowHeap) Push(x interface{}) {
	rh.items = append(rh.items, x.(*Item))
}

func (rh *RowHeap) Pop() interface{} {
	old := rh.items
	n := len(old)
	x := old[n-1]
	rh.items = old[0 : n-1]
	return x
}

type FileSorter struct {
	sc      *StatementContext
	keySize int // size of key slice
	valSize int // size of val slice
	bufSize int // size of buf slice
	buf     []*ComparableRow
	files   []string
	byDesc  []bool
	fetched bool
	rowHeap *RowHeap
}

func (fs *FileSorter) Len() int { return len(fs.buf) }

func (fs *FileSorter) Swap(i, j int) { fs.buf[i], fs.buf[j] = fs.buf[j], fs.buf[i] }

func (fs *FileSorter) Less(i, j int) bool {
	for k := 0; k < fs.keySize; k++ {
		v1 := fs.buf[i].key[k]
		v2 := fs.buf[j].key[k]

		ret, err := v1.CompareDatum(fs.sc, v2)
		if err != nil {
			panic(err)
		}

		if fs.byDesc[k] {
			ret = -ret
		}

		if ret < 0 {
			return true
		} else if ret > 0 {
			return false
		}
	}
	return false
}

func (fs *FileSorter) Input(key []types.Datum, val []types.Datum, handle int64) error {
	if fs.fetched {
		panic(nil)
	}
	tmpDir, err := ioutil.TempDir("", "util_filesort")
	defer os.RemoveAll(tmpDir)
	if err != nil {
		return err
	}

	fileCount := 0
	uniqueFileName := func() string {
		ret := path.Join(tmpDir, strconv.Itoa(fileCount))
		fileCount++
		return ret
	}

	flushMemory := func() {
		sort.Sort(fs)
		fileName := uniqueFileName()
		outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		defer outputFile.Close()
		if err != nil {
			return err
		}

		var rowBytes []byte
		for _, row := range buf {
			rowBytes := codec.EncodeKey(rowBytes, row.key)
			rowBytes := codec.EncodeKey(rowBytes, row.val)
			rowBytes := codec.EncodeKey(rowBytes, NewIntDatum(row.handle))
		}
		_, err = outputFile.Write(rowBytes)
		if err != nil {
			return err
		}
		fs.files = append(fs.files, fileName)
		fs.buf = fs.buf[:0]
	}

	row := &ComparableRow{
		key:    key,
		val:    val,
		handle: handle,
	}
	fs.buf = append(fs.buf, row)

	if len(fs.buf) >= fs.bufSize {
		flushMemory()
	}
}

var fds []*File

func (fs *FileSorter) Output() (val []types.Datum, handle int64) {
	if !fs.fetched {
		if len(fs.buf) > 0 {
			flushMemory()
		}

		heap.Init(fs.rowHeap)

		for _, fname := range fs.files {
			fd, err := os.Open(fname)
			if err != nil {
				panic(err)
			}
			fds = append(fds, fd)
		}

		for id, fd := range fds {
			rowSize := fs.keySize + fs.valSize + 1
			rowBytes := make([]byte, rowSize)
			n, err := fd.Read(rowBytes)
			if err != nil || n != rowSize {
				panic(err)
			}

			row := &ComparableRow{
				key:    codec.Decode(rowBytes, fs.keySize),
				val:    codec.Decode(rowBytes, fs.valSize),
				handle: (codec.Decode(rowBytes, 1)).GetInt64(),
			}

			item := &Item{
				index: id,
				value: row,
			}

			heap.Push(fs.rowHeap, item)
		}

		fs.fetched = true
	}

	if fs.rowHeap.Len() > 0 {
		next := heap.Pop(fs.rowHeap)

		rowSize := fs.keySize + fs.valSize + 1
		rowBytes := make([]byte, rowSize)

		n, err := fds[next.index].Read(rowBytes)
		if err == nil && n == rowSize {
			row := &ComparableRow{
				key:    codec.Decode(rowBytes, fs.keySize),
				val:    codec.Decode(rowBytes, fs.valSize),
				handle: (codec.Decode(rowBytes, 1)).GetInt64(),
			}

			item := &Item{
				index: id,
				value: row,
			}

			heap.Push(fs.rowHeap, item)
		}

		return next.row.val, next.row.handle
	} else {
		for _, fd := range fds {
			fd.Close()
		}
		return nil, 0
	}
}

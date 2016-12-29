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
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
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
	sc     *variable.StatementContext
	items  []*Item
	byDesc []bool
}

func (rh *RowHeap) Len() int { return len(rh.items) }

func (rh *RowHeap) Swap(i, j int) { rh.items[i], rh.items[j] = rh.items[j], rh.items[i] }

func (rh *RowHeap) Less(i, j int) bool {
	for k, desc := range rh.byDesc {
		v1 := rh.items[i].value.key[k]
		v2 := rh.items[j].value.key[k]

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
	sc      *variable.StatementContext
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

var fileCount = 0

func (fs *FileSorter) uniqueFileName() string {
	tmpDir, err := ioutil.TempDir("", "util_filesort")
	defer os.RemoveAll(tmpDir)
	if err != nil {
		panic(err)
	}
	ret := path.Join(tmpDir, strconv.Itoa(fileCount))
	fileCount++
	return ret
}

func (fs *FileSorter) flushMemory() {
	sort.Sort(fs)
	fileName := fs.uniqueFileName()
	outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	defer outputFile.Close()
	if err != nil {
		return
	}

	var rowBytes []byte
	for _, row := range fs.buf {
		rowBytes, err = codec.EncodeKey(rowBytes, row.key...)
		if err != nil {
			panic(err)
		}
		rowBytes, err = codec.EncodeKey(rowBytes, row.val...)
		if err != nil {
			panic(err)
		}
		rowBytes, err = codec.EncodeKey(rowBytes, types.NewIntDatum(row.handle))
		if err != nil {
			panic(err)
		}
	}
	_, err = outputFile.Write(rowBytes)
	if err != nil {
		return
	}
	fs.files = append(fs.files, fileName)
	fs.buf = fs.buf[:0]
}

func (fs *FileSorter) Input(key []types.Datum, val []types.Datum, handle int64) error {
	if fs.fetched {
		panic(nil)
	}

	row := &ComparableRow{
		key:    key,
		val:    val,
		handle: handle,
	}
	fs.buf = append(fs.buf, row)

	if len(fs.buf) >= fs.bufSize {
		fs.flushMemory()
	}
	return nil
}

var fds []*os.File

func (fs *FileSorter) Output() (val []types.Datum, handle int64) {
	if !fs.fetched {
		if len(fs.buf) > 0 {
			fs.flushMemory()
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

			k, err := codec.Decode(rowBytes, fs.keySize)
			if err != nil {
				panic(err)
			}
			v, err := codec.Decode(rowBytes, fs.valSize)
			if err != nil {
				panic(err)
			}
			h, err := codec.Decode(rowBytes, 1)
			if err != nil {
				panic(err)
			}
			row := &ComparableRow{
				key:    k,
				val:    v,
				handle: h[0].GetInt64(),
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
		next := heap.Pop(fs.rowHeap).(*Item)

		rowSize := fs.keySize + fs.valSize + 1
		rowBytes := make([]byte, rowSize)

		n, err := fds[next.index].Read(rowBytes)
		if err == nil && n == rowSize {
			k, err := codec.Decode(rowBytes, fs.keySize)
			if err != nil {
				panic(err)
			}
			v, err := codec.Decode(rowBytes, fs.valSize)
			if err != nil {
				panic(err)
			}
			h, err := codec.Decode(rowBytes, 1)
			if err != nil {
				panic(err)
			}
			row := &ComparableRow{
				key:    k,
				val:    v,
				handle: h[0].GetInt64(),
			}

			item := &Item{
				index: next.index,
				value: row,
			}

			heap.Push(fs.rowHeap, item)
		}

		return next.value.val, next.value.handle
	} else {
		for _, fd := range fds {
			fd.Close()
		}
		return nil, 0
	}
}

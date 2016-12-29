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
	"encoding/binary"
	"fmt"
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
	tmpDir  string
}

func NewFileSorter(sc *variable.StatementContext, keySize int, valSize int, bufSize int, byDesc []bool) (fs *FileSorter) {
	// TODO: sanity check
	// sc != nil
	// keySize == len(byDesc) > 0
	// valSize > 0
	// bufSize > 0
	rh := &RowHeap{sc: sc,
		items:  make([]*Item, 0),
		byDesc: byDesc,
	}

	return &FileSorter{sc: sc,
		keySize: keySize,
		valSize: valSize,
		bufSize: bufSize,
		buf:     make([]*ComparableRow, 0, bufSize),
		files:   make([]string, 0),
		byDesc:  byDesc,
		fetched: false,
		rowHeap: rh,
	}
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
	dir, err := ioutil.TempDir("", "util_filesort")
	if err != nil {
		panic(err)
	}
	fs.tmpDir = dir
	ret := path.Join(fs.tmpDir, strconv.Itoa(fileCount))
	fileCount++
	return ret
}

func (fs *FileSorter) flushMemory() {
	sort.Sort(fs)

	fmt.Printf("flushMemory: %d\n", fs.buf[0].key[0].GetInt64())
	fmt.Printf("flushMemory: %d\n", fs.buf[0].val[0].GetInt64())
	fmt.Printf("flushMemory: %d\n", fs.buf[0].handle)

	fileName := fs.uniqueFileName()

	fmt.Printf("flushMemory: %s\n", fileName)

	outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	defer outputFile.Close()
	if err != nil {
		return
	}

	var body []byte
	for _, row := range fs.buf {
		body, err = codec.EncodeKey(body, row.key...)
		if err != nil {
			panic(err)
		}
		body, err = codec.EncodeKey(body, row.val...)
		if err != nil {
			panic(err)
		}
		body, err = codec.EncodeKey(body, types.NewIntDatum(row.handle))
		if err != nil {
			panic(err)
		}
	}

	fmt.Printf("flushMemory: body size = %d\n", len(body))

	var head = make([]byte, 8)
	binary.BigEndian.PutUint64(head, uint64(len(body)))

	output := append(head, body...)
	fmt.Printf("flushMemory: output size = %d\n", len(output))

	_, err = outputFile.Write(output)
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

	fmt.Printf("Input: %d\n", fs.buf[0].key[0].GetInt64())
	fmt.Printf("Input: %d\n", fs.buf[0].val[0].GetInt64())
	fmt.Printf("Input: %d\n", fs.buf[0].handle)

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

		var (
			n    int
			err  error
			head = make([]byte, 8)
			dcod = make([]types.Datum, 0, fs.keySize+fs.valSize+1)
		)

		for id, fd := range fds {
			n, err = fd.Read(head)
			if err != nil || n != 8 {
				panic(err)
			}

			rowSize := int(binary.BigEndian.Uint64(head))

			rowBytes := make([]byte, rowSize)
			n, err = fd.Read(rowBytes)
			if err != nil || n != rowSize {
				panic(err)
			}

			dcod, err = codec.Decode(rowBytes, fs.keySize+fs.valSize+1)
			if err != nil {
				panic(err)
			}

			row := &ComparableRow{
				key:    dcod[:fs.keySize],
				val:    dcod[fs.keySize : fs.keySize+fs.valSize],
				handle: dcod[fs.keySize+fs.valSize:][0].GetInt64(),
			}

			fmt.Printf("Output: row.handle = %d\n", row.handle)

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

		fmt.Printf("Output: fds size = %d, next.index = %d\n", len(fds), next.index)
		var head = make([]byte, 8)
		n, err := fds[next.index].Read(head)
		fmt.Printf("Output: n = %d\n", n)
		if err == nil && n == 8 {
			//TODO: refine the logic here
			rowSize := int64(binary.BigEndian.Uint64(head))
			fmt.Printf("Output: rowSize = %d\n", rowSize)
			rowBytes := make([]byte, rowSize)
			// TODO: assertion read size == rowSize
			fds[next.index].Read(rowBytes)
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
		os.RemoveAll(fs.tmpDir)
		return nil, 0
	}
}

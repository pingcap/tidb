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

type FileSorter struct {
	numMem int
	byIdx  int
	byDesc bool
	buffer [][]types.Datum
	splits []string
}

func NewFileSorter(numMem int, byIdx int, byDesc bool) *FileSorter {
	fs := new(FileSorter)
	fs.numMem = numMem
	fs.byIdx = byIdx
	fs.byDesc = byDesc
	fs.buffer = make([][]types.Datum, 0, numMem)
	fs.splits = make([]string, 0)
	return fs
}

func (fs *FileSorter) Input(row []types.Datum) error {
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
		//TODO: How to sort types.Datum via `byIdx` and `byDesc`
		sort.Sort(buffer)
		fileName := uniqueFileName()
		outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		defer outputFile.Close()
		if err != nil {
			return err
		}

		for _, row := range buffer {

			rowBytes, err := codec.EncodeKey(nil, row...)
			if err != nil {
				return err
			}
		}
		splits = append(splits, fileName)
		//TODO: flush bytes into file
		buffer = buffer[:0]
	}

	buffer = append(buffer, row)
	if len(buffer) >= numMem {
		flushMemory()
	}
}

func (fs *FileSorter) Output() []types.Datum {
	//TODO: should mark as dumped, any Input afterwards will lead to error
	if len(buffer) > 0 {
		flushMemory()
	}

	//TODO:
	//1. open all files
	//2. DecodeKey and push into heap
}

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

// orderByRow binds a row to its order values, so it can be sorted.
type orderByRow struct {
	key    []types.Datum
	data   []types.Datum
	handle int64
}

type FileSorter struct {
	numMem int
	byDesc []bool
	rows   []*orderByRow
	splits []string
	sc     *StatementContext
}

func (fs *FileSorter) Len() int {
	return len(fs.rows)
}

func (fs *FileSorter) Swap(i, j int) {
	fs.rows[i], fs.rows[j] = fs.rows[j], fs.rows[i]
}

func (fs *FileSorter) Less(i, j int) bool {
	for index, desc := range fs.byDesc {
		v1 := fs.rows[i].key[index]
		v2 := fs.rows[j].key[index]

		ret, err := v1.CompareDatum(fs.sc, v2)
		if err != nil {
			//TODO: error handling
			return true
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

func (fs *FileSorter) Input(key []types.Datum, data []types.Datum, handle int64) error {
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
		for _, row := range rows {
			// serialize row and write to outputFile
			rowBytes := codec.EncodeKey(rowBytes, row.key)
			rowBytes := codec.EncodeValue(rowBytes, row.data)
			rowBytes := codec.EncodeValue(rowBytes, NewIntDatum(row.handle))
			_, err = outputFile.Write(rowBytes)
			if err != nil {
				return err
			}
		}
		splits = append(splits, fileName)
		rows = rows[:0]
	}

	orderRow := &orderByRow{
		key:    key,
		data:   data,
		handle: handle,
	}
	rows = append(rows, orderRow)

	if len(rows) >= numMem {
		flushMemory()
	}
}

func (fs *FileSorter) Output() []types.Datum {
	//TODO: should mark as dumped, any Input afterwards will lead to error
	if len(rows) > 0 {
		flushMemory()
	}

	//TODO:
	//1. open all files
	//2. DecodeKey and push into heap
}

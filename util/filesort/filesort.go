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
	"encoding/gob"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
)

type ComparableItem interface {
	LessThan(other ComparableItem) bool
}

type GobHelper interface {
	EncodeComparable(g *gob.Encoder, item ComparableItem) error
	DecodeComparable(g *gob.Decoder) (ComparableItem, error)
}

type ComparableItems []ComparableItem

func (s ComparableItems) Len() int           { return len(s) }
func (s ComparableItems) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ComparableItems) Less(i, j int) bool { return s[i].LessThan(s[j]) }

func mergeFiles(gobHelper GobHelper, path1, path2, outputPath string) {
	f1, err := os.Open(path1)
	if err != nil {
		panic(err)
	}
	defer f1.Close()
	f2, err := os.Open(path2)
	if err != nil {
		panic(err)
	}
	defer f2.Close()
	fout, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		panic(err)
	}
	defer fout.Close()

	g1 := gob.NewDecoder(f1)
	g2 := gob.NewDecoder(f2)
	gout := gob.NewEncoder(fout)

	var h1, h2 ComparableItem
	var err1, err2 error
	h1, err1 = gobHelper.DecodeComparable(g1)
	if err1 != nil {
		panic(err1)
	}
	h2, err2 = gobHelper.DecodeComparable(g2)
	if err2 != nil {
		panic(err2)
	}

	for err1 != io.EOF && err2 != io.EOF {
		if h1.LessThan(h2) {
			gobHelper.EncodeComparable(gout, h1)
			h1, err1 = gobHelper.DecodeComparable(g1)
		} else {
			gobHelper.EncodeComparable(gout, h2)
			h2, err2 = gobHelper.DecodeComparable(g2)
		}
	}

	for err1 != io.EOF {
		gobHelper.EncodeComparable(gout, h1)
		h1, err1 = gobHelper.DecodeComparable(g1)
	}

	for err2 != io.EOF {
		gobHelper.EncodeComparable(gout, h2)
		h2, err2 = gobHelper.DecodeComparable(g2)
	}
}

func FileSort(numMemory int,
	gobHelper GobHelper,
	inputChan chan ComparableItem, outputChan chan ComparableItem) {
	memoryItems := make(ComparableItems, 0, numMemory)
	unmergedFiles := make([]string, 0)

	tmpDir, err := ioutil.TempDir("", "util_filesort")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)

	fileCount := 0
	uniqueFileName := func() string {
		ret := path.Join(tmpDir, strconv.Itoa(fileCount))
		fileCount++
		return ret
	}

	flushMemory := func() {
		sort.Sort(memoryItems)
		fileName := uniqueFileName()
		outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			panic(err)
		}
		defer outputFile.Close()

		gobEncoder := gob.NewEncoder(outputFile)
		for _, item := range memoryItems {
			err := gobHelper.EncodeComparable(gobEncoder, item)
			if err != nil {
				panic(err)
			}
		}
		unmergedFiles = append(unmergedFiles, fileName)
		memoryItems = memoryItems[:0]
	}

	for i := range inputChan {
		memoryItems = append(memoryItems, i)
		if len(memoryItems) >= numMemory {
			flushMemory()
		}
	}

	if len(memoryItems) > 0 {
		flushMemory()
	}

	for len(unmergedFiles) > 1 {
		mergeName := uniqueFileName()
		mergeFiles(gobHelper, unmergedFiles[0], unmergedFiles[1], mergeName)
		unmergedFiles = append(unmergedFiles[2:], mergeName)
	}

	f, err := os.Open(unmergedFiles[0])
	defer f.Close()
	g := gob.NewDecoder(f)

	for {
		h, err := gobHelper.DecodeComparable(g)
		if err == io.EOF {
			close(outputChan)
			break
		} else if err != nil {
			panic(err)
		} else {
			outputChan <- h
		}
	}

	return
}

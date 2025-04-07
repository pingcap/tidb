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

package chunk

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func stripAuxDataForChunk(chk *Chunk) {
	chk.capacity = 0
	chk.requiredRows = 0
	chk.numVirtualRows = 0
	chk.sel = nil
}

func addAuxDataForChunks(chunks []*Chunk) {
	for _, chk := range chunks {
		chk.capacity = rand.Intn(100)
		chk.requiredRows = rand.Intn(100)
		chk.numVirtualRows = rand.Intn(100)

		selLen := rand.Intn(50) + 1
		chk.sel = make([]int, selLen)
		for i := range selLen {
			chk.sel[i] = rand.Int()
		}
	}
}

func checkAuxDataForChunk(t *testing.T, chk1, chk2 *Chunk) {
	require.Equal(t, chk1.capacity, chk2.capacity)
	require.Equal(t, chk1.requiredRows, chk2.requiredRows)
	require.Equal(t, chk1.numVirtualRows, chk2.numVirtualRows)
	require.Equal(t, len(chk1.sel), len(chk2.sel))

	length := len(chk1.sel)
	for i := range length {
		require.Equal(t, chk1.sel[i], chk2.sel[i])
	}
}

func checkChunk(t *testing.T, chk1, chk2 *Chunk) {
	checkAuxDataForChunk(t, chk1, chk2)
	stripAuxDataForChunk(chk1)
	stripAuxDataForChunk(chk2)

	require.Equal(t, chk1.NumRows(), chk2.NumRows())
	numRows := chk1.NumRows()
	for i := range numRows {
		checkRow(t, chk1.GetRow(i), chk2.GetRow(i))
	}
}

func testImpl(t *testing.T, isNewChunk bool) {
	numChk, numRow := 100, 1000
	chks, fields := initChunks(numChk, numRow)
	addAuxDataForChunks(chks)
	dataInDiskByChunks := NewDataInDiskByChunks(fields)
	defer dataInDiskByChunks.Close()

	for _, chk := range chks {
		err := dataInDiskByChunks.Add(chk)
		require.NoError(t, err)
	}

	chk := NewEmptyChunk(fields)
	var err error
	for i := range numChk {
		if isNewChunk {
			chk, err = dataInDiskByChunks.GetChunk(i)
		} else {
			chk.Reset()
			err = dataInDiskByChunks.FillChunk(i, chk)
		}
		require.NoError(t, err)
		checkChunk(t, chk, chks[i])
	}
}

func testGetChunk(t *testing.T) {
	testImpl(t, true)
}

func testFillChunk(t *testing.T) {
	testImpl(t, false)
}

func TestDataInDiskByChunks(t *testing.T) {
	testGetChunk(t)
	testFillChunk(t)
}

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
	"testing"

	"github.com/stretchr/testify/require"
)

func checkChunk(t *testing.T, chk1, chk2 *Chunk) {
	require.Equal(t, chk1.NumRows(), chk2.NumRows())

	numRows := chk1.NumRows()
	for i := 0; i < numRows; i++ {
		checkRow(t, chk1.GetRow(i), chk2.GetRow(i))
	}
}

// TODO
// var nullable
// fixed nullable
// var not-null
// fixed not-null
// chunk with sel and data
func TestDataInDiskByChunks(t *testing.T) {
	numChk, numRow := 100, 1000
	chks, fields := initChunks(numChk, numRow)
	dataInDiskByChunks := NewDataInDiskByChunks(fields)
	defer dataInDiskByChunks.Close()

	for _, chk := range chks {
		err := dataInDiskByChunks.Add(chk)
		require.NoError(t, err)
	}

	for i := 0; i < numChk; i++ {
		chk, err := dataInDiskByChunks.GetChunk(i)
		require.NoError(t, err)
		checkChunk(t, chk, chks[i])
	}
}

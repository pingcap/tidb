// Copyright 2026 PingCAP, Inc.
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

package rowcodec

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestChunkDecoderReset(t *testing.T) {
	oldColumns := []ColInfo{{ID: 1, Ft: types.NewFieldType(mysql.TypeLonglong)}}
	oldHandleIDs := []int64{1}
	oldDefault := func(int, *chunk.Chunk) error { return nil }
	decoder := NewChunkDecoder(oldColumns, oldHandleIDs, oldDefault, time.UTC)
	decoder.row = row{
		flags:          rowFlagLarge | rowFlagChecksum,
		checksumHeader: checksumFlagExtra,
		numNotNullCols: 1,
		numNullCols:    1,
		colIDs:         []byte{1},
		offsets:        []uint16{1},
		colIDs32:       []uint32{1},
		offsets32:      []uint32{1},
		data:           []byte{1},
		checksum1:      1,
		checksum2:      2,
	}

	newColumns := []ColInfo{{ID: 2, Ft: types.NewFieldType(mysql.TypeVarchar)}}
	newHandleIDs := []int64{2}
	newDefault := func(int, *chunk.Chunk) error { return nil }
	location := time.FixedZone("reset", 3600)
	decoder.Reset(newColumns, newHandleIDs, newDefault, location)

	require.Equal(t, row{}, decoder.row)
	require.Equal(t, newColumns, decoder.columns)
	require.Equal(t, newHandleIDs, decoder.handleColIDs)
	require.Same(t, location, decoder.loc)
	require.NotNil(t, decoder.defDatum)
}

func TestChunkDecoderReuseConfigBuffers(t *testing.T) {
	oldColumns := []ColInfo{
		{ID: 1, Ft: types.NewFieldType(mysql.TypeLonglong)},
		{ID: 2, Ft: types.NewFieldType(mysql.TypeVarchar)},
		{ID: 3, Ft: types.NewFieldType(mysql.TypeBlob)},
	}
	oldHandleIDs := make([]int64, 2, 4)
	oldHandleIDs[0], oldHandleIDs[1] = 1, 2
	decoder := NewChunkDecoder(oldColumns, oldHandleIDs, nil, time.UTC)

	columns, handleIDs := decoder.ReuseConfigBuffers(1)
	require.Len(t, columns, 1)
	require.Empty(t, handleIDs)
	require.Same(t, oldColumns[0].Ft, columns[0].Ft)
	require.Nil(t, oldColumns[1].Ft)
	require.Nil(t, oldColumns[2].Ft)

	columns[0] = ColInfo{ID: 4, Ft: types.NewFieldType(mysql.TypeFloat)}
	handleIDs = append(handleIDs, 4)
	require.Equal(t, int64(4), oldHandleIDs[0])

	decoder.Reset(columns, handleIDs, nil, time.UTC)
	grownColumns, grownHandleIDs := decoder.ReuseConfigBuffers(3)
	require.Len(t, grownColumns, 3)
	require.Empty(t, grownHandleIDs)
	require.Equal(t, int64(4), grownColumns[0].ID)
	require.Nil(t, grownColumns[1].Ft)
	require.Nil(t, grownColumns[2].Ft)
}

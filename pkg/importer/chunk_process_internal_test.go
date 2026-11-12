// Copyright 2019 PingCAP, Inc.
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

package importer

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestQueryChunkReaderChecksColumnCount(t *testing.T) {
	fields := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong), types.NewFieldType(mysql.TypeLonglong)}
	newChunks := func() <-chan QueryChunk {
		chk := chunk.NewChunkWithCapacity(fields, 1)
		chk.AppendInt64(0, 1)
		chk.AppendInt64(1, 2)
		ch := make(chan QueryChunk, 1)
		ch <- QueryChunk{Fields: fields, Chk: chk}
		close(ch)
		return ch
	}
	ctx := context.Background()

	row, closed, err := queryRowEncodeReader(newChunks(), 2)(ctx, nil)
	require.NoError(t, err)
	require.False(t, closed)
	require.Len(t, row.row, 2)

	for _, columnCnt := range []int{1, 3} {
		_, _, err = queryRowEncodeReader(newChunks(), columnCnt)(ctx, nil)
		require.ErrorContains(t, err, "import query returns 2 columns")
	}
}

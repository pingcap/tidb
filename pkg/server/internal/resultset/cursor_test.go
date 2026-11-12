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

package resultset

import (
	"context"
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// failingResultSet returns one full batch, then a shorter batch together with an error.
type failingResultSet struct {
	ResultSet
	calls int
}

func (*failingResultSet) FieldTypes() []*types.FieldType {
	return []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
}

func (rs *failingResultSet) Next(_ context.Context, chk *chunk.Chunk) error {
	rs.calls++
	chk.Reset()
	if rs.calls == 1 {
		chk.AppendInt64(0, 1)
		chk.AppendInt64(0, 2)
		return nil
	}
	chk.AppendInt64(0, 3)
	return errors.New("next failed")
}

func TestLazyRowIteratorNextError(t *testing.T) {
	ctx := context.Background()
	iter := WrapWithLazyCursor(&failingResultSet{}, 2, 2).GetRowIterator()

	require.Equal(t, int64(1), iter.Current(ctx).GetInt64(0))
	require.Equal(t, int64(2), iter.Next(ctx).GetInt64(0))
	require.NoError(t, iter.Error())

	require.Equal(t, iter.End(), iter.Next(ctx))
	require.EqualError(t, iter.Error(), "next failed")
	// The index is past the rows of the first batch. Current must report the end
	// instead of a row at that index.
	require.Equal(t, iter.End(), iter.Current(ctx))
}

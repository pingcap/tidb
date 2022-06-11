// Copyright 2021 PingCAP, Inc.
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

package autoid_test

import (
	"context"
	"math"
	"testing"

	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
)

func TestInMemoryAlloc(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()

	columnInfo := &model.ColumnInfo{
		FieldType: types.NewFieldTypeBuilder().SetFlag(mysql.AutoIncrementFlag).Build(),
	}
	tblInfo := &model.TableInfo{
		Columns: []*model.ColumnInfo{columnInfo},
	}
	alloc := autoid.NewAllocatorFromTempTblInfo(tblInfo)
	require.NotNil(t, alloc)

	// alloc 1
	ctx := context.Background()
	_, id, err := alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), id)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(2), id)

	// alloc N
	_, id, err = alloc.Alloc(ctx, 10, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(12), id)

	// increment > N
	_, id, err = alloc.Alloc(ctx, 1, 10, 1)
	require.NoError(t, err)
	require.Equal(t, int64(21), id)

	// offset
	_, id, err = alloc.Alloc(ctx, 1, 1, 30)
	require.NoError(t, err)
	require.Equal(t, int64(30), id)

	// rebase
	err = alloc.Rebase(context.Background(), int64(40), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(41), id)
	err = alloc.Rebase(context.Background(), int64(10), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(42), id)

	// maxInt64
	err = alloc.Rebase(context.Background(), int64(math.MaxInt64-2), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64-1), id)
	_, _, err = alloc.Alloc(ctx, 1, 1, 1)
	require.True(t, terror.ErrorEqual(err, autoid.ErrAutoincReadFailed))

	// test unsigned
	columnInfo.FieldType.AddFlag(mysql.UnsignedFlag)
	alloc = autoid.NewAllocatorFromTempTblInfo(tblInfo)
	require.NotNil(t, alloc)

	var n uint64 = math.MaxUint64 - 2
	err = alloc.Rebase(context.Background(), int64(n), true)
	require.NoError(t, err)
	_, id, err = alloc.Alloc(ctx, 1, 1, 1)
	require.NoError(t, err)
	require.Equal(t, int64(n+1), id)
	_, _, err = alloc.Alloc(ctx, 1, 1, 1)
	require.True(t, terror.ErrorEqual(err, autoid.ErrAutoincReadFailed))
}

// Copyright 2024 PingCAP, Inc.
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

package commontest

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/server/internal/resultset"
	"github.com/pingcap/tidb/pkg/server/tests/servertestkit"
	"github.com/stretchr/testify/require"
)

func TestLazyRowIterator(t *testing.T) {
	ts := servertestkit.CreateTidbTestSuite(t)
	qctx, err := ts.Tidbdrv.OpenCtx(uint64(0), 0, uint8(mysql.DefaultCollationID), "test", nil, nil)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = Execute(ctx, qctx, "use test")
	require.NoError(t, err)
	_, err = Execute(ctx, qctx, "create table t (id int)")
	require.NoError(t, err)
	// insert 1000 rows
	for i := range 1000 {
		_, err = Execute(ctx, qctx, fmt.Sprintf("insert into t values (%d)", i))
		require.NoError(t, err)
	}

	for _, chkSize := range []struct{ initSize, maxSize int }{
		{1024, 1024}, {512, 512}, {256, 256}, {100, 100}} {
		rs, err := Execute(ctx, qctx, "select * from t")
		require.NoError(t, err)
		crs := resultset.WrapWithLazyCursor(rs, chkSize.initSize, chkSize.maxSize)
		iter := crs.GetRowIterator()
		for i := range 1000 {
			row := iter.Current(ctx)
			require.Equal(t, int64(i), row.GetInt64(0))
			row = iter.Next(ctx)
			if i+1 >= 1000 {
				require.True(t, row.IsEmpty())
			} else {
				require.Equal(t, int64(i+1), row.GetInt64(0))
			}
		}
		require.True(t, iter.Current(ctx).IsEmpty())
		iter.Close()
	}
}

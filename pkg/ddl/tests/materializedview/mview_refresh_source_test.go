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

package materializedview

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/mviewutil"
	"github.com/stretchr/testify/require"
)

func TestMViewRefreshSourceInfoLogSets(t *testing.T) {
	for _, layout := range []string{"bootstrap", "nonclustered_with_extra_column"} {
		t.Run(layout, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := newMViewTestKit(t, store)
			ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
			exec := tk.Session().GetSQLExecutor()
			if layout != "bootstrap" {
				// SQL access must not depend on column offsets or physical handle encoding.
				// Allow replacing the protected NextGen system table for this layout fixture.
				testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/skipCheckReservedSchemaObjInNextGen", "return(true)")
				tk.MustExec("drop table mysql.tidb_mview_fast_refresh_source_info")
				tk.MustExec("create table mysql.tidb_mview_fast_refresh_source_info (MLOG_ID bigint not null, EXTRA int default 7, MVIEW_ID bigint not null, primary key (MVIEW_ID, MLOG_ID) nonclustered, key idx_mlog (MLOG_ID, MVIEW_ID))")
			}
			// Exercise multiple logs and the missing-log placeholder independently of
			// the SQL validator's current single-source restriction.
			tk.MustExec("begin optimistic")
			require.NoError(t, mviewutil.InsertRefreshSourceIDs(ctx, exec, 1001, []int64{0, 10, 11}))
			require.NoError(t, mviewutil.InsertRefreshSourceIDs(ctx, exec, 1002, []int64{10}))
			ids, err := mviewutil.ReadRefreshSourceIDs(ctx, exec, 1001)
			require.NoError(t, err)
			require.Equal(t, []int64{0, 10, 11}, ids)
			tk.MustExec("commit")
			tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info use index(idx_mlog) order by MVIEW_ID, MLOG_ID").Check(testkit.Rows("1001 0", "1001 10", "1001 11", "1002 10"))

			tk.MustExec("begin optimistic")
			err = mviewutil.InsertRefreshSourceIDs(ctx, exec, 1002, []int64{9, 10, 12})
			if err == nil {
				// Optimistic SQL can defer duplicate-key checks until commit.
				err = tk.ExecToErr("commit")
			}
			require.True(t, kv.ErrKeyExists.Equal(err), "%v", err)
			tk.MustExec("rollback")
			tk.MustQuery("select MLOG_ID from mysql.tidb_mview_fast_refresh_source_info where MVIEW_ID = 1002").Check(testkit.Rows("10"))

			tk.MustExec("begin optimistic")
			require.NoError(t, mviewutil.DeleteRefreshSourceInfo(ctx, exec, []int64{1001}))
			tk.MustExec("commit")
			tk.MustQuery("select MVIEW_ID, MLOG_ID from mysql.tidb_mview_fast_refresh_source_info use index(idx_mlog)").Check(testkit.Rows("1002 10"))
		})
	}
}

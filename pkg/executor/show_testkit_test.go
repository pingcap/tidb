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

package executor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func TestShowImportJobsWhere(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	err := tk.ExecToErr("show import jobs where job_id = 1")
	require.ErrorIs(t, err, plannererrors.ErrUnknownColumn)
	err = tk.ExecToErr("show import jobs where id = (select id from mysql.tidb_import_jobs)")
	require.ErrorIs(t, err, dbterror.ErrNotSupportedYet)
	require.ErrorContains(t, err, "subquery in WHERE clause")

	tk.MustExec(`
		insert into mysql.tidb_import_jobs
		(id, create_time, table_schema, table_name, table_id, created_by, parameters, source_file_size, status, step)
		values(1, '2022-01-01 00:00:00', 'test', 't', 1, 'root', '{}', 0, 'pending', '')`)
	tk.MustExec(`
		insert into mysql.tidb_import_jobs
		(id, create_time, table_schema, table_name, table_id, created_by, parameters, source_file_size, status, step)
		values(2, now(), 'target_schema', 't', 11, 'root', '{}', 0, 'finished', '')`)
	tk.MustExec(`
		insert into mysql.tidb_import_jobs
		(id, create_time, table_schema, table_name, table_id, created_by, parameters, source_file_size, status, step)
		values(3, now(), 'test', 'target_table', 111, 'root', '{}', 0, 'failed', '')`)
	tk.MustExec(`
		insert into mysql.tidb_import_jobs
		(id, create_time, table_schema, table_name, table_id, created_by, parameters, source_file_size, status, step)
		values(4, now(), 'test', 't', 1111, 'some_user', '{}', 0, 'pending', '')`)
	requireData := func(rows [][]any, ids []int) {
		require.Len(t, rows, len(ids))
		idMap := make(map[string]struct{})
		for _, id := range ids {
			idMap[fmt.Sprintf("%d", id)] = struct{}{}
		}
		for _, r := range rows {
			require.Contains(t, idMap, r[0])
		}
	}
	requireData(tk.MustQuery("show import jobs where id=2").Rows(), []int{2})
	requireData(tk.MustQuery("show import jobs where create_time <= '2022-01-01 00:00:00'").Rows(), []int{1})
	requireData(tk.MustQuery("show import jobs where table_schema='target_schema'").Rows(), []int{2})
	requireData(tk.MustQuery("show import jobs where table_name='target_table'").Rows(), []int{3})
	requireData(tk.MustQuery("show import jobs where table_id = '11'").Rows(), []int{2})
	requireData(tk.MustQuery("show import jobs where created_by = 'some_user'").Rows(), []int{4})
	requireData(tk.MustQuery("show import jobs where status = 'failed'").Rows(), []int{3})

	requireData(tk.MustQuery("show import jobs where table_name = 't'").Rows(), []int{1, 2, 4})
	requireData(tk.MustQuery("show import jobs where status = 'pending'").Rows(), []int{1, 4})
}

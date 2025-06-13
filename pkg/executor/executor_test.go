// Copyright 2022 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestChangePumpAndDrainer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// change pump or drainer's state need connect to etcd
	// so will meet error "URL scheme must be http, https, unix, or unixs: /tmp/tidb"
	tk.MustMatchErrMsg("change pump to node_state ='paused' for node_id 'pump1'", "URL scheme must be http, https, unix, or unixs.*")
	tk.MustMatchErrMsg("change drainer to node_state ='paused' for node_id 'drainer1'", "URL scheme must be http, https, unix, or unixs.*")
}

func TestAdminCheckPanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists TEST1")
	tk.MustExec("create database TEST1")
	tk.MustExec("use TEST1")

	// Using a known issue from https://github.com/pingcap/tidb/issues/52510, which can cause panic
	tk.MustExec(`
		create table t(id varchar(255) PRIMARY KEY, pid varchar(255), created_at datetime NOT NULL)
		ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)
	tk.MustExec(`insert into t values
		('o', 'productA', '2023-10-01 12:00:00'),
		('1', 'productB', '2023-10-02 13:30:00'),
		('oO', 'productC', '2023-10-03 14:45:00'),
		('0', 'productD', '2023-10-04 15:20:00'),
		('child5', 'productE', '2023-10-05 16:10:00')`)
	tk.MustExec("CREATE INDEX t_idx_created_at ON t (created_at)")

	// Some versions may have fixed this bug, so we also manually inject panic.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockFastAdminCheckPanic", "return"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockFastAdminCheckPanic")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockAdminCheckPanic", "return"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockAdminCheckPanic")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/skipExecPanic", "return"))
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/skipExecPanic")

	// When there are some bugs in the executor/optimizeradmin check should
	// return error no matter whether fast admin check is enabled or not.
	tk.MustExec("set @@tidb_enable_fast_table_check = OFF")
	tk.MustExecToErr("admin check table t")
	tk.MustExec("set @@tidb_enable_fast_table_check = 1")
	tk.MustExecToErr("admin check table t")
}

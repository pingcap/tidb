// Copyright 2025 PingCAP, Inc.
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

//go:build fusion

package executor_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestSetVarForPKDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// For global
	tk.MustQuery(`select @@global.tidb_create_from_select_using_import;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@global.tidb_create_from_select_using_import="ON";`)
	tk.MustQuery(`select @@global.tidb_create_from_select_using_import;`).Check(testkit.Rows("1"))
	tk.MustExec(`set @@global.tidb_create_from_select_using_import=0;`)
	tk.MustQuery(`select @@global.tidb_create_from_select_using_import;`).Check(testkit.Rows("0"))
	// For session
	tk.MustQuery(`select @@session.tidb_create_from_select_using_import;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@global.tidb_create_from_select_using_import="ON";`)
	tk.MustQuery(`select @@session.tidb_create_from_select_using_import;`).Check(testkit.Rows("0"))
	tk.MustExec(`set @@session.tidb_create_from_select_using_import=1;`)
	tk.MustQuery(`select @@session.tidb_create_from_select_using_import;`).Check(testkit.Rows("1"))
}

func TestPingKaiDBSetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test tidbx_fast_path
	tk.MustQuery("select @@global.tidbx_fast_path").Check(testkit.Rows("0")) // default value
	tk.MustExec("set global tidbx_fast_path = 1")
	tk.MustQuery("select @@global.tidbx_fast_path").Check(testkit.Rows("1"))
	require.True(t, variable.EnableFastPath.Load())
	tk.MustExec("set global tidbx_fast_path = 0")
	tk.MustQuery("select @@global.tidbx_fast_path").Check(testkit.Rows("0"))
	require.False(t, variable.EnableFastPath.Load())

	// test tidbx_enable_index_lookup_push_down
	// global scope
	tk.MustQuery("select @@global.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("0")) // default value
	tk.MustExec("set global tidbx_enable_index_lookup_push_down = 1")
	tk.MustQuery("select @@global.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("1"))
	tk.MustExec("set global tidbx_enable_index_lookup_push_down = 0")
	tk.MustQuery("select @@global.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("0"))
	// session scope
	tk.MustQuery("select @@session.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("0")) // default value
	tk.MustExec("set session tidbx_enable_index_lookup_push_down = 1")
	tk.MustQuery("select @@session.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("1"))
	require.True(t, tk.Session().GetSessionVars().EnableIndexLookUpPushDown)
	tk.MustExec("set session tidbx_enable_index_lookup_push_down = 0")
	tk.MustQuery("select @@session.tidbx_enable_index_lookup_push_down").Check(testkit.Rows("0"))
}

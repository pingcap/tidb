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

package telemetry_test

import (
	"testing"

	"github.com/pingcap/tidb/telemetry"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestBuiltinFunctionsUsage(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Clear builtin functions usage
	telemetry.GlobalBuiltinFunctionsUsage.Dump()
	usage := telemetry.GlobalBuiltinFunctionsUsage.Dump()
	require.Equal(t, map[string]uint32{}, usage)

	tk.MustExec("create table t (id int)")
	tk.MustQuery("select id + 1 - 2 from t")
	// Should manually invoke `Session.Close()` to report the usage information
	tk.Session().Close()
	usage = telemetry.GlobalBuiltinFunctionsUsage.Dump()
	require.Equal(t, map[string]uint32{"PlusInt": 1, "MinusInt": 1}, usage)
}

// https://github.com/pingcap/tidb/issues/32459.
func TestBuiltinFunctionsUsageJoinViews(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create view vw_dict as " +
		"select a.table_schema, a.table_name as name, a.column_name, " +
		"a.column_type, a.column_default, a.is_nullable, b.column_comment from " +
		"information_schema.columns a left join information_schema.columns b on " +
		"(a.table_name = b.table_name and a.column_name = b.column_name and b.table_schema = 'accountdb') " +
		"where (a.table_schema = 'query_account') order by a.table_name, a.ordinal_position;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("select * from vw_dict where name = 't'")
}

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

package timeout

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestLoadBindingWarn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int)`)
	tk.MustExec(`create global binding using select * from t1`)
	tk.MustExec(`select * from t1`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning

	sctx := tk.Session()
	sctx.SetValue(bindinfo.GetBindingReturnNilAlways, true)
	tk.MustExec(`select * from t1`)
	warnings := tk.MustQuery(`show warnings`)
	require.True(t, len(warnings.Rows()) == 1)
	sctx.ClearValue(bindinfo.GetBindingReturnNilAlways)
}

func TestFuzzyBindingHintsWithSourceReturningTimeout(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	for _, db := range []string{"db1", "db2", "db3"} {
		tk.MustExec(`create database ` + db)
		tk.MustExec(`use ` + db)
		tk.MustExec(`create table t1 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
		tk.MustExec(`create table t2 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
		tk.MustExec(`create table t3 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	}
	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)

	for _, c := range []struct {
		binding   string
		qTemplate string
	}{
		// use index
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1 where a=1`,
			`select * from %st1 where a=1000`},
	} {
		tk.MustExec(c.binding)
		for _, currentDB := range []string{"db1", "db2", "db3"} {
			tk.MustExec(`use ` + currentDB)
			for _, db := range []string{"db1.", "db2.", "db3.", ""} {
				query := fmt.Sprintf(c.qTemplate, db)
				tk.MustExec(query)
				tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
				sctx := tk.Session()
				sctx.SetValue(bindinfo.GetBindingReturnNil, true)
				sctx.SetValue(bindinfo.GetBindingReturnNilAlways, true)
				sctx.SetValue(bindinfo.LoadBindingNothing, true)
				tk.MustExec(query)
				sctx.ClearValue(bindinfo.GetBindingReturnNil)
				sctx.ClearValue(bindinfo.GetBindingReturnNilAlways)
				sctx.ClearValue(bindinfo.LoadBindingNothing)
				tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
				bindinfo.GetBindingReturnNilBool.Store(false)
			}
		}
	}
}

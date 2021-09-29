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

package bindinfo_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestOptimizeOnlyOnce(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idxa(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idxa)")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/checkOptimizeCountOne", "return"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/checkOptimizeCountOne"))
	}()
	tk.MustQuery("select * from t").Check(testkit.Rows())
}

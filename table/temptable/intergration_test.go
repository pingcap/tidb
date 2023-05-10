// Copyright 2023 PingCAP, Inc.
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

package temptable_test

import (
	"testing"

	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestSelectTemporaryTableUnionView(t *testing.T) {
	// see the issue: https://github.com/pingcap/tidb/issues/42563
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("create view tv as select a from t")
	tk.MustExec("create temporary table t(a int)")
	tk.MustExec("insert into t values(2)")
	tk.MustQuery("select * from tv").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t").Check(testkit.Rows("2"))
	tk.MustQuery("select * from (select a from t union all select a from tv) t1 order by a").Check(testkit.Rows("1", "2"))
}

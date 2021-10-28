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

package executor_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestPointGetForTemporaryTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create global temporary table t1 (id int primary key, val int) on commit delete rows")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (1,1)")
	tk.MustQuery("explain format = 'brief' select * from t1 where id in (1, 2, 3)").
		Check(testkit.Rows("Batch_Point_Get 3.00 root table:t1 handle:[1 2 3], keep order:false, desc:false"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/mockstore/unistore/rpcServerBusy", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/mockstore/unistore/rpcServerBusy"))
	}()

	// Batch point get.
	tk.MustQuery("select * from t1 where id in (1, 2, 3)").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t1 where id in (2, 3)").Check(testkit.Rows())

	// Point get.
	tk.MustQuery("select * from t1 where id = 1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t1 where id = 2").Check(testkit.Rows())
}

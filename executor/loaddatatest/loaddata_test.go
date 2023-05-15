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

package loaddatatest

import (
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/pingcap/tidb/testkit"
)

func TestSplitTableRegion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(id int, name varchar(20), primary key(id) clustered)")
	tk.MustExec("load data local infile '/tmp/a.dat' replace into table t1 fields terminated by ',' enclosed by '' (id,name)")
	ctx := tk.Session().(sessionctx.Context)
	ld, ok := ctx.Value(executor.LoadDataVarKey).(*executor.LoadDataInfo)
	require.True(t, ok)
	defer ctx.SetValue(executor.LoadDataVarKey, nil)
	require.NotNil(t, ld)
	//tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("1 abc", "2 cdef", "3 asdf"))
	time.Sleep(time.Second)
	tk.MustExec("load data local infile '/tmp/b.dat' replace into table t1 fields terminated by ',' enclosed by '' (id,name)")
}

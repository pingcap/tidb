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

package resourcegrouptest

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	mysql "github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestRunawayWatch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(name varchar(19))")
	tk.MustExec("insert into test.t1 values('1')")

	tk.MustExec("create resource group x BURSTABLE RU_PER_SEC=2000 QUERY_LIMIT=(EXEC_ELAPSED='50ms' action KILL WATCH EXACT duration '1m')")
	tk.MustQuery("select * from information_schema.resource_groups where name = 'x'").Check(testkit.Rows("x 2000 MEDIUM YES EXEC_ELAPSED=50ms, ACTION=KILL, WATCH=EXACT[1m0s]"))

	tk.MustQuery("select  /*+ resource_group(x) */ * from t1").Check(testkit.Rows("1"))
	re := require.New(t)
	re.NoError(failpoint.Enable("github.com/pingcap/tidb/store/copr/sleepCoprRequest", fmt.Sprintf("return(%d)", 60)))

	err := tk.QueryToErr("select  /*+ resource_group(x) */ * from t1")
	re.ErrorContains(err, "Killed because of identified as runaway query")
	tk.MustGetErrCode("select  /*+ resource_group(x) */ * from t1", mysql.ErrResourceGroupQueryRunawayQuarantine)

	re.NoError(failpoint.Disable("github.com/pingcap/tidb/store/copr/sleepCoprRequest"))
}

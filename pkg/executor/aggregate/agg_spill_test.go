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

package aggregate_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/testkit"
)

func checkResults(actualRes [][]interface{}, expectedRes map[string]string) bool {
	if len(actualRes) != len(expectedRes) {
		return false
	}

	var key string
	var expectVal string
	var actualVal string
	var ok bool
	for _, row := range actualRes {
		if len(row) != 2 {
			return false
		}

		key, ok = row[0].(string)
		if !ok {
			return false
		}

		expectVal, ok = expectedRes[key]
		if !ok {
			return false
		}

		actualVal, ok = row[1].(string)
		if !ok {
			return false
		}

		if expectVal != actualVal {
			return false
		}
	}
	return true
}

func TestGetCorrectResult(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest", `return(false)`)

	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.test_spill_bin;")
	tk.MustExec("create table test.test_spill_bin(k varchar(30), v int);")
	tk.MustExec("insert into test.test_spill_bin (k, v) values ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), ('dD', 1), ('Dd', 1), ('ee', 1), ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1);")
	log.Info("xzxdebugtt")
	tk.MustExec("drop table if exists test.test_spill_ci;")
	tk.MustExec("create table test.test_spill_ci(k varchar(30), v int) DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;")
	tk.MustExec("insert into test.test_spill_ci (k, v) values ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), ('dD', 1), ('Dd', 1), ('ee', 1), ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1);")
	log.Info("xzxdebugtt")
	hardLimitBytesNum := 1000000
	tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%d;", hardLimitBytesNum))
	log.Info("xzxdebugtt")
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/triggerSpill", fmt.Sprintf("return(%d)", hardLimitBytesNum))
	log.Info("xzxdebugtt")
	// bin collation
	binCollationResult := make(map[string]string)
	binCollationResult["CC"] = "1"
	binCollationResult["aA"] = "2"
	binCollationResult["DD"] = "1"
	binCollationResult["Bb"] = "2"
	binCollationResult["ee"] = "1"
	binCollationResult["bb"] = "2"
	binCollationResult["BB"] = "2"
	binCollationResult["bB"] = "2"
	binCollationResult["Cc"] = "1"
	binCollationResult["Dd"] = "1"
	binCollationResult["dD"] = "1"
	binCollationResult["Aa"] = "2"
	binCollationResult["AA"] = "2"
	binCollationResult["aa"] = "2"
	binCollationResult["cC"] = "1"
	binCollationResult["dd"] = "1"
	binCollationResult["cc"] = "1"
	log.Info("xzxdebugtt")
	res := tk.MustQuery("select k, sum(v) from test_spill_bin group by k;")
	log.Info("xzxdebugtt")
	tk.RequireEqual(true, checkResults(res.Rows(), binCollationResult))

	// ci collation
	ciCollationResult := make(map[string]string)
	ciCollationResult["aa"] = "8"
	ciCollationResult["bb"] = "8"
	ciCollationResult["cc"] = "4"
	ciCollationResult["dd"] = "4"
	ciCollationResult["ee"] = "1"
	res = tk.MustQuery("select k, sum(v) from test_spill_ci group by k;")
	tk.RequireEqual(true, checkResults(res.Rows(), ciCollationResult))
}

// TODO maybe add more random fail?
func TestRandomFail(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.test_spill_random_fail;")
	tk.MustExec("create table test.test_spill_random_fail(k varchar(30), v int);")
	tk.MustExec("insert into test.test_spill_random_fail (k, v) values ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1), ('cc', 1), ('CC', 1), ('cC', 1), ('Cc', 1), ('dd', 1), ('DD', 1), ('dD', 1), ('Dd', 1), ('ee', 1), ('aa', 1), ('AA', 1), ('aA', 1), ('Aa', 1), ('bb', 1), ('BB', 1), ('bB', 1), ('Bb', 1);")

	hardLimitBytesNum := 1000000
	tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%d;", hardLimitBytesNum))
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/triggerSpill", fmt.Sprintf("return(%d)", hardLimitBytesNum))
	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/enableAggSpillIntest", `return(true)`)

	// Test is successful when all sqls are not hung
	for i := 0; i < 50; i++ {
		tk.ExecuteAndResultSetToResultWithCtx("select k, sum(v) from test_spill_random_fail group by k;")
	}
}

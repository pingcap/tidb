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

package sessiontest

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestPagingActRowsAndProcessKeys(t *testing.T) {
	// Close copr-cache
	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.CoprCache.CapacityMB = 0
	})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	session := testkit.NewTestKit(t, store)
	session.MustExec("use test;")
	session.MustExec("drop table if exists t;")
	session.MustExec(`set @@tidb_wait_split_region_finish=1`)
	session.MustExec("create table t(a int,b int,c int,index idx(a,b), primary key(a));")
	// prepare data, insert 10w record
	// [0, 999999]
	for i := 0; i < 100; i++ {
		sql := "insert into t value"
		for j := 0; j < 1000; j++ {
			if j != 0 {
				sql += ","
			}
			sql += "(" + strconv.Itoa(i*1000+j) + "," + strconv.Itoa(i*1000+j) + "," + strconv.Itoa(i*1000+j) + ")"
		}
		session.MustExec(sql)
	}

	testcase := []struct {
		regionNumLowerBound int32
		regionNumUpperBound int32
	}{
		{10, 100},    // [10, 99]
		{100, 500},   // [100,499]
		{500, 1000},  // [500,999]
		{1000, 1001}, // 1000
	}

	openOrClosePaging := []string{
		"set tidb_enable_paging = on;",
		"set tidb_enable_paging = off;",
	}

	sqls := []string{
		"desc analyze select a,b from t;",                         // TableScan
		"desc analyze select /*+ use_index(t,idx) */ a,b from t;", // IndexScan
		"desc analyze select /*+ use_index(t,idx) */ c from t;",   // IndexLookUp
	}

	checkScanOperator := func(strs []any) {
		require.Equal(t, strs[2].(string), "100000")
		if *realtikvtest.WithRealTiKV { // Unistore don't collect process_keys now
			require.True(t, strings.Contains(strs[5].(string), "total_process_keys: 100000"), strs[5])
		}
	}

	checkResult := func(result [][]any) {
		for _, strs := range result {
			if strings.Contains(strs[0].(string), "Scan") {
				checkScanOperator(strs)
			}
		}
	}

	for _, tc := range testcase {
		regionNum := rand.Int31n(tc.regionNumUpperBound-tc.regionNumLowerBound) + tc.regionNumLowerBound
		_ = session.MustQuery(fmt.Sprintf("split table t between (0) and (1000000) regions %v;", regionNum))
		_ = session.MustQuery(fmt.Sprintf("split table t index idx between (0) and (1000000) regions %v;", regionNum))
		for _, sql := range sqls {
			for _, pagingSQL := range openOrClosePaging {
				session.MustExec(pagingSQL)
				rows := session.MustQuery(sql)
				checkResult(rows.Rows())
			}
		}
	}
}

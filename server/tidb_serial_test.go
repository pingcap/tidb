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

package server

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
)

// this test will change `kv.TxnTotalSizeLimit` which may affect other test suites,
// so we must make it running in serial.
func TestLoadData(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestLoadData(t, ts.server)
	ts.runTestLoadDataWithSelectIntoOutfile(t, ts.server)
	ts.runTestLoadDataForSlowLog(t, ts.server)
}

func TestConfigDefaultValue(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestsOnNewDB(t, nil, "config", func(dbt *testkit.DBTestKit) {
		rows := dbt.MustQuery("select @@tidb_slow_log_threshold;")
		ts.checkRows(t, rows, "300")
	})
}

// Fix issue#22540. Change tidb_dml_batch_size,
// then check if load data into table with auto random column works properly.
func TestLoadDataAutoRandom(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestLoadDataAutoRandom(t)
}

func TestLoadDataAutoRandomWithSpecialTerm(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestLoadDataAutoRandomWithSpecialTerm(t)
}

func TestExplainFor(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestExplainForConn(t)
}

func TestStmtCount(t *testing.T) {
	ts, cleanup := createTiDBTest(t)
	defer cleanup()

	ts.runTestStmtCount(t)
}

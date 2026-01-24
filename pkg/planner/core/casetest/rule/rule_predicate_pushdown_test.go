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

package rule

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func runPredicatePushdownTestData(t *testing.T, tk *testkit.TestKit, cascades, name string) {
	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning []string
	}
	predicatePushdownSuiteData := GetPredicatePushdownSuiteData()
	predicatePushdownSuiteData.LoadTestCasesByName(name, t, &input, &output, cascades)
	require.Equal(t, len(input), len(output))
	for i := range input {
		if strings.Contains(input[i], "set") {
			tk.MustExec(input[i])
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + input[i]).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + input[i]).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

func TestConstantPropagateWithCollation(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("create table t (id int primary key, name varchar(20));")
		tk.MustExec(`create table foo(a int, b int, c int, primary key(a));`)
		tk.MustExec(`create table bar(a int, b int, c int, primary key(a));`)
		runPredicatePushdownTestData(t, tk, cascades, "TestConstantPropagateWithCollation")
	})
}

func TestPredicatePushDown(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")

		testKit.MustExec("drop table if exists crm_rd_150m")
		testKit.MustExec(`CREATE TABLE crm_rd_150m (
	product varchar(256) DEFAULT NULL,
		uks varchar(16) DEFAULT NULL,
		brand varchar(256) DEFAULT NULL,
		cin varchar(16) DEFAULT NULL,
		created_date timestamp NULL DEFAULT NULL,
		quantity int(11) DEFAULT NULL,
		amount decimal(11,0) DEFAULT NULL,
		pl_date timestamp NULL DEFAULT NULL,
		customer_first_date timestamp NULL DEFAULT NULL,
		recent_date timestamp NULL DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "crm_rd_150m")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("explain format = 'brief' SELECT /* issue:15110 */ count(*) FROM crm_rd_150m dataset_48 WHERE (CASE WHEN (month(dataset_48.customer_first_date)) <= 30 THEN '新客' ELSE NULL END) IS NOT NULL;")

		testKit.MustExec("drop table if exists t31202")
		testKit.MustExec("create table t31202(a int primary key, b int);")

		// Set the hacked TiFlash replica for explain tests.
		testkit.SetTiFlashReplica(t, dom, "test", "t31202")

		testKit.MustQuery("explain format = 'brief' select /* issue:31202 */ * from t31202;").Check(testkit.Rows(
			"TableReader 10000.00 root  MppVersion: 3, data:ExchangeSender",
			"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
			"  └─TableFullScan 10000.00 mpp[tiflash] table:t31202 keep order:false, stats:pseudo"))

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tikv'")
		testKit.MustQuery("explain format = 'brief' select /* issue:31202 */ * from t31202 use index (primary);").Check(testkit.Rows(
			"TableReader 10000.00 root  data:TableFullScan",
			"└─TableFullScan 10000.00 cop[tikv] table:t31202 keep order:false, stats:pseudo"))
		testKit.MustExec("drop table if exists t31202")
	})
}

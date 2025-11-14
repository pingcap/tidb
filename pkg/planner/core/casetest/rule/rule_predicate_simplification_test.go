// Copyright 2025 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestPredicateSimplification(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
    id VARCHAR(64) PRIMARY KEY
);`)
	tk.MustExec(`CREATE TABLE t2 (
    c1 VARCHAR(64) NOT NULL,
    c2 VARCHAR(64) NOT NULL,
    c3 VARCHAR(64) NOT NULL,
    PRIMARY KEY (c1, c2, c3),
    KEY c3 (c3)
);`)
	tk.MustExec(`CREATE TABLE t3 (
    c1 VARCHAR(64) NOT NULL,
    c2 VARCHAR(64) NOT NULL,
    c3 VARCHAR(64) NOT NULL,
    PRIMARY KEY (c1, c2, c3),
    KEY c3 (c3)
);`)
	tk.MustExec(`CREATE TABLE t4 (
    c1 VARCHAR(64) NOT NULL,
    c2 VARCHAR(64) NOT NULL,
    c3 VARCHAR(64) NOT NULL,
    state VARCHAR(64) NOT NULL DEFAULT 'ACTIVE',
    PRIMARY KEY (c1, c2, c3),
    KEY c3 (c3)
);`)
	tk.MustExec(`CREATE TABLE t5 (
    c1 VARCHAR(64) NOT NULL,
    c2 VARCHAR(64) NOT NULL,
    PRIMARY KEY (c1, c2)
);`)
	tk.MustExec(`CREATE TABLE ISSUE64263  (
  column_1 bigint(20) unsigned NOT NULL,
  column_2 int(10) unsigned NOT NULL,
  column_3 varchar(255) COLLATE utf8mb4_general_ci NOT NULL,
  column_4 int(10) unsigned NOT NULL,
  column_5 text COLLATE utf8mb4_general_ci DEFAULT NULL,
  column_6 datetime NOT NULL,
  PRIMARY KEY (column_1) /*T![clustered_index] CLUSTERED */,
  KEY idx_4_6 (column_4, column_6),
  KEY idx_4 (column_4),
  KEY idx_6 (column_6),
  KEY idx_4_3_2 (column_4, column_3, column_2),
  KEY idx_3_2 (column_3, column_2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;`)
	tk.MustExec(`SET global tidb_opt_fix_control = "44830:ON";`)
	tk.MustExec(`SET tidb_opt_fix_control = "44830:ON";`)
	tk.MustExec(`SET GLOBAL tidb_enable_non_prepared_plan_cache=ON;`)
	tk.MustExec(`SET tidb_enable_non_prepared_plan_cache=ON;`)
	// since the plan may differ under different planner mode, recommend to record explain result to json accordingly.
	var input []string
	var output []struct {
		SQL               string
		Plan              []string
		LastPlanFromCache []string
		Warning           []string
	}
	suite := GetPredicateSimplificationSuiteData()
	suite.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			tk.MustQuery(tt)
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("EXPLAIN FORMAT='brief' " + tt).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
			tk.MustQuery(tt)
			output[i].LastPlanFromCache = testdata.ConvertRowsToStrings(tk.MustQuery("select @@last_plan_from_cache").Rows())
		})
		tk.MustQuery(tt)
		res := tk.MustQuery("EXPLAIN FORMAT='brief' " + tt)
		res.Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
		tk.MustQuery(tt)
		isPlanCacheRes := tk.MustQuery("select @@last_plan_from_cache")
		isPlanCacheRes.Check(testkit.Rows(output[i].LastPlanFromCache...))
	}
}

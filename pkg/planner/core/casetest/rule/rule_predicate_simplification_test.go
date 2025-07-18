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

func TestPredicateSimplification(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
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
		tk.MustExec(`CREATE TABLE t7c899916 (
  col_37 text COLLATE gbk_bin DEFAULT NULL,
  col_38 datetime DEFAULT CURRENT_TIMESTAMP,
  col_39 tinyint unsigned NOT NULL,
  col_40 json NOT NULL,
  col_41 char(140) COLLATE gbk_bin NOT NULL,
  col_42 json DEFAULT NULL,
  col_43 tinytext COLLATE gbk_bin DEFAULT NULL,
  col_44 json DEFAULT NULL,
  col_45 date DEFAULT '2010-01-29',
  col_46 char(221) COLLATE gbk_bin DEFAULT NULL,
  UNIQUE KEY idx_15 (col_41,col_39,col_38)
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_bin`)
		// since the plan may differ under different planner mode, recommend to record explain result to json accordingly.
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		suite := GetPredicateSimplificationSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format=brief " + tt).Rows())
			})
			res := tk.MustQuery("explain format=brief " + tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

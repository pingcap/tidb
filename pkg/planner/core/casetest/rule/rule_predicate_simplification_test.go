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
<<<<<<< HEAD
	// since the plan may differ under different planner mode, recommend to record explain result to json accordingly.
	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	suite := GetPredicateSimplificationSuiteData()
	suite.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format=brief " + tt).Rows())
		})
		res := tk.MustQuery("explain format=brief " + tt)
		res.Check(testkit.Rows(output[i].Plan...))
	}
=======
		tk.MustExec(`create table t6(a int, b int, c int, d int, index(a,b));`)
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
  col_47 timestamp,
  UNIQUE KEY idx_15 (col_41,col_39,col_38)
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_bin`)
		tk.MustExec(`CREATE TABLE tlfdfece63 (
  col_41 timestamp NULL DEFAULT NULL,
  col_42 json NOT NULL,
  col_43 varchar(330) COLLATE utf8_general_ci DEFAULT NULL,
  col_44 char(192) COLLATE utf8_general_ci NOT NULL DEFAULT '^_',
  col_45 text COLLATE utf8_general_ci DEFAULT NULL,
  col_46 double DEFAULT '8900.485367052326',
  col_47 decimal(59,2) DEFAULT NULL,
  col_48 varchar(493) COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (col_44) /*T![clustered_index] NONCLUSTERED */,
  KEY idx_20 (col_41,col_47),
  UNIQUE KEY idx_21 ((cast(col_42 as char(64) array)),col_45(4),col_43(3)),
  KEY idx_22 (col_48(4),col_46)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;`)
		tk.MustExec(`CREATE TABLE tad03b424 (
  col_41 timestamp NULL DEFAULT NULL,
  col_42 json NOT NULL,
  col_43 varchar(330) COLLATE utf8_general_ci DEFAULT NULL,
  col_44 char(192) COLLATE utf8_general_ci NOT NULL DEFAULT '^_',
  col_45 text COLLATE utf8_general_ci DEFAULT NULL,
  col_46 double DEFAULT '8900.485367052326',
  col_47 decimal(59,2) DEFAULT NULL,
  col_48 varchar(493) COLLATE utf8_general_ci DEFAULT NULL,
  PRIMARY KEY (col_44) /*T![clustered_index] NONCLUSTERED */,
  KEY idx_20 (col_41,col_47),
  UNIQUE KEY idx_21 ((cast(col_42 as char(64) array)),col_45(4),col_43(3)),
  KEY idx_22 (col_48(4),col_46)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci`)
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
>>>>>>> 1d746d80a0e (range: wrongly skip the candidate in the extractBestCNFItemRanges (#62585))
}

// Copyright 2019 PingCAP, Inc.
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

package logicalop_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestIssue58743(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`
		CREATE TABLE tlf5d55361 (
			col_9 time NOT NULL,
			col_10 float NOT NULL,
			col_11 json NOT NULL,
			col_12 date NOT NULL,
			col_13 json NOT NULL,
			col_14 tinyint NOT NULL,
			col_15 date DEFAULT NULL,
			col_16 tinyblob NOT NULL,
			col_17 time DEFAULT '03:51:26',
			PRIMARY KEY (col_14, col_9, col_10) /*T![clustered_index] CLUSTERED */
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci
		PARTITION BY RANGE (col_14)
		(
			PARTITION p0 VALUES LESS THAN (-128),
			PARTITION p1 VALUES LESS THAN (-84),
			PARTITION p2 VALUES LESS THAN (-41),
			PARTITION p3 VALUES LESS THAN (-30)
		);
	`)
	tk.MustExec(`
		CREATE TABLE td8d55878 (
			col_26 datetime DEFAULT NULL,
			col_27 time DEFAULT NULL,
			col_28 json DEFAULT NULL,
			col_29 char(186) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT '4-BJKi',
			col_30 date NOT NULL DEFAULT '1998-07-28',
			col_31 datetime NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci;
	`)

	tk.MustQuery(`
		WITH cte_3 (col_128) AS (
			SELECT /*+ USE_INDEX_MERGE(td8d55878 tlf5d55361)*/ 
			MIN(td8d55878.col_26) AS r0
			FROM tlf5d55361
			JOIN td8d55878
			GROUP BY tlf5d55361.col_17
			HAVING tlf5d55361.col_17 <= '20:22:14.00' OR tlf5d55361.col_17 BETWEEN '21:56:23.00' AND '19:42:43.00'
			ORDER BY r0
			LIMIT 772780933
		),
		cte_4 (col_129) AS (
			SELECT /*+ HASH_AGG()*/ 
			SUM(tlf5d55361.col_14) AS r0
			FROM td8d55878
			JOIN tlf5d55361 ON tlf5d55361.col_17 = td8d55878.col_27
			GROUP BY td8d55878.col_30
			HAVING ISNULL(td8d55878.col_30) OR td8d55878.col_30 BETWEEN '2009-09-08' AND '1980-11-17'
		)
		SELECT SUM((cte_4.col_129 IN (0.65, 564617.3335, 45, 0.319, 0.4427)) IS TRUE)
		FROM cte_4
		JOIN cte_3;
	`).Equal(nil)
}

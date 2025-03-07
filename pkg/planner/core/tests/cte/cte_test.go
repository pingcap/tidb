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

package cte

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestCTEWithDifferentSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE USER 'db_a'@'%';")
	tk.MustExec("CREATE USER 'db_b'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `db_a`.* TO 'db_a'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `db_b`.* TO 'db_a'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `db_b`.* TO 'db_b'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `db_b`.* TO 'db_b'@'%';")
	tk.MustExec("create database db_a;")
	tk.MustExec("create database db_b;")
	tk.MustExec("use db_a;")
	tk.MustExec(`CREATE TABLE tmp_table1 (
   id decimal(18,0) NOT NULL,
   row_1 varchar(255) DEFAULT NULL,
   PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`create ALGORITHM=UNDEFINED DEFINER=db_a@'%' SQL SECURITY DEFINER VIEW view_test_v1 as (
                         with rs1 as(
                            select otn.*
                             from tmp_table1 otn
                          )
                        select ojt.* from rs1 ojt
                        )`)
	tk.MustExec("use db_b;")
	tk.MustQuery("explain select * from db_a.view_test_v1;").Check(testkit.Rows(
		"CTEFullScan_11 10000.00 root CTE:rs1 AS ojt data:CTE_0",
		"CTE_0 10000.00 root  Non-Recursive CTE",
		"└─TableReader_9(Seed Part) 10000.00 root  data:TableFullScan_8",
		"  └─TableFullScan_8 10000.00 cop[tikv] table:otn keep order:false, stats:pseudo"))
}

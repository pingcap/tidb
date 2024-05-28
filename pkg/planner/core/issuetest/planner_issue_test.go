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

package issuetest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// It's a case for Columns in tableScan and indexScan with double reader
func TestIssue43461(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index b(b), index b_c(b, c)) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t")

	stmt, err := parser.New().ParseOneStmt("select * from t use index(b) where b > 1 order by b limit 1", "", "")
	require.NoError(t, err)

	p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	var idxLookUpPlan *core.PhysicalIndexLookUpReader
	var ok bool

	for {
		idxLookUpPlan, ok = p.(*core.PhysicalIndexLookUpReader)
		if ok {
			break
		}
		p = p.(base.PhysicalPlan).Children()[0]
	}
	require.True(t, ok)

	is := idxLookUpPlan.IndexPlans[0].(*core.PhysicalIndexScan)
	ts := idxLookUpPlan.TablePlans[0].(*core.PhysicalTableScan)

	require.NotEqual(t, is.Columns, ts.Columns)
}

func TestIssue49721(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`
CREATE TABLE pub_branch (
  id int(5) NOT NULL,
  code varchar(12) NOT NULL,
  type_id int(3) DEFAULT NULL,
  name varchar(64) NOT NULL,
  short_name varchar(32) DEFAULT NULL,
  organ_code varchar(15) DEFAULT NULL,
  parent_code varchar(12) DEFAULT NULL,
  organ_layer tinyint(1) NOT NULL,
  inputcode1 varchar(12) DEFAULT NULL,
  inputcode2 varchar(12) DEFAULT NULL,
  state tinyint(1) NOT NULL,
  modify_empid int(9) NOT NULL,
  modify_time datetime NOT NULL,
  organ_level int(9) DEFAULT NULL,
  address varchar(256) DEFAULT NULL,
  db_user varchar(32) DEFAULT NULL,
  db_password varchar(64) DEFAULT NULL,
  org_no int(3) DEFAULT NULL,
  ord int(5) DEFAULT NULL,
  org_code_mpa varchar(10) DEFAULT NULL,
  org_code_gb varchar(30) DEFAULT NULL,
  wdchis_id int(5) DEFAULT NULL,
  medins_code varchar(32) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY pub_barnch_unique (code),
  KEY idx_pub_branch_parent (parent_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
	tk.MustExec(`
CREATE VIEW udc_branch_test (
  branch_id,
  his_branch_id,
  branch_code,
  branch_name,
  pid,
  his_pid,
  short_name,
  inputcode1,
  inputcode2,
  org_no,
  org_code,
  org_level,
  org_layer,
  address,
  state,
  modify_by,
  modify_time,
  remark
)
AS
SELECT a.id AS branch_id, a.id AS his_branch_id, a.code AS branch_code, a.name AS branch_name
  , a.id + 1000000 AS pid, id AS his_pid, a.short_name AS short_name
  , a.inputcode1 AS inputcode1, a.inputcode2 AS inputcode2, a.id AS org_no, a.code AS org_code, a.organ_level AS org_level
  , a.organ_layer AS org_layer, a.address AS address, a.state AS state, a.modify_empid AS modify_by, a.modify_time AS modify_time
  , NULL AS remark
FROM pub_branch a
WHERE organ_layer = 4
UNION ALL
SELECT a.id + 1000000 AS branch_id, a.id AS his_branch_id, a.code AS branch_code
  , CONCAT(a.name, _UTF8MB4 '(中心)') AS branch_name
  , (
    SELECT id AS id
    FROM pub_branch a
    WHERE organ_layer = 2
      AND state = 1
    LIMIT 1
  ) AS pid, id AS his_pid, a.short_name AS short_name, a.inputcode1 AS inputcode1, a.inputcode2 AS inputcode2
  , a.id AS org_no, a.code AS org_code, a.organ_level AS org_level, a.organ_layer AS org_layer, a.address AS address
  , a.state AS state, 1 AS modify_by, a.modify_time AS modify_time, NULL AS remark
FROM pub_branch a
WHERE organ_layer = 4
UNION ALL
SELECT a.id AS branch_id, a.id AS his_branch_id, a.code AS branch_code, a.name AS branch_name, NULL AS pid
  , id AS his_pid, a.short_name AS short_name, a.inputcode1 AS inputcode1, a.inputcode2 AS inputcode2, a.id AS org_no
  , a.code AS org_code, a.organ_level AS org_level, a.organ_layer AS org_layer, a.address AS address, a.state AS state
  , a.modify_empid AS modify_by, a.modify_time AS modify_time, NULL AS remark
FROM pub_branch a
WHERE organ_layer = 2;
`)
	tk.MustExec(`
CREATE TABLE udc_branch_temp (
  branch_id int(11) NOT NULL AUTO_INCREMENT COMMENT '',
  his_branch_id varchar(20) DEFAULT NULL COMMENT '',
  branch_code varchar(20) DEFAULT NULL COMMENT '',
  branch_name varchar(64) NOT NULL COMMENT '',
  pid int(11) DEFAULT NULL COMMENT '',
  his_pid varchar(20) DEFAULT NULL COMMENT '',
  short_name varchar(64) DEFAULT NULL COMMENT '',
  inputcode1 varchar(12) DEFAULT NULL COMMENT '辅码1',
  inputcode2 varchar(12) DEFAULT NULL COMMENT '辅码2',
  org_no int(11) DEFAULT NULL COMMENT '',
  org_code varchar(20) DEFAULT NULL COMMENT ',',
  org_level tinyint(4) DEFAULT NULL COMMENT '',
  org_layer tinyint(4) DEFAULT NULL COMMENT '',
  address varchar(255) DEFAULT NULL COMMENT '机构地址',
  state tinyint(4) NOT NULL DEFAULT '1' COMMENT '',
  modify_by int(11) NOT NULL COMMENT '',
  modify_time datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  remark varchar(255) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (branch_id) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=1030102 COMMENT='';
`)
	tk.MustQuery(`
SELECT res.*
FROM (
    (
        WITH RECURSIVE d AS (
            SELECT ub.*
            FROM udc_branch_test ub
            WHERE ub.branch_id = 1000102
            UNION ALL
            SELECT ub1.*
            FROM udc_branch_test ub1
            INNER JOIN d ON d.branch_id = ub1.pid
        )
        SELECT d.*
        FROM d
    )
) AS res
WHERE res.state != 2
ORDER BY res.branch_id;
`)
}

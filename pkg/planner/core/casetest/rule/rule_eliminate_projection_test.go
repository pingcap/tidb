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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestWithApply(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t11, t22")
	tk.MustExec("create table t11(id int, name varchar(20))")
	tk.MustExec("create table t22(id int, name varchar(20))")
	tk.MustExec("insert into t11(id, name) values(1, 'test')")
	tk.MustExec("insert into t22(id, name) values(1, 'test')")

	sqls := []string{
		`with temp as (
select id from t22 order by name
)
select (select name from t11 where id = (case when temp.id = 1 then temp.id else temp.id end)) = 'test'
from temp where id = 1`,
		`with temp as (
select id from t22
)
select (select name from t11 where id = (case when temp.id = 1 then temp.id else temp.id end)) = 'test'
from temp where id = 1`,
	}
	for _, sql := range sqls {
		tk.MustQuery(sql).Check(testkit.Rows("1"))
		planRows := tk.MustQuery("explain " + sql).Rows()
		hasApply := false
		for _, row := range planRows {
			if strings.Contains(fmt.Sprintf("%v", row), "Apply") {
				hasApply = true
				break
			}
		}
		require.True(t, hasApply, "plan should contain Apply, sql: %s", sql)
	}
}

func TestElinimateProjectionWithExpressionIndex(t *testing.T) {
	originCfg := config.GetGlobalConfig()
	newCfg := *originCfg
	newCfg.Experimental.AllowsExpressionIndex = true
	config.StoreGlobalConfig(&newCfg)
	defer func() {
		config.StoreGlobalConfig(originCfg)
	}()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec(`CREATE TABLE t1 (
		namespace_id char(64) NOT NULL,
		run_id char(64) NOT NULL,
		start_time datetime(6) NOT NULL,
		execution_time datetime(6) NOT NULL,
		workflow_id varchar(255) NOT NULL,
		status int NOT NULL,
		close_time datetime(6) DEFAULT NULL,
		PRIMARY KEY (namespace_id,run_id) /*T![clustered_index] CLUSTERED */,
		KEY by_execution_time (namespace_id,execution_time,(coalesce(close_time, cast(_utf8mb4'9999-12-31 23:59:59' as datetime))),start_time,run_id),
		KEY by_workflow_id (namespace_id,workflow_id,(coalesce(close_time, cast(_utf8mb4'9999-12-31 23:59:59' as datetime))),start_time,run_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)

	tk.MustExec(`CREATE TABLE t2 (
		namespace_id CHAR(64) NOT NULL,
		run_id CHAR(64) NOT NULL
	);`)

	// This issue can reproduce by running 20 times.
	for i := 0; i < 20; i++ {
		tk.MustQuery(`SELECT close_time
		FROM t1
		LEFT JOIN t2
		USING (namespace_id,run_id)
		ORDER BY  coalesce(close_time, CAST('9999-12-31 23:59:59' AS DATETIME));`).Check(testkit.Rows())
	}
}

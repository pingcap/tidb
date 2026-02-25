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

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
)

// TestEliminateProjectionWithNestedJoinInCorrelatedSubquery tests that projection
// elimination works correctly with nested JOINs inside correlated subqueries.
// This is a regression test for https://github.com/pingcap/tidb/issues/65454
func TestEliminateProjectionWithNestedJoinInCorrelatedSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2, t3;")
	tk.MustExec(`CREATE TABLE t1 (
		id bigint NOT NULL,
		hcode text COLLATE utf8mb4_general_ci NOT NULL,
		PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
	);`)
	tk.MustExec(`CREATE TABLE t2 (
		id bigint NOT NULL,
		bid bigint NOT NULL,
		cid bigint NOT NULL,
		PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
	);`)
	tk.MustExec(`CREATE TABLE t3 (
		id bigint NOT NULL,
		user_id bigint NOT NULL,
		PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
	);`)

	tk.MustExec("INSERT INTO t1 VALUES (1, 'code1'), (2, 'code2');")
	tk.MustExec("INSERT INTO t2 VALUES (1, 1, 1), (2, 2, 2);")
	tk.MustExec("INSERT INTO t3 VALUES (1, 100), (2, 200);")

	// This query has nested JOINs inside a correlated subquery.
	// The projection elimination rule should correctly rebuild JOIN schemas
	// using BuildLogicalJoinSchema instead of ResolveColumnAndReplace.
	tk.MustQuery(`SELECT (SELECT COUNT(t3.user_id) FROM t2
		LEFT JOIN t1 AS cm ON t2.cid = cm.id
		LEFT JOIN t3 ON t2.bid = t3.id
		WHERE cm.hcode = t1.hcode) AS tt
	FROM t1
	WHERE t1.id IN (SELECT MIN(id) FROM t1);`).Check(testkit.Rows("1"))
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

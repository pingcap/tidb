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

package cascades_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestCascadesDrive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(true)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2),(1,1)")

	// simple select for quick debug of memo, the normal test case is in tests/planner/cascades/integration.test.
	tk.MustQuery("select 1").Check(testkit.Rows("1"))
	tk.MustQuery("explain select 1").Check(testkit.Rows(""+
		"Projection_3 1.00 root  1->Column#1",
		"└─TableDual_4 1.00 root  rows:1"))
}

func TestXFormedOperatorShouldDeriveTheirStatsOwn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("CREATE TABLE t1 (  a1 int DEFAULT NULL,  b1 int DEFAULT NULL,  c1 int DEFAULT NULL)")
	tk.MustExec("CREATE TABLE t2 (  a2 int DEFAULT NULL,  b2 int DEFAULT NULL,  KEY idx (a2))")
	// currently we only pull the correlated condition up to apply itself, but we don't convert it to join actively.
	// we just left cascades to it to xform and generate a join operator.
	tk.MustExec("INSERT INTO t1 (a1, b1, c1) VALUES (1, 2, 3), (4, NULL, 5),  (NULL, 6, 7),  (8, 9, NULL),  (10, 11, 12);")
	tk.MustExec("INSERT INTO t2 values (1,1),(2,2),(3,3)")
	for i := 0; i < 10; i++ {
		tk.MustExec("INSERT INTO t2 select * from t2")
	}
	tk.MustExec("analyze table t1, t2")
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(false)
	res1 := tk.MustQuery("explain format=\"brief\" SELECT 1 FROM t1 AS tab WHERE  (EXISTS(SELECT  1 FROM t2 WHERE a2 = a1 ))").String()
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(true)
	res2 := tk.MustQuery("explain format=\"brief\" SELECT 1 FROM t1 AS tab WHERE  (EXISTS(SELECT  1 FROM t2 WHERE a2 = a1 ))").String()
	require.Equal(t, res1, res2)

	tk.Session().GetSessionVars().SetEnableCascadesPlanner(false)
	res1 = tk.MustQuery("explain format=\"brief\" SELECT /*+ inl_join(tab, t2@sel_2) */ 1 FROM t1 AS tab WHERE  (EXISTS(SELECT  1 FROM t2 WHERE a2 = a1 ))").String()
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(true)
	res2 = tk.MustQuery("explain format=\"brief\" SELECT /*+ inl_join(tab, t2@sel_2) */ 1 FROM t1 AS tab WHERE  (EXISTS(SELECT  1 FROM t2 WHERE a2 = a1 ))").String()
	require.Equal(t, res1, res2)

	tk.Session().GetSessionVars().SetEnableCascadesPlanner(false)
	res1 = tk.MustQuery("explain format=\"brief\" SELECT /*+ inl_hash_join(tab, t2@sel_2) */ 1 FROM t1 AS tab WHERE  (EXISTS(SELECT  1 FROM t2 WHERE a2 = a1 ))").String()
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(true)
	res2 = tk.MustQuery("explain format=\"brief\" SELECT /*+ inl_hash_join(tab, t2@sel_2) */ 1 FROM t1 AS tab WHERE  (EXISTS(SELECT  1 FROM t2 WHERE a2 = a1 ))").String()
	require.Equal(t, res1, res2)

	tk.Session().GetSessionVars().SetEnableCascadesPlanner(false)
	res1 = tk.MustQuery("explain format=\"brief\" SELECT /*+ hash_join(tab, t2@sel_2) */ 1 FROM t1 AS tab WHERE  (EXISTS(SELECT  1 FROM t2 WHERE a2 = a1 ))").String()
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(true)
	res2 = tk.MustQuery("explain format=\"brief\" SELECT /*+ hash_join(tab, t2@sel_2) */ 1 FROM t1 AS tab WHERE  (EXISTS(SELECT  1 FROM t2 WHERE a2 = a1 ))").String()
	require.Equal(t, res1, res2)
}

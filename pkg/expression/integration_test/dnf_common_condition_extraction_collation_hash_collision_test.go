// Copyright 2026 PingCAP, Inc.
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

package integration_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestDnfCommonConditionExtractionCollationHashCollision(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("DROP TABLE IF EXISTS t2")
	tk.MustExec("CREATE TABLE t1(id INT PRIMARY KEY, c VARCHAR(10) COLLATE utf8mb4_general_ci, b INT)")
	tk.MustExec("CREATE TABLE t2(id INT PRIMARY KEY, x INT)")
	tk.MustExec("INSERT INTO t1 VALUES (1,'A',1),(2,'a',1),(3,'A',2),(4,'a',2)")
	tk.MustExec("INSERT INTO t2 VALUES (1,1),(2,1),(3,2),(4,2)")
	tk.MustQuery("SELECT t1.id\nFROM t1 JOIN t2\n  ON t1.id = t2.id\n AND ((c = 'A' COLLATE utf8mb4_general_ci AND t1.b = 1)\n   OR (c = 'A' COLLATE utf8mb4_bin AND t1.b = 2))\nORDER BY t1.id;").Check([][]any{
		{"1"},
		{"2"},
		{"3"},
	})
	tk.MustExec("TRUNCATE TABLE t1")
	tk.MustExec("INSERT INTO t1 VALUES (1,'A',1),(2,'a',1),(3,'A',2),(4,'a',2)")
	tk.MustExec("DELETE t1 FROM t1 JOIN t2 ON t1.id = t2.id AND ((c = 'A' COLLATE utf8mb4_general_ci AND t1.b = 1) OR (c = 'A' COLLATE utf8mb4_bin AND t1.b = 2))")
	tk.MustQuery("SELECT id FROM t1 ORDER BY id").Check(testkit.Rows("4"))
}

func TestRemoveDupExprsCollationHashCollision(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t_remove_dup")
	tk.MustExec("CREATE TABLE t_remove_dup(id INT PRIMARY KEY, c VARCHAR(10) COLLATE utf8mb4_general_ci)")
	tk.MustExec("INSERT INTO t_remove_dup VALUES (1,'A'),(2,'a')")
	tk.MustQuery("SELECT id FROM t_remove_dup WHERE c = 'A' COLLATE utf8mb4_general_ci AND c = 'A' COLLATE utf8mb4_bin ORDER BY id").Check(testkit.Rows("1"))
	tk.MustExec("TRUNCATE TABLE t_remove_dup")
	tk.MustExec("INSERT INTO t_remove_dup VALUES (1,'A'),(2,'a')")
	tk.MustExec("DELETE FROM t_remove_dup WHERE c = 'A' COLLATE utf8mb4_general_ci AND c = 'A' COLLATE utf8mb4_bin")
	tk.MustQuery("SELECT id FROM t_remove_dup ORDER BY id").Check(testkit.Rows("2"))
}

func TestIndexMergeCollationHashCollision(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t_hash")
	tk.MustExec("CREATE TABLE t_hash(id INT PRIMARY KEY, c VARCHAR(10) COLLATE utf8mb4_general_ci, j JSON, k INT, INDEX idx_mv(c, (CAST(j->'$.a' AS SIGNED ARRAY))), INDEX idx_k(k))")
	tk.MustExec(`INSERT INTO t_hash VALUES (1,'A','{"a":[1]}',1),(2,'a','{"a":[1]}',1),(3,'B','{"a":[1]}',1),(4,'A','{"a":[2]}',1),(5,'a','{"a":[2]}',1)`)
	tk.MustQuery("SELECT /*+ USE_INDEX_MERGE(t_hash, idx_mv, idx_k) */ id FROM t_hash WHERE c = 'A' COLLATE utf8mb4_general_ci AND c = 'A' COLLATE utf8mb4_bin AND 1 MEMBER OF (j->'$.a') AND k = 1 ORDER BY id").Check(testkit.Rows("1"))
	tk.MustExec("TRUNCATE TABLE t_hash")
	tk.MustExec(`INSERT INTO t_hash VALUES (1,'A','{"a":[1]}',1),(2,'a','{"a":[1]}',1),(3,'B','{"a":[1]}',1),(4,'A','{"a":[2]}',1),(5,'a','{"a":[2]}',1)`)
	tk.MustExec("DELETE /*+ USE_INDEX_MERGE(t_hash, idx_mv, idx_k) */ FROM t_hash WHERE c = 'A' COLLATE utf8mb4_general_ci AND c = 'A' COLLATE utf8mb4_bin AND 1 MEMBER OF (j->'$.a') AND k = 1")
	tk.MustQuery("SELECT id FROM t_hash ORDER BY id").Check(testkit.Rows("2", "3", "4", "5"))
}

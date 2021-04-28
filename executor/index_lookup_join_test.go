// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite1) TestIndexLookupJoinHang(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table idxJoinOuter (a int unsigned)")
	tk.MustExec("create table idxJoinInner (a int unsigned unique)")
	tk.MustExec("insert idxJoinOuter values (1), (1), (1), (1), (1)")
	tk.MustExec("insert idxJoinInner values (1)")
	tk.Se.GetSessionVars().IndexJoinBatchSize = 1
	tk.Se.GetSessionVars().SetIndexLookupJoinConcurrency(1)

	rs, err := tk.Exec("select /*+ INL_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	c.Assert(err, IsNil)
	req := rs.NewChunk()
	for i := 0; i < 5; i++ {
		// FIXME: cannot check err, since err exists,  Panic: [tikv:1690]BIGINT UNSIGNED value is out of range in '(Column#0 - 3)'
		_ = rs.Next(context.Background(), req)
	}
	err = rs.Close()
	c.Assert(err, IsNil)

	rs, err = tk.Exec("select /*+ INL_HASH_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	for i := 0; i < 5; i++ {
		// to fix: cannot check err, since err exists,  Panic: [tikv:1690]BIGINT UNSIGNED value is out of range in '(Column#0 - 3)'
		_ = rs.Next(context.Background(), req)
	}
	err = rs.Close()
	c.Assert(err, IsNil)

	rs, err = tk.Exec("select /*+ INL_MERGE_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	for i := 0; i < 5; i++ {
		// to fix: cannot check err, since err exists,  Panic: [tikv:1690]BIGINT UNSIGNED value is out of range in '(Column#0 - 3)'
		_ = rs.Next(context.Background(), req)
	}
	err = rs.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite1) TestIndexJoinUnionScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t1(id int primary key, a int)")
	tk.MustExec("create table t2(id int primary key, a int, b int, key idx_a(a))")
	tk.MustExec("insert into t2 values (1,1,1),(4,2,4)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(2,2)")
	tk.MustExec("insert into t2 values(2,2,2), (3,3,3)")
	// TableScan below UnionScan
	tk.MustQuery("select /*+ INL_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"2 2 2 2 2",
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"2 2 2 2 2",
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.id").Check(testkit.Rows(
		"2 2 2 2 2",
	))
	// IndexLookUp below UnionScan
	tk.MustQuery("select /*+ INL_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	// INL_MERGE_JOIN is invalid
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2)*/ * from t1 join t2 on t1.a = t2.a").Sort().Check(testkit.Rows(
		"2 2 2 2 2",
		"2 2 4 2 4",
	))
	// IndexScan below UnionScan
	tk.MustQuery("select /*+ INL_JOIN(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2",
		"2 2",
	))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2",
		"2 2",
	))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2)*/ t1.a, t2.a from t1 join t2 on t1.a = t2.a").Check(testkit.Rows(
		"2 2",
		"2 2",
	))
	tk.MustExec("rollback")
}

func (s *testSuite1) TestBatchIndexJoinUnionScan(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table t1(id int primary key, a int)")
	tk.MustExec("create table t2(id int primary key, a int, key idx_a(a))")
	tk.MustExec("set @@session.tidb_init_chunk_size=1")
	tk.MustExec("set @@session.tidb_index_join_batch_size=1")
	tk.MustExec("set @@session.tidb_index_lookup_join_concurrency=4")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values(1,1),(2,1),(3,1),(4,1)")
	tk.MustExec("insert into t2 values(1,1)")
	tk.MustQuery("select /*+ INL_JOIN(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.id").Check(testkit.Rows("4"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.id").Check(testkit.Rows("4"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2)*/ count(*) from t1 join t2 on t1.a = t2.id").Check(testkit.Rows("4"))
	tk.MustExec("rollback")
}

func (s *testSuite1) TestInapplicableIndexJoinHint(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint, b bigint);`)
	tk.MustExec(`create table t2(a bigint, b bigint);`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t1, t2) */ * from t1, t2;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1, t2) */ or /*+ TIDB_INLJ(t1, t2) */ is inapplicable without column equal ON condition`))
	tk.MustQuery(`select /*+ TIDB_INLJ(t1, t2) */ * from t1 join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1, t2) */ or /*+ TIDB_INLJ(t1, t2) */ is inapplicable`))

	tk.MustQuery(`select /*+ INL_HASH_JOIN(t1, t2) */ * from t1, t2;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t1, t2) */ is inapplicable without column equal ON condition`))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t1, t2) */ * from t1 join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t1, t2) */ is inapplicable`))

	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1, t2;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t1, t2) */ is inapplicable without column equal ON condition`))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1 join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t1, t2) */ is inapplicable`))

	tk.MustExec(`drop table if exists t1, t2;`)
	tk.MustExec(`create table t1(a bigint, b bigint, index idx_a(a));`)
	tk.MustExec(`create table t2(a bigint, b bigint);`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t1) */ * from t1 left join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t1) */ or /*+ TIDB_INLJ(t1) */ is inapplicable`))
	tk.MustQuery(`select /*+ TIDB_INLJ(t2) */ * from t1 right join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_JOIN(t2) */ or /*+ TIDB_INLJ(t2) */ is inapplicable`))

	tk.MustQuery(`select /*+ INL_HASH_JOIN(t1) */ * from t1 left join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t1) */ is inapplicable`))
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t2) */ * from t1 right join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t2) */ is inapplicable`))

	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t1) */ * from t1 left join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t1) */ is inapplicable`))
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t2) */ * from t1 right join t2 on t1.a=t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(`Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t2) */ is inapplicable`))
}

func (s *testSuite) TestIndexJoinOverflow(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1(a int)`)
	tk.MustExec(`insert into t1 values (-1)`)
	tk.MustExec(`create table t2(a int unsigned, index idx(a));`)
	tk.MustQuery(`select /*+ INL_JOIN(t2) */ * from t1 join t2 on t1.a = t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`select /*+ INL_HASH_JOIN(t2) */ * from t1 join t2 on t1.a = t2.a;`).Check(testkit.Rows())
	tk.MustQuery(`select /*+ INL_MERGE_JOIN(t2) */ * from t1 join t2 on t1.a = t2.a;`).Check(testkit.Rows())
}

func (s *testSuite5) TestIssue11061(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(c varchar(30), index ix_c(c(10)))")
	tk.MustExec("insert into t1 (c) values('7_chars'), ('13_characters')")
	tk.MustQuery("SELECT /*+ INL_JOIN(t1) */ SUM(LENGTH(c)) FROM t1 WHERE c IN (SELECT t1.c FROM t1)").Check(testkit.Rows("20"))
	tk.MustQuery("SELECT /*+ INL_HASH_JOIN(t1) */ SUM(LENGTH(c)) FROM t1 WHERE c IN (SELECT t1.c FROM t1)").Check(testkit.Rows("20"))
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1) */ SUM(LENGTH(c)) FROM t1 WHERE c IN (SELECT t1.c FROM t1)").Check(testkit.Rows("20"))
}

func (s *testSuite5) TestIndexJoinPartitionTable(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int not null, c int, key idx(c)) partition by hash(b) partitions 30")
	tk.MustExec("insert into t values(1, 27, 2)")
	tk.MustQuery("SELECT /*+ INL_JOIN(t1) */ count(1) FROM t t1 INNER JOIN (SELECT a, max(c) AS c FROM t WHERE b = 27 AND a = 1 GROUP BY a) t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.b = 27").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT /*+ INL_HASH_JOIN(t1) */ count(1) FROM t t1 INNER JOIN (SELECT a, max(c) AS c FROM t WHERE b = 27 AND a = 1 GROUP BY a) t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.b = 27").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1) */ count(1) FROM t t1 INNER JOIN (SELECT a, max(c) AS c FROM t WHERE b = 27 AND a = 1 GROUP BY a) t2 ON t1.a = t2.a AND t1.c = t2.c WHERE t1.b = 27").Check(testkit.Rows("1"))
}

func (s *testSuite5) TestIndexJoinMultiCondition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int not null, key idx_a_b(a,b))")
	tk.MustExec("create table t2(a int not null, b int not null)")
	tk.MustExec("insert into t1 values (0,1), (0,2), (0,3)")
	tk.MustExec("insert into t2 values (0,1), (0,2), (0,3)")
	tk.MustQuery("select /*+ TIDB_INLJ(t1) */ count(*) from t1, t2 where t1.a = t2.a and t1.b < t2.b").Check(testkit.Rows("3"))
}

func (s *testSuite5) TestIssue16887(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_roles, admin_role_has_permissions")
	tk.MustExec("CREATE TABLE `admin_role_has_permissions` (`permission_id` bigint(20) unsigned NOT NULL, `role_id` bigint(20) unsigned NOT NULL, PRIMARY KEY (`permission_id`,`role_id`), KEY `admin_role_has_permissions_role_id_foreign` (`role_id`))")
	tk.MustExec("CREATE TABLE `admin_roles` (`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL COMMENT '角色名称', `created_at` timestamp NULL DEFAULT NULL, `updated_at` timestamp NULL DEFAULT NULL, PRIMARY KEY (`id`))")
	tk.MustExec("INSERT INTO `admin_roles` (`id`, `name`, `created_at`, `updated_at`) VALUES(1, 'admin','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(2, 'developer','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(3, 'analyst','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(4, 'channel_admin','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(5, 'test','2020-04-27 02:40:08', '2020-04-27 02:40:08')")
	tk.MustExec("INSERT INTO `admin_role_has_permissions` (`permission_id`, `role_id`) VALUES(1, 1),(2, 1),(3, 1),(4, 1),(5, 1),(6, 1),(7, 1),(8, 1),(9, 1),(10, 1),(11, 1),(12, 1),(13, 1),(14, 1),(15, 1),(16, 1),(17, 1),(18, 1),(19, 1),(20, 1),(21, 1),(22, 1),(23, 1),(24, 1),(25, 1),(26, 1),(27, 1),(28, 1),(29, 1),(30, 1),(31, 1),(32, 1),(33, 1),(34, 1),(35, 1),(36, 1),(37, 1),(38, 1),(39, 1),(40, 1),(41, 1),(42, 1),(43, 1),(44, 1),(45, 1),(46, 1),(47, 1),(48, 1),(49, 1),(50, 1),(51, 1),(52, 1),(53, 1),(54, 1),(55, 1),(56, 1),(57, 1),(58, 1),(59, 1),(60, 1),(61, 1),(62, 1),(63, 1),(64, 1),(65, 1),(66, 1),(67, 1),(68, 1),(69, 1),(70, 1),(71, 1),(72, 1),(73, 1),(74, 1),(75, 1),(76, 1),(77, 1),(78, 1),(79, 1),(80, 1),(81, 1),(82, 1),(83, 1),(5, 4),(6, 4),(7, 4),(84, 5),(85, 5),(86, 5)")
	rows := tk.MustQuery("SELECT /*+ inl_merge_join(admin_role_has_permissions) */ `admin_roles`.* FROM `admin_roles` INNER JOIN `admin_role_has_permissions` ON `admin_roles`.`id` = `admin_role_has_permissions`.`role_id` WHERE `admin_role_has_permissions`.`permission_id`\n IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67)").Rows()
	c.Assert(len(rows), Equals, 70)
	rows = tk.MustQuery("show warnings").Rows()
	c.Assert(len(rows) > 0, Equals, true)
}

func (s *testSuite5) TestIndexJoinEnumSetIssue19233(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists i;")
	tk.MustExec("drop table if exists p1;")
	tk.MustExec("drop table if exists p2;")
	tk.MustExec(`CREATE TABLE p1 (type enum('HOST_PORT') NOT NULL, UNIQUE KEY (type)) ;`)
	tk.MustExec(`CREATE TABLE p2 (type set('HOST_PORT') NOT NULL, UNIQUE KEY (type)) ;`)
	tk.MustExec(`CREATE TABLE i (objectType varchar(64) NOT NULL);`)
	tk.MustExec(`insert into i values ('SWITCH');`)
	tk.MustExec(`create table t like i;`)
	tk.MustExec(`insert into t values ('HOST_PORT');`)
	tk.MustExec(`insert into t select * from t;`)
	tk.MustExec(`insert into t select * from t;`)
	tk.MustExec(`insert into t select * from t;`)
	tk.MustExec(`insert into t select * from t;`)
	tk.MustExec(`insert into t select * from t;`)
	tk.MustExec(`insert into t select * from t;`)

	tk.MustExec(`insert into i select * from t;`)

	tk.MustExec(`insert into p1 values('HOST_PORT');`)
	tk.MustExec(`insert into p2 values('HOST_PORT');`)
	for _, table := range []string{"p1", "p2"} {
		for _, hint := range []string{"INL_HASH_JOIN", "INL_MERGE_JOIN", "INL_JOIN"} {
			sql := fmt.Sprintf(`select /*+ %s(%s) */ * from i, %s where i.objectType = %s.type;`, hint, table, table, table)
			rows := tk.MustQuery(sql).Rows()
			c.Assert(len(rows), Equals, 64)
			for i := 0; i < len(rows); i++ {
				c.Assert(fmt.Sprint(rows[i][0]), Equals, "HOST_PORT")
			}
			rows = tk.MustQuery("show warnings").Rows()
			c.Assert(len(rows), Equals, 0)
		}
	}
}

func (s *testSuite5) TestIssue19411(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t1  (c_int int, primary key (c_int))")
	tk.MustExec("create table t2  (c_int int, primary key (c_int)) partition by hash (c_int) partitions 4")
	tk.MustExec("insert into t1 values (1)")
	tk.MustExec("insert into t2 values (1)")
	tk.MustExec("begin")
	tk.MustExec("insert into t1 values (2)")
	tk.MustExec("insert into t2 values (2)")
	tk.MustQuery("select /*+ INL_JOIN(t1,t2) */ * from t1 left join t2 on t1.c_int = t2.c_int").Check(testkit.Rows(
		"1 1",
		"2 2"))
	tk.MustExec("commit")
}

func (s *testSuite5) TestIssue23653(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key(c_str), unique key(c_int), unique key(c_str))")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key(c_int, c_str(4)), key(c_int), unique key(c_str))")
	tk.MustExec("insert into t1 values (1, 'cool buck'), (2, 'reverent keller')")
	tk.MustExec("insert into t2 select * from t1")
	tk.MustQuery("select /*+ inl_join(t2) */ * from t1, t2 where t1.c_str = t2.c_str and t1.c_int = t2.c_int and t1.c_int = 2").Check(testkit.Rows(
		"2 reverent keller 2 reverent keller"))
}

func (s *testSuite5) TestIssue23656(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c_int int, c_str varchar(40), primary key(c_int, c_str(4)))")
	tk.MustExec("create table t2 like t1")
	tk.MustExec("insert into t1 values (1, 'clever jang'), (2, 'blissful aryabhata')")
	tk.MustExec("insert into t2 select * from t1")
	tk.MustQuery("select /*+ inl_join(t2) */ * from t1 join t2 on t1.c_str = t2.c_str where t1.c_int = t2.c_int;").Check(testkit.Rows(
		"1 clever jang 1 clever jang",
		"2 blissful aryabhata 2 blissful aryabhata"))
}

func (s *testSuite5) TestIssue23722(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b char(10), c blob, primary key (c(5)) clustered);")
	tk.MustExec("insert into t values (20301,'Charlie',x'7a');")
	tk.MustQuery("select * from t;").Check(testkit.Rows("20301 Charlie z"))
	tk.MustQuery("select * from t where c in (select c from t where t.c >= 'a');").Check(testkit.Rows("20301 Charlie z"))

	// Test lookup content exceeds primary key prefix.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b char(10), c varchar(255), primary key (c(5)) clustered);")
	tk.MustExec("insert into t values (20301,'Charlie','aaaaaaa');")
	tk.MustQuery("select * from t;").Check(testkit.Rows("20301 Charlie aaaaaaa"))
	tk.MustQuery("select * from t where c in (select c from t where t.c >= 'a');").Check(testkit.Rows("20301 Charlie aaaaaaa"))

	// Test the original case.
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`CREATE TABLE t (
		col_15 decimal(49,3),
		col_16 smallint(5),
		col_17 char(118) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT 'tLOOjbIXuuLKPFjkLo',
		col_18 set('Alice','Bob','Charlie','David') NOT NULL,
		col_19 tinyblob,
		PRIMARY KEY (col_19(5),col_16) /*T![clustered_index] NONCLUSTERED */,
		UNIQUE KEY idx_10 (col_19(5),col_16)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec("INSERT INTO `t` VALUES (38799.400,20301,'KETeFZhkoxnwMAhA','Charlie',x'7a7968584570705a647179714e56');")
	tk.MustQuery("select  t.* from t where col_19 in  " +
		"( select col_19 from t where t.col_18 <> 'David' and t.col_19 >= 'jDzNn' ) " +
		"order by col_15 , col_16 , col_17 , col_18 , col_19;").Check(testkit.Rows("38799.400 20301 KETeFZhkoxnwMAhA Charlie zyhXEppZdqyqNV"))
}

func (s *testSuite5) TestPartitionTableIndexJoinAndIndexReader(c *C) {
	if israce.RaceEnabled {
		c.Skip("exhaustive types test, skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec(`create table t (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec("create table tnormal (a int, b int, key(a), key(b))")
	nRows := 64
	values := make([]string, 0, nRows)
	for i := 0; i < nRows; i++ {
		values = append(values, fmt.Sprintf("(%v, %v)", rand.Intn(nRows), rand.Intn(nRows)))
	}
	tk.MustExec(fmt.Sprintf("insert into t values %v", strings.Join(values, ", ")))
	tk.MustExec(fmt.Sprintf("insert into tnormal values %v", strings.Join(values, ", ")))

	randRange := func() (int, int) {
		a, b := rand.Intn(nRows), rand.Intn(nRows)
		if a > b {
			return b, a
		}
		return a, b
	}
	for i := 0; i < nRows; i++ {
		lb, rb := randRange()
		cond := fmt.Sprintf("(t2.b between %v and %v)", lb, rb)
		result := tk.MustQuery("select t1.a from tnormal t1, tnormal t2 where t1.a=t2.b and " + cond).Sort().Rows()
		tk.MustQuery("select /*+ TIDB_INLJ(t1, t2) */ t1.a from t t1, t t2 where t1.a=t2.b and " + cond).Sort().Check(result)
	}
}

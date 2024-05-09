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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package join_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestIndexLookupJoinHang(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table idxJoinOuter (a int unsigned)")
	tk.MustExec("create table idxJoinInner (a int unsigned unique)")
	tk.MustExec("insert idxJoinOuter values (1), (1), (1), (1), (1)")
	tk.MustExec("insert idxJoinInner values (1)")
	tk.Session().GetSessionVars().IndexJoinBatchSize = 1
	tk.Session().GetSessionVars().SetIndexLookupJoinConcurrency(1)

	rs, err := tk.Exec("select /*+ INL_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	for i := 0; i < 5; i++ {
		// FIXME: cannot check err, since err exists,  Panic: [tikv:1690]BIGINT UNSIGNED value is out of range in '(Column#0 - 3)'
		_ = rs.Next(context.Background(), req)
	}
	err = rs.Close()
	require.NoError(t, err)

	rs, err = tk.Exec("select /*+ INL_HASH_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	require.NoError(t, err)
	req = rs.NewChunk(nil)
	for i := 0; i < 5; i++ {
		// to fix: cannot check err, since err exists,  Panic: [tikv:1690]BIGINT UNSIGNED value is out of range in '(Column#0 - 3)'
		_ = rs.Next(context.Background(), req)
	}
	err = rs.Close()
	require.NoError(t, err)

	rs, err = tk.Exec("select /*+ INL_MERGE_JOIN(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	require.NoError(t, err)
	req = rs.NewChunk(nil)
	for i := 0; i < 5; i++ {
		// to fix: cannot check err, since err exists,  Panic: [tikv:1690]BIGINT UNSIGNED value is out of range in '(Column#0 - 3)'
		_ = rs.Next(context.Background(), req)
	}
	err = rs.Close()
	require.NoError(t, err)
}

func TestIssue16887(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_roles, admin_role_has_permissions")
	tk.MustExec("CREATE TABLE `admin_role_has_permissions` (`permission_id` bigint(20) unsigned NOT NULL, `role_id` bigint(20) unsigned NOT NULL, PRIMARY KEY (`permission_id`,`role_id`), KEY `admin_role_has_permissions_role_id_foreign` (`role_id`))")
	tk.MustExec("CREATE TABLE `admin_roles` (`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT, `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL COMMENT '角色名称', `created_at` timestamp NULL DEFAULT NULL, `updated_at` timestamp NULL DEFAULT NULL, PRIMARY KEY (`id`))")
	tk.MustExec("INSERT INTO `admin_roles` (`id`, `name`, `created_at`, `updated_at`) VALUES(1, 'admin','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(2, 'developer','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(3, 'analyst','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(4, 'channel_admin','2020-04-27 02:40:03', '2020-04-27 02:40:03'),(5, 'test','2020-04-27 02:40:08', '2020-04-27 02:40:08')")
	tk.MustExec("INSERT INTO `admin_role_has_permissions` (`permission_id`, `role_id`) VALUES(1, 1),(2, 1),(3, 1),(4, 1),(5, 1),(6, 1),(7, 1),(8, 1),(9, 1),(10, 1),(11, 1),(12, 1),(13, 1),(14, 1),(15, 1),(16, 1),(17, 1),(18, 1),(19, 1),(20, 1),(21, 1),(22, 1),(23, 1),(24, 1),(25, 1),(26, 1),(27, 1),(28, 1),(29, 1),(30, 1),(31, 1),(32, 1),(33, 1),(34, 1),(35, 1),(36, 1),(37, 1),(38, 1),(39, 1),(40, 1),(41, 1),(42, 1),(43, 1),(44, 1),(45, 1),(46, 1),(47, 1),(48, 1),(49, 1),(50, 1),(51, 1),(52, 1),(53, 1),(54, 1),(55, 1),(56, 1),(57, 1),(58, 1),(59, 1),(60, 1),(61, 1),(62, 1),(63, 1),(64, 1),(65, 1),(66, 1),(67, 1),(68, 1),(69, 1),(70, 1),(71, 1),(72, 1),(73, 1),(74, 1),(75, 1),(76, 1),(77, 1),(78, 1),(79, 1),(80, 1),(81, 1),(82, 1),(83, 1),(5, 4),(6, 4),(7, 4),(84, 5),(85, 5),(86, 5)")
	rows := tk.MustQuery("SELECT /*+ inl_merge_join(admin_role_has_permissions) */ `admin_roles`.* FROM `admin_roles` INNER JOIN `admin_role_has_permissions` ON `admin_roles`.`id` = `admin_role_has_permissions`.`role_id` WHERE `admin_role_has_permissions`.`permission_id`\n IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67)").Rows()
	require.Len(t, rows, 70)
	rows = tk.MustQuery("show warnings").Rows()
	require.Less(t, 0, len(rows))
}

func TestPartitionTableIndexJoinAndIndexReader(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec(`create table t (a int, b int, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec("create table tnormal (a int, b int, key(a), key(b))")
	nRows := 512
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

func TestIssue45716(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_mem_quota_query = 120000;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(a int, index(a));")
	tk.MustExec("create table t2(a int, index(a));")
	tk.MustExec("insert into t1 values (1), (2);")
	tk.MustExec("insert into t2 values (1),(1),(2),(2);")

	failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/inlNewInnerPanic", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/inlNewInnerPanic")
	err := tk.QueryToErr("select /*+ inl_join(t2) */ * from t1 join t2 on t1.a = t2.a;")
	tk.MustContainErrMsg(err.Error(), "test inlNewInnerPanic")
}

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

package core_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaskingPolicyProjection(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, c varchar(10))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b')")
	tk.MustExec("create masking policy p on t(c) as concat(c, 'x') enable")

	tk.MustQuery("select c from t order by id").Check(testkit.Rows("ax", "bx"))
	tk.MustQuery("select concat(c, '-') from t where c = 'a'").Check(testkit.Rows("ax-"))

	// Predicate should still use original value.
	rows := tk.MustQuery("select c from t where c = 'ax'").Rows()
	require.Len(t, rows, 0)
}

func TestMaskingPolicyPointGet(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_pointget")
	tk.MustExec("create table t_pointget(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_pointget values (1, 'secret'), (2, 'hidden')")
	tk.MustExec("create masking policy p_pointget on t_pointget(c) as concat('***', c) enable")

	// Test Point_Get with primary key lookup
	tk.MustQuery("select c from t_pointget where id = 1").Check(testkit.Rows("***secret"))
	tk.MustQuery("select c from t_pointget where id = 2").Check(testkit.Rows("***hidden"))

	// Test Point_Get with multiple columns
	tk.MustQuery("select id, c from t_pointget where id = 1").Check(testkit.Rows("1 ***secret"))

	// Test Point_Get with expression using masked column
	tk.MustQuery("select concat(c, '-') from t_pointget where id = 1").Check(testkit.Rows("***secret-"))

	// Test Point_Get with unique index lookup
	tk.MustExec("create unique index idx_c on t_pointget(c)")
	tk.MustQuery("select c from t_pointget where c = 'secret'").Check(testkit.Rows("***secret"))

	// Predicate should still use original value
	rows := tk.MustQuery("select c from t_pointget where c = '***secret'").Rows()
	require.Len(t, rows, 0)
}

func TestMaskingPolicyBatchPointGet(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_batch_pointget")
	tk.MustExec("create table t_batch_pointget(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_batch_pointget values (1, 'secret'), (2, 'hidden'), (3, 'masked')")
	tk.MustExec("create masking policy p_batch on t_batch_pointget(c) as concat('***', c) enable")

	// Test BatchPointGet with WHERE pk IN (...)
	tk.MustQuery("select c from t_batch_pointget where id in (1, 2)").Check(testkit.Rows("***secret", "***hidden"))

	// Test BatchPointGet with multiple values
	tk.MustQuery("select c from t_batch_pointget where id in (1, 2, 3)").Check(testkit.Rows("***secret", "***hidden", "***masked"))

	// Test BatchPointGet with multiple columns
	tk.MustQuery("select id, c from t_batch_pointget where id in (1, 2)").Check(testkit.Rows("1 ***secret", "2 ***hidden"))

	// Test BatchPointGet with expression using masked column
	tk.MustQuery("select concat(c, '-') from t_batch_pointget where id in (1)").Check(testkit.Rows("***secret-"))

	// Test BatchPointGet with unique index IN clause
	tk.MustExec("create unique index idx_c on t_batch_pointget(c)")
	tk.MustQuery("select c from t_batch_pointget where c in ('secret', 'hidden')").Check(testkit.Rows("***secret", "***hidden"))

	// Predicate should still use original value
	rows := tk.MustQuery("select c from t_batch_pointget where c in ('***secret')").Rows()
	require.Len(t, rows, 0)

	// Test single value in IN (might use PointGet or BatchPointGet)
	tk.MustQuery("select c from t_batch_pointget where id in (1)").Check(testkit.Rows("***secret"))
}

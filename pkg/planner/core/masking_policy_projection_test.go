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

	"github.com/pingcap/tidb/pkg/parser/auth"
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
	tk.MustExec("create masking policy p_pointget on t_pointget(c) as mask_full(c, '*') enable")

	// Test Point_Get with primary key lookup
	tk.MustQuery("select c from t_pointget where id = 1").Check(testkit.Rows("******"))
	tk.MustQuery("select c from t_pointget where id = 2").Check(testkit.Rows("******"))

	// Test Point_Get with multiple columns
	tk.MustQuery("select id, c from t_pointget where id = 1").Check(testkit.Rows("1 ******"))

	// Test Point_Get with expression using masked column
	tk.MustQuery("select concat(c, '-') from t_pointget where id = 1").Check(testkit.Rows("******-"))

	// Test Point_Get with unique index lookup
	tk.MustExec("create unique index idx_c on t_pointget(c)")
	tk.MustQuery("select c from t_pointget where c = 'secret'").Check(testkit.Rows("******"))

	// Predicate should still use original value
	rows := tk.MustQuery("select c from t_pointget where c = '******'").Rows()
	require.Len(t, rows, 0)
}

func TestMaskingPolicyBlobAndClob(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_blob_clob")
	tk.MustExec("create table t_blob_clob(id int primary key, c longtext, b longblob, b2 longblob)")
	tk.MustExec("insert into t_blob_clob values (1, 'secret', x'31323334', x'616263')")
	tk.MustExec("create masking policy p_clob on t_blob_clob(c) as mask_full(c, '#') enable")
	tk.MustExec("create masking policy p_blob_full on t_blob_clob(b) as mask_full(b, '*') enable")
	tk.MustExec("create masking policy p_blob_null on t_blob_clob(b2) as mask_null(b2) enable")

	tk.MustQuery("select c, hex(b), b2 is null from t_blob_clob").
		Check(testkit.Rows("###### 2A2A2A2A 1"))
}

func TestMaskingPolicyCurrentIdentityOperators(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tkRoot := testkit.NewTestKit(t, store)
	require.NoError(t, tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tkRoot.MustExec("use test")

	tkRoot.MustExec("drop table if exists t_identity")
	tkRoot.MustExec("create table t_identity(id int primary key, c varchar(20))")
	tkRoot.MustExec("insert into t_identity values (1, 'secret')")
	tkRoot.MustExec("drop user if exists u_identity")
	tkRoot.MustExec("create user u_identity")
	tkRoot.MustExec("grant select on test.t_identity to u_identity")
	tkRoot.MustExec(`create masking policy p_identity on t_identity(c) as
		case when current_user() != 'root@%' then mask_full(c, '*') else c end enable`)

	tkRoot.MustQuery("select c from t_identity").Check(testkit.Rows("secret"))

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u_identity", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")
	tkUser.MustQuery("select c from t_identity").Check(testkit.Rows("******"))

	tkRoot.MustExec(`alter table t_identity modify masking policy p_identity set expression =
		case when current_role() = 'NONE' then mask_full(c, '*') else c end`)
	tkUser.MustQuery("select c from t_identity").Check(testkit.Rows("******"))
	tkRoot.MustExec(`alter table t_identity modify masking policy p_identity set expression =
		case when current_role() != 'NONE' then mask_full(c, '*') else c end`)
	tkUser.MustQuery("select c from t_identity").Check(testkit.Rows("secret"))
}

func TestMaskingPolicyCurrentRoleSwitchImmediate(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tkRoot := testkit.NewTestKit(t, store)
	require.NoError(t, tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tkRoot.MustExec("use test")

	tkRoot.MustExec("drop table if exists t_role_switch")
	tkRoot.MustExec("drop user if exists u_role_switch")
	tkRoot.MustExec("drop role if exists r_mask")
	tkRoot.MustExec("create table t_role_switch(id int primary key, c varchar(20))")
	tkRoot.MustExec("insert into t_role_switch values (1, 'secret')")
	tkRoot.MustExec("create user u_role_switch")
	tkRoot.MustExec("create role r_mask")
	tkRoot.MustExec("grant r_mask to u_role_switch")
	tkRoot.MustExec("grant select on test.t_role_switch to u_role_switch")
	tkRoot.MustExec("create masking policy p_role_switch on t_role_switch(c) as case when current_role() = '`r_mask`@`%`' then c else mask_full(c, '*') end enable")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u_role_switch", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")
	tkUser.MustExec("set role r_mask")
	tkUser.MustQuery("select c from t_role_switch").Check(testkit.Rows("secret"))
	tkUser.MustExec("set role none")
	tkUser.MustQuery("select c from t_role_switch").Check(testkit.Rows("******"))
	tkUser.MustExec("set role r_mask")
	tkUser.MustQuery("select c from t_role_switch").Check(testkit.Rows("secret"))
}

func TestMaskingPolicyBatchPointGet(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_batch_pointget")
	tk.MustExec("create table t_batch_pointget(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_batch_pointget values (1, 'secret'), (2, 'hidden'), (3, 'masked')")
	tk.MustExec("create masking policy p_batch on t_batch_pointget(c) as mask_full(c, '*') enable")

	// Test BatchPointGet with WHERE pk IN (...) - this should return masked values
	tk.MustQuery("select c from t_batch_pointget where id in (1, 2)").Check(testkit.Rows("******", "******"))

	// Test BatchPointGet with multiple values
	tk.MustQuery("select c from t_batch_pointget where id in (1, 2, 3)").Check(testkit.Rows("******", "******", "******"))

	// Test BatchPointGet with multiple columns
	tk.MustQuery("select id, c from t_batch_pointget where id in (1, 2)").Check(testkit.Rows("1 ******", "2 ******"))

	// Test BatchPointGet with expression using masked column
	tk.MustQuery("select concat(c, '-') from t_batch_pointget where id in (1)").Check(testkit.Rows("******-"))

	// Test BatchPointGet with unique index IN clause
	tk.MustExec("create unique index idx_c on t_batch_pointget(c)")
	tk.MustQuery("select c from t_batch_pointget where c in ('secret', 'hidden')").Check(testkit.Rows("******", "******"))

	// Predicate should still use original value (should not find masked values)
	rows := tk.MustQuery("select c from t_batch_pointget where c in ('******')").Rows()
	require.Len(t, rows, 0)

	// Test single value in IN (might use PointGet or BatchPointGet)
	tk.MustQuery("select c from t_batch_pointget where id in (1)").Check(testkit.Rows("******"))
}

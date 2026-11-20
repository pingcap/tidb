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
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	core "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaskingPolicyRestrictOnSubquerySources(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tkRoot := testkit.NewTestKit(t, store)
	require.NoError(t, tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tkRoot.MustExec("use test")

	tkRoot.MustExec("drop table if exists src_restrict, src_none, dst")
	tkRoot.MustExec("create table src_restrict(c varchar(20))")
	tkRoot.MustExec("create table src_none(c varchar(20))")
	tkRoot.MustExec("create table dst(c varchar(20))")
	tkRoot.MustExec("insert into src_restrict values ('secret')")
	tkRoot.MustExec("insert into src_none values ('secret')")

	tkRoot.MustExec(`create masking policy p_restrict on src_restrict(c) as
		case when current_user() = 'root@%' then c else mask_full(c) end
		restrict on (insert_into_select, update_select, delete_select) enable`)
	tkRoot.MustExec(`create masking policy p_none on src_none(c) as
		case when current_user() = 'root@%' then c else mask_full(c) end enable`)

	tkRoot.MustExec("create user if not exists 'u1'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test.* to 'u1'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")

	tkUser.MustGetErrCode("insert into dst select c from src_restrict", errno.ErrAccessDeniedToMaskedColumn)
	tkUser.MustGetErrCode("update dst set c = (select c from src_restrict limit 1)", errno.ErrAccessDeniedToMaskedColumn)
	tkUser.MustExec("insert into dst values ('secret')")
	tkUser.MustGetErrCode("delete from dst where c = (select c from src_restrict limit 1)", errno.ErrAccessDeniedToMaskedColumn)

	tkUser.MustExec("insert into dst select c from src_none")
	tkUser.MustQuery("select c from dst order by c").Check(testkit.Rows("XXXXXX", "secret"))

	tkRoot.MustExec("insert into dst select c from src_restrict")
	tkRoot.MustExec("update dst set c = (select c from src_restrict limit 1)")
	tkRoot.MustExec("delete from dst where c = (select c from src_restrict limit 1)")
}

func TestMaskingPolicyRestrictOnConstantDerivedOutput(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table src_constant(c varchar(20))")
	tk.MustExec("create table dst_constant(c varchar(20))")
	tk.MustExec("insert into src_constant values ('secret')")
	tk.MustExec("create masking policy p on src_constant(c) as 'X' restrict on (insert_into_select)")
	for _, source := range []string{
		"select c from src_constant",
		"select concat(c, '!') as v from src_constant",
		"select v from (select concat(c, '!') as v from src_constant) d",
		"with x as (select concat(c, '!') as v from src_constant) select v from x",
	} {
		tk.MustGetErrCode("insert into dst_constant "+source, errno.ErrAccessDeniedToMaskedColumn)
	}
	tk.MustExec("insert into dst_constant select 'public' from src_constant")
	tk.MustExec("alter table src_constant modify masking policy p set restrict on none")
	tk.MustExec("insert into dst_constant select concat(c, '!') from src_constant")
	tk.MustQuery("select c from dst_constant order by c").Check(testkit.Rows("X!", "public"))
}

func TestMaskingPolicyRestrictOnNoneToggle(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tkRoot := testkit.NewTestKit(t, store)
	require.NoError(t, tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tkRoot.MustExec("use test")

	tkRoot.MustExec("drop table if exists src_toggle, dst_toggle")
	tkRoot.MustExec("create table src_toggle(c varchar(20))")
	tkRoot.MustExec("create table dst_toggle(c varchar(20))")
	tkRoot.MustExec("insert into src_toggle values ('secret')")

	tkRoot.MustExec(`create masking policy p_toggle on src_toggle(c) as
		case when current_user() = 'root@%' then c else mask_full(c) end
		restrict on (insert_into_select) enable`)

	tkRoot.MustExec("create user if not exists 'u_toggle'@'%'")
	tkRoot.MustExec("grant select, insert on test.* to 'u_toggle'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u_toggle", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")

	tkUser.MustGetErrCode("insert into dst_toggle select c from src_toggle", errno.ErrAccessDeniedToMaskedColumn)
	tkRoot.MustExec("alter table src_toggle modify masking policy p_toggle set restrict on none")
	tkUser.MustExec("insert into dst_toggle select c from src_toggle")
	tkUser.MustQuery("select c from dst_toggle").Check(testkit.Rows("XXXXXX"))
}

func TestMaskingPolicyUpdateDerivedTableRestrictions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table update_source(c varchar(20))")
	root.MustExec("create table update_target(c varchar(20))")
	root.MustExec("insert into update_source values ('secret')")
	root.MustExec("insert into update_target values ('unchanged')")
	root.MustExec("create masking policy p on update_source(c) as case when current_user()='root@%' then c else mask_full(c) end restrict on (update_select)")
	root.MustExec("create user derived_update_user")
	root.MustExec("grant select,update on test.* to derived_update_user")
	user := testkit.NewTestKit(t, store)
	require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "derived_update_user", Hostname: "%"}, nil, nil, nil))
	user.MustExec("use test")
	for _, sql := range []string{
		"update update_target join (select c from update_source) s on true set update_target.c=s.c",
		"with s as (select c from update_source) update update_target join s on true set update_target.c=s.c",
	} {
		user.MustGetErrCode(sql, errno.ErrAccessDeniedToMaskedColumn)
		root.MustQuery("select c from update_target").Check(testkit.Rows("unchanged"))
	}
	user.MustExec("update update_target set c=concat(c,'!')")
	root.MustExec("update update_target join (select c from update_source) s on true set update_target.c=s.c")
	root.MustQuery("select c from update_target").Check(testkit.Rows("secret"))
}

func TestMaskingPolicyRestrictionSourceBindings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database source_db")
	tk.MustExec("use test")
	tk.MustExec("create table protected(c varchar(20))")
	tk.MustExec("create table source_db.protected(c varchar(20))")
	tk.MustExec("create table plain(c varchar(20))")
	tk.MustExec("create table binding_dst(c varchar(20))")
	tk.MustExec("insert into source_db.protected values ('public')")
	tk.MustExec("insert into plain values ('plain')")
	tk.MustExec("create masking policy p on protected(c) as mask_full(c) restrict on (insert_into_select)")
	tk.MustExec("insert into binding_dst with x as (select c from source_db.protected) select c from x")
	tk.MustExec("insert into binding_dst select protected.c from (select 'constant' as c from plain) protected")
	tk.MustQuery("select c from binding_dst order by c").Check(testkit.Rows("constant", "public"))
	tk.MustExec("create masking policy p on source_db.protected(c) as mask_full(c) restrict on (insert_into_select)")
	tk.MustGetErrCode("insert into binding_dst with x as (select c from source_db.protected) select c from x", errno.ErrAccessDeniedToMaskedColumn)
	tk.MustGetErrCode("insert into binding_dst select p.c from (select c from source_db.protected) p", errno.ErrAccessDeniedToMaskedColumn)
}

func TestMaskingPolicyRestrictOnInsertSelectStar(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tkRoot := testkit.NewTestKit(t, store)
	require.NoError(t, tkRoot.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tkRoot.MustExec("use test")

	tkRoot.MustExec("drop table if exists payment_details, payment_details_copy")
	tkRoot.MustExec("create table payment_details (id int primary key, customer_id int, card_no varchar(20), expiry_date date)")
	tkRoot.MustExec("insert into payment_details values (1, 1, '23233438477283', '2030-05-06')")
	tkRoot.MustExec(`create masking policy p_pan_mask on payment_details(card_no) as
		case when current_user() = 'root@%' then card_no else mask_partial(card_no, 6, 4, '*') end enable`)
	tkRoot.MustExec("alter table payment_details modify masking policy p_pan_mask set restrict on (insert_into_select)")
	tkRoot.MustExec("create table payment_details_copy like payment_details")

	tkRoot.MustExec("create user if not exists 'u_star'@'%'")
	tkRoot.MustExec("grant select, insert on test.* to 'u_star'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{Username: "u_star", Hostname: "%"}, nil, nil, nil))
	tkUser.MustExec("use test")
	tkUser.MustGetErrCode("insert into payment_details_copy select * from payment_details", errno.ErrAccessDeniedToMaskedColumn)
}

func TestMaskingPolicyRestrictRequiresIdentity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create user 'u_restrict'@'%'")
	root.MustExec("grant select, insert, update, delete on test.* to 'u_restrict'@'%'")
	user := testkit.NewTestKit(t, store)
	require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "u_restrict", Hostname: "%"}, nil, nil, nil))
	user.MustExec("use test")
	cases := []struct{ tp, value, masked string }{
		{"decimal(10,2)", "123.45", "mask_full(c)"},
		{"float", "123.45", "mask_full(c)"},
		{"double", "123.45", "mask_full(c)"},
		{"varchar(64)", "'secret'", "case when c like 'TIDB_MASKING_SENTINEL_%' then c else mask_full(c) end"},
	}
	for i, tc := range cases {
		src, dst := fmt.Sprintf("restrict_src_%d", i), fmt.Sprintf("restrict_dst_%d", i)
		root.MustExec(fmt.Sprintf("create table %s(c %s)", src, tc.tp))
		root.MustExec(fmt.Sprintf("create table %s(c %s)", dst, tc.tp))
		root.MustExec(fmt.Sprintf("insert into %s values (%s)", src, tc.value))
		root.MustExec(fmt.Sprintf("create masking policy p on %s(c) as case when current_user() = 'root@%%' then c else %s end restrict on (insert_into_select, update_select, delete_select) enable", src, tc.masked))
		user.MustGetErrCode(fmt.Sprintf("insert into %s select c from %s", dst, src), errno.ErrAccessDeniedToMaskedColumn)
		user.MustGetErrCode(fmt.Sprintf("update %s set c = (select c from %s limit 1)", dst, src), errno.ErrAccessDeniedToMaskedColumn)
		user.MustGetErrCode(fmt.Sprintf("delete from %s where c = (select c from %s limit 1)", dst, src), errno.ErrAccessDeniedToMaskedColumn)
		root.MustExec(fmt.Sprintf("insert into %s select c from %s", dst, src))
	}
}

func TestMaskingPolicyRestrictRoleBranches(t *testing.T) {
	for _, instance := range []bool{false, true} {
		t.Run(fmt.Sprint(instance), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			root := testkit.NewTestKit(t, store)
			root.MustExec("create user restrict_role_user")
			root.MustExec("create role restrict_unmask_role")
			root.MustExec("grant restrict_unmask_role to restrict_role_user")
			root.MustExec("grant select,insert,update,delete on test.* to restrict_role_user")
			root.MustExec("create table test.role_src(c varchar(20))")
			root.MustExec("create table test.role_dst(c varchar(20))")
			root.MustExec("insert into test.role_src values ('secret')")
			root.MustExec("create masking policy p on test.role_src(c) as case when current_role() != 'NONE' then c else mask_full(c) end restrict on (insert_into_select,update_select,delete_select)")
			user := testkit.NewTestKit(t, store)
			require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "restrict_role_user", Hostname: "%"}, nil, nil, nil))
			user.MustExec("set tidb_enable_plan_cache_for_subquery=1")
			ctx := context.Background()
			if instance {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			for _, sql := range []string{
				"insert into test.role_dst select c from test.role_src",
				"update test.role_dst set c=c where c in (select c from test.role_src)",
				"delete from test.role_dst where c in (select c from test.role_src)",
			} {
				user.MustExec("set role all")
				user.MustExecWithContext(ctx, "prepare st from '"+sql+"'")
				for range 2 {
					user.MustExecWithContext(ctx, "execute st")
				}
				user.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
				user.MustExec("set role none")
				_, err := user.ExecWithContext(ctx, "execute st")
				require.Error(t, err)
				require.ErrorContains(t, err, "Access to masked column")
				user.MustExec("set role all")
				user.MustExecWithContext(ctx, "execute st")
				user.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
				user.MustExec("deallocate prepare st")
			}
		})
	}
}

func TestMaskingPolicyRestrictInstancePlanCache(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprint("prepared=", prepared), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			root := testkit.NewTestKit(t, store)
			root.MustExec("create user 'restrict_host_user'@'%'")
			root.MustExec("create role restrict_host_role")
			root.MustExec("grant restrict_host_role to 'restrict_host_user'@'%'")
			root.MustExec("grant select,insert on test.* to 'restrict_host_user'@'%'")
			root.MustExec("create table test.host_src(c varchar(64))")
			root.MustExec("create table test.host_dst(c varchar(64))")
			root.MustExec("insert into test.host_src values ('secret')")
			root.MustExec("create masking policy p on test.host_src(c) as case when user()='restrict_host_user@192.0.2.1' or current_role()!='NONE' then c else mask_full(c) end restrict on (insert_into_select)")
			ctx := context.WithValue(context.Background(), core.PlanCacheKeyEnableInstancePlanCache{}, true)
			sql := "insert into test.host_dst select c from test.host_src"
			for sessionIdx, host := range []string{"192.0.2.1", "192.0.2.2", "192.0.2.1"} {
				tk := testkit.NewTestKit(t, store)
				require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "restrict_host_user", Hostname: host}, nil, nil, nil))
				executeSQL := sql
				tk.MustExec("set role all")
				if prepared {
					tk.MustExecWithContext(ctx, "prepare st from '"+sql+"'")
					executeSQL = "execute st"
				} else {
					tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
					tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=1")
					if sessionIdx == 1 {
						// Build the session-local statement descriptor before revoking access.
						tk.MustExecWithContext(ctx, executeSQL)
						tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
					}
				}
				tk.MustExec("set role none")
				cloned := false
				execCtx := context.WithValue(ctx, core.PlanCacheKeyTestClone{}, func(base.Plan, base.Plan) { cloned = true })
				if sessionIdx == 1 {
					_, err := tk.ExecWithContext(execCtx, executeSQL)
					require.ErrorContains(t, err, "Access to masked column")
					require.True(t, cloned, "authorization must run after looking up the other session's cached plan")
					continue
				}
				for range 2 {
					tk.MustExecWithContext(execCtx, executeSQL)
				}
				require.True(t, cloned)
				tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
			}
			rows := testkit.Rows("secret", "secret", "secret", "secret")
			if !prepared {
				rows = append(rows, []any{"secret"})
			}
			root.MustQuery("select c from test.host_dst").Check(rows)
		})
	}
}

func TestMaskingPolicyRestrictAggregateSources(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table src_agg(c varchar(20))")
	root.MustExec("create table dst_agg(c varchar(20))")
	root.MustExec("insert into src_agg values ('secret')")
	root.MustExec("insert into dst_agg values ('secret')")
	root.MustExec("create masking policy p on src_agg(c) as case when current_user()='root@%' then c else mask_full(c) end restrict on (insert_into_select, update_select, delete_select)")
	root.MustExec("create user aggregate_user")
	root.MustExec("grant select,insert,update,delete on test.* to aggregate_user")
	user := testkit.NewTestKit(t, store)
	require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "aggregate_user", Hostname: "%"}, nil, nil, nil))
	user.MustExec("use test")
	for _, agg := range []string{"max", "min"} {
		for _, sql := range []string{
			"delete from dst_agg where c = (select " + agg + "(c) from src_agg)",
			"update dst_agg set c=c where c = (select " + agg + "(c) from src_agg)",
			"insert into dst_agg select " + agg + "(c) from src_agg",
		} {
			user.MustGetErrCode(sql, errno.ErrAccessDeniedToMaskedColumn)
		}
	}
	root.MustExec("delete from dst_agg where c = (select max(c) from src_agg)")
	root.MustQuery("select count(*) from dst_agg").Check(testkit.Rows("0"))
}

func TestMaskingPolicyRestrictUsesScanSource(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database restrict_archive")
	tk.MustExec("create database restrict_app")
	tk.MustExec("use restrict_app")
	tk.MustExec("create table restrict_archive.items(c varchar(20))")
	tk.MustExec("create table items(c varchar(20))")
	tk.MustExec("create table plain(c varchar(20))")
	tk.MustExec("create table dst(c varchar(20))")
	tk.MustExec("insert into restrict_archive.items values ('public')")
	tk.MustExec("insert into items values ('secret')")
	tk.MustExec("insert into plain values ('public')")
	tk.MustExec("create masking policy p on items(c) as 'X' restrict on (insert_into_select)")
	tk.MustExec("create view v_items as select c from items")

	// Names that match the protected table without reading it are not restricted.
	for _, source := range []string{
		// A CTE over the table of the same name in another database.
		"with x as (select c from restrict_archive.items) select c from x",
		"with x as (select c from restrict_archive.items) select c from x union all select c from x",
		"select c from restrict_archive.items",
		"select c from (select c from restrict_archive.items) d",
		// A derived table or a CTE named after the protected table.
		"select items.c from (select 'public' as c from plain) as items",
		"select c from (select 'public' as c from plain) as items",
		"select items.c from (select c from plain) as items",
		"select items.c from plain as items",
		"with items as (select c from plain) select c from items",
	} {
		tk.MustExec("insert into dst " + source)
	}
	tk.MustQuery("select distinct c from dst").Check(testkit.Rows("public"))

	// Reads of the protected column stay restricted under any name.
	for _, source := range []string{
		"select c from items",
		"select plain.c from items as plain",
		"select c from restrict_app.items",
		"with x as (select c from items) select c from x",
		"with x as (select c from items) select c from x union all select c from x",
		"with plain as (select concat(c, '') as c from items) select c from plain",
		"select d.v from (select concat(c, '!') as v from items) d",
		"select c from v_items",
		"select max(c) from items",
		"select l.c from items l join plain r using (c)",
		"select r.c from plain l natural join items r",
		"select c from items union all select c from items",
		"select c from items group by c with rollup",
	} {
		tk.MustGetErrCode("insert into dst "+source, errno.ErrAccessDeniedToMaskedColumn)
	}
	tk.MustExec("create temporary table items(c varchar(20))")
	tk.MustExec("insert into dst select c from items")
	tk.MustGetErrCode("insert into dst select c from v_items", errno.ErrAccessDeniedToMaskedColumn)
}

func TestMaskingPolicyRestrictTightenedAfterPrepare(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table src_tighten(id int primary key, c varchar(20))")
	root.MustExec("create table dst_tighten(id int primary key, c varchar(20))")
	root.MustExec("insert into src_tighten values (1, 'secret')")
	root.MustExec("insert into dst_tighten values (1, 'old'), (2, 'secret')")
	root.MustExec(`create masking policy p_tighten on src_tighten(c) as
		case when current_user() = 'root@%' then c else mask_full(c) end restrict on none enable`)
	root.MustExec("create user 'u_tighten'@'%'")
	root.MustExec("grant select, insert, update, delete on test.* to 'u_tighten'@'%'")

	for _, tc := range []struct {
		restrictOn, sql string
		cacheable       bool
	}{
		{"insert_into_select", "insert into dst_tighten select id + 10, c from src_tighten", true},
		// Keep the scalar form as an uncacheable control; the JOIN form must hit.
		{"update_select", "update dst_tighten set c = (select c from src_tighten where id = 1) where id = 1", false},
		{"update_select", "update dst_tighten d join (select id,c from src_tighten) s on d.id=s.id set d.c=s.c", true},
		{"delete_select", "delete from dst_tighten where c in (select c from src_tighten)", true},
	} {
		for _, planCache := range []string{"1", "0"} {
			t.Run(tc.sql+"/plan_cache="+planCache, func(t *testing.T) {
				// Bound to the subtest, so a failure does not stop the other subtests.
				root := testkit.NewTestKit(t, store)
				require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
				root.MustExec("use test")
				root.MustExec("alter table src_tighten modify masking policy p_tighten set restrict on none")
				user := testkit.NewTestKit(t, store)
				t.Cleanup(user.Session().Close)
				require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "u_tighten", Hostname: "%"}, nil, nil, nil))
				user.MustExec("use test")
				user.MustExec("set tidb_enable_prepared_plan_cache = " + planCache)
				user.MustExec("begin")
				user.MustExec("prepare st from '" + tc.sql + "'")
				user.MustExec("execute st")
				user.MustExec("rollback")
				user.MustExec("begin")
				user.MustExec("execute st")
				cacheHit := user.MustQuery("select @@last_plan_from_cache")
				user.MustExec("rollback")
				expectedHit := "0"
				if tc.cacheable {
					expectedHit = planCache
				}
				cacheHit.Check(testkit.Rows(expectedHit))
				// A statement prepared while the operation was permitted must
				// observe a restriction added afterwards.
				root.MustExec("alter table src_tighten modify masking policy p_tighten set restrict on (" + tc.restrictOn + ")")
				before := root.MustQuery("select * from dst_tighten order by id").Rows()
				user.MustGetErrCode("execute st", errno.ErrAccessDeniedToMaskedColumn)
				user.MustGetErrCode(tc.sql, errno.ErrAccessDeniedToMaskedColumn)
				root.MustQuery("select * from dst_tighten order by id").Check(before)
			})
		}
	}
}

func TestMaskingPolicyUpdateRestrictOnDirtyAndCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create user union_scan_user")
	root.MustExec("grant select,insert,update on test.* to union_scan_user")
	for _, table := range []string{"t_dirty_update", "t_cached_update"} {
		root.MustExec("create table " + table + "(id int primary key, c varchar(20), n varchar(20))")
		root.MustExec("insert into " + table + " values (1,'secret','x')")
		root.MustExec("create masking policy p on " + table + "(c) as case when current_user()='root@%' then c else mask_full(c) end restrict on (update_select)")
	}
	root.MustExec("alter table t_cached_update cache")
	user := testkit.NewTestKit(t, store)
	require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "union_scan_user", Hostname: "%"}, nil, nil, nil))
	user.MustExec("use test")
	const update = "update %s set n = 'y' where c = 'secret'"
	// A plain target-table read keeps ordinary UPDATE semantics. That must not
	// depend on the table being read through a union scan, as a table that is
	// dirty in the transaction or cached is.
	user.MustExec(fmt.Sprintf(update, "t_dirty_update"))
	user.MustExec("begin")
	user.MustExec("insert into t_dirty_update values (2,'other','x')")
	user.MustExec(fmt.Sprintf(update, "t_dirty_update"))
	user.MustExec("rollback")
	for range 3 {
		user.MustQuery("select id from t_cached_update").Check(testkit.Rows("1"))
	}
	user.MustExec(fmt.Sprintf(update, "t_cached_update"))
	// A derived table over the same dirty table is still a SELECT source.
	user.MustExec("begin")
	user.MustExec("insert into t_dirty_update values (3,'other','x')")
	user.MustGetErrCode("update t_cached_update d join (select id, c from t_dirty_update) s on d.id = s.id set d.n = s.c", errno.ErrAccessDeniedToMaskedColumn)
	user.MustExec("rollback")
}

func TestMaskingPolicyDeleteRestrictOnFromSources(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table delete_src(c varchar(20))")
	root.MustExec("create table delete_dst(c varchar(20))")
	root.MustExec("insert into delete_src values ('secret')")
	root.MustExec("insert into delete_dst values ('secret')")
	root.MustExec("create view delete_view as select c from delete_src")
	root.MustExec("create masking policy p on delete_src(c) as case when current_user()='root@%' then c else mask_full(c) end restrict on (delete_select)")
	root.MustExec("create user delete_source_user")
	root.MustExec("grant select,delete on test.* to delete_source_user")
	user := testkit.NewTestKit(t, store)
	require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "delete_source_user", Hostname: "%"}, nil, nil, nil))
	user.MustExec("use test")
	for _, sql := range []string{
		"delete d from delete_dst d join (select c from delete_src) s on d.c=s.c",
		"delete d from delete_dst d join delete_view s on d.c=s.c",
		"with s as (select c from delete_src) delete d from delete_dst d join s on d.c=s.c",
		"with s as (select c from delete_src union all select c from delete_src) delete d from delete_dst d join s on d.c=s.c",
	} {
		user.MustGetErrCode(sql, errno.ErrAccessDeniedToMaskedColumn)
		root.MustQuery("select * from delete_dst").Check(testkit.Rows("secret"))
		root.MustExec("begin")
		root.MustExec(sql)
		root.MustQuery("select * from delete_dst").Check(testkit.Rows())
		root.MustExec("rollback")
	}
	user.MustExec("delete d from delete_dst d join (select 'secret' as c from delete_src) s on d.c=s.c")
	root.MustQuery("select * from delete_dst").Check(testkit.Rows())
	user.MustExec("delete from delete_src where c='secret'")
}

func TestMaskingPolicyScalarSemiJoinLineage(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	tk.MustExec("create table semi_source(c varchar(20))")
	tk.MustExec("insert into semi_source values('secret')")
	tk.MustExec("create table semi_target(v int)")
	tk.MustExec("create masking policy p on semi_source(c) as mask_full(c) restrict on(insert_into_select)")
	for i, expr := range []string{"c in(select 'secret')", "c not in(select 'secret')", "c=any(select 'secret')"} {
		tk.MustGetErrCode("insert into semi_target select "+expr+" from semi_source", errno.ErrAccessDeniedToMaskedColumn)
		require.ErrorContains(t, tk.ExecToErr("select "+expr+" from semi_source"), "masking policy cannot be preserved")
		expected := testkit.Rows("1")
		if i == 1 {
			expected = testkit.Rows()
		}
		tk.MustQuery("select 1 from semi_source where " + expr).Check(expected)
	}
	tk.MustQuery("select * from semi_target").Check(testkit.Rows())
	tk.MustExec("alter table semi_source disable masking policy p")
	tk.MustExec("insert into semi_target select c in(select 'secret') from semi_source")
	tk.MustQuery("select * from semi_target").Check(testkit.Rows("1"))
}

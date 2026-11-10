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
				require.ErrorContains(t, err, "Access denied to masked column")
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
					require.ErrorContains(t, err, "Access denied to masked column")
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

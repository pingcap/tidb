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

package executor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/extension/enterprise/audit"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func TestLBACAccessControl(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac")
	tkRoot.MustExec("use test_lbac")
	tkRoot.MustExec("drop table if exists sales")
	tkRoot.MustExec("drop user if exists 'alice'@'%'")
	tkRoot.MustExec("drop user if exists 'bob'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP_SECRET','SECRET','CONFIDENTIAL','UNCLASSIFIED')")
	tkRoot.MustExec("create security label component dept set ('HR','FIN','DEV','OPS')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'AMERICAS' under 'WORLD', 'USA' under 'AMERICAS', 'CANADA' under 'AMERICAS')")
	tkRoot.MustExec("create security policy data_access components classif, dept, region")

	tkRoot.MustExec("create security label data_access.top_secret_fin_usa component classif 'TOP_SECRET' component dept 'FIN' component region 'USA'")
	tkRoot.MustExec("create security label data_access.confidential_hr_ca component classif 'CONFIDENTIAL' component dept 'HR' component region 'CANADA'")
	tkRoot.MustExec("create security label data_access.top_secret_fin_usa_col component classif 'TOP_SECRET' component dept 'FIN' component region 'USA'")

	tkRoot.MustExec("create table sales (id int primary key, item varchar(100), amount decimal(12,2) secured with top_secret_fin_usa_col, row_seclabel securitylabel not null) security policy data_access")
	tkRoot.MustExec("insert into sales (id, item, amount, row_seclabel) values (1, 'Widget', 100.00, seclabel('DATA_ACCESS','TOP_SECRET|FIN|USA'))")
	tkRoot.MustExec("insert into sales (id, item, amount, row_seclabel) values (2, 'Gizmo', 200.00, seclabel('DATA_ACCESS','CONFIDENTIAL|HR|CANADA'))")

	tkRoot.MustExec("create user 'alice'@'%'")
	tkRoot.MustExec("create user 'bob'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac.sales to 'alice'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac.sales to 'bob'@'%'")

	tkRoot.MustExec("grant security label data_access.confidential_hr_ca to user 'alice'@'%' for read access")
	tkRoot.MustExec("grant security label data_access.top_secret_fin_usa to user 'alice'@'%' for write access")
	tkRoot.MustExec("grant security label data_access.top_secret_fin_usa to user 'bob'@'%' for read access, write access")

	tkAlice := testkit.NewTestKit(t, store)
	require.NoError(t, tkAlice.Session().Auth(&auth.UserIdentity{
		Username:     "alice",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkAlice.MustExec("use test_lbac")
	tkAlice.MustQuery("select id, item from sales order by id").Check(testkit.Rows("2 Gizmo"))
	tkAlice.MustQuery("select s.id from sales s order by s.id").Check(testkit.Rows("2"))
	tkAlice.MustGetErrCode("select s.id from sales s where s.amount > 0", errno.ErrColumnaccessDenied)
	tkAlice.MustGetErrCode("select s.id from sales s order by s.amount", errno.ErrColumnaccessDenied)
	tkAlice.MustGetErrCode("select amount from sales", errno.ErrColumnaccessDenied)
	tkAlice.MustGetErrCode("select amount + 1 from sales", errno.ErrColumnaccessDenied)
	tkAlice.MustGetErrCode("select * from sales s", errno.ErrColumnaccessDenied)
	tkAlice.MustExec("insert into sales (id, item, amount, row_seclabel) values (3, 'Gadget', 300.00, seclabel('DATA_ACCESS','TOP_SECRET|FIN|USA'))")
	tkAlice.MustGetDBError("insert into sales (id, item, amount, row_seclabel) values (4, 'Widget', 400.00, seclabel('DATA_ACCESS','CONFIDENTIAL|HR|CANADA'))", exeerrors.ErrRowLabelUnAccessible)

	tkBob := testkit.NewTestKit(t, store)
	require.NoError(t, tkBob.Session().Auth(&auth.UserIdentity{
		Username:     "bob",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkBob.MustExec("use test_lbac")
	tkBob.MustQuery("select id, item, amount from sales order by id").Check(testkit.Rows("1 Widget 100.00", "3 Gadget 300.00"))
	tkBob.MustGetDBError("update sales set amount = 500 where id = 2", exeerrors.ErrRowLabelUnAccessible)
	tkBob.MustExec("delete from sales where id = 1")
	tkRoot.MustQuery("select id from sales order by id").Check(testkit.Rows("2", "3"))
}

func TestLBACAuditFDPIFC1AccessControl(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_audit_ifc")
	tkRoot.MustExec("use test_lbac_audit_ifc")
	tkRoot.MustExec("drop table if exists docs_sec")
	tkRoot.MustExec("drop table if exists docs_row")
	tkRoot.MustExec("drop user if exists 'label_only'@'%'")
	tkRoot.MustExec("drop user if exists 'select_only'@'%'")
	tkRoot.MustExec("drop user if exists 'both'@'%'")
	tkRoot.MustExec("drop user if exists 'writer'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP','LOW')")
	tkRoot.MustExec("create security label component dept set ('ENG')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'US' under 'WORLD')")
	tkRoot.MustExec("create security policy audit_ifc components classif, dept, region restrict not authorized write security label")

	tkRoot.MustExec("create security label audit_ifc.top_eng_us component classif 'TOP' component dept 'ENG' component region 'US'")
	tkRoot.MustExec("create security label audit_ifc.low_eng_us component classif 'LOW' component dept 'ENG' component region 'US'")
	tkRoot.MustExec("create security label audit_ifc.top_eng_us_col component classif 'TOP' component dept 'ENG' component region 'US'")

	tkRoot.MustExec("create table docs_sec (id int primary key, secret int secured with top_eng_us_col, row_seclabel securitylabel not null) security policy audit_ifc")
	tkRoot.MustExec("create table docs_row (id int primary key, row_seclabel securitylabel not null) security policy audit_ifc")
	tkRoot.MustExec("insert into docs_sec values (1, 100, seclabel('AUDIT_IFC','TOP|ENG|US'))")
	tkRoot.MustExec("insert into docs_sec values (2, 200, seclabel('AUDIT_IFC','LOW|ENG|US'))")

	tkRoot.MustExec("create user 'label_only'@'%'")
	tkRoot.MustExec("create user 'select_only'@'%'")
	tkRoot.MustExec("create user 'both'@'%'")
	tkRoot.MustExec("create user 'writer'@'%'")

	tkRoot.MustExec("grant create on test_lbac_audit_ifc.* to 'label_only'@'%'")
	tkRoot.MustExec("grant security label audit_ifc.low_eng_us to user 'label_only'@'%' for read access")
	tkRoot.MustExec("grant select on test_lbac_audit_ifc.docs_sec to 'select_only'@'%'")
	tkRoot.MustExec("grant select on test_lbac_audit_ifc.docs_sec to 'both'@'%'")
	tkRoot.MustExec("grant security label audit_ifc.low_eng_us to user 'both'@'%' for read access")
	tkRoot.MustExec("grant insert on test_lbac_audit_ifc.docs_row to 'writer'@'%'")
	tkRoot.MustExec("grant security label audit_ifc.low_eng_us to user 'writer'@'%' for write access")

	tkLabelOnly := testkit.NewTestKit(t, store)
	require.NoError(t, tkLabelOnly.Session().Auth(&auth.UserIdentity{
		Username:     "label_only",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkLabelOnly.MustExec("use test_lbac_audit_ifc")
	tkLabelOnly.MustGetErrCode("select id from docs_sec", errno.ErrColumnaccessDenied)

	tkSelectOnly := testkit.NewTestKit(t, store)
	require.NoError(t, tkSelectOnly.Session().Auth(&auth.UserIdentity{
		Username:     "select_only",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkSelectOnly.MustExec("use test_lbac_audit_ifc")
	tkSelectOnly.MustQuery("select id from docs_sec order by id").Check(testkit.Rows())
	tkSelectOnly.MustGetErrCode("select secret from docs_sec", errno.ErrColumnaccessDenied)

	tkBoth := testkit.NewTestKit(t, store)
	require.NoError(t, tkBoth.Session().Auth(&auth.UserIdentity{
		Username:     "both",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkBoth.MustExec("use test_lbac_audit_ifc")
	tkBoth.MustQuery("select id from docs_sec order by id").Check(testkit.Rows("2"))
	tkBoth.MustGetErrCode("select secret from docs_sec", errno.ErrColumnaccessDenied)

	tkWriter := testkit.NewTestKit(t, store)
	require.NoError(t, tkWriter.Session().Auth(&auth.UserIdentity{
		Username:     "writer",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkWriter.MustExec("use test_lbac_audit_ifc")
	tkWriter.MustExec("insert into docs_row values (1, seclabel('AUDIT_IFC','LOW|ENG|US'))")
	tkWriter.MustGetDBError("insert into docs_row values (2, seclabel('AUDIT_IFC','TOP|ENG|US'))", exeerrors.ErrRowLabelUnAccessible)
}

func TestLBACAuditFDPIFF2SecurityAttributes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_audit_iff")
	tkRoot.MustExec("use test_lbac_audit_iff")
	tkRoot.MustExec("drop table if exists t_sec")
	tkRoot.MustExec("drop user if exists 'manage_user'@'%'")
	tkRoot.MustExec("drop user if exists 'exempt_user'@'%'")
	tkRoot.MustExec("drop user if exists 'reader'@'%'")

	tkRoot.MustExec("create security label component classif array ('A','B')")
	tkRoot.MustExec("create security label component dept set ('HR','ENG')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'EU' under 'WORLD')")
	tkRoot.MustExec("create security policy audit_iff components classif, dept, region")

	tkRoot.MustExec("create security label audit_iff.a_hr_eu component classif 'A' component dept 'HR' component region 'EU'")
	tkRoot.MustExec("create security label audit_iff.a_hr_eu_col component classif 'A' component dept 'HR' component region 'EU'")

	tkRoot.MustExec("create table t_sec (id int primary key, secret int secured with a_hr_eu_col, row_seclabel securitylabel not null) security policy audit_iff")
	tkRoot.MustExec("insert into t_sec values (1, 900, seclabel('AUDIT_IFF','A|HR|EU'))")

	tkRoot.MustExec("create user 'manage_user'@'%'")
	tkRoot.MustExec("create user 'exempt_user'@'%'")
	tkRoot.MustExec("create user 'reader'@'%'")
	tkRoot.MustExec("grant select on test_lbac_audit_iff.t_sec to 'manage_user'@'%'")
	tkRoot.MustExec("grant select on test_lbac_audit_iff.t_sec to 'exempt_user'@'%'")
	tkRoot.MustExec("grant select on test_lbac_audit_iff.t_sec to 'reader'@'%'")
	tkRoot.MustExec("grant security label audit_iff.a_hr_eu to user 'reader'@'%' for read access")

	tkManage := testkit.NewTestKit(t, store)
	require.NoError(t, tkManage.Session().Auth(&auth.UserIdentity{
		Username:     "manage_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkManage.MustExec("use test_lbac_audit_iff")
	tkManage.MustGetErrCode("grant security label audit_iff.a_hr_eu to user 'manage_user'@'%' for read access", errno.ErrSpecificAccessDenied)

	tkReader := testkit.NewTestKit(t, store)
	require.NoError(t, tkReader.Session().Auth(&auth.UserIdentity{
		Username:     "reader",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkReader.MustExec("use test_lbac_audit_iff")
	tkReader.MustQuery("select id, secret from t_sec order by id").Check(testkit.Rows("1 900"))

	tkExempt := testkit.NewTestKit(t, store)
	require.NoError(t, tkExempt.Session().Auth(&auth.UserIdentity{
		Username:     "exempt_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkExempt.MustExec("use test_lbac_audit_iff")
	tkExempt.MustQuery("select id from t_sec order by id").Check(testkit.Rows())
	tkExempt.MustGetErrCode("select secret from t_sec", errno.ErrColumnaccessDenied)

	tkRoot.MustExec("grant exemption on rule all for audit_iff to user 'exempt_user'@'%'")
	tkExempt.MustQuery("select id, secret from t_sec order by id").Check(testkit.Rows("1 900"))
}

func TestLBACAuditFDPETC2OutputAssociation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_audit_output")
	tkRoot.MustExec("use test_lbac_audit_output")
	tkRoot.MustExec("drop table if exists t_output")
	tkRoot.MustExec("drop user if exists 'out_reader'@'%'")

	tkRoot.MustExec("create security label component classif array ('HIGH','LOW')")
	tkRoot.MustExec("create security policy audit_output components classif")
	tkRoot.MustExec("create security label audit_output.high component classif 'HIGH'")

	tkRoot.MustExec("create table t_output (id int primary key, row_seclabel securitylabel not null) security policy audit_output")
	tkRoot.MustExec("insert into t_output values (1, seclabel('AUDIT_OUTPUT','HIGH'))")

	tkRoot.MustExec("create user 'out_reader'@'%'")
	tkRoot.MustExec("grant select on test_lbac_audit_output.t_output to 'out_reader'@'%'")
	tkRoot.MustExec("grant security label audit_output.high to user 'out_reader'@'%' for read access")

	tkReader := testkit.NewTestKit(t, store)
	require.NoError(t, tkReader.Session().Auth(&auth.UserIdentity{
		Username:     "out_reader",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkReader.MustExec("use test_lbac_audit_output")
	tkReader.MustQuery("select id, seclabel_to_char('AUDIT_OUTPUT', row_seclabel) from t_output order by id").Check(testkit.Rows("1 HIGH"))
}

func TestLBACAuditFDPITC1InputControl(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_audit_itc")
	tkRoot.MustExec("use test_lbac_audit_itc")
	tkRoot.MustExec("drop table if exists t_in")
	tkRoot.MustExec("drop user if exists 'writer_label_only'@'%'")
	tkRoot.MustExec("drop user if exists 'writer_priv_only'@'%'")
	tkRoot.MustExec("drop user if exists 'writer_ok'@'%'")

	tkRoot.MustExec("create security label component classif array ('A','B')")
	tkRoot.MustExec("create security policy audit_itc components classif")
	tkRoot.MustExec("create security label audit_itc.label_a component classif 'A'")

	tkRoot.MustExec("create table t_in (id int primary key, row_seclabel securitylabel not null) security policy audit_itc")

	tkRoot.MustExec("create user 'writer_label_only'@'%'")
	tkRoot.MustExec("create user 'writer_priv_only'@'%'")
	tkRoot.MustExec("create user 'writer_ok'@'%'")

	tkRoot.MustExec("grant create on test_lbac_audit_itc.* to 'writer_label_only'@'%'")
	tkRoot.MustExec("grant security label audit_itc.label_a to user 'writer_label_only'@'%' for write access")
	tkRoot.MustExec("grant insert on test_lbac_audit_itc.t_in to 'writer_priv_only'@'%'")
	tkRoot.MustExec("grant insert on test_lbac_audit_itc.t_in to 'writer_ok'@'%'")
	tkRoot.MustExec("grant security label audit_itc.label_a to user 'writer_ok'@'%' for write access")

	tkLabelOnly := testkit.NewTestKit(t, store)
	require.NoError(t, tkLabelOnly.Session().Auth(&auth.UserIdentity{
		Username:     "writer_label_only",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkLabelOnly.MustExec("use test_lbac_audit_itc")
	tkLabelOnly.MustGetErrCode("insert into t_in values (1, seclabel('AUDIT_ITC','A'))", errno.ErrTableaccessDenied)

	tkPrivOnly := testkit.NewTestKit(t, store)
	require.NoError(t, tkPrivOnly.Session().Auth(&auth.UserIdentity{
		Username:     "writer_priv_only",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkPrivOnly.MustExec("use test_lbac_audit_itc")
	tkPrivOnly.MustGetDBError("insert into t_in values (1, seclabel('AUDIT_ITC','A'))", exeerrors.ErrRowLabelUnAccessible)

	tkWriter := testkit.NewTestKit(t, store)
	require.NoError(t, tkWriter.Session().Auth(&auth.UserIdentity{
		Username:     "writer_ok",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkWriter.MustExec("use test_lbac_audit_itc")
	tkWriter.MustExec("insert into t_in values (1, seclabel('AUDIT_ITC','A'))")
	tkRoot.MustQuery("select count(*) from t_in").Check(testkit.Rows("1"))
}

func TestLBACWriteControlOverrideRestrict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_wc")
	tkRoot.MustExec("use test_lbac_wc")
	tkRoot.MustExec("drop table if exists sales_override")
	tkRoot.MustExec("drop table if exists sales_restrict")
	tkRoot.MustExec("drop user if exists 'alice'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP_SECRET','CONFIDENTIAL')")
	tkRoot.MustExec("create security label component dept set ('FIN','HR')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'USA' under 'WORLD')")

	tkRoot.MustExec("create security policy data_access_override components classif, dept, region override not authorized write security label")
	tkRoot.MustExec("create security policy data_access_restrict components classif, dept, region restrict not authorized write security label")

	tkRoot.MustExec("create security label data_access_override.top_secret_fin_usa_ov component classif 'TOP_SECRET' component dept 'FIN' component region 'USA'")
	tkRoot.MustExec("create security label data_access_override.top_secret_hr_usa_ov component classif 'TOP_SECRET' component dept 'HR' component region 'USA'")
	tkRoot.MustExec("create security label data_access_restrict.top_secret_fin_usa_rs component classif 'TOP_SECRET' component dept 'FIN' component region 'USA'")
	tkRoot.MustExec("create security label data_access_restrict.top_secret_hr_usa_rs component classif 'TOP_SECRET' component dept 'HR' component region 'USA'")

	tkRoot.MustExec("create table sales_override (id int primary key, amount int, row_seclabel securitylabel not null) security policy data_access_override")
	tkRoot.MustExec("create table sales_restrict (id int primary key, amount int, row_seclabel securitylabel not null) security policy data_access_restrict")

	tkRoot.MustExec("create user 'alice'@'%'")
	tkRoot.MustExec("grant select, insert, update on test_lbac_wc.sales_override to 'alice'@'%'")
	tkRoot.MustExec("grant select, insert, update on test_lbac_wc.sales_restrict to 'alice'@'%'")
	tkRoot.MustExec("grant security label data_access_override.top_secret_fin_usa_ov to user 'alice'@'%' for write access")
	tkRoot.MustExec("grant security label data_access_restrict.top_secret_fin_usa_rs to user 'alice'@'%' for write access")

	tkAlice := testkit.NewTestKit(t, store)
	require.NoError(t, tkAlice.Session().Auth(&auth.UserIdentity{
		Username:     "alice",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkAlice.MustExec("use test_lbac_wc")

	tkAlice.MustExec("insert into sales_override (id, amount, row_seclabel) values (1, 100, seclabel('DATA_ACCESS_OVERRIDE','TOP_SECRET|HR|USA'))")
	tkRoot.MustQuery("select seclabel_to_char('DATA_ACCESS_OVERRIDE', row_seclabel) from sales_override where id = 1").Check(testkit.Rows("TOP_SECRET|FIN|USA"))
	tkAlice.MustExec("update sales_override set row_seclabel = seclabel('DATA_ACCESS_OVERRIDE','TOP_SECRET|HR|USA') where id = 1")
	tkRoot.MustQuery("select seclabel_to_char('DATA_ACCESS_OVERRIDE', row_seclabel) from sales_override where id = 1").Check(testkit.Rows("TOP_SECRET|FIN|USA"))
	tkAlice.MustExec("insert into sales_restrict (id, amount, row_seclabel) values (1, 100, seclabel('DATA_ACCESS_RESTRICT','TOP_SECRET|FIN|USA'))")
	tkAlice.MustGetDBError("update sales_restrict set row_seclabel = seclabel('DATA_ACCESS_RESTRICT','TOP_SECRET|HR|USA') where id = 1", exeerrors.ErrRowLabelUnAccessible)
	tkAlice.MustGetDBError("insert into sales_restrict (id, amount, row_seclabel) values (1, 100, seclabel('DATA_ACCESS_RESTRICT','TOP_SECRET|HR|USA'))", exeerrors.ErrRowLabelUnAccessible)
}

func TestLBACCompanyScenario(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_company")
	tkRoot.MustExec("use test_lbac_company")
	tkRoot.MustExec("drop table if exists salary")
	tkRoot.MustExec("drop user if exists 'boss'@'%'")
	tkRoot.MustExec("drop user if exists 'hr'@'%'")
	tkRoot.MustExec("drop user if exists 'fin'@'%'")
	tkRoot.MustExec("drop user if exists 'emp_a'@'%'")
	tkRoot.MustExec("drop user if exists 'emp_b'@'%'")
	tkRoot.MustExec("drop user if exists 'no_label'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP_SECRET','SECRET','CONFIDENTIAL','PUBLIC')")
	tkRoot.MustExec("create security label component dept set ('EXEC','HR','FIN','ENG')")
	tkRoot.MustExec("create security label component person set ('BOSS','HR1','FIN1','EMP_A','EMP_B')")
	tkRoot.MustExec("create security policy corp_access components classif, dept, person restrict not authorized write security label")

	tkRoot.MustExec("create security label corp_access.exec_top component classif 'TOP_SECRET' component dept 'EXEC' component person 'BOSS'")
	tkRoot.MustExec("create security label corp_access.hr_conf component classif 'CONFIDENTIAL' component dept 'HR' component person 'HR1'")
	tkRoot.MustExec("create security label corp_access.fin_conf component classif 'CONFIDENTIAL' component dept 'FIN' component person 'FIN1'")
	tkRoot.MustExec("create security label corp_access.emp_a_public component classif 'PUBLIC' component dept 'ENG' component person 'EMP_A'")
	tkRoot.MustExec("create security label corp_access.emp_b_public component classif 'PUBLIC' component dept 'ENG' component person 'EMP_B'")
	tkRoot.MustExec("create security label corp_access.all_people_top component classif 'TOP_SECRET' component dept 'EXEC','HR','FIN','ENG' component person 'BOSS','HR1','FIN1','EMP_A','EMP_B'")
	tkRoot.MustExec("create security label corp_access.hr_write_non_exec component classif 'SECRET' component dept 'HR','ENG' component person 'HR1','EMP_A','EMP_B'")
	tkRoot.MustExec("create security label corp_access.emp_b_write_self component classif 'PUBLIC' component dept 'ENG' component person 'EMP_B'")

	tkRoot.MustExec("create table salary (id int primary key, name varchar(32), department varchar(16), salary int, row_seclabel securitylabel not null) security policy corp_access")
	tkRoot.MustExec("insert into salary values (1, 'boss', 'EXEC', 100000, seclabel('CORP_ACCESS','TOP_SECRET|EXEC|BOSS'))")
	tkRoot.MustExec("insert into salary values (2, 'hr', 'HR', 8000, seclabel('CORP_ACCESS','CONFIDENTIAL|HR|HR1'))")
	tkRoot.MustExec("insert into salary values (3, 'fin', 'FIN', 9000, seclabel('CORP_ACCESS','CONFIDENTIAL|FIN|FIN1'))")
	tkRoot.MustExec("insert into salary values (4, 'emp_a', 'ENG', 5000, seclabel('CORP_ACCESS','PUBLIC|ENG|EMP_A'))")
	tkRoot.MustExec("insert into salary values (5, 'emp_b', 'ENG', 5200, seclabel('CORP_ACCESS','PUBLIC|ENG|EMP_B'))")

	tkRoot.MustExec("create user 'boss'@'%'")
	tkRoot.MustExec("create user 'hr'@'%'")
	tkRoot.MustExec("create user 'fin'@'%'")
	tkRoot.MustExec("create user 'emp_a'@'%'")
	tkRoot.MustExec("create user 'emp_b'@'%'")
	tkRoot.MustExec("create user 'no_label'@'%'")

	tkRoot.MustExec("grant select, update on test_lbac_company.salary to 'boss'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_company.salary to 'hr'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_company.salary to 'fin'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_company.salary to 'emp_a'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_company.salary to 'emp_b'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_company.salary to 'no_label'@'%'")

	tkRoot.MustExec("grant exemption on rule all for corp_access to user 'boss'@'%'")
	tkRoot.MustExec("grant security label corp_access.all_people_top to user 'hr'@'%' for read access")
	tkRoot.MustExec("grant security label corp_access.hr_write_non_exec to user 'hr'@'%' for write access")
	tkRoot.MustExec("grant security label corp_access.all_people_top to user 'fin'@'%' for read access")
	tkRoot.MustExec("grant security label corp_access.all_people_top to user 'fin'@'%' for write access")
	tkRoot.MustExec("grant security label corp_access.emp_a_public to user 'emp_a'@'%' for read access")
	tkRoot.MustExec("grant security label corp_access.emp_b_public to user 'emp_b'@'%' for read access")
	tkRoot.MustExec("grant security label corp_access.emp_b_write_self to user 'emp_b'@'%' for write access")

	tkBoss := testkit.NewTestKit(t, store)
	require.NoError(t, tkBoss.Session().Auth(&auth.UserIdentity{
		Username:     "boss",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkBoss.MustExec("use test_lbac_company")
	tkBoss.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkBoss.MustExec("update salary set salary = salary + 100 where name = 'boss'")
	tkBoss.MustExec("update salary set salary = salary + 100 where name = 'emp_a'")
	tkBoss.MustQuery("select id, name, department, salary, seclabel_to_char('CORP_ACCESS', row_seclabel) from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100100 TOP_SECRET|EXEC|BOSS",
		"2 hr HR 8000 CONFIDENTIAL|HR|HR1",
		"3 fin FIN 9000 CONFIDENTIAL|FIN|FIN1",
		"4 emp_a ENG 5100 PUBLIC|ENG|EMP_A",
		"5 emp_b ENG 5200 PUBLIC|ENG|EMP_B",
	))

	// Insert a row label string without a matching security label definition.
	tkRoot.MustContainErrMsg("insert into salary values (6, 'hr_shadow', 'HR', 7000, seclabel('CORP_ACCESS','TOP_SECRET|HR|HR1'))", "security label not found")
	tkRoot.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))

	tkHR := testkit.NewTestKit(t, store)
	require.NoError(t, tkHR.Session().Auth(&auth.UserIdentity{
		Username:     "hr",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkHR.MustExec("use test_lbac_company")
	tkHR.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkHR.MustExec("update salary set salary = salary + 50 where name = 'emp_b'")
	tkHR.MustGetDBError("update salary set salary = salary + 50 where name = 'boss'", exeerrors.ErrRowLabelUnAccessible)
	tkHR.MustGetDBError("update salary set salary = salary + 50 where name = 'fin'", exeerrors.ErrRowLabelUnAccessible)
	tkHR.MustExec("insert into salary (id, name, department, salary, row_seclabel) values (60, 'hr_tmp', 'ENG', 6100, seclabel('CORP_ACCESS','PUBLIC|ENG|EMP_B'))")
	tkHR.MustGetDBError("insert into salary (id, name, department, salary, row_seclabel) values (61, 'hr_tmp_denied', 'EXEC', 100000, seclabel('CORP_ACCESS','TOP_SECRET|EXEC|BOSS'))", exeerrors.ErrRowLabelUnAccessible)
	tkHR.MustExec("delete from salary where id = 60")
	tkHR.MustGetDBError("delete from salary where name = 'boss'", exeerrors.ErrRowLabelUnAccessible)
	tkHR.MustQuery("select id, name, department, salary, seclabel_to_char('CORP_ACCESS', row_seclabel) from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100100 TOP_SECRET|EXEC|BOSS",
		"2 hr HR 8000 CONFIDENTIAL|HR|HR1",
		"3 fin FIN 9000 CONFIDENTIAL|FIN|FIN1",
		"4 emp_a ENG 5100 PUBLIC|ENG|EMP_A",
		"5 emp_b ENG 5250 PUBLIC|ENG|EMP_B",
	))

	tkFIN := testkit.NewTestKit(t, store)
	require.NoError(t, tkFIN.Session().Auth(&auth.UserIdentity{
		Username:     "fin",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkFIN.MustExec("use test_lbac_company")
	tkFIN.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkFIN.MustExec("update salary set salary = salary + 25 where name = 'boss'")
	tkFIN.MustExec("update salary set salary = salary + 25 where name = 'emp_b'")
	tkFIN.MustExec("insert into salary (id, name, department, salary, row_seclabel) values (62, 'fin_tmp', 'FIN', 9100, seclabel('CORP_ACCESS','TOP_SECRET|EXEC|BOSS'))")
	tkFIN.MustExec("delete from salary where id = 62")
	tkFIN.MustQuery("select id, name, department, salary, seclabel_to_char('CORP_ACCESS', row_seclabel) from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100125 TOP_SECRET|EXEC|BOSS",
		"2 hr HR 8000 CONFIDENTIAL|HR|HR1",
		"3 fin FIN 9000 CONFIDENTIAL|FIN|FIN1",
		"4 emp_a ENG 5100 PUBLIC|ENG|EMP_A",
		"5 emp_b ENG 5275 PUBLIC|ENG|EMP_B",
	))

	tkEmpA := testkit.NewTestKit(t, store)
	require.NoError(t, tkEmpA.Session().Auth(&auth.UserIdentity{
		Username:     "emp_a",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkEmpA.MustExec("use test_lbac_company")
	tkEmpA.MustQuery("select name from salary order by id").Check(testkit.Rows("emp_a"))
	tkEmpA.MustGetDBError("update salary set salary = salary + 10 where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)
	tkEmpA.MustGetDBError("insert into salary (id, name, department, salary, row_seclabel) values (63, 'emp_a_tmp', 'ENG', 5100, seclabel('CORP_ACCESS','PUBLIC|ENG|EMP_A'))", exeerrors.ErrRowLabelUnAccessible)
	tkEmpA.MustGetDBError("delete from salary where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)
	tkEmpA.MustQuery("select id, name, department, salary, seclabel_to_char('CORP_ACCESS', row_seclabel) from salary").Check(testkit.Rows("4 emp_a ENG 5100 PUBLIC|ENG|EMP_A"))

	tkEmpB := testkit.NewTestKit(t, store)
	require.NoError(t, tkEmpB.Session().Auth(&auth.UserIdentity{
		Username:     "emp_b",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkEmpB.MustExec("use test_lbac_company")
	tkEmpB.MustQuery("select name from salary order by id").Check(testkit.Rows("emp_b"))
	tkEmpB.MustExec("update salary set salary = salary + 10 where name = 'emp_b'")
	tkEmpB.MustGetDBError("update salary set salary = salary + 10 where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)
	tkEmpB.MustExec("insert into salary (id, name, department, salary, row_seclabel) values (64, 'emp_b_tmp', 'ENG', 5300, seclabel('CORP_ACCESS','PUBLIC|ENG|EMP_B'))")
	tkEmpB.MustExec("delete from salary where id = 64")
	tkEmpB.MustGetDBError("delete from salary where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)
	tkEmpB.MustQuery("select id, name, department, salary, seclabel_to_char('CORP_ACCESS', row_seclabel) from salary").Check(testkit.Rows("5 emp_b ENG 5285 PUBLIC|ENG|EMP_B"))

	tkNoLabel := testkit.NewTestKit(t, store)
	require.NoError(t, tkNoLabel.Session().Auth(&auth.UserIdentity{
		Username:     "no_label",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkNoLabel.MustExec("use test_lbac_company")
	tkNoLabel.MustQuery("select count(*) from salary").Check(testkit.Rows("0"))
	tkNoLabel.MustGetDBError("insert into salary (id, name, department, salary, row_seclabel) values (6, 'no_label', 'ENG', 4800, seclabel('CORP_ACCESS','PUBLIC|ENG|EMP_A'))", exeerrors.ErrRowLabelUnAccessible)
	tkNoLabel.MustGetDBError("update salary set salary = salary + 10 where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)
	tkNoLabel.MustGetDBError("delete from salary where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)
	tkNoLabel.MustQuery("select id, name, department, salary, seclabel_to_char('CORP_ACCESS', row_seclabel) from salary").Check(testkit.Rows())

	tkRoot.MustExec("alter table salary drop column row_seclabel")

	tkBoss.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkBoss.MustExec("update salary set salary = salary + 1 where name = 'emp_a'")
	tkBoss.MustGetErrCode("insert into salary (id, name, department, salary) values (10, 'boss_insert', 'EXEC', 120000)", errno.ErrColumnaccessDenied)
	tkBoss.MustGetErrCode("delete from salary where name = 'emp_a'", errno.ErrTableaccessDenied)
	tkBoss.MustQuery("select * from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100125",
		"2 hr HR 8000",
		"3 fin FIN 9000",
		"4 emp_a ENG 5101",
		"5 emp_b ENG 5285",
	))

	tkHR.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkHR.MustExec("update salary set salary = salary + 1 where name = 'boss'")
	tkHR.MustExec("insert into salary (id, name, department, salary) values (71, 'hr_insert', 'HR', 8100)")
	tkHR.MustExec("delete from salary where id = 71")
	tkHR.MustQuery("select * from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100126",
		"2 hr HR 8000",
		"3 fin FIN 9000",
		"4 emp_a ENG 5101",
		"5 emp_b ENG 5285",
	))

	tkFIN.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkFIN.MustExec("update salary set salary = salary + 1 where name = 'emp_b'")
	tkFIN.MustExec("insert into salary (id, name, department, salary) values (72, 'fin_insert', 'FIN', 9200)")
	tkFIN.MustExec("delete from salary where id = 72")
	tkFIN.MustQuery("select * from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100126",
		"2 hr HR 8000",
		"3 fin FIN 9000",
		"4 emp_a ENG 5101",
		"5 emp_b ENG 5286",
	))

	tkEmpA.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkEmpA.MustExec("update salary set salary = salary + 5 where name = 'emp_a'")
	tkEmpA.MustExec("insert into salary (id, name, department, salary) values (73, 'emp_a_insert', 'ENG', 5100)")
	tkEmpA.MustExec("delete from salary where id = 73")
	tkEmpA.MustQuery("select * from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100126",
		"2 hr HR 8000",
		"3 fin FIN 9000",
		"4 emp_a ENG 5106",
		"5 emp_b ENG 5286",
	))

	tkEmpB.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkEmpB.MustExec("update salary set salary = salary + 5 where name = 'emp_b'")
	tkEmpB.MustExec("insert into salary (id, name, department, salary) values (74, 'emp_b_insert', 'ENG', 5300)")
	tkEmpB.MustExec("delete from salary where id = 74")
	tkEmpB.MustQuery("select * from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100126",
		"2 hr HR 8000",
		"3 fin FIN 9000",
		"4 emp_a ENG 5106",
		"5 emp_b ENG 5291",
	))

	tkNoLabel.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkNoLabel.MustExec("insert into salary (id, name, department, salary) values (6, 'no_label', 'ENG', 4800)")
	tkNoLabel.MustQuery("select count(*) from salary").Check(testkit.Rows("6"))
	tkNoLabel.MustExec("update salary set salary = salary + 10 where id = 6")
	tkNoLabel.MustExec("delete from salary where id = 6")
	tkNoLabel.MustQuery("select count(*) from salary").Check(testkit.Rows("5"))
	tkNoLabel.MustQuery("select * from salary").Sort().Check(testkit.Rows(
		"1 boss EXEC 100126",
		"2 hr HR 8000",
		"3 fin FIN 9000",
		"4 emp_a ENG 5106",
		"5 emp_b ENG 5291",
	))
}

func TestLBACMultiPolicyScenario(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_multi")
	tkRoot.MustExec("use test_lbac_multi")
	tkRoot.MustExec("drop table if exists salary")
	tkRoot.MustExec("drop table if exists projects")
	tkRoot.MustExec("drop user if exists 'boss'@'%'")
	tkRoot.MustExec("drop user if exists 'hr'@'%'")
	tkRoot.MustExec("drop user if exists 'fin'@'%'")
	tkRoot.MustExec("drop user if exists 'eng_us'@'%'")
	tkRoot.MustExec("drop user if exists 'emp_a'@'%'")
	tkRoot.MustExec("drop user if exists 'emp_b'@'%'")
	tkRoot.MustExec("drop user if exists 'ops'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP_SECRET','SECRET','CONFIDENTIAL','PUBLIC')")
	tkRoot.MustExec("create security label component dept set ('EXEC','HR','FIN','ENG','OPS')")
	tkRoot.MustExec("create security label component region tree ('GLOBAL' root, 'AMER' under 'GLOBAL', 'EMEA' under 'GLOBAL', 'APAC' under 'GLOBAL', 'US' under 'AMER', 'CA' under 'AMER', 'DE' under 'EMEA', 'CN' under 'APAC')")
	tkRoot.MustExec("create security label component person set ('BOSS','HR1','FIN1','ENG_US','ENG_CN','EMP_A','EMP_B')")
	tkRoot.MustExec("create security label component domain set ('PAYROLL','ROADMAP')")

	tkRoot.MustExec("create security policy pay_policy components classif, dept, region, person restrict not authorized write security label")
	tkRoot.MustExec("create security policy proj_policy components classif, domain, region override not authorized write security label")

	tkRoot.MustExec("create security label pay_policy.pay_exec_top component classif 'TOP_SECRET' component dept 'EXEC' component region 'GLOBAL' component person 'BOSS'")
	tkRoot.MustExec("create security label pay_policy.pay_hr_us component classif 'CONFIDENTIAL' component dept 'HR' component region 'US' component person 'HR1'")
	tkRoot.MustExec("create security label pay_policy.pay_fin_de component classif 'CONFIDENTIAL' component dept 'FIN' component region 'DE' component person 'FIN1'")
	tkRoot.MustExec("create security label pay_policy.pay_eng_us component classif 'PUBLIC' component dept 'ENG' component region 'US' component person 'ENG_US'")
	tkRoot.MustExec("create security label pay_policy.pay_eng_cn component classif 'PUBLIC' component dept 'ENG' component region 'CN' component person 'ENG_CN'")
	tkRoot.MustExec("create security label pay_policy.pay_emp_a component classif 'PUBLIC' component dept 'ENG' component region 'US' component person 'EMP_A'")
	tkRoot.MustExec("create security label pay_policy.pay_emp_b component classif 'PUBLIC' component dept 'ENG' component region 'US' component person 'EMP_B'")
	tkRoot.MustExec("create security label pay_policy.pay_all_top component classif 'TOP_SECRET' component dept 'EXEC','HR','FIN','ENG','OPS' component region 'GLOBAL' component person 'BOSS','HR1','FIN1','ENG_US','ENG_CN','EMP_A','EMP_B'")
	tkRoot.MustExec("create security label pay_policy.pay_hr_write component classif 'SECRET' component dept 'HR','ENG' component region 'GLOBAL' component person 'HR1','ENG_US','ENG_CN','EMP_A','EMP_B'")
	tkRoot.MustExec("create security label pay_policy.pay_eng_amer component classif 'PUBLIC' component dept 'ENG' component region 'AMER' component person 'ENG_US','EMP_A','EMP_B'")

	tkRoot.MustExec("create security label proj_policy.proj_top_us component classif 'TOP_SECRET' component domain 'ROADMAP' component region 'US'")
	tkRoot.MustExec("create security label proj_policy.proj_conf_amer component classif 'CONFIDENTIAL' component domain 'ROADMAP' component region 'AMER'")
	tkRoot.MustExec("create security label proj_policy.proj_public_cn component classif 'PUBLIC' component domain 'PAYROLL' component region 'CN'")

	tkRoot.MustExec("create table salary (id int primary key, name varchar(32), department varchar(16), region varchar(8), salary int, row_seclabel securitylabel not null) security policy pay_policy")
	tkRoot.MustExec("insert into salary values (1, 'boss', 'EXEC', 'GLOBAL', 100000, seclabel('PAY_POLICY','TOP_SECRET|EXEC|GLOBAL|BOSS'))")
	tkRoot.MustExec("insert into salary values (2, 'hr', 'HR', 'US', 8000, seclabel('PAY_POLICY','CONFIDENTIAL|HR|US|HR1'))")
	tkRoot.MustExec("insert into salary values (3, 'fin', 'FIN', 'DE', 9000, seclabel('PAY_POLICY','CONFIDENTIAL|FIN|DE|FIN1'))")
	tkRoot.MustExec("insert into salary values (4, 'eng_us', 'ENG', 'US', 7000, seclabel('PAY_POLICY','PUBLIC|ENG|US|ENG_US'))")
	tkRoot.MustExec("insert into salary values (5, 'eng_cn', 'ENG', 'CN', 6800, seclabel('PAY_POLICY','PUBLIC|ENG|CN|ENG_CN'))")
	tkRoot.MustExec("insert into salary values (6, 'emp_a', 'ENG', 'US', 5000, seclabel('PAY_POLICY','PUBLIC|ENG|US|EMP_A'))")
	tkRoot.MustExec("insert into salary values (7, 'emp_b', 'ENG', 'US', 5200, seclabel('PAY_POLICY','PUBLIC|ENG|US|EMP_B'))")

	tkRoot.MustExec("create table projects (id int primary key, domain varchar(16), region varchar(8), budget int, row_seclabel securitylabel not null) security policy proj_policy")
	tkRoot.MustExec("insert into projects values (1, 'ROADMAP', 'US', 1000, seclabel('PROJ_POLICY','TOP_SECRET|ROADMAP|US'))")
	tkRoot.MustExec("insert into projects values (2, 'ROADMAP', 'AMER', 800, seclabel('PROJ_POLICY','CONFIDENTIAL|ROADMAP|AMER'))")
	tkRoot.MustExec("insert into projects values (3, 'PAYROLL', 'CN', 200, seclabel('PROJ_POLICY','PUBLIC|PAYROLL|CN'))")

	tkRoot.MustExec("create user 'boss'@'%'")
	tkRoot.MustExec("create user 'hr'@'%'")
	tkRoot.MustExec("create user 'fin'@'%'")
	tkRoot.MustExec("create user 'eng_us'@'%'")
	tkRoot.MustExec("create user 'emp_a'@'%'")
	tkRoot.MustExec("create user 'emp_b'@'%'")
	tkRoot.MustExec("create user 'ops'@'%'")

	tkRoot.MustExec("grant select, update on test_lbac_multi.salary to 'boss'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_multi.salary to 'hr'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_multi.salary to 'fin'@'%'")
	tkRoot.MustExec("grant select, insert, delete on test_lbac_multi.salary to 'eng_us'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_multi.salary to 'emp_a'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_multi.salary to 'emp_b'@'%'")
	tkRoot.MustExec("grant select, update on test_lbac_multi.projects to 'boss'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_multi.projects to 'ops'@'%'")

	tkRoot.MustExec("grant exemption on rule all for pay_policy to user 'boss'@'%'")
	tkRoot.MustExec("grant exemption on rule all for proj_policy to user 'boss'@'%'")
	tkRoot.MustExec("grant security label pay_policy.pay_all_top to user 'hr'@'%' for read access")
	tkRoot.MustExec("grant security label pay_policy.pay_hr_write to user 'hr'@'%' for write access")
	tkRoot.MustExec("grant security label pay_policy.pay_all_top to user 'fin'@'%' for all access")
	tkRoot.MustExec("grant security label pay_policy.pay_eng_amer to user 'eng_us'@'%' for read access")
	tkRoot.MustExec("grant security label pay_policy.pay_emp_a to user 'emp_a'@'%' for read access")
	tkRoot.MustExec("grant security label pay_policy.pay_emp_b to user 'emp_b'@'%' for all access")
	tkRoot.MustExec("grant security label proj_policy.proj_conf_amer to user 'ops'@'%' for all access")

	tkBoss := testkit.NewTestKit(t, store)
	require.NoError(t, tkBoss.Session().Auth(&auth.UserIdentity{
		Username:     "boss",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkBoss.MustExec("use test_lbac_multi")
	tkBoss.MustQuery("select count(*) from salary").Check(testkit.Rows("7"))
	tkBoss.MustExec("update salary set salary = salary + 200 where name = 'fin'")
	tkBoss.MustQuery("select count(*) from projects").Check(testkit.Rows("3"))

	tkHR := testkit.NewTestKit(t, store)
	require.NoError(t, tkHR.Session().Auth(&auth.UserIdentity{
		Username:     "hr",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkHR.MustExec("use test_lbac_multi")
	tkHR.MustQuery("select count(*) from salary").Check(testkit.Rows("7"))
	tkHR.MustExec("update salary set salary = salary + 50 where name = 'emp_b'")
	tkHR.MustGetDBError("update salary set salary = salary + 50 where name = 'boss'", exeerrors.ErrRowLabelUnAccessible)
	tkHR.MustGetDBError("update salary set salary = salary + 50 where name = 'fin'", exeerrors.ErrRowLabelUnAccessible)
	tkHR.MustExec("insert into salary (id, name, department, region, salary, row_seclabel) values (80, 'hr_tmp', 'ENG', 'US', 6100, seclabel('PAY_POLICY','PUBLIC|ENG|US|EMP_B'))")
	tkHR.MustGetDBError("insert into salary (id, name, department, region, salary, row_seclabel) values (81, 'hr_tmp_denied', 'EXEC', 'GLOBAL', 100000, seclabel('PAY_POLICY','TOP_SECRET|EXEC|GLOBAL|BOSS'))", exeerrors.ErrRowLabelUnAccessible)
	tkHR.MustExec("delete from salary where id = 80")
	tkHR.MustGetDBError("delete from salary where name = 'boss'", exeerrors.ErrRowLabelUnAccessible)

	tkFIN := testkit.NewTestKit(t, store)
	require.NoError(t, tkFIN.Session().Auth(&auth.UserIdentity{
		Username:     "fin",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkFIN.MustExec("use test_lbac_multi")
	tkFIN.MustQuery("select count(*) from salary").Check(testkit.Rows("7"))
	tkFIN.MustExec("update salary set salary = salary + 25 where name = 'boss'")
	tkFIN.MustExec("update salary set salary = salary + 25 where name = 'emp_a'")
	tkFIN.MustExec("insert into salary (id, name, department, region, salary, row_seclabel) values (82, 'fin_tmp', 'EXEC', 'GLOBAL', 9100, seclabel('PAY_POLICY','TOP_SECRET|EXEC|GLOBAL|BOSS'))")
	tkFIN.MustExec("delete from salary where id = 82")

	tkEng := testkit.NewTestKit(t, store)
	require.NoError(t, tkEng.Session().Auth(&auth.UserIdentity{
		Username:     "eng_us",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkEng.MustExec("use test_lbac_multi")
	tkEng.MustQuery("select name from salary order by id").Check(testkit.Rows("eng_us", "emp_a", "emp_b"))
	tkEng.MustGetDBError("insert into salary (id, name, department, region, salary, row_seclabel) values (83, 'eng_tmp', 'ENG', 'US', 7000, seclabel('PAY_POLICY','PUBLIC|ENG|US|ENG_US'))", exeerrors.ErrRowLabelUnAccessible)
	tkEng.MustGetDBError("delete from salary where name = 'eng_us'", exeerrors.ErrRowLabelUnAccessible)

	tkEmpA := testkit.NewTestKit(t, store)
	require.NoError(t, tkEmpA.Session().Auth(&auth.UserIdentity{
		Username:     "emp_a",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkEmpA.MustExec("use test_lbac_multi")
	tkEmpA.MustQuery("select name from salary order by id").Check(testkit.Rows("emp_a"))
	tkEmpA.MustGetDBError("update salary set salary = salary + 10 where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)
	tkEmpA.MustGetDBError("insert into salary (id, name, department, region, salary, row_seclabel) values (84, 'emp_a_tmp', 'ENG', 'US', 5100, seclabel('PAY_POLICY','PUBLIC|ENG|US|EMP_A'))", exeerrors.ErrRowLabelUnAccessible)
	tkEmpA.MustGetDBError("delete from salary where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)

	tkEmpB := testkit.NewTestKit(t, store)
	require.NoError(t, tkEmpB.Session().Auth(&auth.UserIdentity{
		Username:     "emp_b",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkEmpB.MustExec("use test_lbac_multi")
	tkEmpB.MustQuery("select name from salary order by id").Check(testkit.Rows("emp_b"))
	tkEmpB.MustExec("update salary set salary = salary + 10 where name = 'emp_b'")
	tkEmpB.MustGetDBError("update salary set salary = salary + 10 where name = 'emp_a'", exeerrors.ErrRowLabelUnAccessible)
	tkEmpB.MustExec("insert into salary (id, name, department, region, salary, row_seclabel) values (85, 'emp_b_tmp', 'ENG', 'US', 5300, seclabel('PAY_POLICY','PUBLIC|ENG|US|EMP_B'))")
	tkEmpB.MustExec("delete from salary where id = 85")

	tkOps := testkit.NewTestKit(t, store)
	require.NoError(t, tkOps.Session().Auth(&auth.UserIdentity{
		Username:     "ops",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkOps.MustExec("use test_lbac_multi")
	tkOps.MustQuery("select id from projects order by id").Check(testkit.Rows("2"))
	tkOps.MustExec("insert into projects values (4, 'ROADMAP', 'US', 900, seclabel('PROJ_POLICY','TOP_SECRET|ROADMAP|US'))")
	tkOps.MustQuery("select seclabel_to_char('PROJ_POLICY', row_seclabel) from projects where id = 4").
		Check(testkit.Rows("CONFIDENTIAL|ROADMAP|AMER"))
	tkOps.MustExec("delete from projects where id = 4")
	tkOps.MustGetDBError("delete from projects where id = 1", exeerrors.ErrRowLabelUnAccessible)
}

func TestLBACPolicyScopedExemption(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_exempt_scope")
	tkRoot.MustExec("use test_lbac_exempt_scope")
	tkRoot.MustExec("drop table if exists sales_a")
	tkRoot.MustExec("drop table if exists sales_b")
	tkRoot.MustExec("drop user if exists 'scoped'@'%'")

	tkRoot.MustExec("create security label component scope_classif array ('A')")
	tkRoot.MustExec("create security policy policy_a components scope_classif")
	tkRoot.MustExec("create security policy policy_b components scope_classif")
	tkRoot.MustExec("create security label policy_a.label_a component scope_classif 'A'")
	tkRoot.MustExec("create security label policy_b.label_b component scope_classif 'A'")
	tkRoot.MustExec("create security label policy_a.label_a_col component scope_classif 'A'")
	tkRoot.MustExec("create security label policy_b.label_b_col component scope_classif 'A'")

	tkRoot.MustExec("create table sales_a (id int primary key, amount int secured with label_a_col, row_seclabel securitylabel not null) security policy policy_a")
	tkRoot.MustExec("create table sales_b (id int primary key, amount int secured with label_b_col, row_seclabel securitylabel not null) security policy policy_b")
	tkRoot.MustExec("insert into sales_a values (1, 100, seclabel('POLICY_A','A'))")
	tkRoot.MustExec("insert into sales_b values (1, 200, seclabel('POLICY_B','A'))")

	tkRoot.MustExec("create user 'scoped'@'%'")
	tkRoot.MustExec("grant select on test_lbac_exempt_scope.sales_a to 'scoped'@'%'")
	tkRoot.MustExec("grant select on test_lbac_exempt_scope.sales_b to 'scoped'@'%'")

	tkScoped := testkit.NewTestKit(t, store)
	require.NoError(t, tkScoped.Session().Auth(&auth.UserIdentity{
		Username:     "scoped",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkScoped.MustExec("use test_lbac_exempt_scope")
	tkScoped.MustGetErrCode("select amount from sales_a", errno.ErrColumnaccessDenied)
	tkScoped.MustGetErrCode("select amount from sales_b", errno.ErrColumnaccessDenied)

	tkRoot.MustExec("grant exemption on rule all for policy_a to user 'scoped'@'%'")
	tkScoped.MustQuery("select amount from sales_a").Check(testkit.Rows("100"))
	tkScoped.MustGetErrCode("select amount from sales_b", errno.ErrColumnaccessDenied)
}

func TestLBACRowFilterAndInvalidLabel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_err")
	tkRoot.MustExec("use test_lbac_err")
	tkRoot.MustExec("drop table if exists sales")
	tkRoot.MustExec("drop user if exists 'mallory'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP_SECRET','CONFIDENTIAL')")
	tkRoot.MustExec("create security label component dept set ('FIN')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'USA' under 'WORLD')")
	tkRoot.MustExec("create security policy data_access_err components classif, dept, region")
	tkRoot.MustExec("create security label data_access_err.top_secret_fin_usa component classif 'TOP_SECRET' component dept 'FIN' component region 'USA'")
	tkRoot.MustExec("create security label data_access_err.confidential_fin_usa component classif 'CONFIDENTIAL' component dept 'FIN' component region 'USA'")
	tkRoot.MustExec("create security label data_access_err.top_secret_fin_usa_col component classif 'TOP_SECRET' component dept 'FIN' component region 'USA'")

	tkRoot.MustExec("create table sales (id int primary key, amount int secured with top_secret_fin_usa_col, row_seclabel securitylabel not null) security policy data_access_err")
	tkRoot.MustExec("insert into sales values (1, 100, seclabel('DATA_ACCESS_ERR','TOP_SECRET|FIN|USA'))")
	tkRoot.MustExec("insert into sales values (2, 200, seclabel('DATA_ACCESS_ERR','CONFIDENTIAL|FIN|USA'))")

	tkRoot.MustExec("create user 'mallory'@'%'")
	tkRoot.MustExec("create user 'writer'@'%'")
	tkRoot.MustExec("grant select, insert, delete on test_lbac_err.sales to 'mallory'@'%'")
	tkRoot.MustExec("grant insert on test_lbac_err.sales to 'writer'@'%'")
	tkRoot.MustExec("grant security label data_access_err.top_secret_fin_usa to user 'writer'@'%' for write access")

	tkMallory := testkit.NewTestKit(t, store)
	require.NoError(t, tkMallory.Session().Auth(&auth.UserIdentity{
		Username:     "mallory",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkMallory.MustExec("use test_lbac_err")
	tkMallory.MustQuery("select id from sales order by id").Check(testkit.Rows())
	tkMallory.MustGetErrCode("select amount from sales", errno.ErrColumnaccessDenied)
	tkMallory.MustGetDBError("delete from sales where id = 1", exeerrors.ErrRowLabelUnAccessible)

	tkWriter := testkit.NewTestKit(t, store)
	require.NoError(t, tkWriter.Session().Auth(&auth.UserIdentity{
		Username:     "writer",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkWriter.MustExec("use test_lbac_err")
	tkWriter.MustContainErrMsg("insert into sales (id, amount, row_seclabel) values (3, 300, seclabel('DATA_ACCESS_ERR','SECRET|FIN|USA'))", "security label not found")
}

func TestLBACUpdateSecuredColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_update_col")
	tkRoot.MustExec("use test_lbac_update_col")
	tkRoot.MustExec("drop table if exists sales")
	tkRoot.MustExec("drop user if exists 'upd_user'@'%'")

	tkRoot.MustExec("create security label component upd_classif array ('A','B')")
	tkRoot.MustExec("create security label component upd_dept set ('HR')")
	tkRoot.MustExec("create security policy upd_policy components upd_classif, upd_dept")
	tkRoot.MustExec("create security label upd_policy.row_hr component upd_classif 'B' component upd_dept 'HR'")
	tkRoot.MustExec("create security label upd_policy.col_hr component upd_classif 'A' component upd_dept 'HR'")

	tkRoot.MustExec("create table sales (id int primary key, item varchar(16), amount int secured with col_hr, row_seclabel securitylabel not null) security policy upd_policy")
	tkRoot.MustExec("insert into sales values (1, 'item1', 10, seclabel('UPD_POLICY','B|HR'))")

	tkRoot.MustExec("create user 'upd_user'@'%'")
	tkRoot.MustExec("grant select, update on test_lbac_update_col.sales to 'upd_user'@'%'")
	tkRoot.MustExec("grant security label upd_policy.row_hr to user 'upd_user'@'%' for write access")

	tkUpd := testkit.NewTestKit(t, store)
	require.NoError(t, tkUpd.Session().Auth(&auth.UserIdentity{
		Username:     "upd_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkUpd.MustExec("use test_lbac_update_col")
	tkUpd.MustExec("update sales set item = 'item2' where id = 1")
	tkUpd.MustGetErrCode("update sales set amount = 20 where id = 1", errno.ErrColumnaccessDenied)
	tkRoot.MustQuery("select item, amount from sales where id = 1").Check(testkit.Rows("item2 10"))
}

func TestLBACReplaceRowLabelAccess(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_replace")
	tkRoot.MustExec("use test_lbac_replace")
	tkRoot.MustExec("drop table if exists t_replace")
	tkRoot.MustExec("drop user if exists 'rep_user'@'%'")

	tkRoot.MustExec("create security label component rep_lvl array ('HIGH','LOW')")
	tkRoot.MustExec("create security policy rep_policy components rep_lvl")
	tkRoot.MustExec("create security label rep_policy.rep_high component rep_lvl 'HIGH'")
	tkRoot.MustExec("create security label rep_policy.rep_low component rep_lvl 'LOW'")

	tkRoot.MustExec("create table t_replace (id int primary key, row_seclabel securitylabel not null) security policy rep_policy")
	tkRoot.MustExec("insert into t_replace values (1, seclabel('REP_POLICY','HIGH'))")

	tkRoot.MustExec("create user 'rep_user'@'%'")
	tkRoot.MustExec("grant select, insert, delete on test_lbac_replace.t_replace to 'rep_user'@'%'")
	tkRoot.MustExec("grant security label rep_policy.rep_low to user 'rep_user'@'%' for write access")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{
		Username:     "rep_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkUser.MustExec("use test_lbac_replace")
	tkUser.MustGetDBError("replace into t_replace values (1, seclabel('REP_POLICY','LOW'))", exeerrors.ErrRowLabelUnAccessible)

	tkRoot.MustExec("grant security label rep_policy.rep_high to user 'rep_user'@'%' for write access")
	tkUser.MustExec("replace into t_replace values (1, seclabel('REP_POLICY','LOW'))")
	tkRoot.MustQuery("select seclabel_to_char('REP_POLICY', row_seclabel) from t_replace").Check(testkit.Rows("LOW"))
}

func TestLBACOnDuplicateSecuredColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_dup")
	tkRoot.MustExec("use test_lbac_dup")
	tkRoot.MustExec("drop table if exists t_dup")
	tkRoot.MustExec("drop user if exists 'dup_user'@'%'")

	tkRoot.MustExec("create security label component dup_lvl array ('HIGH','LOW')")
	tkRoot.MustExec("create security policy dup_policy components dup_lvl")
	tkRoot.MustExec("create security label dup_policy.row_low component dup_lvl 'LOW'")
	tkRoot.MustExec("create security label dup_policy.col_high component dup_lvl 'HIGH'")

	tkRoot.MustExec("create table t_dup (id int primary key, amount int secured with col_high, row_seclabel securitylabel not null) security policy dup_policy")
	tkRoot.MustExec("insert into t_dup values (1, 10, seclabel('DUP_POLICY','LOW'))")

	tkRoot.MustExec("create user 'dup_user'@'%'")
	tkRoot.MustExec("grant select, insert, update on test_lbac_dup.t_dup to 'dup_user'@'%'")
	tkRoot.MustExec("grant security label dup_policy.row_low to user 'dup_user'@'%' for write access")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{
		Username:     "dup_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkUser.MustExec("use test_lbac_dup")
	tkUser.MustGetErrCode("insert into t_dup values (1, 20, seclabel('DUP_POLICY','LOW')) on duplicate key update amount = 30", errno.ErrColumnaccessDenied)
}

func TestLBACNullRowLabelDelete(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_null_label")
	tkRoot.MustExec("use test_lbac_null_label")
	tkRoot.MustExec("drop table if exists sales")
	tkRoot.MustExec("drop user if exists 'null_user'@'%'")

	tkRoot.MustExec("create security label component null_classif array ('A')")
	tkRoot.MustExec("create security label component null_dept set ('HR')")
	tkRoot.MustExec("create security policy null_policy components null_classif, null_dept")
	tkRoot.MustExec("create security label null_policy.label_hr component null_classif 'A' component null_dept 'HR'")

	tkRoot.MustExec("create table sales (id int primary key, dept varchar(8), row_seclabel securitylabel) security policy null_policy")
	tkRoot.MustExec("insert into sales values (1, 'HR', null)")
	tkRoot.MustExec("insert into sales values (2, 'HR', seclabel('NULL_POLICY','A|HR'))")

	tkRoot.MustExec("create user 'null_user'@'%'")
	tkRoot.MustExec("grant select, delete on test_lbac_null_label.sales to 'null_user'@'%'")
	tkRoot.MustExec("grant security label null_policy.label_hr to user 'null_user'@'%' for write access")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{
		Username:     "null_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkUser.MustExec("use test_lbac_null_label")
	tkUser.MustGetDBError("delete from sales where id = 1", exeerrors.ErrRowLabelUnAccessible)
	tkRoot.MustQuery("select count(*) from sales").Check(testkit.Rows("2"))
}

func TestLBACColumnAccessAndExemption(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_col")
	tkRoot.MustExec("use test_lbac_col")
	tkRoot.MustExec("drop table if exists sales")
	tkRoot.MustExec("drop user if exists 'analyst'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP_SECRET','CONFIDENTIAL')")
	tkRoot.MustExec("create security label component dept set ('FIN')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'USA' under 'WORLD')")
	tkRoot.MustExec("create security policy data_access_col components classif, dept, region")

	tkRoot.MustExec("create security label data_access_col.top_secret_fin_usa component classif 'TOP_SECRET' component dept 'FIN' component region 'USA'")
	tkRoot.MustExec("create security label data_access_col.confidential_fin_usa component classif 'CONFIDENTIAL' component dept 'FIN' component region 'USA'")
	tkRoot.MustExec("create security label data_access_col.top_secret_fin_usa_col component classif 'TOP_SECRET' component dept 'FIN' component region 'USA'")

	tkRoot.MustExec("create table sales (id int primary key, amount int secured with top_secret_fin_usa_col, row_seclabel securitylabel not null) security policy data_access_col")
	tkRoot.MustExec("insert into sales values (1, 100, seclabel('DATA_ACCESS_COL','TOP_SECRET|FIN|USA'))")

	tkRoot.MustExec("create user 'analyst'@'%'")
	tkRoot.MustExec("grant select, insert on test_lbac_col.sales to 'analyst'@'%'")
	tkRoot.MustExec("grant security label data_access_col.confidential_fin_usa to user 'analyst'@'%' for read access")

	tkAnalyst := testkit.NewTestKit(t, store)
	require.NoError(t, tkAnalyst.Session().Auth(&auth.UserIdentity{
		Username:     "analyst",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkAnalyst.MustExec("use test_lbac_col")
	tkAnalyst.MustQuery("select id from sales order by id").Check(testkit.Rows())
	tkAnalyst.MustGetErrCode("select amount from sales", errno.ErrColumnaccessDenied)
	tkAnalyst.MustGetErrCode("insert into sales (id, amount, row_seclabel) values (2, 200, seclabel('DATA_ACCESS_COL','TOP_SECRET|FIN|USA'))", errno.ErrColumnaccessDenied)

	tkRoot.MustExec("grant exemption on rule all for data_access_col to user 'analyst'@'%'")
	tkRoot.MustQuery("select rule from mysql.user_exemptions where user_name = 'analyst' and policy_name = 'data_access_col' order by rule").Check(testkit.Rows("all"))

	tkAnalyst.MustQuery("select id from sales order by id").Check(testkit.Rows("1"))
	tkAnalyst.MustQuery("select amount from sales order by id").Check(testkit.Rows("100"))
	tkAnalyst.MustExec("insert into sales (id, amount, row_seclabel) values (2, 200, seclabel('DATA_ACCESS_COL','TOP_SECRET|FIN|USA'))")
	tkRoot.MustQuery("select count(*) from sales").Check(testkit.Rows("2"))

	tkRoot.MustExec("revoke exemption on rule all for data_access_col from user 'analyst'@'%'")
	tkRoot.MustQuery("select count(*) from mysql.user_exemptions where user_name = 'analyst' and policy_name = 'data_access_col'").Check(testkit.Rows("0"))

	tkAnalyst.MustQuery("select id from sales order by id").Check(testkit.Rows())
	tkAnalyst.MustGetErrCode("select amount from sales", errno.ErrColumnaccessDenied)
	tkAnalyst.MustGetErrCode("insert into sales (id, amount, row_seclabel) values (3, 300, seclabel('DATA_ACCESS_COL','TOP_SECRET|FIN|USA'))", errno.ErrColumnaccessDenied)
}

func TestLBACDisableLabelSecurity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_disable")
	tkRoot.MustExec("use test_lbac_disable")
	tkRoot.MustExec("drop table if exists sales")
	tkRoot.MustExec("drop user if exists 'no_label'@'%'")

	tkRoot.MustExec("create security label component classif array ('A')")
	tkRoot.MustExec("create security policy data_access components classif")
	tkRoot.MustExec("create security label data_access.label_a component classif 'A'")

	tkRoot.MustExec("create table sales (id int primary key, amount int secured with label_a, row_seclabel securitylabel not null) security policy data_access")
	tkRoot.MustExec("insert into sales values (1, 100, seclabel('DATA_ACCESS','A'))")

	tkRoot.MustExec("create user 'no_label'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on test_lbac_disable.sales to 'no_label'@'%'")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{
		Username:     "no_label",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkUser.MustExec("use test_lbac_disable")

	tkUser.MustQuery("select id from sales order by id").Check(testkit.Rows())
	tkUser.MustGetErrCode("select amount from sales", errno.ErrColumnaccessDenied)
	tkUser.MustGetErrCode("insert into sales (id, amount, row_seclabel) values (2, 200, seclabel('DATA_ACCESS','A'))", errno.ErrColumnaccessDenied)
	tkUser.MustGetErrCode("update sales set amount = 200 where id = 1", errno.ErrColumnaccessDenied)
	tkUser.MustGetDBError("delete from sales where id = 1", exeerrors.ErrRowLabelUnAccessible)

	tkRoot.MustExec("set global pkdb_lbac = false")

	tkUser.MustQuery("select id from sales order by id").Check(testkit.Rows("1"))
	tkUser.MustQuery("select amount from sales order by id").Check(testkit.Rows("100"))
	tkUser.MustExec("insert into sales (id, amount, row_seclabel) values (2, 200, seclabel('DATA_ACCESS','A'))")
	tkUser.MustExec("update sales set amount = 300 where id = 1")
	tkUser.MustExec("delete from sales where id = 1")
	tkUser.MustQuery("select id, amount from sales order by id").Check(testkit.Rows("2 200"))
}

func TestLBACTablePolicyAndColumnLabel(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global pkdb_lbac = true")
	tk.MustExec("create database if not exists test_lbac_ddl")
	tk.MustExec("use test_lbac_ddl")

	tk.MustExec("create security label component ddl_classif array ('A','B')")
	tk.MustExec("create security policy ddl_policy components ddl_classif")
	tk.MustExec("create security policy ddl_policy_other components ddl_classif")
	tk.MustExec("create security label ddl_policy.label_a component ddl_classif 'A'")
	tk.MustExec("create security label ddl_policy.label_b component ddl_classif 'B'")
	tk.MustExec("create security label ddl_policy_other.label_other component ddl_classif 'A'")

	tk.MustExec("create table t_sec (id int primary key, val int secured with label_a, row_seclabel securitylabel not null) security policy ddl_policy")
	tk.MustQuery("show create table t_sec").Check(testkit.Rows(
		"t_sec CREATE TABLE `t_sec` (\n" +
			"  `id` int(11) NOT NULL,\n" +
			"  `val` int(11) DEFAULT NULL SECURED WITH `label_a`,\n" +
			"  `row_seclabel` securitylabel NOT NULL,\n" +
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SECURITY POLICY `ddl_policy`"))

	tk.MustExec("create table t_multi (id int primary key, a int secured with label_a, b int secured with label_b, row_seclabel securitylabel not null) security policy ddl_policy")
	tk.MustQuery("show create table t_multi").Check(testkit.Rows(
		"t_multi CREATE TABLE `t_multi` (\n" +
			"  `id` int(11) NOT NULL,\n" +
			"  `a` int(11) DEFAULT NULL SECURED WITH `label_a`,\n" +
			"  `b` int(11) DEFAULT NULL SECURED WITH `label_b`,\n" +
			"  `row_seclabel` securitylabel NOT NULL,\n" +
			"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SECURITY POLICY `ddl_policy`"))
	tk.MustContainErrMsg("create table t_multi_bad (id int primary key, a int secured with label_a, b int secured with label_other, row_seclabel securitylabel not null) security policy ddl_policy", "security label not found")
	tk.MustContainErrMsg("create table t_missing_policy (id int, row_seclabel securitylabel not null) security policy missing_policy", "policy not found")
	tk.MustContainErrMsg("create table t_no_policy (id int, val int secured with label_a)", "security policy missing")
	tk.MustContainErrMsg("create table t_bad_label (id int, val int secured with label_other, row_seclabel securitylabel not null) security policy ddl_policy", "security label not found")
	tk.MustContainErrMsg("create table t (l1 securitylabel, l2 securitylabel)", "multiple SECURITYLABEL columns in table")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test_lbac_ddl"), pmodel.NewCIStr("t_sec"))
	require.NoError(t, err)
	tbInfo := tbl.Meta()
	require.Equal(t, "ddl_policy", tbInfo.SecurityPolicy.O)
	for _, col := range tbInfo.Columns {
		switch col.Name.L {
		case "val":
			require.Equal(t, "label_a", col.SecurityLabel.O)
		case "row_seclabel":
			require.Equal(t, col.ID, tbInfo.GetSecurityLabelColumnID())
			require.True(t, col.FieldType.IsSecurityLabel())
		default:
			require.Nil(t, col.SecurityLabel)
		}
	}

	tk.MustExec("create table t_add (id int primary key, row_seclabel securitylabel not null) security policy ddl_policy")
	tk.MustExec("alter table t_add add column val int secured with label_a")
	tk.MustContainErrMsg("alter table t_add add column val2 int secured with label_other", "security label not found")

	tk.MustExec("create table t_add_no_policy (id int)")
	tk.MustContainErrMsg("alter table t_add_no_policy add column val int secured with label_a", "security policy missing")

	tk.MustExec("create table t_mod (id int primary key, val int, row_seclabel securitylabel not null) security policy ddl_policy")
	tk.MustExec("alter table t_mod modify column val int secured with label_b")
	tk.MustContainErrMsg("alter table t_mod modify column val int secured with label_other", "security label not found")

	tk.MustExec("create table t_mod_no_policy (id int primary key, val int)")
	tk.MustContainErrMsg("alter table t_mod_no_policy modify column val int secured with label_a", "security policy missing")
}

func TestLBACCreateSecurityLabelComponent(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create security label component comp_array array ('A','B')")
	tk.MustQuery("select type, component_values from mysql.security_label_components where name = 'comp_array'").
		Check(testkit.Rows(`ARRAY ["A", "B"]`))

	tk.MustExec("create security label component comp_set set ('HR','FIN','OPS')")
	tk.MustQuery("select type, component_values from mysql.security_label_components where name = 'comp_set'").
		Check(testkit.Rows(`SET ["HR", "FIN", "OPS"]`))

	tk.MustExec("create security label component comp_tree tree ('WORLD' root, 'USA' under 'WORLD', 'CAN' under 'WORLD')")
	tk.MustQuery("select type, component_values from mysql.security_label_components where name = 'comp_tree'").
		Check(testkit.Rows(`TREE [{"name": "WORLD"}, {"name": "USA", "parent": "WORLD"}, {"name": "CAN", "parent": "WORLD"}]`))

	tk.MustExec("create security label component comp_tree_deep tree ('ROOT' root, 'A' under 'ROOT', 'B' under 'A', 'C' under 'B')")
	tk.MustQuery("select type, component_values from mysql.security_label_components where name = 'comp_tree_deep'").
		Check(testkit.Rows(`TREE [{"name": "ROOT"}, {"name": "A", "parent": "ROOT"}, {"name": "B", "parent": "A"}, {"name": "C", "parent": "B"}]`))

	tk.MustContainErrMsg("create security label component comp_set_dup set ('HR','HR')", "duplicate component value")
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'comp_set_dup'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label component comp_array array ('C')", "component already exists")
	tk.MustQuery("select type, component_values from mysql.security_label_components where name = 'comp_array'").
		Check(testkit.Rows(`ARRAY ["A", "B"]`))

	tk.MustGetErrCode("create security label component comp_bad map ('A')", errno.ErrParse)
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'comp_bad'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label component comp_bad_val array ('')", "invalid component value")
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'comp_bad_val'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label component comp_dup array ('A','A')", "duplicate component value")
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'comp_dup'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label component comp_tree_noroot tree ('A' under 'B')", "invalid component value")
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'comp_tree_noroot'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label component comp_tree_multiroot tree ('A' root, 'B' root)", "invalid component value")
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'comp_tree_multiroot'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label component comp_tree_missing tree ('A' root, 'B' under 'X')", "invalid component value")
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'comp_tree_missing'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label component comp_tree_dup tree ('A' root, 'A' under 'A')", "duplicate component value")
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'comp_tree_dup'").Check(testkit.Rows("0"))
}

func TestLBACCreateSecurityPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create security label component pol_classif array ('A','B')")
	tk.MustExec("create security label component pol_dept set ('HR','FIN')")
	tk.MustExec("create security label component pol_region tree ('WORLD' root, 'USA' under 'WORLD')")

	tk.MustExec("create security policy pol_default components pol_classif, pol_dept, pol_region")
	tk.MustQuery("select write_control, component_names from mysql.security_policies where name = 'pol_default'").
		Check(testkit.Rows(`RESTRICT ["pol_classif", "pol_dept", "pol_region"]`))

	tk.MustExec("create security policy pol_override components pol_classif, pol_dept override not authorized write security label")
	tk.MustQuery("select write_control, component_names from mysql.security_policies where name = 'pol_override'").
		Check(testkit.Rows(`OVERRIDE ["pol_classif", "pol_dept"]`))

	tk.MustExec("create security policy pol_restrict components pol_region, pol_classif restrict not authorized write security label")
	tk.MustQuery("select write_control, component_names from mysql.security_policies where name = 'pol_restrict'").
		Check(testkit.Rows(`RESTRICT ["pol_region", "pol_classif"]`))

	tk.MustContainErrMsg("create security policy pol_dup components pol_classif, pol_classif", "duplicate component in policy")
	tk.MustQuery("select count(*) from mysql.security_policies where name = 'pol_dup'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security policy pol_missing components pol_missing", "component not found")
	tk.MustQuery("select count(*) from mysql.security_policies where name = 'pol_missing'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security policy pol_default components pol_classif, pol_dept", "policy already exists")
	tk.MustQuery("select write_control, component_names from mysql.security_policies where name = 'pol_default'").
		Check(testkit.Rows(`RESTRICT ["pol_classif", "pol_dept", "pol_region"]`))
}

func TestLBACCreateSecurityLabel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create security label component lbl_classif array ('A','B')")
	tk.MustExec("create security label component lbl_dept set ('HR','FIN','OPS')")
	tk.MustExec("create security label component lbl_region tree ('WORLD' root, 'USA' under 'WORLD', 'EU' under 'WORLD')")

	tk.MustExec("create security policy lbl_policy components lbl_classif, lbl_dept, lbl_region")

	tk.MustExec("create security label lbl_policy.label_simple component lbl_classif 'A' component lbl_dept 'HR' component lbl_region 'USA'")
	tk.MustQuery(`select policy_name, components from mysql.security_labels where name = 'label_simple'`).
		Check(testkit.Rows(`lbl_policy {"lbl_classif": "A", "lbl_dept": ["HR"], "lbl_region": "USA"}`))

	tk.MustExec("create security label lbl_policy.label_multi component lbl_classif 'B' component lbl_dept 'FIN','OPS' component lbl_region 'EU'")
	tk.MustQuery(`select JSON_LENGTH(JSON_EXTRACT(components, '$.lbl_dept')),
		JSON_CONTAINS(JSON_EXTRACT(components, '$.lbl_dept'), '"FIN"', '$'),
		JSON_CONTAINS(JSON_EXTRACT(components, '$.lbl_dept'), '"OPS"', '$')
		from mysql.security_labels where name = 'label_multi'`).
		Check(testkit.Rows("2 1 1"))
	tk.MustQuery(`select policy_name, components from mysql.security_labels where name = 'label_multi'`).
		Check(testkit.Rows(`lbl_policy {"lbl_classif": "B", "lbl_dept": ["FIN", "OPS"], "lbl_region": "EU"}`))

	tk.MustContainErrMsg("create security label lbl_policy.label_simple component lbl_classif 'A' component lbl_dept 'HR' component lbl_region 'USA'", "label already exists")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_simple'").Check(testkit.Rows("1"))

	tk.MustContainErrMsg("create security label missing_policy.label_x component lbl_classif 'A' component lbl_dept 'HR' component lbl_region 'USA'", "policy not found")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_x'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label lbl_policy.label_missing component lbl_classif 'A' component lbl_dept 'HR'", "invalid component for label")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_missing'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label lbl_policy.label_extra component lbl_classif 'A' component lbl_dept 'HR' component lbl_region 'USA' component lbl_extra 'X'", "invalid component for label")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_extra'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label lbl_policy.label_bad_array component lbl_classif 'A','B' component lbl_dept 'HR' component lbl_region 'USA'", "invalid component value")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_bad_array'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label lbl_policy.label_bad_set component lbl_classif 'A' component lbl_dept 'HR','HR' component lbl_region 'USA'", "duplicate component value")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_bad_set'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label lbl_policy.label_bad_value component lbl_classif 'A' component lbl_dept 'BAD' component lbl_region 'USA'", "invalid component value")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_bad_value'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("create security label lbl_policy.label_bad_tree component lbl_classif 'A' component lbl_dept 'HR' component lbl_region 'ASIA'", "invalid component value")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_bad_tree'").Check(testkit.Rows("0"))
}

func TestLBACDropSecurityLabelComponent(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create security label component drop_comp array ('A','B')")
	tk.MustExec("create security policy drop_pol components drop_comp")
	tk.MustContainErrMsg("drop security label component drop_comp", "component is in use")

	tk.MustExec("drop security policy drop_pol")
	tk.MustExec("drop security label component drop_comp")
	tk.MustQuery("select count(*) from mysql.security_label_components where name = 'drop_comp'").Check(testkit.Rows("0"))

	tk.MustContainErrMsg("drop security label component missing_comp", "component not found")
}

func TestLBACDropSecurityPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_lbac_drop")
	tk.MustExec("use test_lbac_drop")

	tk.MustExec("create security label component pol_comp array ('A')")

	tk.MustExec("create security policy pol_label components pol_comp")
	tk.MustExec("create security label pol_label.l1 component pol_comp 'A'")
	tk.MustContainErrMsg("drop security policy pol_label", "policy is in use")
	tk.MustExec("drop security label pol_label.l1")
	tk.MustExec("drop security policy pol_label")
	tk.MustQuery("select count(*) from mysql.security_policies where name = 'pol_label'").Check(testkit.Rows("0"))

	tk.MustExec("create security policy pol_table components pol_comp")
	tk.MustExec("create table pol_table_t (id int, row_seclabel securitylabel not null) security policy pol_table")
	tk.MustContainErrMsg("drop security policy pol_table", "policy is in use by tables")
	tk.MustExec("drop table pol_table_t")
	tk.MustExec("drop security policy pol_table")

	tk.MustExec("drop user if exists 'pol_exempt_user'@'%'")
	tk.MustExec("create security policy pol_exempt components pol_comp")
	tk.MustExec("create user 'pol_exempt_user'@'%'")
	tk.MustExec("grant exemption on rule all for pol_exempt to user 'pol_exempt_user'@'%'")
	tk.MustContainErrMsg("drop security policy pol_exempt", "policy is in use by exemptions")
	tk.MustExec("revoke exemption on rule all for pol_exempt from user 'pol_exempt_user'@'%'")
	tk.MustExec("drop security policy pol_exempt")
	tk.MustExec("drop user 'pol_exempt_user'@'%'")

	tk.MustContainErrMsg("drop security policy missing_policy", "policy not found")
}

func TestLBACDropSecurityLabel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_lbac_drop")
	tk.MustExec("use test_lbac_drop")

	tk.MustExec("create security label component lbl_comp array ('A','B')")
	tk.MustExec("create security policy lbl_policy components lbl_comp")
	tk.MustExec("create security label lbl_policy.label_user component lbl_comp 'A'")
	tk.MustExec("create security label lbl_policy.label_col component lbl_comp 'B'")

	tk.MustExec("drop user if exists 'label_user'@'%'")
	tk.MustExec("create user 'label_user'@'%'")
	tk.MustExec("grant security label lbl_policy.label_user to user 'label_user'@'%' for read access")
	tk.MustContainErrMsg("drop security label lbl_policy.label_user", "label is in use by users")
	tk.MustExec("revoke security label lbl_policy.label_user from user 'label_user'@'%'")
	tk.MustExec("drop security label lbl_policy.label_user")
	tk.MustQuery("select count(*) from mysql.security_labels where name = 'label_user'").Check(testkit.Rows("0"))

	tk.MustExec("create table lbl_table (id int, amount int secured with label_col, row_seclabel securitylabel not null) security policy lbl_policy")
	tk.MustContainErrMsg("drop security label lbl_policy.label_col", "label is in use by columns")
	tk.MustExec("drop table lbl_table")
	tk.MustExec("drop security label lbl_policy.label_col")

	tk.MustContainErrMsg("drop security label lbl_policy.missing", "label not found")
	tk.MustExec("drop user 'label_user'@'%'")
}

func TestLBACGrantRevokeSecurityLabel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create security label component grant_classif array ('A','B')")
	tk.MustExec("create security label component grant_dept set ('HR','FIN')")
	tk.MustExec("create security label component grant_region tree ('WORLD' root, 'USA' under 'WORLD')")
	tk.MustExec("create security policy grant_policy components grant_classif, grant_dept, grant_region")
	tk.MustExec("create security label grant_policy.label_rw component grant_classif 'A' component grant_dept 'HR' component grant_region 'USA'")
	tk.MustExec("create security label grant_policy.label_all component grant_classif 'B' component grant_dept 'FIN' component grant_region 'USA'")

	tk.MustExec("create user 'grant_user1'@'%'")
	tk.MustExec("create user 'grant_user2'@'%'")

	tk.MustExec("grant security label grant_policy.label_rw to user 'grant_user1'@'%' for read access")
	tk.MustQuery(`select host, access_types from mysql.user_security_labels where user_name = 'grant_user1' and label_name = 'label_rw'`).
		Check(testkit.Rows("% READ"))

	tk.MustExec("grant security label grant_policy.label_rw to user 'grant_user1'@'%' for write access")
	tk.MustQuery(`select host, access_types from mysql.user_security_labels where user_name = 'grant_user1' and label_name = 'label_rw'`).
		Check(testkit.Rows("% ALL"))

	tk.MustExec("grant security label grant_policy.label_all to user 'grant_user2'@'%' for all access")
	tk.MustQuery(`select host, access_types from mysql.user_security_labels where user_name = 'grant_user2' and label_name = 'label_all'`).
		Check(testkit.Rows("% ALL"))

	tk.MustContainErrMsg("grant security label grant_policy.label_missing to user 'grant_user1'@'%' for read access", "label not found")
	tk.MustQuery("select count(*) from mysql.user_security_labels where label_name = 'label_missing'").Check(testkit.Rows("0"))

	tk.MustGetErrCode("grant security label grant_policy.label_rw to user 'grant_missing'@'%' for read access", errno.ErrCannotUser)
	tk.MustQuery("select count(*) from mysql.user_security_labels where user_name = 'grant_missing' and label_name = 'label_rw'").Check(testkit.Rows("0"))

	tk.MustExec("revoke security label grant_policy.label_rw from user 'grant_user1'@'%'")
	tk.MustQuery("select count(*) from mysql.user_security_labels where user_name = 'grant_user1' and label_name = 'label_rw'").Check(testkit.Rows("0"))

	tk.MustExec("revoke security label grant_policy.label_rw from user 'grant_user1'@'%'")
	tk.MustQuery("select count(*) from mysql.user_security_labels where user_name = 'grant_user1' and label_name = 'label_rw'").Check(testkit.Rows("0"))

	tk.MustGetErrCode("revoke security label grant_policy.label_rw from user 'grant_missing'@'%'", errno.ErrCannotUser)
	tk.MustQuery("select count(*) from mysql.user_security_labels where user_name = 'grant_missing' and label_name = 'label_rw'").Check(testkit.Rows("0"))

	tk.MustGetErrCode("revoke security label grant_policy.label_rw from user 'grant_missing2'@'%'", errno.ErrCannotUser)
	tk.MustQuery("select count(*) from mysql.user_security_labels where user_name = 'grant_missing2' and label_name = 'label_rw'").Check(testkit.Rows("0"))

	tk.MustExec("revoke security label grant_policy.label_all from user 'grant_user2'@'%'")
	tk.MustQuery("select count(*) from mysql.user_security_labels where user_name = 'grant_user2' and label_name = 'label_all'").Check(testkit.Rows("0"))
}

func TestLBACGrantRevokeExemption(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create security label component ex_classif array ('A','B')")
	tk.MustExec("create security policy ex_policy components ex_classif")

	tk.MustExec("create user 'ex_user1'@'%'")
	tk.MustExec("create user 'ex_user2'@'%'")

	tk.MustGetErrCode("grant exemption on rule `access` for ex_policy to user 'ex_user1'@'%'", errno.ErrParse)

	tk.MustExec("grant exemption on rule all for ex_policy to user 'ex_user1'@'%'")
	tk.MustQuery("select rule from mysql.user_exemptions where user_name = 'ex_user1' and policy_name = 'ex_policy'").Check(testkit.Rows("all"))

	tk.MustExec("grant exemption on rule all for ex_policy to user 'ex_user2'@'%'")
	tk.MustQuery("select rule from mysql.user_exemptions where user_name = 'ex_user2' and policy_name = 'ex_policy'").Check(testkit.Rows("all"))

	tk.MustContainErrMsg("grant exemption on rule all for ex_policy to user 'ex_user1'@'%'", "user exemption already exists")
	tk.MustQuery("select count(*) from mysql.user_exemptions where user_name = 'ex_user1' and policy_name = 'ex_policy' and rule = 'all'").Check(testkit.Rows("1"))

	tk.MustGetErrCode("grant exemption on rule all for ex_policy to user 'ex_missing'@'%'", errno.ErrCannotUser)
	tk.MustQuery("select count(*) from mysql.user_exemptions where user_name = 'ex_missing' and policy_name = 'ex_policy' and rule = 'all'").Check(testkit.Rows("0"))

	tk.MustExec("revoke exemption on rule all for ex_policy from user 'ex_user1'@'%'")
	tk.MustQuery("select count(*) from mysql.user_exemptions where user_name = 'ex_user1' and policy_name = 'ex_policy'").Check(testkit.Rows("0"))

	tk.MustExec("revoke exemption on rule all for ex_policy from user 'ex_user1'@'%'")
	tk.MustQuery("select count(*) from mysql.user_exemptions where user_name = 'ex_user1' and policy_name = 'ex_policy'").Check(testkit.Rows("0"))

	tk.MustGetErrCode("revoke exemption on rule all for ex_policy from user 'ex_missing'@'%'", errno.ErrCannotUser)
	tk.MustQuery("select count(*) from mysql.user_exemptions where user_name = 'ex_missing' and policy_name = 'ex_policy'").Check(testkit.Rows("0"))

	tk.MustGetErrCode("revoke exemption on rule all for ex_policy from user 'ex_missing2'@'%'", errno.ErrCannotUser)
	tk.MustQuery("select count(*) from mysql.user_exemptions where user_name = 'ex_missing2' and policy_name = 'ex_policy'").Check(testkit.Rows("0"))

	tk.MustExec("revoke exemption on rule all for ex_policy from user 'ex_user2'@'%'")
	tk.MustQuery("select count(*) from mysql.user_exemptions where user_name = 'ex_user2' and policy_name = 'ex_policy'").Check(testkit.Rows("0"))
}

func TestLBACShowGrants(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))

	tk.MustExec("set global pkdb_lbac = true")
	tk.MustExec("create security label component sg_classif array ('A','B')")
	tk.MustExec("create security policy sg_policy components sg_classif")
	tk.MustExec("create security label sg_policy.sg_label component sg_classif 'A'")
	tk.MustExec("create user 'sg_user'@'%'")
	tk.MustExec("grant security label sg_policy.sg_label to user 'sg_user'@'%' for read access")
	tk.MustExec("grant exemption on rule all for sg_policy to user 'sg_user'@'%'")

	tk.MustQuery("show grants for 'sg_user'@'%'").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'sg_user'@'%'",
		"GRANT SECURITY LABEL `sg_policy`.`sg_label` TO USER 'sg_user'@'%' FOR READ ACCESS",
		"GRANT EXEMPTION ON RULE ALL FOR `sg_policy` TO USER 'sg_user'@'%'",
	))

	tk.MustExec("create security label component ex_classif array ('A','B')")
	tk.MustExec("create security policy ex_policy components ex_classif")
	tk.MustExec("create user 'ex_user'@'%'")
	tk.MustExec("grant exemption on rule all for ex_policy to user 'ex_user'@'%'")

	tk.MustQuery("show grants for 'ex_user'@'%'").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'ex_user'@'%'",
		"GRANT EXEMPTION ON RULE ALL FOR `ex_policy` TO USER 'ex_user'@'%'",
	))
}

func TestLBACPrivilegeChecks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_priv")
	tkRoot.MustExec("use test_lbac_priv")
	tkRoot.MustExec("drop user if exists 'alice'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP_SECRET')")
	tkRoot.MustExec("create security policy p1 components classif")
	tkRoot.MustExec("create security label p1.l1 component classif 'TOP_SECRET'")
	tkRoot.MustExec("create user 'alice'@'%'")
	tkRoot.MustExec("grant select on test_lbac_priv.* to 'alice'@'%'")

	tkAlice := testkit.NewTestKit(t, store)
	require.NoError(t, tkAlice.Session().Auth(&auth.UserIdentity{
		Username:     "alice",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkAlice.MustExec("use test_lbac_priv")

	tkAlice.MustGetErrCode("create security label component classif2 array ('SECRET')", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("create security policy p2 components classif", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("create security label p1.l2 component classif 'TOP_SECRET'", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("drop security label component classif", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("drop security policy p1", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("drop security label p1.l1", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("grant security label p1.l1 to user 'alice'@'%' for read access", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("revoke security label p1.l1 from user 'alice'@'%'", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("grant exemption on rule all for p1 to user 'alice'@'%'", errno.ErrSpecificAccessDenied)
	tkAlice.MustGetErrCode("revoke exemption on rule all for p1 from user 'alice'@'%'", errno.ErrSpecificAccessDenied)
}

func TestLBACPrivilegeChecksWithSecurityAdminRole(t *testing.T) {
	restoreFunc := executor.MockSetDutySeparationModeEnable()
	defer restoreFunc()

	audit.Register4Test()

	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_sec_admin")
	tkRoot.MustExec("use test_lbac_sec_admin")
	tkRoot.MustExec("drop user if exists 'sec_admin'@'%'")
	tkRoot.MustExec("drop user if exists 'sec_user'@'%'")
	tkRoot.MustExec("create user 'sec_admin'@'%'")
	tkRoot.MustExec("create user 'sec_user'@'%'")
	tkRoot.MustExec("grant security_admin to 'sec_admin'@'%'")

	tkAdmin := testkit.NewTestKit(t, store)
	require.NoError(t, tkAdmin.Session().Auth(&auth.UserIdentity{
		Username:     "sec_admin",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkAdmin.MustExec("set role security_admin")
	tkAdmin.MustExec("use test_lbac_sec_admin")

	tkAdmin.MustExec("create security label component sec_classif array ('A')")
	tkAdmin.MustExec("create security policy sec_policy components sec_classif")
	tkAdmin.MustExec("create security label sec_policy.sec_label component sec_classif 'A'")
	tkAdmin.MustExec("grant security label sec_policy.sec_label to user 'sec_user'@'%' for read access")
	tkAdmin.MustExec("grant exemption on rule all for sec_policy to user 'sec_user'@'%'")
}

func TestLBACSetTreeDominance(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_dom")
	tkRoot.MustExec("use test_lbac_dom")
	tkRoot.MustExec("drop table if exists sales")
	tkRoot.MustExec("drop user if exists 'dom_user'@'%'")

	tkRoot.MustExec("create security label component classif set ('A','B')")
	tkRoot.MustExec("create security label component dept set ('HR','FIN','OPS')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'AMER' under 'WORLD', 'EU' under 'WORLD', 'US' under 'AMER')")
	tkRoot.MustExec("create security policy dom_policy components classif, dept, region")
	tkRoot.MustExec("create security label dom_policy.user_label component classif 'A' component dept 'HR','FIN' component region 'AMER'")
	tkRoot.MustExec("create security label dom_policy.row_fin_us component classif 'A' component dept 'FIN' component region 'US'")
	tkRoot.MustExec("create security label dom_policy.row_hr_amer component classif 'A' component dept 'HR' component region 'AMER'")
	tkRoot.MustExec("create security label dom_policy.row_ops_eu component classif 'A' component dept 'OPS' component region 'EU'")

	tkRoot.MustExec("create table sales (id int primary key, row_seclabel securitylabel not null) security policy dom_policy")
	tkRoot.MustExec("insert into sales values (1, seclabel('DOM_POLICY','A|FIN|US'))")
	tkRoot.MustExec("insert into sales values (2, seclabel('DOM_POLICY','A|HR|AMER'))")
	tkRoot.MustExec("insert into sales values (3, seclabel('DOM_POLICY','A|OPS|EU'))")

	tkRoot.MustExec("create user 'dom_user'@'%'")
	tkRoot.MustExec("grant select on test_lbac_dom.sales to 'dom_user'@'%'")
	tkRoot.MustExec("grant security label dom_policy.user_label to user 'dom_user'@'%' for read access")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{
		Username:     "dom_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkUser.MustExec("use test_lbac_dom")
	tkUser.MustQuery("select id from sales order by id").Check(testkit.Rows("1", "2"))
}

func TestLBACOverridePreferredWriteLabel(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_override_pref")
	tkRoot.MustExec("use test_lbac_override_pref")
	tkRoot.MustExec("drop table if exists t_dom")
	tkRoot.MustExec("drop table if exists t_fallback")
	tkRoot.MustExec("drop user if exists 'writer'@'%'")

	tkRoot.MustExec("create security label component dept_dom set ('A','Z','OPS')")
	tkRoot.MustExec("create security policy pol_dom components dept_dom override not authorized write security label")
	tkRoot.MustExec("create security label pol_dom.label_subset component dept_dom 'A'")
	tkRoot.MustExec("create security label pol_dom.label_superset component dept_dom 'A','Z'")
	tkRoot.MustExec("create security label pol_dom.label_ops component dept_dom 'OPS'")
	tkRoot.MustExec("create table t_dom (id int primary key, row_seclabel securitylabel not null) security policy pol_dom")

	tkRoot.MustExec("create security label component dept_fallback set ('FIN','HR','OPS')")
	tkRoot.MustExec("create security policy pol_fallback components dept_fallback override not authorized write security label")
	tkRoot.MustExec("create security label pol_fallback.label_fin component dept_fallback 'FIN'")
	tkRoot.MustExec("create security label pol_fallback.label_hr component dept_fallback 'HR'")
	tkRoot.MustExec("create security label pol_fallback.label_ops_fb component dept_fallback 'OPS'")
	tkRoot.MustExec("create table t_fallback (id int primary key, row_seclabel securitylabel not null) security policy pol_fallback")

	tkRoot.MustExec("create user 'writer'@'%'")
	tkRoot.MustExec("grant insert on test_lbac_override_pref.t_dom to 'writer'@'%'")
	tkRoot.MustExec("grant insert on test_lbac_override_pref.t_fallback to 'writer'@'%'")
	tkRoot.MustExec("grant security label pol_dom.label_subset to user 'writer'@'%' for write access")
	tkRoot.MustExec("grant security label pol_dom.label_superset to user 'writer'@'%' for write access")
	tkRoot.MustExec("grant security label pol_fallback.label_fin to user 'writer'@'%' for write access")
	tkRoot.MustExec("grant security label pol_fallback.label_hr to user 'writer'@'%' for write access")

	tkWriter := testkit.NewTestKit(t, store)
	require.NoError(t, tkWriter.Session().Auth(&auth.UserIdentity{
		Username:     "writer",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkWriter.MustExec("use test_lbac_override_pref")
	tkWriter.MustExec("insert into t_dom values (1, seclabel('POL_DOM','OPS'))")
	tkWriter.MustExec("insert into t_fallback values (1, seclabel('POL_FALLBACK','OPS'))")

	tkRoot.MustQuery("select seclabel_to_char('POL_DOM', row_seclabel) from t_dom").Check(testkit.Rows("A,Z"))
	tkRoot.MustQuery("select seclabel_to_char('POL_FALLBACK', row_seclabel) from t_fallback").Check(testkit.Rows("FIN"))
}

func TestLBACRawLabelInsertNormalize(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_raw")
	tkRoot.MustExec("use test_lbac_raw")
	tkRoot.MustExec("drop table if exists t_raw")
	tkRoot.MustExec("drop user if exists 'raw_user'@'%'")

	tkRoot.MustExec("create security label component classif set ('A','B')")
	tkRoot.MustExec("create security policy raw_policy components classif")
	tkRoot.MustExec("create security label raw_policy.label_a component classif 'A'")
	tkRoot.MustExec("create table t_raw (id int primary key, row_seclabel securitylabel not null) security policy raw_policy")

	tkRoot.MustExec("create user 'raw_user'@'%'")
	tkRoot.MustExec("grant select, insert on test_lbac_raw.t_raw to 'raw_user'@'%'")
	tkRoot.MustExec("grant security label raw_policy.label_a to user 'raw_user'@'%' for read access, write access")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{
		Username:     "raw_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkUser.MustExec("use test_lbac_raw")
	tkUser.MustExec("insert into t_raw values (1, 'A')")
	tkUser.MustQuery("select seclabel_to_char('RAW_POLICY', row_seclabel) from t_raw").Check(testkit.Rows("A"))
}

func TestLBACJoinRowFilter(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists test_lbac_join")
	tkRoot.MustExec("use test_lbac_join")
	tkRoot.MustExec("drop table if exists t_sec")
	tkRoot.MustExec("drop table if exists t_plain")
	tkRoot.MustExec("drop user if exists 'join_user'@'%'")

	tkRoot.MustExec("create security label component classif set ('A','B')")
	tkRoot.MustExec("create security policy join_policy components classif")
	tkRoot.MustExec("create security label join_policy.label_a component classif 'A'")
	tkRoot.MustExec("create security label join_policy.label_b component classif 'B'")

	tkRoot.MustExec("create table t_sec (id int primary key, v int, row_seclabel securitylabel not null) security policy join_policy")
	tkRoot.MustExec("create table t_plain (id int primary key, note varchar(10))")
	tkRoot.MustExec("insert into t_sec values (1, 10, seclabel('JOIN_POLICY','A')), (2, 20, seclabel('JOIN_POLICY','B'))")
	tkRoot.MustExec("insert into t_plain values (1, 'ok'), (2, 'ok')")

	tkRoot.MustExec("create user 'join_user'@'%'")
	tkRoot.MustExec("grant select on test_lbac_join.t_sec to 'join_user'@'%'")
	tkRoot.MustExec("grant select on test_lbac_join.t_plain to 'join_user'@'%'")
	tkRoot.MustExec("grant security label join_policy.label_a to user 'join_user'@'%' for read access")

	tkUser := testkit.NewTestKit(t, store)
	require.NoError(t, tkUser.Session().Auth(&auth.UserIdentity{
		Username:     "join_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkUser.MustExec("use test_lbac_join")
	tkUser.MustQuery("select s.id from t_sec s join t_plain p on s.id = p.id order by s.id").Check(testkit.Rows("1"))
}

func TestLBACDocExamples(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists lbac_doc")
	tkRoot.MustExec("use lbac_doc")
	tkRoot.MustExec("drop table if exists sales")
	tkRoot.MustExec("drop table if exists sales_override")
	tkRoot.MustExec("drop user if exists 'alice'@'%'")
	tkRoot.MustExec("drop user if exists 'label_user'@'%'")
	tkRoot.MustExec("drop user if exists 'exempt_user'@'%'")
	tkRoot.MustExec("drop user if exists 'writer'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP','CONF','PUB')")
	tkRoot.MustExec("create security label component dept set ('FIN','HR','ENG')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'AMER' under 'WORLD', 'US' under 'AMER')")

	tkRoot.MustExec("create security policy corp_policy components classif, dept, region")
	tkRoot.MustExec("create security policy corp_policy_override components classif, dept, region override not authorized write security label")

	tkRoot.MustExec("create security label corp_policy.top_fin_us component classif 'TOP' component dept 'FIN' component region 'US'")
	tkRoot.MustExec("create security label corp_policy.conf_hr_us component classif 'CONF' component dept 'HR' component region 'US'")
	tkRoot.MustExec("create security label corp_policy.top_fin_hr_amer component classif 'TOP' component dept 'FIN','HR' component region 'AMER'")
	tkRoot.MustExec("create security label corp_policy.col_top_fin_us component classif 'TOP' component dept 'FIN' component region 'US'")
	tkRoot.MustExec("create security label corp_policy_override.ov_top_fin_us component classif 'TOP' component dept 'FIN' component region 'US'")
	tkRoot.MustExec("create security label corp_policy_override.ov_top_fin_hr_amer component classif 'TOP' component dept 'FIN','HR' component region 'AMER'")
	tkRoot.MustExec("create security label corp_policy_override.ov_conf_hr_us component classif 'CONF' component dept 'HR' component region 'US'")

	tkRoot.MustExec("create table sales (id int primary key, item varchar(16), amount int secured with col_top_fin_us, row_seclabel securitylabel not null) security policy corp_policy")
	tkRoot.MustExec("create table sales_override (id int primary key, row_seclabel securitylabel not null) security policy corp_policy_override")

	tkRoot.MustExec("create user 'alice'@'%'")
	tkRoot.MustExec("grant select on lbac_doc.sales to 'alice'@'%'")
	tkRoot.MustExec("grant security label corp_policy.conf_hr_us to user 'alice'@'%' for read access")
	tkRoot.MustExec("create user 'label_user'@'%'")
	tkRoot.MustExec("grant security label corp_policy.top_fin_us to user 'label_user'@'%' for read access, write access")
	tkRoot.MustExec("revoke security label corp_policy.top_fin_us from user 'label_user'@'%'")

	tkRoot.MustExec("create user 'exempt_user'@'%'")
	tkRoot.MustExec("grant select on lbac_doc.sales to 'exempt_user'@'%'")

	tkRoot.MustExec("create security label component tmp_comp array ('A')")
	tkRoot.MustExec("create security policy tmp_policy components tmp_comp")
	tkRoot.MustExec("create security label tmp_policy.tmp_label component tmp_comp 'A'")
	tkRoot.MustExec("drop security label tmp_policy.tmp_label")
	tkRoot.MustExec("drop security policy tmp_policy")
	tkRoot.MustExec("drop security label component tmp_comp")

	tkRoot.MustQuery("select seclabel_to_char('CORP_POLICY', seclabel('CORP_POLICY','TOP|FIN|US')) as label").Check(testkit.Rows("TOP|FIN|US"))
	tkRoot.MustQuery("select lbac_dominates(seclabel('CORP_POLICY','TOP|FIN,HR|AMER'), seclabel('CORP_POLICY','CONF|HR|US')) as dominates").Check(testkit.Rows("1"))
	tkRoot.MustQuery("select label_accessible('10:M:R20', '30:M,E:R20,R40') as accessible").Check(testkit.Rows("1"))

	tkRoot.MustExec("insert into sales values (1, 'Widget', 100, seclabel('CORP_POLICY','TOP|FIN|US')), (2, 'Gizmo', 200, seclabel('CORP_POLICY','CONF|HR|US'))")

	tkAlice := testkit.NewTestKit(t, store)
	require.NoError(t, tkAlice.Session().Auth(&auth.UserIdentity{
		Username:     "alice",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkAlice.MustExec("use lbac_doc")
	tkAlice.MustQuery("select id, item from sales order by id").Check(testkit.Rows("2 Gizmo"))
	tkAlice.MustGetErrCode("select amount from sales", errno.ErrColumnaccessDenied)

	tkExempt := testkit.NewTestKit(t, store)
	require.NoError(t, tkExempt.Session().Auth(&auth.UserIdentity{
		Username:     "exempt_user",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkExempt.MustExec("use lbac_doc")
	tkExempt.MustGetErrCode("select amount from sales order by id", errno.ErrColumnaccessDenied)

	tkRoot.MustExec("grant exemption on rule all for corp_policy to user 'exempt_user'@'%'")
	tkExempt.MustQuery("select amount from sales order by id").Check(testkit.Rows("100", "200"))
	tkRoot.MustExec("revoke exemption on rule all for corp_policy from user 'exempt_user'@'%'")

	tkRoot.MustExec("create user 'writer'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on lbac_doc.sales to 'writer'@'%'")
	tkRoot.MustExec("grant select, insert on lbac_doc.sales_override to 'writer'@'%'")

	tkWriter := testkit.NewTestKit(t, store)
	require.NoError(t, tkWriter.Session().Auth(&auth.UserIdentity{
		Username:     "writer",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkWriter.MustExec("use lbac_doc")
	tkWriter.MustGetErrCode("insert into sales (id, item, amount, row_seclabel) values (3, 'Gadget', 300, seclabel('CORP_POLICY','TOP|FIN|US'))", errno.ErrColumnaccessDenied)

	tkRoot.MustExec("grant security label corp_policy.col_top_fin_us to user 'writer'@'%' for write access")

	tkWriter.MustExec("insert into sales (id, item, amount, row_seclabel) values (3, 'Gadget', 300, seclabel('CORP_POLICY','TOP|FIN|US'))")
	tkWriter.MustGetDBError("insert into sales (id, item, amount, row_seclabel) values (4, 'Widget', 400, seclabel('CORP_POLICY','CONF|HR|US'))", exeerrors.ErrRowLabelUnAccessible)
	tkWriter.MustExec("update sales set item = 'Widget2' where id = 1")
	tkWriter.MustGetDBError("update sales set item = 'Denied' where id = 2", exeerrors.ErrRowLabelUnAccessible)
	tkWriter.MustGetDBError("delete from sales where id = 2", exeerrors.ErrRowLabelUnAccessible)

	tkRoot.MustExec("grant security label corp_policy_override.ov_top_fin_us to user 'writer'@'%' for read access, write access")
	tkWriter.MustExec("insert into sales_override values (1, seclabel('CORP_POLICY_OVERRIDE','CONF|HR|US'))")
	tkWriter.MustQuery("select seclabel_to_char('CORP_POLICY_OVERRIDE', row_seclabel) from sales_override where id = 1").Check(testkit.Rows("TOP|FIN|US"))

	tkRoot.MustExec("set global tidb_enable_procedure = on")
	tkRoot.MustExec("admin lbac enable")
}

func TestLBACDocComprehensiveScenario(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tkRoot := testkit.NewTestKit(t, store)
	tkRoot.MustExec("set global pkdb_lbac = true")
	tkRoot.MustExec("create database if not exists lbac_company_demo")
	tkRoot.MustExec("use lbac_company_demo")
	tkRoot.MustExec("drop table if exists salary")
	tkRoot.MustExec("drop user if exists 'boss'@'%'")
	tkRoot.MustExec("drop user if exists 'hr'@'%'")
	tkRoot.MustExec("drop user if exists 'emp_b'@'%'")
	tkRoot.MustExec("drop user if exists 'guest'@'%'")

	tkRoot.MustExec("create security label component classif array ('TOP','CONF','PUB')")
	tkRoot.MustExec("create security label component dept set ('EXEC','HR','FIN','ENG')")
	tkRoot.MustExec("create security label component region tree ('WORLD' root, 'AMER' under 'WORLD', 'US' under 'AMER', 'EMEA' under 'WORLD')")

	tkRoot.MustExec("create security policy corp_access components classif, dept, region")

	tkRoot.MustExec("create security label corp_access.exec_top_world component classif 'TOP' component dept 'EXEC' component region 'WORLD'")
	tkRoot.MustExec("create security label corp_access.hr_conf_us component classif 'CONF' component dept 'HR' component region 'US'")
	tkRoot.MustExec("create security label corp_access.fin_conf_amer component classif 'CONF' component dept 'FIN' component region 'AMER'")
	tkRoot.MustExec("create security label corp_access.eng_pub_us component classif 'PUB' component dept 'ENG' component region 'US'")
	tkRoot.MustExec("create security label corp_access.hr_non_exec component classif 'CONF' component dept 'ENG','HR' component region 'AMER'")
	tkRoot.MustExec("create security label corp_access.salary_top_exec component classif 'TOP' component dept 'EXEC' component region 'WORLD'")

	tkRoot.MustExec("create table salary (id int primary key, name varchar(16), dept varchar(8), region varchar(8), salary int secured with salary_top_exec, row_seclabel securitylabel not null) security policy corp_access")
	tkRoot.MustExec("insert into salary values (1, 'boss', 'EXEC', 'WORLD', 100000, seclabel('CORP_ACCESS','TOP|EXEC|WORLD'))")
	tkRoot.MustExec("insert into salary values (2, 'hr', 'HR', 'US', 8000, seclabel('CORP_ACCESS','CONF|HR|US'))")
	tkRoot.MustExec("insert into salary values (3, 'fin', 'FIN', 'AMER', 9000, seclabel('CORP_ACCESS','CONF|FIN|AMER'))")
	tkRoot.MustExec("insert into salary values (4, 'emp_b', 'ENG', 'US', 5000, seclabel('CORP_ACCESS','PUB|ENG|US'))")

	tkRoot.MustExec("create user 'boss'@'%'")
	tkRoot.MustExec("create user 'hr'@'%'")
	tkRoot.MustExec("create user 'emp_b'@'%'")
	tkRoot.MustExec("create user 'guest'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on lbac_company_demo.salary to 'boss'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on lbac_company_demo.salary to 'hr'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on lbac_company_demo.salary to 'emp_b'@'%'")
	tkRoot.MustExec("grant select, insert, update, delete on lbac_company_demo.salary to 'guest'@'%'")

	tkRoot.MustExec("grant exemption on rule all for corp_access to user 'boss'@'%'")
	tkRoot.MustExec("grant security label corp_access.hr_non_exec to user 'hr'@'%' for read access, write access")
	tkRoot.MustExec("grant security label corp_access.eng_pub_us to user 'emp_b'@'%' for read access, write access")

	tkBoss := testkit.NewTestKit(t, store)
	require.NoError(t, tkBoss.Session().Auth(&auth.UserIdentity{
		Username:     "boss",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkBoss.MustExec("use lbac_company_demo")
	tkBoss.MustQuery("select id, salary from salary order by id").Check(testkit.Rows(
		"1 100000",
		"2 8000",
		"3 9000",
		"4 5000",
	))
	tkBoss.MustExec("insert into salary values (5, 'boss2', 'EXEC', 'WORLD', 110000, seclabel('CORP_ACCESS','TOP|EXEC|WORLD'))")

	tkHR := testkit.NewTestKit(t, store)
	require.NoError(t, tkHR.Session().Auth(&auth.UserIdentity{
		Username:     "hr",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkHR.MustExec("use lbac_company_demo")
	tkHR.MustQuery("select id, name from salary order by id").Check(testkit.Rows(
		"2 hr",
		"4 emp_b",
	))
	tkHR.MustGetErrCode("select salary from salary", errno.ErrColumnaccessDenied)
	tkHR.MustGetErrCode("insert into salary values (6, 'eng2', 'ENG', 'US', 5200, seclabel('CORP_ACCESS','PUB|ENG|US'))", errno.ErrColumnaccessDenied)

	tkEmpB := testkit.NewTestKit(t, store)
	require.NoError(t, tkEmpB.Session().Auth(&auth.UserIdentity{
		Username:     "emp_b",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkEmpB.MustExec("use lbac_company_demo")
	tkEmpB.MustExec("update salary set name = 'emp_b2' where id = 4")
	tkEmpB.MustGetDBError("update salary set name = 'hacked' where id = 2", exeerrors.ErrRowLabelUnAccessible)
	tkEmpB.MustGetDBError("delete from salary where id = 2", exeerrors.ErrRowLabelUnAccessible)

	tkGuest := testkit.NewTestKit(t, store)
	require.NoError(t, tkGuest.Session().Auth(&auth.UserIdentity{
		Username:     "guest",
		Hostname:     "127.0.0.1",
		AuthHostname: "%",
	}, nil, nil, nil))
	tkGuest.MustExec("use lbac_company_demo")
	tkGuest.MustQuery("select id from salary order by id").Check(testkit.Rows())

	tkRoot.MustExec("revoke exemption on rule all for corp_access from user 'boss'@'%'")
	tkBoss.MustQuery("select id from salary order by id").Check(testkit.Rows())
	tkBoss.MustGetErrCode("select salary from salary", errno.ErrColumnaccessDenied)
}

func TestLBACShowGrantsAfterHandleReset(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global pkdb_lbac = true")
	tk.MustExec("create security label component sg_classif array ('A','B')")
	tk.MustExec("create security policy sg_policy components sg_classif")
	tk.MustExec("create security label sg_policy.sg_label component sg_classif 'A'")
	tk.MustExec("create user 'sg_user'@'%'")
	tk.MustExec("grant security label sg_policy.sg_label to user 'sg_user'@'%' for read access")
	tk.MustExec("grant exemption on rule all for sg_policy to user 'sg_user'@'%'")

	// Simulate restart: the privilege handle is recreated and needs to reload privilege tables.
	handle := privileges.NewHandle(tk.Session().GetRestrictedSQLExecutor())
	require.NoError(t, handle.Update())
	pm := privileges.NewUserPrivileges(handle, nil)
	privilege.BindPrivilegeManager(tk.Session(), pm)

	grants, err := pm.ShowGrants(tk.Session(), &auth.UserIdentity{Username: "sg_user", Hostname: "%"}, nil)
	require.NoError(t, err)
	expected := []string{
		"GRANT USAGE ON *.* TO 'sg_user'@'%'",
		"GRANT SECURITY LABEL `sg_policy`.`sg_label` TO USER 'sg_user'@'%' FOR READ ACCESS",
		"GRANT EXEMPTION ON RULE ALL FOR `sg_policy` TO USER 'sg_user'@'%'",
	}
	require.True(t, testutil.CompareUnorderedStringSlice(grants, expected), fmt.Sprintf("grants: %v, expected: %v", grants, expected))
}

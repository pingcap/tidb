// Copyright 2015 PingCAP, Inc.
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

package privileges_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testutil"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/sem"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/stretchr/testify/require"
)

func TestCheckDBPrivilege(t *testing.T) {
	store := createStoreAndPrepareDB(t)
	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER 'testcheck'@'localhost';`)
	rootTk.MustExec(`CREATE USER 'testcheck_tmp'@'localhost';`)

	tk := testkit.NewTestKit(t, store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "testcheck", Hostname: "localhost"}, nil, nil, nil))
	pc := privilege.GetPrivilegeManager(tk.Session())
	require.False(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv))

	rootTk.MustExec(`GRANT SELECT ON *.* TO  'testcheck'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))

	rootTk.MustExec(`GRANT Update ON test.* TO  'testcheck'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))

	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "testcheck", Hostname: "localhost"})
	rootTk.MustExec(`GRANT 'testcheck'@'localhost' TO 'testcheck_tmp'@'localhost';`)
	tk2 := testkit.NewTestKit(t, store)
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "testcheck_tmp", Hostname: "localhost"}, nil, nil, nil))
	pc = privilege.GetPrivilegeManager(tk2.Session())
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv))
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))
}

func TestCheckTablePrivilege(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER 'test1'@'localhost';`)
	rootTk.MustExec(`CREATE USER 'test1_tmp'@'localhost';`)

	tk := testkit.NewTestKit(t, store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test1", Hostname: "localhost"}, nil, nil, nil))
	pc := privilege.GetPrivilegeManager(tk.Session())
	require.False(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv))

	rootTk.MustExec(`GRANT SELECT ON *.* TO  'test1'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv))

	rootTk.MustExec(`GRANT Update ON test.* TO  'test1'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv))

	activeRoles = append(activeRoles, &auth.RoleIdentity{Username: "test1", Hostname: "localhost"})
	tk2 := testkit.NewTestKit(t, store)
	rootTk.MustExec(`GRANT 'test1'@'localhost' TO 'test1_tmp'@'localhost';`)
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "test1_tmp", Hostname: "localhost"}, nil, nil, nil))
	pc2 := privilege.GetPrivilegeManager(tk2.Session())
	require.True(t, pc2.RequestVerification(activeRoles, "test", "test", "", mysql.SelectPriv))
	require.True(t, pc2.RequestVerification(activeRoles, "test", "test", "", mysql.UpdatePriv))
	require.False(t, pc2.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv))

	rootTk.MustExec(`GRANT Index ON test.test TO  'test1'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv))
	require.True(t, pc2.RequestVerification(activeRoles, "test", "test", "", mysql.IndexPriv))
}

func TestCheckViewPrivilege(t *testing.T) {
	store := createStoreAndPrepareDB(t)
	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec("use test")
	rootTk.MustExec(`CREATE USER 'vuser'@'localhost';`)
	rootTk.MustExec(`CREATE VIEW v AS SELECT * FROM test;`)

	tk := testkit.NewTestKit(t, store)
	activeRoles := make([]*auth.RoleIdentity, 0)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "vuser", Hostname: "localhost"}, nil, nil, nil))
	pc := privilege.GetPrivilegeManager(tk.Session())
	require.False(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv))

	rootTk.MustExec(`GRANT SELECT ON test.v TO 'vuser'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.ShowViewPriv))

	rootTk.MustExec(`GRANT SHOW VIEW ON test.v TO 'vuser'@'localhost';`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.SelectPriv))
	require.True(t, pc.RequestVerification(activeRoles, "test", "v", "", mysql.ShowViewPriv))
}

func TestCheckPrivilegeWithRoles(t *testing.T) {
	store := createStoreAndPrepareDB(t)
	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER 'test_role'@'localhost';`)
	rootTk.MustExec(`CREATE ROLE r_1, r_2, r_3;`)
	rootTk.MustExec(`GRANT r_1, r_2, r_3 TO 'test_role'@'localhost';`)

	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_role", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`SET ROLE r_1, r_2;`)
	rootTk.MustExec(`SET DEFAULT ROLE r_1 TO 'test_role'@'localhost';`)
	// test bogus role for current user.
	err := tk.ExecToErr(`SET DEFAULT ROLE roledoesnotexist TO 'test_role'@'localhost';`)
	require.True(t, terror.ErrorEqual(err, exeerrors.ErrRoleNotGranted))

	rootTk.MustExec(`GRANT SELECT ON test.* TO r_1;`)
	pc := privilege.GetPrivilegeManager(tk.Session())
	activeRoles := tk.Session().GetSessionVars().ActiveRoles
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.SelectPriv))
	require.False(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))
	rootTk.MustExec(`GRANT UPDATE ON test.* TO r_2;`)
	require.True(t, pc.RequestVerification(activeRoles, "test", "", "", mysql.UpdatePriv))

	tk.MustExec(`SET ROLE NONE;`)
	require.Equal(t, 0, len(tk.Session().GetSessionVars().ActiveRoles))
	tk.MustExec(`SET ROLE DEFAULT;`)
	require.Equal(t, 1, len(tk.Session().GetSessionVars().ActiveRoles))
	tk.MustExec(`SET ROLE ALL;`)
	require.Equal(t, 3, len(tk.Session().GetSessionVars().ActiveRoles))
	tk.MustExec(`SET ROLE ALL EXCEPT r_1, r_2;`)
	require.Equal(t, 1, len(tk.Session().GetSessionVars().ActiveRoles))
}

// TestErrorMessage checks that the identity in error messages matches the mysql.user table one.
// MySQL is inconsistent in its error messages, as some match the loginHost and others the
// identity from mysql.user. In TiDB we now use the identity from mysql.user in error messages
// for consistency.
func TestErrorMessage(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER wildcard`)
	rootTk.MustExec(`CREATE USER specifichost@192.168.1.1`)
	rootTk.MustExec(`GRANT SELECT on test.* TO wildcard`)
	rootTk.MustExec(`GRANT SELECT on test.* TO specifichost@192.168.1.1`)

	wildTk := testkit.NewTestKit(t, store)

	// The session.Auth() func will populate the AuthUsername and AuthHostname fields.
	// We don't have to explicitly specify them.
	require.NoError(t, wildTk.Session().Auth(&auth.UserIdentity{Username: "wildcard", Hostname: "192.168.1.1"}, nil, nil, nil))
	require.EqualError(t, wildTk.ExecToErr("use mysql;"), "[executor:1044]Access denied for user 'wildcard'@'%' to database 'mysql'")

	specificTk := testkit.NewTestKit(t, store)
	require.NoError(t, specificTk.Session().Auth(&auth.UserIdentity{Username: "specifichost", Hostname: "192.168.1.1"}, nil, nil, nil))
	require.EqualError(t, specificTk.ExecToErr("use mysql;"), "[executor:1044]Access denied for user 'specifichost'@'192.168.1.1' to database 'mysql'")
}

func TestDropTablePrivileges(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ctx, _ := tk.Session().(sessionctx.Context)
	tk.MustExec(`CREATE TABLE todrop(c int);`)
	// ctx.GetSessionVars().User = "root@localhost"
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`CREATE USER 'drop'@'localhost';`)
	tk.MustExec(`GRANT Select ON test.todrop TO  'drop'@'localhost';`)

	// ctx.GetSessionVars().User = "drop@localhost"
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "drop", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`SELECT * FROM todrop;`)
	require.Error(t, tk.ExecToErr("DROP TABLE todrop;"))

	tk = testkit.NewTestKit(t, store)
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "localhost"}
	tk.MustExec(`GRANT Drop ON test.todrop TO  'drop'@'localhost';`)

	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ctx.GetSessionVars().User = &auth.UserIdentity{Username: "drop", Hostname: "localhost"}
	tk.MustExec(`DROP TABLE todrop;`)
}

func TestAlterUserStmt(t *testing.T) {
	store := createStoreAndPrepareDB(t)
	tk := testkit.NewTestKit(t, store)

	// high privileged user setting password for other user (passes)
	tk.MustExec("CREATE USER superuser2, nobodyuser2, nobodyuser3, nobodyuser4, nobodyuser5, semuser1, semuser2, semuser3, semuser4")
	tk.MustExec("GRANT ALL ON *.* TO superuser2")
	tk.MustExec("GRANT CREATE USER ON *.* TO nobodyuser2")
	tk.MustExec("GRANT SYSTEM_USER ON *.* TO nobodyuser4")
	tk.MustExec("GRANT UPDATE ON mysql.user TO nobodyuser5, semuser1")
	tk.MustExec("GRANT RESTRICTED_TABLES_ADMIN ON *.* TO semuser1")
	tk.MustExec("GRANT RESTRICTED_USER_ADMIN ON *.* TO semuser1, semuser2, semuser3")
	tk.MustExec("GRANT SYSTEM_USER ON *.* to semuser3") // user is both restricted + has SYSTEM_USER (or super)

	sem.Enable()
	defer sem.Disable()

	// When SEM is enabled, even though we have UPDATE privilege on mysql.user, it explicitly
	// denies writeable privileges to system schemas unless RESTRICTED_TABLES_ADMIN is granted.
	// so the previous method of granting to the table instead of CREATE USER will fail now.
	// This is intentional because SEM plugs directly into the privilege manager to DENY
	// any request for UpdatePriv on mysql.user even if the privilege exists in the internal mysql.user table.

	// UpdatePriv on mysql.user
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "nobodyuser5", Hostname: "localhost"}, nil, nil, nil))
	err := tk.ExecToErr("ALTER USER 'nobodyuser2' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")

	// actual CreateUserPriv
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "nobodyuser2", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec("ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	tk.MustExec("ALTER USER 'nobodyuser3' IDENTIFIED BY ''")

	// UpdatePriv on mysql.user but also has RESTRICTED_TABLES_ADMIN
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "semuser1", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec("ALTER USER 'nobodyuser2' IDENTIFIED BY ''")
	tk.MustExec("ALTER USER 'nobodyuser3' IDENTIFIED BY ''")

	// As it has (RESTRICTED_TABLES_ADMIN + UpdatePriv on mysql.user) + RESTRICTED_USER_ADMIN it can modify other restricted_user_admins like semuser2
	// and it can modify semuser3 because RESTRICTED_USER_ADMIN does not also need SYSTEM_USER
	tk.MustExec("ALTER USER 'semuser1' IDENTIFIED BY ''")
	tk.MustExec("ALTER USER 'semuser2' IDENTIFIED BY ''")
	tk.MustExec("ALTER USER 'semuser3' IDENTIFIED BY ''")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "superuser2", Hostname: "localhost"}, nil, nil, nil))
	err = tk.ExecToErr("ALTER USER 'semuser1' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_USER_ADMIN privilege(s) for this operation")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "semuser4", Hostname: "localhost"}, nil, nil, nil))
	// has restricted_user_admin but not CREATE USER or (update on mysql.user + RESTRICTED_TABLES_ADMIN)
	tk.MustExec("ALTER USER 'semuser4' IDENTIFIED BY ''") // can modify self
	err = tk.ExecToErr("ALTER USER 'nobodyuser3' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	err = tk.ExecToErr("ALTER USER 'semuser1' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
	err = tk.ExecToErr("ALTER USER 'semuser3' IDENTIFIED BY 'newpassword'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
}

func TestShowViewPriv(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`DROP VIEW IF EXISTS test.v`)
	tk.MustExec(`CREATE VIEW test.v AS SELECT 1`)
	tk.MustExec("CREATE USER vnobody, vshowview, vselect, vshowandselect")
	tk.MustExec("GRANT SHOW VIEW ON test.v TO vshowview")
	tk.MustExec("GRANT SELECT ON test.v TO vselect")
	tk.MustExec("GRANT SHOW VIEW, SELECT ON test.v TO vshowandselect")

	tests := []struct {
		userName     string
		showViewErr  string
		showTableErr string
		explainErr   string
		explainRes   string
		descErr      string
		descRes      string
		tablesNum    string
		columnsNum   string
	}{
		{"vnobody",
			"[planner:1142]SELECT command denied to user 'vnobody'@'%' for table 'v'",
			"[planner:1142]SHOW VIEW command denied to user 'vnobody'@'%' for table 'v'",
			"[executor:1142]SELECT command denied to user 'vnobody'@'%' for table 'v'",
			"",
			"[executor:1142]SELECT command denied to user 'vnobody'@'%' for table 'v'",
			"",
			"0",
			"0",
		},
		{"vshowview",
			"[planner:1142]SELECT command denied to user 'vshowview'@'%' for table 'v'",
			"",
			"[executor:1142]SELECT command denied to user 'vshowview'@'%' for table 'v'",
			"",
			"[executor:1142]SELECT command denied to user 'vshowview'@'%' for table 'v'",
			"",
			"1",
			"0",
		},
		{"vselect",
			"[planner:1142]SHOW VIEW command denied to user 'vselect'@'%' for table 'v'",
			"[planner:1142]SHOW VIEW command denied to user 'vselect'@'%' for table 'v'",
			"",
			"1 bigint(1) NO  <nil> ",
			"",
			"1 bigint(1) NO  <nil> ",
			"1",
			"1",
		},
		{"vshowandselect",
			"",
			"",
			"",
			"1 bigint(1) NO  <nil> ",
			"",
			"1 bigint(1) NO  <nil> ",
			"1",
			"1",
		},
	}

	for _, test := range tests {
		tk.Session().Auth(&auth.UserIdentity{Username: test.userName, Hostname: "localhost"}, nil, nil, nil)
		err := tk.ExecToErr("SHOW CREATE VIEW test.v")
		if test.showViewErr != "" {
			require.EqualError(t, err, test.showViewErr, test)
		} else {
			require.NoError(t, err, test)
		}
		err = tk.ExecToErr("SHOW CREATE TABLE test.v")
		if test.showTableErr != "" {
			require.EqualError(t, err, test.showTableErr, test)
		} else {
			require.NoError(t, err, test)
		}
		if test.explainErr != "" {
			err = tk.QueryToErr("explain test.v")
			require.EqualError(t, err, test.explainErr, test)
		} else {
			tk.MustQuery("explain test.v").Check(testkit.Rows(test.explainRes))
		}
		if test.descErr != "" {
			err = tk.QueryToErr("explain test.v")
			require.EqualError(t, err, test.descErr, test)
		} else {
			tk.MustQuery("desc test.v").Check(testkit.Rows(test.descRes))
		}
		tk.MustQuery("select count(*) from information_schema.tables where table_schema='test' and table_name='v'").Check(testkit.Rows(test.tablesNum))
		tk.MustQuery("select count(*) from information_schema.columns where table_schema='test' and table_name='v'").Check(testkit.Rows(test.columnsNum))
	}
}

func TestCheckCertBasedAuth(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER 'r1'@'localhost';`)
	tk.MustExec(`CREATE USER 'r2'@'localhost' require none;`)
	tk.MustExec(`CREATE USER 'r3'@'localhost' require ssl;`)
	tk.MustExec(`CREATE USER 'r4'@'localhost' require x509;`)
	tk.MustExec(`CREATE USER 'r5'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1' cipher 'TLS_AES_128_GCM_SHA256'`)
	tk.MustExec(`CREATE USER 'r6'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	tk.MustExec(`CREATE USER 'r7_issuer_only'@'localhost' require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'`)
	tk.MustExec(`CREATE USER 'r8_subject_only'@'localhost' require subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	tk.MustExec(`CREATE USER 'r9_subject_disorder'@'localhost' require subject '/ST=Beijing/C=ZH/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	tk.MustExec(`CREATE USER 'r10_issuer_disorder'@'localhost' require issuer '/ST=California/C=US/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'`)
	tk.MustExec(`CREATE USER 'r11_cipher_only'@'localhost' require cipher 'TLS_AES_256_GCM_SHA384'`)
	tk.MustExec(`CREATE USER 'r12_old_tidb_user'@'localhost'`)
	tk.MustExec("DELETE FROM mysql.global_priv WHERE `user` = 'r12_old_tidb_user' and `host` = 'localhost'")
	tk.MustExec(`CREATE USER 'r13_broken_user'@'localhost'require issuer '/C=US/ST=California/L=San Francisco/O=PingCAP/OU=TiDB/CN=TiDB admin'
		subject '/C=ZH/ST=Beijing/L=Haidian/O=PingCAP.Inc/OU=TiDB/CN=tester1'`)
	tk.MustExec("UPDATE mysql.global_priv set priv = 'abc' where `user` = 'r13_broken_user' and `host` = 'localhost'")
	tk.MustExec(`CREATE USER 'r14_san_only_pass'@'localhost' require san 'URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me1'`)
	tk.MustExec(`CREATE USER 'r15_san_only_fail'@'localhost' require san 'URI:spiffe://mesh.pingcap.com/ns/timesh/sa/me2'`)

	defer func() {
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
		tk.MustExec("drop user 'r1'@'localhost'")
		tk.MustExec("drop user 'r2'@'localhost'")
		tk.MustExec("drop user 'r3'@'localhost'")
		tk.MustExec("drop user 'r4'@'localhost'")
		tk.MustExec("drop user 'r5'@'localhost'")
		tk.MustExec("drop user 'r6'@'localhost'")
		tk.MustExec("drop user 'r7_issuer_only'@'localhost'")
		tk.MustExec("drop user 'r8_subject_only'@'localhost'")
		tk.MustExec("drop user 'r9_subject_disorder'@'localhost'")
		tk.MustExec("drop user 'r10_issuer_disorder'@'localhost'")
		tk.MustExec("drop user 'r11_cipher_only'@'localhost'")
		tk.MustExec("drop user 'r12_old_tidb_user'@'localhost'")
		tk.MustExec("drop user 'r13_broken_user'@'localhost'")
		tk.MustExec("drop user 'r14_san_only_pass'@'localhost'")
		tk.MustExec("drop user 'r15_san_only_fail'@'localhost'")
	}()

	// test without ssl or ca
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil, nil))

	// test use ssl without ca
	tk.Session().GetSessionVars().TLSConnectionState = &tls.ConnectionState{VerifiedChains: nil}
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil, nil))

	// test use ssl with signed but info wrong ca.
	tk.Session().GetSessionVars().TLSConnectionState = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{}}}}
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil, nil))

	// test a all pass case
	tk.Session().GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "US"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "ZH"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Haidian"),
				util.MockPkixAttribute(util.Organization, "PingCAP.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256, func(cert *x509.Certificate) {
			var url url.URL
			err := url.UnmarshalBinary([]byte("spiffe://mesh.pingcap.com/ns/timesh/sa/me1"))
			require.NoError(t, err)
			cert.URIs = append(cert.URIs, &url)
		})
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r3", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r4", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r14_san_only_pass", Hostname: "localhost"}, nil, nil, nil))

	// test require but give nothing
	tk.Session().GetSessionVars().TLSConnectionState = nil
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil, nil))

	// test mismatch cipher
	tk.Session().GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "US"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "ZH"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Haidian"),
				util.MockPkixAttribute(util.Organization, "PingCAP.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_256_GCM_SHA384)
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r5", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r6", Hostname: "localhost"}, nil, nil, nil)) // not require cipher
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r11_cipher_only", Hostname: "localhost"}, nil, nil, nil))

	// test only subject or only issuer
	tk.Session().GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "US"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "AZ"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Shijingshang"),
				util.MockPkixAttribute(util.Organization, "CAPPing.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester2"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r7_issuer_only", Hostname: "localhost"}, nil, nil, nil))
	tk.Session().GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "AU"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin2"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "ZH"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Haidian"),
				util.MockPkixAttribute(util.Organization, "PingCAP.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r8_subject_only", Hostname: "localhost"}, nil, nil, nil))

	// test disorder issuer or subject
	tk.Session().GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "ZH"),
				util.MockPkixAttribute(util.Province, "Beijing"),
				util.MockPkixAttribute(util.Locality, "Haidian"),
				util.MockPkixAttribute(util.Organization, "PingCAP.Inc"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "tester1"),
			},
		},
		tls.TLS_AES_128_GCM_SHA256)
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r9_subject_disorder", Hostname: "localhost"}, nil, nil, nil))
	tk.Session().GetSessionVars().TLSConnectionState = connectionState(
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{
				util.MockPkixAttribute(util.Country, "US"),
				util.MockPkixAttribute(util.Province, "California"),
				util.MockPkixAttribute(util.Locality, "San Francisco"),
				util.MockPkixAttribute(util.Organization, "PingCAP"),
				util.MockPkixAttribute(util.OrganizationalUnit, "TiDB"),
				util.MockPkixAttribute(util.CommonName, "TiDB admin"),
			},
		},
		pkix.Name{
			Names: []pkix.AttributeTypeAndValue{},
		},
		tls.TLS_AES_128_GCM_SHA256)
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r10_issuer_disorder", Hostname: "localhost"}, nil, nil, nil))

	// test mismatch san
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r15_san_only_fail", Hostname: "localhost"}, nil, nil, nil))

	// test old data and broken data
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "r12_old_tidb_user", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r13_broken_user", Hostname: "localhost"}, nil, nil, nil))
}

func connectionState(issuer, subject pkix.Name, cipher uint16, opt ...func(c *x509.Certificate)) *tls.ConnectionState {
	cert := &x509.Certificate{Issuer: issuer, Subject: subject}
	for _, o := range opt {
		o(cert)
	}
	return &tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{cert}},
		CipherSuite:    cipher,
	}
}

func TestCheckAuthenticate(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER 'u1'@'localhost';`)
	tk.MustExec(`CREATE USER 'u2'@'localhost' identified by 'abc';`)
	tk.MustExec(`CREATE USER 'u3@example.com'@'localhost';`)
	tk.MustExec(`CREATE USER u4@localhost;`)

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil, nil))
	salt := []byte{85, 92, 45, 22, 58, 79, 107, 6, 122, 125, 58, 80, 12, 90, 103, 32, 90, 10, 74, 82}
	authentication := []byte{24, 180, 183, 225, 166, 6, 81, 102, 70, 248, 199, 143, 91, 204, 169, 9, 161, 171, 203, 33}
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, authentication, salt, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u3@example.com", Hostname: "localhost"}, nil, nil, nil))
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil, nil))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("drop user 'u1'@'localhost'")
	tk1.MustExec("drop user 'u2'@'localhost'")
	tk1.MustExec("drop user 'u3@example.com'@'localhost'")
	tk1.MustExec("drop user u4@localhost")

	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u3@example.com", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, nil, nil, nil))

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("create role 'r1'@'localhost'")
	tk2.MustExec("create role 'r2'@'localhost'")
	tk2.MustExec("create role 'r3@example.com'@'localhost'")
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r1", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r2", Hostname: "localhost"}, nil, nil, nil))
	require.Error(t, tk.Session().Auth(&auth.UserIdentity{Username: "r3@example.com", Hostname: "localhost"}, nil, nil, nil))

	tk1.MustExec("drop user 'r1'@'localhost'")
	tk1.MustExec("drop user 'r2'@'localhost'")
	tk1.MustExec("drop user 'r3@example.com'@'localhost'")
}

func TestUseDB(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	// high privileged user
	tk.MustExec("CREATE USER 'usesuper'")
	tk.MustExec("CREATE USER 'usenobody'")
	tk.MustExec("GRANT ALL ON *.* TO 'usesuper'")
	// without grant option
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil, nil))
	require.Error(t, tk.ExecToErr("GRANT SELECT ON mysql.* TO 'usenobody'"))
	// with grant option
	tk = testkit.NewTestKit(t, store)
	// high privileged user
	tk.MustExec("GRANT ALL ON *.* TO 'usesuper' WITH GRANT OPTION")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("use mysql")
	// low privileged user
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "usenobody", Hostname: "localhost", AuthUsername: "usenobody", AuthHostname: "%"}, nil, nil, nil))
	err := tk.ExecToErr("use mysql")
	require.Error(t, err)

	// try again after privilege granted
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("GRANT SELECT ON mysql.* TO 'usenobody'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "usenobody", Hostname: "localhost", AuthUsername: "usenobody", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("use mysql")

	// test `use db` for role.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "usesuper", Hostname: "localhost", AuthUsername: "usesuper", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec(`CREATE DATABASE app_db`)
	tk.MustExec(`CREATE ROLE 'app_developer'`)
	tk.MustExec(`GRANT ALL ON app_db.* TO 'app_developer'`)
	tk.MustExec(`CREATE USER 'dev'@'localhost'`)
	tk.MustExec(`GRANT 'app_developer' TO 'dev'@'localhost'`)
	tk.MustExec(`SET DEFAULT ROLE 'app_developer' TO 'dev'@'localhost'`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "dev", Hostname: "localhost", AuthUsername: "dev", AuthHostname: "localhost"}, nil, nil, nil))
	tk.MustExec("use app_db")
	err = tk.ExecToErr("use mysql")
	require.Error(t, err)
}

func TestConfigPrivilege(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`DROP USER IF EXISTS tcd1`)
	tk.MustExec(`CREATE USER tcd1`)
	tk.MustExec(`GRANT ALL ON *.* to tcd1`)
	tk.MustExec(`DROP USER IF EXISTS tcd2`)
	tk.MustExec(`CREATE USER tcd2`)
	tk.MustExec(`GRANT ALL ON *.* to tcd2`)
	tk.MustExec(`REVOKE CONFIG ON *.* FROM tcd2`)

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "tcd1", Hostname: "localhost", AuthHostname: "tcd1", AuthUsername: "%"}, nil, nil, nil))
	tk.MustExec(`SHOW CONFIG`)
	tk.MustExec(`SET CONFIG TIKV testkey="testval"`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "tcd2", Hostname: "localhost", AuthHostname: "tcd2", AuthUsername: "%"}, nil, nil, nil))
	err := tk.ExecToErr(`SHOW CONFIG`)
	require.Error(t, err)
	require.Regexp(t, "you need \\(at least one of\\) the CONFIG privilege\\(s\\) for this operation$", err.Error())
	err = tk.ExecToErr(`SET CONFIG TIKV testkey="testval"`)
	require.Error(t, err)
	require.Regexp(t, "you need \\(at least one of\\) the CONFIG privilege\\(s\\) for this operation$", err.Error())
	tk.MustExec(`DROP USER tcd1, tcd2`)
}

func TestShowCreateTable(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER tsct1, tsct2`)
	tk.MustExec(`GRANT select ON mysql.* to tsct2`)

	// should fail
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "tsct1", Hostname: "localhost", AuthUsername: "tsct1", AuthHostname: "%"}, nil, nil, nil))
	err := tk.ExecToErr(`SHOW CREATE TABLE mysql.user`)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))

	// should pass
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "tsct2", Hostname: "localhost", AuthUsername: "tsct2", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec(`SHOW CREATE TABLE mysql.user`)
}

func TestAnalyzeTable(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	// high privileged user
	tk.MustExec("CREATE USER 'asuper'")
	tk.MustExec("CREATE USER 'anobody'")
	tk.MustExec("GRANT ALL ON *.* TO 'asuper' WITH GRANT OPTION")
	tk.MustExec("CREATE DATABASE atest")
	tk.MustExec("use atest")
	tk.MustExec("CREATE TABLE t1 (a int)")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("analyze table mysql.user")
	// low privileged user
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil, nil))
	err := tk.ExecToErr("analyze table t1")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
	require.EqualError(t, err, "[planner:1142]INSERT command denied to user 'anobody'@'%' for table 't1'")

	err = tk.ExecToErr("select * from t1")
	require.EqualError(t, err, "[planner:1142]SELECT command denied to user 'anobody'@'%' for table 't1'")

	// try again after SELECT privilege granted
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("GRANT SELECT ON atest.* TO 'anobody'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil, nil))
	err = tk.ExecToErr("analyze table t1")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
	require.EqualError(t, err, "[planner:1142]INSERT command denied to user 'anobody'@'%' for table 't1'")
	// Add INSERT privilege and it should work.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "asuper", Hostname: "localhost", AuthUsername: "asuper", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("GRANT INSERT ON atest.* TO 'anobody'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "anobody", Hostname: "localhost", AuthUsername: "anobody", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("analyze table t1")
}

func TestSystemSchema(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	// This test tests no privilege check for INFORMATION_SCHEMA database.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER 'u1'@'localhost';`)
	tk.MustExec(`GRANT SELECT ON *.* TO 'u1'@'localhost';`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`select * from information_schema.tables`)
	tk.MustExec(`select * from information_schema.key_column_usage`)
	err := tk.ExecToErr("create table information_schema.t(a int)")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "denied to user"))
	err = tk.ExecToErr("drop table information_schema.tables")
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "denied to user"))
	err = tk.ExecToErr("update information_schema.tables set table_name = 'tst' where table_name = 'mysql'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPrivilegeCheckFail))

	// Test metric_schema.
	tk.MustExec(`select * from metrics_schema.tidb_query_duration`)
	err = tk.ExecToErr("drop table metrics_schema.tidb_query_duration")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
	err = tk.ExecToErr("update metrics_schema.tidb_query_duration set instance = 'tst'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPrivilegeCheckFail))
	err = tk.ExecToErr("delete from metrics_schema.tidb_query_duration")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
	err = tk.ExecToErr("create table metric_schema.t(a int)")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))

	tk.MustGetErrCode("create table metrics_schema.t (id int);", errno.ErrTableaccessDenied)
	tk.MustGetErrCode("create table performance_schema.t (id int);", errno.ErrTableaccessDenied)
}

func TestPerformanceSchema(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	// This test tests no privilege check for INFORMATION_SCHEMA database.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER 'u1'@'localhost';`)

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil, nil))
	err := tk.ExecToErr("select * from performance_schema.events_statements_summary_by_digest where schema_name = 'tst'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`GRANT SELECT ON *.* TO 'u1'@'localhost';`)

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec("select * from performance_schema.events_statements_summary_by_digest where schema_name = 'tst'")
	tk.MustExec(`select * from performance_schema.events_statements_summary_by_digest`)
	err = tk.ExecToErr("drop table performance_schema.events_statements_summary_by_digest")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
	err = tk.ExecToErr("update performance_schema.events_statements_summary_by_digest set schema_name = 'tst'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPrivilegeCheckFail))
	err = tk.ExecToErr("delete from performance_schema.events_statements_summary_by_digest")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
	err = tk.ExecToErr("create table performance_schema.t(a int)")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
}

func TestMetricsSchema(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER nobody, msprocess, msselect")
	tk.MustExec("GRANT Process ON *.* TO msprocess")
	tk.MustExec("GRANT SELECT ON metrics_schema.* TO msselect")

	tests := []struct {
		stmt     string
		user     string
		checkErr func(err error)
	}{
		{
			"SHOW CREATE DATABASE metrics_schema",
			"nobody",
			func(err error) {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(err, exeerrors.ErrDBaccessDenied))
			},
		},
		{
			"SHOW CREATE DATABASE metrics_schema",
			"msprocess",
			func(err error) {
				require.NoError(t, err)
			},
		},
		{
			"SHOW CREATE DATABASE metrics_schema",
			"msselect",
			func(err error) {
				require.NoError(t, err)
			},
		},
		{
			"SELECT * FROM metrics_schema.up",
			"nobody",
			func(err error) {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
			},
		},
		{
			"SELECT * FROM metrics_schema.up",
			"msprocess",
			func(err error) {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "pd unavailable"))
			},
		},
		{
			"SELECT * FROM metrics_schema.up",
			"msselect",
			func(err error) {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "pd unavailable"))
			},
		},
		{
			"SELECT * FROM information_schema.metrics_summary",
			"nobody",
			func(err error) {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied))
			},
		},
		{
			"SELECT * FROM information_schema.metrics_summary",
			"msprocess",
			func(err error) {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "pd unavailable"))
			},
		},
		{
			"SELECT * FROM information_schema.metrics_summary_by_label",
			"nobody",
			func(err error) {
				require.Error(t, err)
				require.True(t, terror.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied))
			},
		},
		{
			"SELECT * FROM information_schema.metrics_summary_by_label",
			"msprocess",
			func(err error) {
				require.Error(t, err)
				require.True(t, strings.Contains(err.Error(), "pd unavailable"))
			},
		},
	}

	for _, test := range tests {
		tk.Session().Auth(&auth.UserIdentity{
			Username: test.user,
			Hostname: "localhost",
		}, nil, nil, nil)

		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
		rs, err := tk.Session().ExecuteInternal(ctx, test.stmt)
		if err == nil {
			_, err = session.GetRows4Test(context.Background(), tk.Session(), rs)
		}
		if rs != nil {
			require.NoError(t, rs.Close())
		}
		test.checkErr(err)
	}
}

func TestAdminCommand(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`CREATE USER 'test_admin'@'localhost';`)
	tk.MustExec(`CREATE TABLE t(a int)`)

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_admin", Hostname: "localhost"}, nil, nil, nil))
	err := tk.ExecToErr("ADMIN SHOW DDL JOBS")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPrivilegeCheckFail))
	err = tk.ExecToErr("ADMIN CHECK TABLE t")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPrivilegeCheckFail))

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec("ADMIN SHOW DDL JOBS")
}

func TestLoadDataPrivilege(t *testing.T) {
	// Create file.
	path := "/tmp/load_data_priv.csv"
	fp, err := os.Create(path)
	require.NoError(t, err)
	require.NotNil(t, fp)
	defer func() {
		require.NoError(t, fp.Close())
		require.NoError(t, os.Remove(path))
	}()
	_, err = fp.WriteString("1\n")
	require.NoError(t, err)

	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`CREATE USER 'test_load'@'localhost';`)
	tk.MustExec(`CREATE TABLE t_load(a int)`)

	tk.MustExec(`GRANT SELECT on *.* to 'test_load'@'localhost'`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil, nil))
	err = tk.ExecToErr("LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' INTO TABLE t_load")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`GRANT INSERT on *.* to 'test_load'@'localhost'`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil, nil))
	err = tk.ExecToErr("LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' INTO TABLE t_load")
	require.ErrorContains(t, err, "reader is nil")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk.MustExec(`GRANT INSERT on *.* to 'test_load'@'localhost'`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_load", Hostname: "localhost"}, nil, nil, nil))
	err = tk.ExecToErr("LOAD DATA LOCAL INFILE '/tmp/load_data_priv.csv' REPLACE INTO TABLE t_load")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrTableaccessDenied))
}

func TestAuthHost(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	rootTk := testkit.NewTestKit(t, store)
	tk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER 'test_auth_host'@'%';`)
	rootTk.MustExec(`GRANT ALL ON *.* TO 'test_auth_host'@'%' WITH GRANT OPTION;`)

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_auth_host", Hostname: "192.168.0.10"}, nil, nil, nil))
	tk.MustExec("CREATE USER 'test_auth_host'@'192.168.%';")
	tk.MustExec("GRANT SELECT ON *.* TO 'test_auth_host'@'192.168.%';")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "test_auth_host", Hostname: "192.168.0.10"}, nil, nil, nil))
	err := tk.ExecToErr("create user test_auth_host_a")
	require.Error(t, err)

	rootTk.MustExec("DROP USER 'test_auth_host'@'192.168.%';")
	rootTk.MustExec("DROP USER 'test_auth_host'@'%';")
}

func TestDefaultRoles(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER 'testdefault'@'localhost';`)
	rootTk.MustExec(`CREATE ROLE 'testdefault_r1'@'localhost', 'testdefault_r2'@'localhost';`)
	rootTk.MustExec(`GRANT 'testdefault_r1'@'localhost', 'testdefault_r2'@'localhost' TO 'testdefault'@'localhost';`)

	tk := testkit.NewTestKit(t, store)
	pc := privilege.GetPrivilegeManager(tk.Session())

	ret := pc.GetDefaultRoles("testdefault", "localhost")
	require.Len(t, ret, 0)

	rootTk.MustExec(`SET DEFAULT ROLE ALL TO 'testdefault'@'localhost';`)
	ret = pc.GetDefaultRoles("testdefault", "localhost")
	require.Len(t, ret, 2)

	rootTk.MustExec(`SET DEFAULT ROLE NONE TO 'testdefault'@'localhost';`)
	ret = pc.GetDefaultRoles("testdefault", "localhost")
	require.Len(t, ret, 0)
}

func TestUserTableConsistency(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewAsyncTestKit(t, store)
	ctx := tk.OpenSession(context.Background(), "test")
	tk.MustExec(ctx, "create user superadmin")
	tk.MustExec(ctx, "grant all privileges on *.* to 'superadmin'")

	// GrantPriv is not in AllGlobalPrivs any more, see pingcap/parser#581
	require.Equal(t, len(mysql.Priv2UserCol), len(mysql.AllGlobalPrivs)+1)

	var buf bytes.Buffer
	var res bytes.Buffer
	buf.WriteString("select ")
	i := 0
	for _, priv := range mysql.AllGlobalPrivs {
		if i != 0 {
			buf.WriteString(", ")
			res.WriteString(" ")
		}
		buf.WriteString(mysql.Priv2UserCol[priv])
		res.WriteString("Y")
		i++
	}
	buf.WriteString(" from mysql.user where user = 'superadmin'")
	tk.MustQuery(ctx, buf.String()).Check(testkit.Rows(res.String()))
}

func TestDynamicPrivs(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec("CREATE USER notsuper")
	rootTk.MustExec("CREATE USER otheruser")
	rootTk.MustExec("CREATE ROLE anyrolename")

	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "notsuper", Hostname: "%"}, nil, nil, nil))

	// test SYSTEM_VARIABLES_ADMIN
	err := tk.ExecToErr("SET GLOBAL wait_timeout = 86400")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	rootTk.MustExec("GRANT SYSTEM_VARIABLES_admin ON *.* TO notsuper")
	tk.MustExec("SET GLOBAL wait_timeout = 86400")

	// test ROLE_ADMIN
	err = tk.ExecToErr("GRANT anyrolename TO otheruser")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or ROLE_ADMIN privilege(s) for this operation")
	rootTk.MustExec("GRANT ROLE_ADMIN ON *.* TO notsuper")
	tk.MustExec("GRANT anyrolename TO otheruser")

	// revoke SYSTEM_VARIABLES_ADMIN, confirm it is dropped
	rootTk.MustExec("REVOKE SYSTEM_VARIABLES_AdmIn ON *.* FROM notsuper")
	err = tk.ExecToErr("SET GLOBAL wait_timeout = 86000")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")

	// grant super, confirm that it is also a substitute for SYSTEM_VARIABLES_ADMIN
	rootTk.MustExec("GRANT SUPER ON *.* TO notsuper")
	tk.MustExec("SET GLOBAL wait_timeout = 86400")

	// revoke SUPER, assign SYSTEM_VARIABLES_ADMIN to anyrolename.
	// confirm that a dynamic privilege can be inherited from a role.
	rootTk.MustExec("REVOKE SUPER ON *.* FROM notsuper")
	rootTk.MustExec("GRANT SYSTEM_VARIABLES_AdmIn ON *.* TO anyrolename")
	rootTk.MustExec("GRANT anyrolename TO notsuper")

	// It's not a default role, this should initially fail:
	err = tk.ExecToErr("SET GLOBAL wait_timeout = 86400")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	tk.MustExec("SET ROLE anyrolename")
	tk.MustExec("SET GLOBAL wait_timeout = 87000")
}

func TestDynamicGrantOption(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec("CREATE USER varuser1")
	rootTk.MustExec("CREATE USER varuser2")
	rootTk.MustExec("CREATE USER varuser3")

	rootTk.MustExec("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser1")
	rootTk.MustExec("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser2 WITH GRANT OPTION")

	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "varuser1", Hostname: "%"}, nil, nil, nil))
	err := tk1.ExecToErr("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser3")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the GRANT OPTION privilege(s) for this operation")

	tk2 := testkit.NewTestKit(t, store)
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "varuser2", Hostname: "%"}, nil, nil, nil))
	tk2.MustExec("GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO varuser3")
}

func TestSecurityEnhancedModeRestrictedTables(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	// This provides an integration test of the tests in util/security/security_test.go
	cloudAdminTK := testkit.NewTestKit(t, store)
	cloudAdminTK.MustExec("CREATE USER cloudadmin")
	cloudAdminTK.MustExec("GRANT RESTRICTED_TABLES_ADMIN, SELECT ON *.* to cloudadmin")
	cloudAdminTK.MustExec("GRANT CREATE ON mysql.* to cloudadmin")
	cloudAdminTK.MustExec("CREATE USER uroot")
	cloudAdminTK.MustExec("GRANT ALL ON *.* to uroot WITH GRANT OPTION") // A "MySQL" all powerful user.
	require.NoError(t, cloudAdminTK.Session().Auth(&auth.UserIdentity{Username: "cloudadmin", Hostname: "%"}, nil, nil, nil))
	urootTk := testkit.NewTestKit(t, store)
	require.NoError(t, urootTk.Session().Auth(&auth.UserIdentity{Username: "uroot", Hostname: "%"}, nil, nil, nil))

	sem.Enable()
	defer sem.Disable()

	err := urootTk.ExecToErr("use metrics_schema")
	require.EqualError(t, err, "[executor:1044]Access denied for user 'uroot'@'%' to database 'metrics_schema'")

	err = urootTk.ExecToErr("SELECT * FROM metrics_schema.uptime")
	require.EqualError(t, err, "[planner:1142]SELECT command denied to user 'uroot'@'%' for table 'uptime'")

	err = urootTk.ExecToErr("CREATE TABLE mysql.abcd (a int)")
	require.EqualError(t, err, "[planner:1142]CREATE command denied to user 'uroot'@'%' for table 'abcd'")

	cloudAdminTK.MustExec("USE metrics_schema")
	cloudAdminTK.MustExec("SELECT * FROM metrics_schema.uptime")
	cloudAdminTK.MustExec("CREATE TABLE mysql.abcd (a int)")
}

func TestSecurityEnhancedModeInfoschema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("CREATE USER uroot1, uroot2, uroot3")
	tk.MustExec("GRANT SUPER ON *.* to uroot1 WITH GRANT OPTION") // super not process
	tk.MustExec("GRANT SUPER, PROCESS, RESTRICTED_TABLES_ADMIN ON *.* to uroot2 WITH GRANT OPTION")
	tk.Session().Auth(&auth.UserIdentity{
		Username: "uroot1",
		Hostname: "localhost",
	}, nil, nil, nil)

	sem.Enable()
	defer sem.Disable()

	// Even though we have super, we still can't read protected information from tidb_servers_info, cluster_* tables
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.tidb_servers_info WHERE ip IS NOT NULL`).Check(testkit.Rows("0"))
	err := tk.QueryToErr(`SELECT COUNT(*) FROM information_schema.cluster_info WHERE status_address IS NOT NULL`)
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the PROCESS privilege(s) for this operation")

	// That is unless we have the RESTRICTED_TABLES_ADMIN privilege
	tk.Session().Auth(&auth.UserIdentity{
		Username: "uroot2",
		Hostname: "localhost",
	}, nil, nil, nil)

	// flip from is NOT NULL etc
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.tidb_servers_info WHERE ip IS NULL`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.cluster_info WHERE status_address IS NULL`).Check(testkit.Rows("0"))
}

func TestSecurityEnhancedLocalBackupRestore(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER backuprestore")
	tk.MustExec("GRANT BACKUP_ADMIN,RESTORE_ADMIN ON *.* to backuprestore")
	tk.Session().Auth(&auth.UserIdentity{
		Username: "backuprestore",
		Hostname: "localhost",
	}, nil, nil, nil)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	// Prior to SEM nolocal has permission, the error should be because backup requires tikv
	_, err := tk.Session().ExecuteInternal(ctx, "BACKUP DATABASE * TO 'Local:///tmp/test';")
	require.EqualError(t, err, "BACKUP requires tikv store, not unistore")

	_, err = tk.Session().ExecuteInternal(ctx, "RESTORE DATABASE * FROM 'LOCAl:///tmp/test';")
	require.EqualError(t, err, "RESTORE requires tikv store, not unistore")

	sem.Enable()
	defer sem.Disable()

	// With SEM enabled nolocal does not have permission, but yeslocal does.
	_, err = tk.Session().ExecuteInternal(ctx, "BACKUP DATABASE * TO 'local:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'local storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(ctx, "BACKUP DATABASE * TO 'file:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'local storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(ctx, "BACKUP DATABASE * TO '/tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'local storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(ctx, "RESTORE DATABASE * FROM 'LOCAl:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'local storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(ctx, "BACKUP DATABASE * TO 'hdfs:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'hdfs storage' is not supported when security enhanced mode is enabled")

	_, err = tk.Session().ExecuteInternal(ctx, "RESTORE DATABASE * FROM 'HDFS:///tmp/test';")
	require.EqualError(t, err, "[planner:8132]Feature 'hdfs storage' is not supported when security enhanced mode is enabled")
}

func TestSecurityEnhancedModeSysVars(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER svroot1, svroot2")
	tk.MustExec("GRANT SUPER ON *.* to svroot1 WITH GRANT OPTION")
	tk.MustExec("GRANT SELECT ON performance_schema.* to svroot1")
	tk.MustExec("GRANT SUPER, RESTRICTED_VARIABLES_ADMIN ON *.* to svroot2")
	tk.MustExec("GRANT SELECT ON performance_schema.* to svroot2")

	sem.Enable()
	defer sem.Disable()

	// svroot1 has SUPER but in SEM will be restricted
	tk.Session().Auth(&auth.UserIdentity{
		Username:     "svroot1",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil, nil)

	tk.MustQuery(`SHOW VARIABLES LIKE 'tidb_force_priority'`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.variables_info WHERE variable_name = 'tidb_force_priority'`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM performance_schema.session_variables WHERE variable_name = 'tidb_force_priority'`).Check(testkit.Rows())
	tk.MustQuery(`SHOW GLOBAL VARIABLES LIKE 'tidb_enable_telemetry'`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.variables_info WHERE variable_name = 'tidb_enable_telemetry'`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM performance_schema.session_variables WHERE variable_name = 'tidb_enable_telemetry'`).Check(testkit.Rows())
	tk.MustQuery(`SHOW GLOBAL VARIABLES LIKE 'tidb_top_sql_max_time_series_count'`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.variables_info WHERE variable_name = 'tidb_top_sql_max_time_series_count'`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM performance_schema.session_variables WHERE variable_name = 'tidb_top_sql_max_time_series_count'`).Check(testkit.Rows())
	tk.MustQuery(`SHOW GLOBAL VARIABLES LIKE 'tidb_top_sql_max_meta_count'`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.variables_info WHERE variable_name = 'tidb_top_sql_max_meta_count'`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM performance_schema.session_variables WHERE variable_name = 'tidb_top_sql_max_meta_count'`).Check(testkit.Rows())

	_, err := tk.Exec("SET @@global.tidb_force_priority = 'NO_PRIORITY'")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")
	_, err = tk.Exec("SET GLOBAL tidb_enable_telemetry = OFF")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")
	_, err = tk.Exec("SET GLOBAL tidb_top_sql_max_time_series_count = 100")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")

	_, err = tk.Exec("SELECT @@global.tidb_force_priority")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")
	_, err = tk.Exec("SELECT @@global.tidb_enable_telemetry")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")
	_, err = tk.Exec("SELECT @@global.tidb_top_sql_max_time_series_count")
	require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_VARIABLES_ADMIN privilege(s) for this operation")

	tk.Session().Auth(&auth.UserIdentity{
		Username:     "svroot2",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil, nil)

	tk.MustQuery(`SHOW VARIABLES LIKE 'tidb_force_priority'`).Check(testkit.Rows("tidb_force_priority NO_PRIORITY"))
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.variables_info WHERE variable_name = 'tidb_top_sql_max_meta_count'`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT COUNT(*) FROM performance_schema.session_variables WHERE variable_name = 'tidb_top_sql_max_meta_count'`).Check(testkit.Rows("1"))
	tk.MustQuery(`SHOW GLOBAL VARIABLES LIKE 'tidb_enable_telemetry'`).Check(testkit.Rows("tidb_enable_telemetry OFF"))
	tk.MustQuery(`SELECT COUNT(*) FROM information_schema.variables_info WHERE variable_name = 'tidb_enable_telemetry'`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT COUNT(*) FROM performance_schema.session_variables WHERE variable_name = 'tidb_enable_telemetry'`).Check(testkit.Rows("1"))

	// should not actually make any change.
	tk.MustExec("SET @@global.tidb_force_priority = 'NO_PRIORITY'")
	tk.MustExec("SET GLOBAL tidb_enable_telemetry = ON")

	tk.MustQuery(`SELECT @@global.tidb_force_priority`).Check(testkit.Rows("NO_PRIORITY"))
	tk.MustQuery(`SELECT @@global.tidb_enable_telemetry`).Check(testkit.Rows("0"))

	tk.MustQuery(`SELECT @@hostname`).Check(testkit.Rows(variable.DefHostname))
	sem.Disable()
	if hostname, err := os.Hostname(); err == nil {
		tk.MustQuery(`SELECT @@hostname`).Check(testkit.Rows(hostname))
	}
}

// TestViewDefiner tests that default roles are correctly applied in the algorithm definer
// See: https://github.com/pingcap/tidb/issues/24414
func TestViewDefiner(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE issue24414")
	tk.MustExec("USE issue24414")
	tk.MustExec(`create table table1(
		col1 int,
		col2 int,
		col3 int
		)`)
	tk.MustExec(`insert into table1 values (1,1,1),(2,2,2)`)
	tk.MustExec(`CREATE ROLE 'ACL-mobius-admin'`)
	tk.MustExec(`CREATE USER 'mobius-admin'`)
	tk.MustExec(`CREATE USER 'mobius-admin-no-role'`)
	tk.MustExec(`GRANT Select,Insert,Update,Delete,Create,Drop,Alter,Index,Create View,Show View ON issue24414.* TO 'ACL-mobius-admin'@'%'`)
	tk.MustExec(`GRANT Select,Insert,Update,Delete,Create,Drop,Alter,Index,Create View,Show View ON issue24414.* TO 'mobius-admin-no-role'@'%'`)
	tk.MustExec(`GRANT 'ACL-mobius-admin'@'%' to 'mobius-admin'@'%'`)
	tk.MustExec(`SET DEFAULT ROLE ALL TO 'mobius-admin'`)
	// create tables
	tk.MustExec(`CREATE ALGORITHM = UNDEFINED DEFINER = 'mobius-admin'@'127.0.0.1' SQL SECURITY DEFINER VIEW test_view (col1 , col2 , col3) AS SELECT * from table1`)
	tk.MustExec(`CREATE ALGORITHM = UNDEFINED DEFINER = 'mobius-admin-no-role'@'127.0.0.1' SQL SECURITY DEFINER VIEW test_view2 (col1 , col2 , col3) AS SELECT * from table1`)

	// all examples should work
	tk.MustExec("select * from test_view")
	tk.MustExec("select * from test_view2")
}

func TestSecurityEnhancedModeRestrictedUsers(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER ruroot1, ruroot2, ruroot3")
	tk.MustExec("CREATE ROLE notimportant")
	tk.MustExec("GRANT SUPER, CREATE USER ON *.* to ruroot1 WITH GRANT OPTION")
	tk.MustExec("GRANT SUPER, RESTRICTED_USER_ADMIN,  CREATE USER  ON *.* to ruroot2 WITH GRANT OPTION")
	tk.MustExec("GRANT RESTRICTED_USER_ADMIN ON *.* to ruroot3")
	tk.MustExec("GRANT notimportant TO ruroot2, ruroot3")

	sem.Enable()
	defer sem.Disable()

	stmts := []string{
		"SET PASSWORD for ruroot3 = 'newpassword'",
		"REVOKE notimportant FROM ruroot3",
		"REVOKE SUPER ON *.* FROM ruroot3",
		"DROP USER ruroot3",
	}

	// ruroot1 has SUPER but in SEM will be restricted
	tk.Session().Auth(&auth.UserIdentity{
		Username:     "ruroot1",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil, nil)

	for _, stmt := range stmts {
		_, err := tk.Exec(stmt)
		require.EqualError(t, err, "[planner:1227]Access denied; you need (at least one of) the RESTRICTED_USER_ADMIN privilege(s) for this operation")
	}

	// Switch to ruroot2, it should be permitted
	tk.Session().Auth(&auth.UserIdentity{
		Username:     "ruroot2",
		Hostname:     "localhost",
		AuthUsername: "uroot",
		AuthHostname: "%",
	}, nil, nil, nil)

	for _, stmt := range stmts {
		tk.MustExec(stmt)
	}
}

func TestDynamicPrivsRegistration(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	pm := privilege.GetPrivilegeManager(tk.Session())

	count := len(privileges.GetDynamicPrivileges())

	require.False(t, pm.IsDynamicPrivilege("ACDC_ADMIN"))
	require.Nil(t, privileges.RegisterDynamicPrivilege("ACDC_ADMIN"))
	require.True(t, pm.IsDynamicPrivilege("ACDC_ADMIN"))
	require.Len(t, privileges.GetDynamicPrivileges(), count+1)

	require.False(t, pm.IsDynamicPrivilege("iAmdynamIC"))
	require.Nil(t, privileges.RegisterDynamicPrivilege("IAMdynamic"))
	require.True(t, pm.IsDynamicPrivilege("IAMdyNAMIC"))
	require.Len(t, privileges.GetDynamicPrivileges(), count+2)

	require.Equal(t, "privilege name is longer than 32 characters", privileges.RegisterDynamicPrivilege("THIS_PRIVILEGE_NAME_IS_TOO_LONG_THE_MAX_IS_32_CHARS").Error())
	require.False(t, pm.IsDynamicPrivilege("THIS_PRIVILEGE_NAME_IS_TOO_LONG_THE_MAX_IS_32_CHARS"))

	tk = testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER privassigntest")

	// Check that all privileges registered are assignable to users,
	// including the recently registered ACDC_ADMIN
	for _, priv := range privileges.GetDynamicPrivileges() {
		sqlGrant, err := sqlescape.EscapeSQL("GRANT %n ON *.* TO privassigntest", priv)
		require.NoError(t, err)
		tk.MustExec(sqlGrant)
	}
	// Check that all privileges registered are revokable
	for _, priv := range privileges.GetDynamicPrivileges() {
		sqlGrant, err := sqlescape.EscapeSQL("REVOKE %n ON *.* FROM privassigntest", priv)
		require.NoError(t, err)
		tk.MustExec(sqlGrant)
	}
}

func TestInfoSchemaUserPrivileges(t *testing.T) {
	// Being able to read all privileges from information_schema.user_privileges requires a very specific set of permissions.
	// SUPER user is not sufficient. It was observed in MySQL to require SELECT on mysql.*
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE USER isnobody, isroot, isselectonmysqluser, isselectonmysql")
	tk.MustExec("GRANT SUPER ON *.* TO isroot")
	tk.MustExec("GRANT SELECT ON mysql.user TO isselectonmysqluser")
	tk.MustExec("GRANT SELECT ON mysql.* TO isselectonmysql")

	// First as Nobody
	tk.Session().Auth(&auth.UserIdentity{
		Username: "isnobody",
		Hostname: "localhost",
	}, nil, nil, nil)

	// I can see myself, but I can not see other users
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows("'isnobody'@'%' def USAGE NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows())

	// Basically the same result as as isselectonmysqluser
	tk.Session().Auth(&auth.UserIdentity{
		Username: "isselectonmysqluser",
		Hostname: "localhost",
	}, nil, nil, nil)

	// Now as isselectonmysqluser
	// Tests discovered issue that SELECT on mysql.user is not sufficient. It must be on mysql.*
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows("'isselectonmysqluser'@'%' def USAGE NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysql'@'%'"`).Check(testkit.Rows())

	// Now as root
	tk.Session().Auth(&auth.UserIdentity{
		Username: "isroot",
		Hostname: "localhost",
	}, nil, nil, nil)

	// I can see myself, but I can not see other users
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows("'isroot'@'%' def SUPER NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows())

	// Now as isselectonmysqluser
	tk.Session().Auth(&auth.UserIdentity{
		Username: "isselectonmysql",
		Hostname: "localhost",
	}, nil, nil, nil)

	// Now as isselectonmysqluser
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isnobody'@'%'"`).Check(testkit.Rows("'isnobody'@'%' def USAGE NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isroot'@'%'"`).Check(testkit.Rows("'isroot'@'%' def SUPER NO"))
	tk.MustQuery(`SELECT * FROM information_schema.user_privileges WHERE grantee = "'isselectonmysqluser'@'%'"`).Check(testkit.Rows("'isselectonmysqluser'@'%' def USAGE NO"))
}

// Issues https://github.com/pingcap/tidb/issues/25972 and https://github.com/pingcap/tidb/issues/26451
func TestGrantOptionAndRevoke(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP USER IF EXISTS u1, u2, u3, ruser")
	tk.MustExec("CREATE USER u1, u2, u3, ruser")
	tk.MustExec("GRANT ALL ON *.* TO ruser WITH GRANT OPTION")
	tk.MustExec("GRANT SELECT ON *.* TO u1 WITH GRANT OPTION")
	tk.MustExec("GRANT UPDATE, DELETE on db.* TO u1")

	tk.Session().Auth(&auth.UserIdentity{
		Username: "ruser",
		Hostname: "localhost",
	}, nil, nil, nil)

	tk.MustQuery(`SHOW GRANTS FOR u1`).Check(testkit.Rows("GRANT SELECT ON *.* TO 'u1'@'%' WITH GRANT OPTION", "GRANT UPDATE,DELETE ON `db`.* TO 'u1'@'%'"))

	tk.MustExec("GRANT SELECT ON d1.* to u2")
	tk.MustExec("GRANT SELECT ON d2.* to u2 WITH GRANT OPTION")
	tk.MustExec("GRANT SELECT ON d3.* to u2")
	tk.MustExec("GRANT SELECT ON d4.* to u2")
	tk.MustExec("GRANT SELECT ON d5.* to u2")
	tk.MustQuery(`SHOW GRANTS FOR u2;`).Sort().Check(testkit.Rows(
		"GRANT SELECT ON `d1`.* TO 'u2'@'%'",
		"GRANT SELECT ON `d2`.* TO 'u2'@'%' WITH GRANT OPTION",
		"GRANT SELECT ON `d3`.* TO 'u2'@'%'",
		"GRANT SELECT ON `d4`.* TO 'u2'@'%'",
		"GRANT SELECT ON `d5`.* TO 'u2'@'%'",
		"GRANT USAGE ON *.* TO 'u2'@'%'",
	))

	tk.MustExec("grant all on hchwang.* to u3 with grant option")
	tk.MustQuery(`SHOW GRANTS FOR u3;`).Check(testkit.Rows("GRANT USAGE ON *.* TO 'u3'@'%'", "GRANT ALL PRIVILEGES ON `hchwang`.* TO 'u3'@'%' WITH GRANT OPTION"))
	tk.MustExec("revoke all on hchwang.* from u3")
	tk.MustQuery(`SHOW GRANTS FOR u3;`).Check(testkit.Rows("GRANT USAGE ON *.* TO 'u3'@'%'", "GRANT USAGE ON `hchwang`.* TO 'u3'@'%' WITH GRANT OPTION"))

	// Same again but with column privileges.

	tk.MustExec("DROP TABLE IF EXISTS test.testgrant")
	tk.MustExec("CREATE TABLE test.testgrant (a int)")
	tk.MustExec("grant all on test.testgrant to u3 with grant option")
	tk.MustExec("revoke all on test.testgrant from u3")
	tk.MustQuery(`SHOW GRANTS FOR u3`).Sort().Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'u3'@'%'",
		"GRANT USAGE ON `hchwang`.* TO 'u3'@'%' WITH GRANT OPTION",
		"GRANT USAGE ON `test`.`testgrant` TO 'u3'@'%' WITH GRANT OPTION",
	))
}

func createStoreAndPrepareDB(t *testing.T) kv.Storage {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("create database if not exists test1")
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));`)
	tk.MustExec(fmt.Sprintf("create database if not exists %s;", mysql.SystemDB))
	tk.MustExec(session.CreateUserTable)
	tk.MustExec(session.CreateDBPrivTable)
	tk.MustExec(session.CreateTablePrivTable)
	tk.MustExec(session.CreateColumnPrivTable)
	return store
}

func TestDashboardClientDynamicPriv(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE ROLE dc_r1")
	tk.MustExec("CREATE USER dc_u1")
	tk.MustExec("GRANT dc_r1 TO dc_u1")
	tk.MustExec("SET DEFAULT ROLE dc_r1 TO dc_u1")

	tk1 := testkit.NewTestKit(t, store)
	tk1.Session().Auth(&auth.UserIdentity{
		Username: "dc_u1",
		Hostname: "localhost",
	}, nil, nil, nil)
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
	))
	tk.MustExec("GRANT DASHBOARD_CLIENT ON *.* TO dc_r1")
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
		"GRANT DASHBOARD_CLIENT ON *.* TO 'dc_u1'@'%'",
	))
	tk.MustExec("REVOKE DASHBOARD_CLIENT ON *.* FROM dc_r1")
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
	))
	tk.MustExec("GRANT DASHBOARD_CLIENT ON *.* TO dc_u1")
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
		"GRANT DASHBOARD_CLIENT ON *.* TO 'dc_u1'@'%'",
	))
	tk.MustExec("REVOKE DASHBOARD_CLIENT ON *.* FROM dc_u1")
	tk1.MustQuery("SHOW GRANTS FOR CURRENT_USER()").Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'dc_u1'@'%'",
		"GRANT 'dc_r1'@'%' TO 'dc_u1'@'%'",
	))
}

func TestGrantCreateTmpTables(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE create_tmp_table_db")
	tk.MustExec("USE create_tmp_table_db")
	tk.MustExec("CREATE USER u1")
	tk.MustExec("CREATE TABLE create_tmp_table_table (a int)")
	tk.MustExec("GRANT CREATE TEMPORARY TABLES on create_tmp_table_db.* to u1")
	tk.MustExec("GRANT CREATE TEMPORARY TABLES on *.* to u1")
	tk.MustGetErrCode("GRANT CREATE TEMPORARY TABLES on create_tmp_table_db.tmp to u1", mysql.ErrIllegalGrantForTable)
	// Must set a session user to avoid null pointer dereference
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR u1").Check(testkit.Rows(
		`GRANT CREATE TEMPORARY TABLES ON *.* TO 'u1'@'%'`,
		"GRANT CREATE TEMPORARY TABLES ON `create_tmp_table_db`.* TO 'u1'@'%'"))
	tk.MustExec("DROP USER u1")
	tk.MustExec("DROP DATABASE create_tmp_table_db")
}

func TestCreateTmpTablesPriv(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	createStmt := "CREATE TEMPORARY TABLE test.tmp(id int)"
	dropStmt := "DROP TEMPORARY TABLE IF EXISTS test.tmp"

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(dropStmt)
	tk.MustExec("CREATE TABLE test.t(id int primary key)")
	tk.MustExec("CREATE SEQUENCE test.tmp")
	tk.MustExec("CREATE USER vcreate, vcreate_tmp, vcreate_tmp_all")
	tk.MustExec("GRANT CREATE, USAGE ON test.* TO vcreate")
	tk.MustExec("GRANT CREATE TEMPORARY TABLES, USAGE ON test.* TO vcreate_tmp")
	tk.MustExec("GRANT CREATE TEMPORARY TABLES, USAGE ON *.* TO vcreate_tmp_all")

	tk.Session().Auth(&auth.UserIdentity{Username: "vcreate", Hostname: "localhost"}, nil, nil, nil)
	err := tk.ExecToErr(createStmt)
	require.EqualError(t, err, "[planner:1044]Access denied for user 'vcreate'@'%' to database 'test'")
	tk.Session().Auth(&auth.UserIdentity{Username: "vcreate_tmp", Hostname: "localhost"}, nil, nil, nil)
	tk.MustExec(createStmt)
	tk.MustExec(dropStmt)
	tk.Session().Auth(&auth.UserIdentity{Username: "vcreate_tmp_all", Hostname: "localhost"}, nil, nil, nil)
	// TODO: issue #29280 to be fixed.
	//err = tk.ExecToErr(createStmt)
	//require.EqualError(t, err, "[planner:1044]Access denied for user 'vcreate_tmp_all'@'%' to database 'test'")

	tests := []struct {
		sql     string
		errcode int
	}{
		{
			sql: "create temporary table tmp(id int primary key)",
		},
		{
			sql: "insert into tmp value(1)",
		},
		{
			sql: "insert into tmp value(1) on duplicate key update id=1",
		},
		{
			sql: "replace tmp values(1)",
		},
		{
			sql:     "insert into tmp select * from t",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "update tmp set id=1 where id=1",
		},
		{
			sql:     "update tmp t1, t t2 set t1.id=t2.id where t1.id=t2.id",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "delete from tmp where id=1",
		},
		{
			sql:     "delete t1 from tmp t1 join t t2 where t1.id=t2.id",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "select * from tmp where id=1",
		},
		{
			sql: "select * from tmp where id in (1,2)",
		},
		{
			sql: "select * from tmp",
		},
		{
			sql:     "select * from tmp join t where tmp.id=t.id",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "(select * from tmp) union (select * from t)",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "create temporary table tmp1 like t",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "create table tmp(id int primary key)",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "create table t(id int primary key)",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "analyze table tmp",
		},
		{
			sql:     "analyze table tmp, t",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "show create table tmp",
		},
		// TODO: issue #29281 to be fixed.
		//{
		//	sql: "show create table t",
		//	errcode: mysql.ErrTableaccessDenied,
		//},
		{
			sql:     "drop sequence tmp",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "alter table tmp add column c1 char(10)",
			errcode: errno.ErrUnsupportedDDLOperation,
		},
		{
			sql: "truncate table tmp",
		},
		{
			sql:     "drop temporary table t",
			errcode: mysql.ErrBadTable,
		},
		{
			sql:     "drop table t",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql:     "drop table t, tmp",
			errcode: mysql.ErrTableaccessDenied,
		},
		{
			sql: "drop temporary table tmp",
		},
	}

	tk.Session().Auth(&auth.UserIdentity{Username: "vcreate_tmp", Hostname: "localhost"}, nil, nil, nil)
	tk.MustExec("use test")
	tk.MustExec(dropStmt)
	for _, test := range tests {
		if test.errcode == 0 {
			tk.MustExec(test.sql)
		} else {
			tk.MustGetErrCode(test.sql, test.errcode)
		}
	}
	for i, test := range tests {
		preparedStmt := fmt.Sprintf("prepare stmt%d from '%s'", i, test.sql)
		executeStmt := fmt.Sprintf("execute stmt%d", i)
		if test.errcode == 0 {
			tk.MustExec(preparedStmt)
			tk.MustExec(executeStmt)
		} else {
			_, err := tk.Exec(preparedStmt)
			if err != nil {
				tk.MustGetErrCode(preparedStmt, test.errcode)
			} else {
				tk.MustGetErrCode(executeStmt, test.errcode)
			}
		}
	}
}

func TestGrantEvent(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("CREATE DATABASE event_db")
	tk.MustExec("USE event_db")
	tk.MustExec("CREATE USER u1")
	tk.MustExec("CREATE TABLE event_table (a int)")
	tk.MustExec("GRANT EVENT on event_db.* to u1")
	tk.MustExec("GRANT EVENT on *.* to u1")
	// Must set a session user to avoid null pointer dereferencing
	tk.Session().Auth(&auth.UserIdentity{
		Username: "root",
		Hostname: "localhost",
	}, nil, nil, nil)
	tk.MustQuery("SHOW GRANTS FOR u1").Check(testkit.Rows(
		`GRANT EVENT ON *.* TO 'u1'@'%'`,
		"GRANT EVENT ON `event_db`.* TO 'u1'@'%'"))
	tk.MustExec("DROP USER u1")
	tk.MustExec("DROP DATABASE event_db")
}

func TestSkipGrantTable(t *testing.T) {
	save := config.GetGlobalConfig()
	config.UpdateGlobal(func(c *config.Config) { c.Security.SkipGrantTable = true })
	defer config.StoreGlobalConfig(save)

	store := createStoreAndPrepareDB(t)

	// Issue 29317
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`CREATE USER 'test1'@'%';`)
	tk.MustExec(`GRANT BACKUP_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTORE_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RELOAD ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SHUTDOWN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_VARIABLES_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_STATUS_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_CONNECTION_ADMIN, CONNECTION_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_USER_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_TABLES_ADMIN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT PROCESS ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SHUTDOWN ON *.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SELECT, INSERT, UPDATE, DELETE ON mysql.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SELECT ON information_schema.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT SELECT ON performance_schema.* TO 'test1'@'%';`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON *.* TO root;`)
	tk.MustExec(`revoke SHUTDOWN on *.* from root;`)
	tk.MustExec(`revoke CONFIG on *.* from root;`)

	tk.MustExec(`CREATE USER 'test2'@'%' IDENTIFIED BY '12345';`)
	tk.MustExec(`GRANT PROCESS, CONFIG ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT SHOW DATABASES ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT DASHBOARD_CLIENT ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_VARIABLES_ADMIN ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_STATUS_ADMIN ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_TABLES_ADMIN ON *.* TO 'test2'@'%';`)
	tk.MustExec(`GRANT RESTRICTED_USER_ADMIN ON *.* TO 'test2'@'%';`)
}

func TestIssue29823(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create user u1")
	tk.MustExec("create role r1")
	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("grant select on t1 to r1")
	tk.MustExec("grant r1 to u1")

	tk2 := testkit.NewTestKit(t, store)
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	tk2.MustExec("set role all")
	tk2.MustQuery("select current_role()").Check(testkit.Rows("`r1`@`%`"))
	tk2.MustQuery("select * from test.t1").Check(testkit.Rows())
	tk2.MustQuery("show databases like 'test'").Check(testkit.Rows("test"))
	tk2.MustQuery("show tables from test").Check(testkit.Rows("t1"))

	tk.MustExec("revoke r1 from u1")
	tk2.MustQuery("select current_role()").Check(testkit.Rows("`r1`@`%`"))
	err := tk2.ExecToErr("select * from test.t1")
	require.EqualError(t, err, "[planner:1142]SELECT command denied to user 'u1'@'%' for table 't1'")
	tk2.MustQuery("show databases like 'test'").Check(testkit.Rows())
	err = tk2.QueryToErr("show tables from test")
	require.EqualError(t, err, "[executor:1044]Access denied for user 'u1'@'%' to database 'test'")
}

func TestIssue37488(t *testing.T) {
	store := createStoreAndPrepareDB(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE USER dba_test@'%';")
	tk.MustExec("GRANT SELECT,INSERT,UPDATE,DELETE ON test.* TO 'dba_test'@'%';")
	tk.MustExec("CREATE USER dba_test@'192.168.%';")
	tk.MustExec("GRANT SELECT,INSERT,UPDATE,DELETE,CREATE,DROP,ALTER ON test.* TO 'dba_test'@'192.168.%';")

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "dba_test", Hostname: "192.168.13.15"}, nil, nil, nil))
	tk.MustQuery("select current_user()").Check(testkit.Rows("dba_test@192.168.%"))
	tk.MustExec("DROP TABLE IF EXISTS a;") // succ
}

func TestCheckPasswordExpired(t *testing.T) {
	sessionVars := variable.NewSessionVars(nil)
	sessionVars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	record := privileges.NewUserRecord("%", "root")
	userPrivilege := privileges.NewUserPrivileges(privileges.NewHandle(), nil)

	record.PasswordExpired = true
	_, err := userPrivilege.CheckPasswordExpired(sessionVars, &record)
	require.ErrorContains(t, err, "Your password has expired. To log in you must change it using a client that supports expired passwords")

	record.PasswordExpired = false
	err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), variable.DefaultPasswordLifetime, "2")
	require.NoError(t, err)
	// use default_password_lifetime
	record.PasswordLifeTime = -1
	record.PasswordLastChanged = time.Now().AddDate(0, 0, -2)
	time.Sleep(time.Second)
	_, err = userPrivilege.CheckPasswordExpired(sessionVars, &record)
	require.ErrorContains(t, err, "Your password has expired. To log in you must change it using a client that supports expired passwords")
	record.PasswordLastChanged = time.Now().AddDate(0, 0, -1)
	_, err = userPrivilege.CheckPasswordExpired(sessionVars, &record)
	require.NoError(t, err)

	// never expire
	record.PasswordLifeTime = 0
	record.PasswordLastChanged = time.Now().AddDate(0, 0, -10)
	_, err = userPrivilege.CheckPasswordExpired(sessionVars, &record)
	require.NoError(t, err)

	// expire with the specified time
	record.PasswordLifeTime = 3
	record.PasswordLastChanged = time.Now().AddDate(0, 0, -3)
	time.Sleep(time.Second)
	_, err = userPrivilege.CheckPasswordExpired(sessionVars, &record)
	require.ErrorContains(t, err, "Your password has expired. To log in you must change it using a client that supports expired passwords")
	record.PasswordLastChanged = time.Now().AddDate(0, 0, -2)
	_, err = userPrivilege.CheckPasswordExpired(sessionVars, &record)
	require.NoError(t, err)
}

func TestPasswordExpireWithoutSandBoxMode(t *testing.T) {
	store := createStoreAndPrepareDB(t)
	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER 'testuser'@'localhost' PASSWORD EXPIRE`)

	// PASSWORD EXPIRE
	user := &auth.UserIdentity{Username: "testuser", Hostname: "localhost"}
	tk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(user, nil, nil, nil)
	require.ErrorContains(t, err, "Your password has expired")

	// PASSWORD EXPIRE NEVER
	rootTk.MustExec(`ALTER USER 'testuser'@'localhost' IDENTIFIED BY '' PASSWORD EXPIRE NEVER`)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)

	// PASSWORD EXPIRE INTERVAL N DAY
	rootTk.MustExec(`ALTER USER 'testuser'@'localhost' PASSWORD EXPIRE INTERVAL 2 DAY`)
	rootTk.MustExec(`UPDATE mysql.user SET password_last_changed = (now() - INTERVAL 1 DAY) where user='testuser'`)
	rootTk.MustExec(`FLUSH PRIVILEGES`)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)
	rootTk.MustExec(`UPDATE mysql.user SET password_last_changed = (now() - INTERVAL 2 DAY) where user='testuser'`)
	rootTk.MustExec(`FLUSH PRIVILEGES`)
	time.Sleep(2 * time.Second)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.ErrorContains(t, err, "Your password has expired")

	// PASSWORD EXPIRE DEFAULT
	rootTk.MustExec(`ALTER USER 'testuser'@'localhost' PASSWORD EXPIRE DEFAULT`)
	rootTk.MustExec(`SET GLOBAL default_password_lifetime = 2`)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.ErrorContains(t, err, "Your password has expired")
	rootTk.MustExec(`SET GLOBAL default_password_lifetime = 3`)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)
}

func TestPasswordExpireWithSandBoxMode(t *testing.T) {
	store := createStoreAndPrepareDB(t)
	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER 'testuser'@'localhost' PASSWORD EXPIRE`)
	variable.IsSandBoxModeEnabled.Store(true)

	// PASSWORD EXPIRE
	user := &auth.UserIdentity{Username: "testuser", Hostname: "localhost"}
	tk := testkit.NewTestKit(t, store)
	err := tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, tk.Session().InSandBoxMode())
	tk.Session().DisableSandBoxMode()

	// PASSWORD EXPIRE NEVER
	rootTk.MustExec(`ALTER USER 'testuser'@'localhost' IDENTIFIED BY '' PASSWORD EXPIRE NEVER`)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)
	require.False(t, tk.Session().InSandBoxMode())

	// PASSWORD EXPIRE INTERVAL N DAY
	rootTk.MustExec(`ALTER USER 'testuser'@'localhost' PASSWORD EXPIRE INTERVAL 2 DAY`)
	rootTk.MustExec(`UPDATE mysql.user SET password_last_changed = (now() - INTERVAL 1 DAY) where user='testuser'`)
	rootTk.MustExec(`FLUSH PRIVILEGES`)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)
	require.False(t, tk.Session().InSandBoxMode())
	rootTk.MustExec(`UPDATE mysql.user SET password_last_changed = (now() - INTERVAL 2 DAY) where user='testuser'`)
	rootTk.MustExec(`FLUSH PRIVILEGES`)
	time.Sleep(2 * time.Second)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, tk.Session().InSandBoxMode())
	tk.Session().DisableSandBoxMode()

	// PASSWORD EXPIRE DEFAULT
	rootTk.MustExec(`ALTER USER 'testuser'@'localhost' PASSWORD EXPIRE DEFAULT`)
	rootTk.MustExec(`SET GLOBAL default_password_lifetime = 2`)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, tk.Session().InSandBoxMode())
	tk.Session().DisableSandBoxMode()
	rootTk.MustExec(`SET GLOBAL default_password_lifetime = 3`)
	err = tk.Session().Auth(user, nil, nil, nil)
	require.NoError(t, err)
	require.False(t, tk.Session().InSandBoxMode())
}

func TestVerificationInfoWithSessionTokenPlugin(t *testing.T) {
	// prepare signing certs
	tempDir := t.TempDir()
	certPath := filepath.Join(tempDir, "test1_cert.pem")
	keyPath := filepath.Join(tempDir, "test1_key.pem")
	err := util.CreateCertificates(certPath, keyPath, 4096, x509.RSA, x509.UnknownSignatureAlgorithm)
	require.NoError(t, err)
	sessionstates.SetKeyPath(keyPath)
	sessionstates.SetCertPath(certPath)

	// prepare user
	store := createStoreAndPrepareDB(t)
	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec(`CREATE USER 'testuser'@'localhost' PASSWORD EXPIRE`)
	// prepare session token
	token, err := sessionstates.CreateSessionToken("testuser")
	require.NoError(t, err)
	tokenBytes, err := json.Marshal(token)
	require.NoError(t, err)

	// Test password expiration without sandbox.
	user := &auth.UserIdentity{Username: "testuser", Hostname: "localhost", AuthPlugin: mysql.AuthTiDBSessionToken}
	tk := testkit.NewTestKit(t, store)
	err = tk.Session().Auth(user, tokenBytes, nil, nil)
	require.NoError(t, err)
	require.False(t, tk.Session().InSandBoxMode())

	// Test password expiration with sandbox.
	variable.IsSandBoxModeEnabled.Store(true)
	err = tk.Session().Auth(user, tokenBytes, nil, nil)
	require.NoError(t, err)
	require.False(t, tk.Session().InSandBoxMode())

	// Enable resource group.
	variable.EnableResourceControl.Store(true)
	err = tk.Session().Auth(user, tokenBytes, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "default", tk.Session().GetSessionVars().ResourceGroupName)

	// Non-default resource group.
	rootTk.MustExec("CREATE RESOURCE GROUP rg1 RU_PER_SEC = 999")
	rootTk.MustExec(`ALTER USER 'testuser'@'localhost' RESOURCE GROUP rg1`)
	err = tk.Session().Auth(user, tokenBytes, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "rg1", tk.Session().GetSessionVars().ResourceGroupName)

	// Wrong token
	err = tk.Session().Auth(user, nil, nil, nil)
	require.ErrorContains(t, err, "Access denied")
}

func TestNilHandleInConnectionVerification(t *testing.T) {
	config.GetGlobalConfig().Security.SkipGrantTable = true
	privileges.SkipWithGrant = true
	defer func() {
		config.GetGlobalConfig().Security.SkipGrantTable = false
		privileges.SkipWithGrant = false
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: `%`}, nil, nil, nil))
}

func testShowGrantsSQLMode(t *testing.T, tk *testkit.TestKit, expected []string) {
	pc := privilege.GetPrivilegeManager(tk.Session())
	gs, err := pc.ShowGrants(tk.Session(), &auth.UserIdentity{Username: "show_sql_mode", Hostname: "localhost"}, nil)
	require.NoError(t, err)
	require.Len(t, gs, 2)
	require.True(t, testutil.CompareUnorderedStringSlice(gs, expected), fmt.Sprintf("gs: %v, expected: %v", gs, expected))
}

func TestShowGrantsSQLMode(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	tk := testkit.NewTestKit(t, store)
	ctx, _ := tk.Session().(sessionctx.Context)
	tk.MustExec(`CREATE USER 'show_sql_mode'@'localhost' identified by '123';`)
	tk.MustExec(`GRANT Select ON test.* TO 'show_sql_mode'@'localhost';`)

	testShowGrantsSQLMode(t, tk, []string{
		"GRANT USAGE ON *.* TO 'show_sql_mode'@'localhost'",
		"GRANT SELECT ON `test`.* TO 'show_sql_mode'@'localhost'",
	})

	ctx.GetSessionVars().SQLMode = mysql.SetSQLMode(ctx.GetSessionVars().SQLMode, mysql.ModeANSIQuotes)
	testShowGrantsSQLMode(t, tk, []string{
		"GRANT USAGE ON *.* TO 'show_sql_mode'@'localhost'",
		"GRANT SELECT ON \"test\".* TO 'show_sql_mode'@'localhost'",
	})
}

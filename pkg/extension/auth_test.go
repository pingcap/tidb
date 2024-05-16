package extension_test

import (
	"crypto/sha1"
	"crypto/tls"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege/conn"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"testing"
)

type MockAuthPlugin struct {
	mock.Mock
}

func (p *MockAuthPlugin) Name() string {
	return p.Called().String(0)
}

func (p *MockAuthPlugin) AuthenticateUser(authUser string, pwd string, auth, salt []byte, connState *tls.ConnectionState, authConn conn.AuthConn) error {
	return p.Called(authUser, pwd, auth, salt, connState, authConn).Error(0)
}

func (p *MockAuthPlugin) ValidateAuthString(hash string) bool {
	return p.Called(hash).Bool(0)
}

func (p *MockAuthPlugin) GenerateAuthString(password string) (string, bool) {
	args := p.Called(password)
	return args.String(0), args.Bool(1)
}

func (p *MockAuthPlugin) VerifyDynamicPrivilege(activeRoles []*auth.RoleIdentity, user, host, privName string, withGrant bool, connState *tls.ConnectionState) bool {
	return p.Called(activeRoles, user, host, privName, withGrant, connState).Bool(0)
}

func (p *MockAuthPlugin) VerifyPrivilege(roles []*auth.RoleIdentity, user, host, db, table, column string, priv mysql.PrivilegeType, connState *tls.ConnectionState) bool {
	return p.Called(roles, user, host, db, table, column, priv, connState).Bool(0)
}

func sha1Password(s string) []byte {
	crypt := sha1.New()
	crypt.Write([]byte(s))
	hashStage1 := crypt.Sum(nil)
	crypt.Reset()
	crypt.Write(hashStage1)
	hashStage2 := crypt.Sum(nil)
	crypt.Reset()
	crypt.Write(hashStage2)
	hashStage3 := crypt.Sum(nil)
	for i := range hashStage3 {
		hashStage3[i] ^= hashStage1[i]
	}
	return hashStage3
}

func TestAuthPlugin(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	authChecks := map[string]*extension.AuthPlugin{}
	p := new(MockAuthPlugin)
	p.On("Name").Return("authentication_test_plugin")
	p.On("AuthenticateUser", "u2", "encodedpassword", []byte("1"), mock.Anything, mock.Anything, mock.Anything).Return(nil)
	p.On("AuthenticateUser", "u2", "encodedpassword", []byte("2"), mock.Anything, mock.Anything, mock.Anything).Return(errors.New("authentication failed"))
	p.On("AuthenticateUser", "u2", "anotherencodedpassword", []byte("1"), mock.Anything, mock.Anything, mock.Anything).Return(nil)
	p.On("AuthenticateUser", "u2", "yetanotherencodedpassword", []byte("1"), mock.Anything, mock.Anything, mock.Anything).Return(nil)
	p.On("AuthenticateUser", "u2", "yetanotherencodedpassword2", []byte("1"), mock.Anything, mock.Anything, mock.Anything).Return(nil)
	p.On("ValidateAuthString", mock.Anything).Return(true)
	p.On("GenerateAuthString", "rawpassword").Return("encodedpassword", true)
	p.On("GenerateAuthString", "anotherrawpassword").Return("anotherencodedpassword", true)
	p.On("GenerateAuthString", "yetanotherrawpassword").Return("yetanotherencodedpassword", true)
	p.On("GenerateAuthString", "yetanotherrawpassword2").Return("yetanotherencodedpassword2", true)
	p.On("VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.AllPrivMask, mock.Anything).Return(true)
	p.On("VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.SelectPriv, mock.Anything).Return(true)
	p.On("VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.InsertPriv, mock.Anything).Return(false)
	p.On("VerifyDynamicPrivilege", mock.Anything, "u2", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false)

	authChecks[p.Name()] = &extension.AuthPlugin{
		Name:                     p.Name(),
		AuthenticateUser:         p.AuthenticateUser,
		ValidateAuthString:       p.ValidateAuthString,
		GenerateAuthString:       p.GenerateAuthString,
		VerifyPrivilege:          p.VerifyPrivilege,
		VerifyDynamicPrivilege:   p.VerifyDynamicPrivilege,
		RequiredClientSidePlugin: mysql.AuthNativePassword,
	}

	require.NoError(t, extension.Register(
		"extension_authentication_plugin",
		extension.WithCustomAuthPlugins(authChecks),
		extension.WithCustomSysVariables([]*variable.SysVar{
			{
				Scope:          variable.ScopeGlobal,
				Name:           "extension_authentication_plugin",
				Value:          mysql.AuthNativePassword,
				Type:           variable.TypeEnum,
				PossibleValues: maps.Keys(authChecks),
			},
		}),
		extension.WithBootstrap(func(_ extension.BootstrapContext) error {
			return nil
		}),
	))
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetAuthPlugins(authChecks)
	tk.MustExec("use test")

	// Create user with an invalid plugin should not work.
	tk.MustContainErrMsg("create user 'u2'@'localhost' identified with 'bad_plugin' by 'rawpassword'", "[executor:1524]Plugin 'bad_plugin' is not loaded")

	// Create user with a valid plugin should work.
	tk.MustExec("create user 'u2'@'localhost' identified with 'authentication_test_plugin' as 'rawpassword'")
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='u2' and host='localhost'`).Check(testkit.Rows("u2 authentication_test_plugin"))
	p.AssertCalled(t, "GenerateAuthString", "rawpassword")

	// Alter user with an invalid plugin should not work.
	tk.MustContainErrMsg("alter user 'u2'@'localhost' identified with 'bad_plugin' by 'rawpassword'", "[executor:1524]Plugin 'bad_plugin' is not loaded")

	tk1 := testkit.NewTestKit(t, store)
	tk1.Session().SetAuthPlugins(authChecks)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk1.MustExec("use test")

	// Authentication tests.
	tk2 := testkit.NewTestKit(t, store)
	tk2.Session().SetAuthPlugins(authChecks)
	// Login using wrong password should fail
	require.EqualError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("2"), nil, nil), "[privilege:1045]Access denied for user 'u2'@'localhost' (using password: YES)")
	// Corret password should pass
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should authenticate using plugin impl.
	p.AssertNumberOfCalls(t, "AuthenticateUser", 2)
	p.AssertCalled(t, "ValidateAuthString", "encodedpassword")

	// Change password should work using ALTER USER statement.
	tk.MustExec("alter user 'u2'@'localhost' identified with 'authentication_test_plugin' by 'anotherrawpassword'")
	p.AssertCalled(t, "GenerateAuthString", "anotherrawpassword")
	// Corret password should pass
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should authenticate using plugin impl.
	p.AssertNumberOfCalls(t, "AuthenticateUser", 3)
	p.AssertCalled(t, "ValidateAuthString", "anotherencodedpassword")

	// Change password using SET PASSWORD statement should work using root user.
	tk.MustExec("set password for 'u2'@'localhost'='yetanotherrawpassword'")
	p.AssertCalled(t, "GenerateAuthString", "yetanotherrawpassword")
	// Corret password should pass
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should authenticate using plugin impl.
	p.AssertNumberOfCalls(t, "AuthenticateUser", 4)
	p.AssertCalled(t, "ValidateAuthString", "yetanotherencodedpassword")

	// Change password using SET PASSWORD statement should work using the user itself.
	tk2.MustExec("set password='yetanotherrawpassword2'")
	p.AssertCalled(t, "GenerateAuthString", "yetanotherrawpassword2")
	// Corret password should pass
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should authenticate using plugin impl.
	p.AssertNumberOfCalls(t, "AuthenticateUser", 5)
	p.AssertCalled(t, "ValidateAuthString", "yetanotherencodedpassword2")

	// Authorization tests.
	tk1.MustExec("create table t1(id int primary key, v int)")
	tk.MustExec("grant select, insert on test.t1 TO u2@localhost")
	tk2.MustExec("use test")
	tk1.MustExec("insert into t1 values (1, 10), (2, 20)")

	tk1.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	tk1.MustQuery("select * from t1").Check(testkit.Rows("1 10", "2 20"))
	// First user is not a plugin user, so it should not verify privilege using plugin impl.
	p.AssertNotCalled(t, "VerifyPrivilege")

	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 10", "2 20"))
	require.EqualError(t, tk2.ExecToErr("insert into t1 values (3, 30)"), "[planner:1142]INSERT command denied to user 'u2'@'localhost' for table 't1'")
	// Should verify privilege using plugin impl.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 3)
	p.AssertCalled(t, "VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.SelectPriv, mock.Anything)
	p.AssertCalled(t, "VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.InsertPriv, mock.Anything)

	require.EqualError(t, tk2.ExecToErr("delete from t1 where id=1"), "[planner:1142]DELETE command denied to user 'u2'@'localhost' for table 't1'")
	// Should not verify delete privilege using plugin impl since privilege is not granted in mysql.
	p.AssertNotCalled(t, "VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.DeletePriv, mock.Anything)

	require.EqualError(t, tk2.ExecToErr("SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER'"), "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	p.AssertNotCalled(t, "VerifyDynamicPrivilege", mock.Anything, "u2", mock.Anything, "SYSTEM_VARIABLES_ADMIN", false, mock.Anything)
	tk.MustExec("GRANT SYSTEM_VARIABLES_ADMIN on *.* TO u2@localhost")
	require.EqualError(t, tk2.ExecToErr("SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER'"), "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	// Should verify dynamic privilege using plugin impl.
	p.AssertCalled(t, "VerifyDynamicPrivilege", mock.Anything, "u2", mock.Anything, "SYSTEM_VARIABLES_ADMIN", false, mock.Anything)
}

func TestAuthPluginSwitchPlugins(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	authChecks := map[string]*extension.AuthPlugin{}
	p := new(MockAuthPlugin)
	p.On("Name").Return("authentication_test_plugin")
	p.On("AuthenticateUser", "u2", "encodedpassword", []byte("1"), mock.Anything, mock.Anything, mock.Anything).Return(nil)
	p.On("AuthenticateUser", "u2", "encodedpassword", []byte("2"), mock.Anything, mock.Anything, mock.Anything).Return(errors.New("authentication failed"))
	p.On("AuthenticateUser", "u2", "anotherencodedpassword", []byte("1"), mock.Anything, mock.Anything, mock.Anything).Return(nil)
	p.On("ValidateAuthString", mock.Anything).Return(true)
	p.On("GenerateAuthString", "rawpassword").Return("encodedpassword", true)
	p.On("GenerateAuthString", "anotherrawpassword").Return("anotherencodedpassword", true)
	p.On("VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.AllPrivMask, mock.Anything).Return(true)
	p.On("VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.SelectPriv, mock.Anything).Return(true)
	p.On("VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.InsertPriv, mock.Anything).Return(false)
	p.On("VerifyDynamicPrivilege", mock.Anything, "u2", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false)

	authChecks[p.Name()] = &extension.AuthPlugin{
		Name:                     p.Name(),
		AuthenticateUser:         p.AuthenticateUser,
		ValidateAuthString:       p.ValidateAuthString,
		GenerateAuthString:       p.GenerateAuthString,
		VerifyPrivilege:          p.VerifyPrivilege,
		VerifyDynamicPrivilege:   p.VerifyDynamicPrivilege,
		RequiredClientSidePlugin: mysql.AuthNativePassword,
	}

	require.NoError(t, extension.Register(
		"extension_authentication_plugin",
		extension.WithCustomAuthPlugins(authChecks),
		extension.WithCustomSysVariables([]*variable.SysVar{
			{
				Scope:          variable.ScopeGlobal,
				Name:           "extension_authentication_plugin",
				Value:          mysql.AuthNativePassword,
				Type:           variable.TypeEnum,
				PossibleValues: maps.Keys(authChecks),
			},
		}),
		extension.WithBootstrap(func(_ extension.BootstrapContext) error {
			return nil
		}),
	))
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetAuthPlugins(authChecks)
	tk.MustExec("use test")

	// Create user with a valid plugin should work.
	tk.MustExec("create user 'u2'@'localhost' identified with 'authentication_test_plugin' as 'rawpassword'")
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='u2' and host='localhost'`).Check(testkit.Rows("u2 authentication_test_plugin"))
	p.AssertCalled(t, "GenerateAuthString", "rawpassword")

	tk1 := testkit.NewTestKit(t, store)
	tk1.Session().SetAuthPlugins(authChecks)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk1.MustExec("use test")

	// Authentication tests.
	tk2 := testkit.NewTestKit(t, store)
	tk2.Session().SetAuthPlugins(authChecks)
	// Corret password should pass
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should authenticate using plugin impl.
	p.AssertNumberOfCalls(t, "AuthenticateUser", 1)
	p.AssertCalled(t, "ValidateAuthString", "encodedpassword")

	// Authorization tests.
	tk1.MustExec("create table t1(id int primary key, v int)")
	tk.MustExec("grant select, insert on test.t1 TO u2@localhost")
	tk2.MustExec("use test")
	tk1.MustExec("insert into t1 values (1, 10), (2, 20)")

	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 10", "2 20"))
	require.EqualError(t, tk2.ExecToErr("insert into t1 values (3, 30)"), "[planner:1142]INSERT command denied to user 'u2'@'localhost' for table 't1'")
	// Should verify privilege using plugin impl.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 3)
	p.AssertCalled(t, "VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.SelectPriv, mock.Anything)
	p.AssertCalled(t, "VerifyPrivilege", mock.Anything, "u2", "localhost", "test", "t1", mock.Anything, mysql.InsertPriv, mock.Anything)

	tk.MustExec("alter user 'u2'@'localhost' identified with 'mysql_native_password' by '123'")
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='u2' and host='localhost'`).Check(testkit.Rows("u2 mysql_native_password"))
	// Existing session should still use plugin.
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 10", "2 20"))
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 5)

	// New session should not use plugin.
	tk2.RefreshSession()
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, sha1Password("123"), nil, nil))
	tk2.MustExec("use test")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	tk2.MustExec("insert into t1 values (3, 30), (4, 40)")
	tk2.MustQuery("select * from t1 where id=3").Check(testkit.Rows("3 30"))
	// Should not verify privilege using plugin impl now that the user is not using the plugin.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 5)

	// Switch back to plugin should work.
	tk.MustExec("alter user 'u2'@'localhost' identified with 'authentication_test_plugin' by 'rawpassword'")
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='u2' and host='localhost'`).Check(testkit.Rows("u2 authentication_test_plugin"))
	tk2.RefreshSession()
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	tk2.MustExec("use test")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	require.EqualError(t, tk2.ExecToErr("insert into t1 values (5, 50)"), "[planner:1142]INSERT command denied to user 'u2'@'localhost' for table 't1'")
	// Now it should verify privilege using plugin impl.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 7)
}

func TestCreateUserWhenGrant(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	authChecks := map[string]*extension.AuthPlugin{}
	p := new(MockAuthPlugin)
	p.On("Name").Return("authentication_test_plugin")
	p.On("ValidateAuthString", mock.Anything).Return(true)
	p.On("GenerateAuthString", "xxx").Return("encodedpassword", true)

	authChecks[p.Name()] = &extension.AuthPlugin{
		Name:                     p.Name(),
		AuthenticateUser:         p.AuthenticateUser,
		ValidateAuthString:       p.ValidateAuthString,
		GenerateAuthString:       p.GenerateAuthString,
		RequiredClientSidePlugin: mysql.AuthNativePassword,
	}

	require.NoError(t, extension.Register(
		"extension_authentication_plugin",
		extension.WithCustomAuthPlugins(authChecks),
		extension.WithCustomSysVariables([]*variable.SysVar{
			{
				Scope:          variable.ScopeGlobal,
				Name:           "extension_authentication_plugin",
				Value:          mysql.AuthNativePassword,
				Type:           variable.TypeEnum,
				PossibleValues: maps.Keys(authChecks),
			},
		}),
		extension.WithBootstrap(func(_ extension.BootstrapContext) error {
			return nil
		}),
	))
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetAuthPlugins(authChecks)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
	// This only applies to sql_mode:NO_AUTO_CREATE_USER off
	tk.MustExec(`SET SQL_MODE=''`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON *.* to 'test'@'%' IDENTIFIED WITH 'authentication_test_plugin' AS 'xxx'`)
	// Make sure user is created automatically when grant to a non-exists one.
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='test' and host='%'`).Check(
		testkit.Rows("test authentication_test_plugin"),
	)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
	// Grant without a password.
	tk.MustExec(`GRANT ALL PRIVILEGES ON *.* to 'test'@'%'`)
	// Make sure user is created automatically when grant to a non-exists one.
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='test' and host='%'`).Check(
		testkit.Rows("test mysql_native_password"),
	)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
}

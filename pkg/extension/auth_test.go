// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extension_test

import (
	"crypto/sha1"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockAuthPlugin struct {
	mock.Mock
}

func (p *MockAuthPlugin) Name() string {
	return p.Called().String(0)
}

func (p *MockAuthPlugin) AuthenticateUser(ctx extension.AuthenticateRequest) error {
	return p.Called(ctx).Error(0)
}

func (p *MockAuthPlugin) ValidateAuthString(hash string) bool {
	return p.Called(hash).Bool(0)
}

func (p *MockAuthPlugin) GenerateAuthString(password string) (string, bool) {
	args := p.Called(password)
	return args.String(0), args.Bool(1)
}

func (p *MockAuthPlugin) VerifyDynamicPrivilege(ctx extension.VerifyDynamicPrivRequest) bool {
	return p.Called(ctx).Bool(0)
}

func (p *MockAuthPlugin) VerifyPrivilege(ctx extension.VerifyStaticPrivRequest) bool {
	return p.Called(ctx).Bool(0)
}

type AuthenticateContextMatcher struct {
	expected extension.AuthenticateRequest
}

func (m AuthenticateContextMatcher) Matches(x any) bool {
	ctx, ok := x.(extension.AuthenticateRequest)
	if !ok {
		return false
	}
	return ctx.User == m.expected.User &&
		ctx.StoredAuthString == m.expected.StoredAuthString &&
		string(ctx.InputAuthString) == string(m.expected.InputAuthString)
}

type DynamicMatcher struct {
	expected extension.VerifyDynamicPrivRequest
}

func (m DynamicMatcher) Matches(x any) bool {
	ctx, ok := x.(extension.VerifyDynamicPrivRequest)
	if !ok {
		return false
	}
	return ctx.User == m.expected.User &&
		ctx.DynamicPriv == m.expected.DynamicPriv &&
		ctx.WithGrant == m.expected.WithGrant
}

type StaticMatcher struct {
	expected extension.VerifyStaticPrivRequest
}

func (m StaticMatcher) Matches(x any) bool {
	ctx, ok := x.(extension.VerifyStaticPrivRequest)
	if !ok {
		return false
	}
	return ctx.User == m.expected.User &&
		ctx.StaticPriv == m.expected.StaticPriv &&
		ctx.DB == m.expected.DB
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

	p := new(MockAuthPlugin)
	p.On("Name").Return("authentication_test_plugin")

	authnMatcher1 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u2", StoredAuthString: "encodedpassword", InputAuthString: []byte("1")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher1).Return(nil)
	authnMatcher2 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u2", StoredAuthString: "encodedpassword", InputAuthString: []byte("2")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher2).Return(errors.New("authentication failed"))
	authnMatcher3 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u2", StoredAuthString: "anotherencodedpassword", InputAuthString: []byte("1")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher3).Return(nil)
	authnMatcher4 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u2", StoredAuthString: "yetanotherencodedpassword", InputAuthString: []byte("1")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher4).Return(nil)
	authnMatcher5 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u2", StoredAuthString: "yetanotherencodedpassword2", InputAuthString: []byte("1")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher5).Return(nil)

	p.On("ValidateAuthString", mock.Anything).Return(true)
	p.On("GenerateAuthString", "rawpassword").Return("encodedpassword", true)
	p.On("GenerateAuthString", "anotherrawpassword").Return("anotherencodedpassword", true)
	p.On("GenerateAuthString", "yetanotherrawpassword").Return("yetanotherencodedpassword", true)
	p.On("GenerateAuthString", "yetanotherrawpassword2").Return("yetanotherencodedpassword2", true)
	authzMatcher1 := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u2", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.AllPrivMask}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", authzMatcher1).Return(true)
	authzMatcher2 := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u2", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.SelectPriv}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", authzMatcher2).Return(true)
	authzMatcher3 := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u2", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.InsertPriv}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", authzMatcher3).Return(false)
	authzMatcher4 := mock.MatchedBy(func(ctx extension.VerifyDynamicPrivRequest) bool {
		return DynamicMatcher{expected: extension.VerifyDynamicPrivRequest{User: "u2"}}.Matches(ctx)
	})
	p.On("VerifyDynamicPrivilege", authzMatcher4).Return(false)
	deleteMatcher := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u2", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.DeletePriv}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", deleteMatcher).Return(false)
	sysVarAdminMatcher := mock.MatchedBy(func(ctx extension.VerifyDynamicPrivRequest) bool {
		return DynamicMatcher{expected: extension.VerifyDynamicPrivRequest{User: "u2", DynamicPriv: "SYSTEM_VARIABLES_ADMIN", WithGrant: false}}.Matches(ctx)
	})
	p.On("VerifyDynamicPrivilege", sysVarAdminMatcher).Return(false)

	authChecks := []*extension.AuthPlugin{{
		Name:                     p.Name(),
		AuthenticateUser:         p.AuthenticateUser,
		ValidateAuthString:       p.ValidateAuthString,
		GenerateAuthString:       p.GenerateAuthString,
		VerifyPrivilege:          p.VerifyPrivilege,
		VerifyDynamicPrivilege:   p.VerifyDynamicPrivilege,
		RequiredClientSidePlugin: mysql.AuthNativePassword,
	}}

	require.NoError(t, extension.Register(
		"extension_authentication_plugin",
		extension.WithCustomAuthPlugins(authChecks),
		extension.WithCustomSysVariables([]*variable.SysVar{
			{
				Scope:          vardef.ScopeGlobal,
				Name:           "extension_authentication_plugin",
				Value:          mysql.AuthNativePassword,
				Type:           vardef.TypeEnum,
				PossibleValues: []string{p.Name()},
			},
		}),
		extension.WithBootstrap(func(_ extension.BootstrapContext) error {
			return nil
		}),
	))
	require.NoError(t, extension.Setup())
	ext, err := extension.GetExtensions()
	require.NoError(t, err)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetExtensions(ext.NewSessionExtensions())
	tk.MustExec("use test")

	// Create user with an invalid plugin should not work.
	tk.MustContainErrMsg("create user 'u2'@'localhost' identified with 'bad_plugin' by 'rawpassword'", "[executor:1524]Plugin 'bad_plugin' is not loaded")

	// Create user with a valid plugin should work.
	tk.MustExec("create user 'u2'@'localhost' identified with 'authentication_test_plugin' as 'encodedpassword'")
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='u2' and host='localhost'`).Check(testkit.Rows("u2 authentication_test_plugin"))
	p.AssertCalled(t, "ValidateAuthString", "encodedpassword")
	p.AssertNumberOfCalls(t, "ValidateAuthString", 1)
	p.AssertNotCalled(t, "GenerateAuthString")

	// Alter user with an invalid plugin should not work.
	tk.MustContainErrMsg("alter user 'u2'@'localhost' identified with 'bad_plugin' by 'rawpassword'", "[executor:1524]Plugin 'bad_plugin' is not loaded")

	tk1 := testkit.NewTestKit(t, store)
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk1.MustExec("use test")

	// Authentication tests.
	tk2 := testkit.NewTestKit(t, store)
	tk.Session().SetExtensions(ext.NewSessionExtensions())
	// Login using wrong password should fail
	require.EqualError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("2"), nil, nil), "[privilege:1045]Access denied for user 'u2'@'localhost' (using password: YES)")
	// Correct password should pass
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should authenticate using plugin impl.
	p.AssertNumberOfCalls(t, "AuthenticateUser", 2)
	p.AssertCalled(t, "ValidateAuthString", "encodedpassword")
	p.AssertNumberOfCalls(t, "ValidateAuthString", 3)

	// Change password should work using ALTER USER statement.
	tk.MustExec("alter user 'u2'@'localhost' identified with 'authentication_test_plugin' by 'anotherrawpassword'")
	p.AssertCalled(t, "GenerateAuthString", "anotherrawpassword")
	// Correct password should pass
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

	tk2.Session().SetExtensions(ext.NewSessionExtensions())
	// Change password using SET PASSWORD statement should work using the user itself.
	tk2.MustExec("set password='yetanotherrawpassword2'")
	p.AssertCalled(t, "GenerateAuthString", "yetanotherrawpassword2")
	// Correct password should pass
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should authenticate using plugin impl.
	p.AssertNumberOfCalls(t, "AuthenticateUser", 5)
	p.AssertCalled(t, "ValidateAuthString", "yetanotherencodedpassword2")
	tk2.Session().SetExtensions(ext.NewSessionExtensions())

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
	p.AssertCalled(t, "VerifyPrivilege", authzMatcher2)
	p.AssertCalled(t, "VerifyPrivilege", authzMatcher3)

	require.EqualError(t, tk2.ExecToErr("delete from t1 where id=1"), "[planner:1142]DELETE command denied to user 'u2'@'localhost' for table 't1'")
	// Should not verify delete privilege using plugin impl since privilege is not granted in mysql.
	p.AssertNotCalled(t, "VerifyPrivilege", deleteMatcher)

	require.EqualError(t, tk2.ExecToErr("SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER'"), "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	p.AssertNotCalled(t, "VerifyDynamicPrivilege", sysVarAdminMatcher)
	tk.MustExec("GRANT SYSTEM_VARIABLES_ADMIN on *.* TO u2@localhost")
	require.EqualError(t, tk2.ExecToErr("SET GLOBAL sql_mode = 'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER'"), "[planner:1227]Access denied; you need (at least one of) the SUPER or SYSTEM_VARIABLES_ADMIN privilege(s) for this operation")
	// Should verify dynamic privilege using plugin impl.
	p.AssertCalled(t, "VerifyDynamicPrivilege", sysVarAdminMatcher)
}

func TestAuthPluginSwitchPlugins(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	p := new(MockAuthPlugin)
	p.On("Name").Return("authentication_test_plugin")
	authnMatcher1 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u2", StoredAuthString: "rawpassword", InputAuthString: []byte("1")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher1).Return(nil)
	authnMatcher2 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u2", StoredAuthString: "encodedpassword", InputAuthString: []byte("1")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher2).Return(nil)

	p.On("ValidateAuthString", mock.Anything).Return(true)
	p.On("GenerateAuthString", "rawpassword").Return("encodedpassword", true)

	allPrivMatcher := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u2", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.AllPrivMask}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", allPrivMatcher).Return(true)
	selectMatcher := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u2", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.SelectPriv}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", selectMatcher).Return(true)
	insertMatcher := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u2", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.InsertPriv}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", insertMatcher).Return(false)

	authChecks := []*extension.AuthPlugin{{
		Name:                     p.Name(),
		AuthenticateUser:         p.AuthenticateUser,
		ValidateAuthString:       p.ValidateAuthString,
		GenerateAuthString:       p.GenerateAuthString,
		VerifyPrivilege:          p.VerifyPrivilege,
		VerifyDynamicPrivilege:   p.VerifyDynamicPrivilege,
		RequiredClientSidePlugin: mysql.AuthNativePassword,
	}}

	require.NoError(t, extension.Register(
		"extension_authentication_plugin",
		extension.WithCustomAuthPlugins(authChecks),
		extension.WithCustomSysVariables([]*variable.SysVar{
			{
				Scope:          vardef.ScopeGlobal,
				Name:           "extension_authentication_plugin",
				Value:          mysql.AuthNativePassword,
				Type:           vardef.TypeEnum,
				PossibleValues: []string{p.Name()},
			},
		}),
		extension.WithBootstrap(func(_ extension.BootstrapContext) error {
			return nil
		}),
	))
	ext, err := extension.GetExtensions()
	require.NoError(t, err)

	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetExtensions(ext.NewSessionExtensions())
	tk.MustExec("use test")

	// Create user with a valid plugin should work.
	tk.MustExec("create user 'u2'@'localhost' identified with 'authentication_test_plugin' as 'rawpassword'")
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='u2' and host='localhost'`).Check(testkit.Rows("u2 authentication_test_plugin"))
	p.AssertCalled(t, "ValidateAuthString", "rawpassword")

	tk1 := testkit.NewTestKit(t, store)
	tk1.Session().SetExtensions(ext.NewSessionExtensions())
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost"}, nil, nil, nil))
	tk1.MustExec("use test")
	tk1.Session().SetExtensions(ext.NewSessionExtensions())

	// Authentication tests.
	tk2 := testkit.NewTestKit(t, store)
	tk2.Session().SetExtensions(ext.NewSessionExtensions())
	// Correct password should pass
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should authenticate using plugin impl.
	p.AssertNumberOfCalls(t, "AuthenticateUser", 1)
	p.AssertCalled(t, "ValidateAuthString", "rawpassword")

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
	p.AssertCalled(t, "VerifyPrivilege", selectMatcher)
	p.AssertCalled(t, "VerifyPrivilege", insertMatcher)

	tk.MustExec("alter user 'u2'@'localhost' identified with 'mysql_native_password' by '123'")
	tk.MustQuery(`SELECT user, plugin FROM mysql.user WHERE user='u2' and host='localhost'`).Check(testkit.Rows("u2 mysql_native_password"))
	// Existing session should still use plugin.
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	tk2.MustQuery("select * from t1").Check(testkit.Rows("1 10", "2 20"))
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 5)

	// New session should not use plugin.
	tk2.RefreshSession()
	tk2.Session().SetExtensions(ext.NewSessionExtensions())
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
	p.AssertCalled(t, "GenerateAuthString", "rawpassword")
	tk2.RefreshSession()
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, []byte("1"), nil, nil))
	tk2.Session().SetExtensions(ext.NewSessionExtensions())
	tk2.MustExec("use test")
	tk2.MustQuery("select * from t1 where id=1").Check(testkit.Rows("1 10"))
	require.EqualError(t, tk2.ExecToErr("insert into t1 values (5, 50)"), "[planner:1142]INSERT command denied to user 'u2'@'localhost' for table 't1'")
	// Now it should verify privilege using plugin impl.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 7)
}

func TestCreateUserWhenGrant(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	p := new(MockAuthPlugin)
	p.On("Name").Return("authentication_test_plugin")
	p.On("ValidateAuthString", mock.Anything).Return(true)
	p.On("GenerateAuthString", "xxx").Return("encodedpassword", true)

	authChecks := []*extension.AuthPlugin{{
		Name:                     p.Name(),
		AuthenticateUser:         p.AuthenticateUser,
		ValidateAuthString:       p.ValidateAuthString,
		GenerateAuthString:       p.GenerateAuthString,
		RequiredClientSidePlugin: mysql.AuthNativePassword,
	}}

	require.NoError(t, extension.Register(
		"extension_authentication_plugin",
		extension.WithCustomAuthPlugins(authChecks),
		extension.WithCustomSysVariables([]*variable.SysVar{
			{
				Scope:          vardef.ScopeGlobal,
				Name:           "extension_authentication_plugin",
				Value:          mysql.AuthNativePassword,
				Type:           vardef.TypeEnum,
				PossibleValues: []string{p.Name()},
			},
		}),
		extension.WithBootstrap(func(_ extension.BootstrapContext) error {
			return nil
		}),
	))
	require.NoError(t, extension.Setup())
	ext, err := extension.GetExtensions()
	require.NoError(t, err)

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetExtensions(ext.NewSessionExtensions())
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

func TestCreateViewWithPluginUser(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	p := new(MockAuthPlugin)
	p.On("Name").Return("authentication_test_plugin")
	authnMatcher1 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u1", StoredAuthString: "rawpassword", InputAuthString: []byte("1")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher1).Return(nil).Return(nil)

	p.On("ValidateAuthString", mock.Anything).Return(true)
	p.On("GenerateAuthString", "rawpassword").Return("encodedpassword", true)

	allPrivMatcherHost1 := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u1", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.AllPrivMask}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", allPrivMatcherHost1).Return(true)
	createViewMatcher := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u1", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.CreateViewPriv}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", createViewMatcher).Return(true)

	authChecks := []*extension.AuthPlugin{{
		Name:                     p.Name(),
		AuthenticateUser:         p.AuthenticateUser,
		ValidateAuthString:       p.ValidateAuthString,
		GenerateAuthString:       p.GenerateAuthString,
		VerifyPrivilege:          p.VerifyPrivilege,
		VerifyDynamicPrivilege:   p.VerifyDynamicPrivilege,
		RequiredClientSidePlugin: mysql.AuthNativePassword,
	}}

	require.NoError(t, extension.Register(
		"extension_authentication_plugin",
		extension.WithCustomAuthPlugins(authChecks),
		extension.WithCustomSysVariables([]*variable.SysVar{
			{
				Scope:          vardef.ScopeGlobal,
				Name:           "extension_authentication_plugin",
				Value:          mysql.AuthNativePassword,
				Type:           vardef.TypeEnum,
				PossibleValues: []string{p.Name()},
			},
		}),
	))
	ext, err := extension.GetExtensions()
	require.NoError(t, err)
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetExtensions(ext.NewSessionExtensions())
	// Set up the table using root user.
	tk.MustExec("use test")
	tk.MustExec("create table t1(id int primary key, v int)")
	tk.MustExec("insert into t1 values (1, 10), (2, 20)")

	// Create user u1 with plugin.
	tk.MustExec("create user 'u1' identified with 'authentication_test_plugin' as 'rawpassword'")
	tk.MustExec("grant select, insert, create view on test.* TO u1")
	// Create another user u2 without plugin.
	tk.MustExec("create user 'u2'")
	tk.MustExec("grant select on test.* TO u2")

	tk1 := testkit.NewTestKit(t, store)
	tk1.Session().SetExtensions(ext.NewSessionExtensions())
	// Create one session for u1. This session does not have SELECT privilege.
	require.NoError(t, tk1.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, []byte("1"), nil, nil))
	tk1.MustExec("use test")

	selectMatcher := mock.MatchedBy(func(ctx extension.VerifyStaticPrivRequest) bool {
		return StaticMatcher{expected: extension.VerifyStaticPrivRequest{User: "u1", Host: "localhost", DB: "test", Table: "t1", StaticPriv: mysql.SelectPriv}}.Matches(ctx)
	})
	p.On("VerifyPrivilege", selectMatcher).Return(false).Once()
	// Create view should not work.
	require.ErrorContains(t, tk1.ExecToErr("create view v1 as select * from t1"), "[planner:1142]SELECT command denied to user 'u1'@'%' for table 't1'")
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 1)

	tk2 := testkit.NewTestKit(t, store)
	tk2.Session().SetExtensions(ext.NewSessionExtensions())
	// Create another session for u1. This session has SELECT privilege.
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, []byte("1"), nil, nil))
	tk2.MustExec("use test")
	p.On("VerifyPrivilege", selectMatcher).Return(true)
	// Create view should work.
	tk2.MustExec("create view v1 as select * from t1")
	tk2.MustQuery("select * from v1").Check(testkit.Rows("1 10", "2 20"))
	// Session 1 should also be able to access the view now.
	tk1.MustQuery("select * from v1").Check(testkit.Rows("1 10", "2 20"))
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 5)

	tk3 := testkit.NewTestKit(t, store)
	tk3.Session().SetExtensions(ext.NewSessionExtensions())
	// Create another session for u1, logging in with a different host. This session has SELECT privilege.
	require.NoError(t, tk3.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil, nil))
	tk3.MustExec("use test")
	// Now reject the SELECT privilege check for u1, but u2 should be able to access the view.
	p.On("VerifyPrivilege", selectMatcher).Return(false)
	tk3.MustQuery("select * from v1").Check(testkit.Rows("1 10", "2 20"))
	// Should not verify privilege using the plugin since the definer is not in-session.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 5)
}

func TestPluginUserModification(t *testing.T) {
	defer extension.Reset()
	extension.Reset()

	p := new(MockAuthPlugin)
	p.On("Name").Return("authentication_test_plugin")
	authnMatcher1 := mock.MatchedBy(func(ctx extension.AuthenticateRequest) bool {
		return AuthenticateContextMatcher{expected: extension.AuthenticateRequest{User: "u4", StoredAuthString: "rawpassword", InputAuthString: []byte("1")}}.Matches(ctx)
	})
	p.On("AuthenticateUser", authnMatcher1).Return(nil)

	p.On("ValidateAuthString", mock.Anything).Return(true)
	p.On("GenerateAuthString", mock.Anything).Return("encodedrandompassword", true)
	p.On("VerifyPrivilege", mock.Anything).Return(true)
	p.On("VerifyDynamicPrivilege", mock.Anything).Return(true)

	authChecks := []*extension.AuthPlugin{{
		Name:                     p.Name(),
		AuthenticateUser:         p.AuthenticateUser,
		ValidateAuthString:       p.ValidateAuthString,
		GenerateAuthString:       p.GenerateAuthString,
		VerifyPrivilege:          p.VerifyPrivilege,
		VerifyDynamicPrivilege:   p.VerifyDynamicPrivilege,
		RequiredClientSidePlugin: mysql.AuthNativePassword,
	}}

	require.NoError(t, extension.Register(
		"extension_authentication_plugin",
		extension.WithCustomAuthPlugins(authChecks),
		extension.WithCustomSysVariables([]*variable.SysVar{
			{
				Scope:          vardef.ScopeGlobal,
				Name:           "extension_authentication_plugin",
				Value:          mysql.AuthNativePassword,
				Type:           vardef.TypeEnum,
				PossibleValues: []string{p.Name()},
			},
		}),
	))
	ext, err := extension.GetExtensions()
	require.NoError(t, err)
	require.NoError(t, extension.Setup())

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().SetExtensions(ext.NewSessionExtensions())

	// Create user u1 with plugin with super privilege
	tk.MustExec("create user 'u1' identified with 'authentication_test_plugin' as 'rawpassword'")
	tk.MustExec("grant super on *.* to u1")
	// Create a non-plugin user u2 with create user privilege.
	tk.MustExec("create user 'u2'")
	tk.MustExec("grant create user on *.* to u2")
	// Create a non-plugin user u3 with create user and system_user privilege. It also needs the super privilege so we can run set password on u1.
	tk.MustExec("create user 'u3'")
	tk.MustExec("grant create user, system_user, super on *.* to u3")
	// Create another super plugin user u4 to check if it can run admin operations on u3.
	tk.MustExec("create user 'u4' identified with 'authentication_test_plugin' as 'rawpassword'")
	tk.MustExec("grant create user, super on *.* to u4")

	tk2 := testkit.NewTestKit(t, store)
	tk2.Session().SetExtensions(ext.NewSessionExtensions())
	require.NoError(t, tk2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "localhost"}, nil, nil, nil))
	// User u2 should not be able to alter user u1 or drop it.
	tk2.MustContainErrMsg("drop user u1", "[planner:1227]Access denied; you need (at least one of) the SYSTEM_USER or SUPER privilege(s) for this operation")
	tk2.MustContainErrMsg("alter user u1 identified with 'authentication_test_plugin' as 'randompassword'", "[planner:1227]Access denied; you need (at least one of) the SYSTEM_USER or SUPER privilege(s) for this operation")
	// Should not even call the plugin to verify privilege of u1 at all.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 0)
	p.AssertNumberOfCalls(t, "VerifyDynamicPrivilege", 0)

	tk3 := testkit.NewTestKit(t, store)
	tk3.Session().SetExtensions(ext.NewSessionExtensions())
	require.NoError(t, tk3.Session().Auth(&auth.UserIdentity{Username: "u3", Hostname: "localhost"}, nil, nil, nil))
	// Should be able to run these admin operations on u1.
	tk3.MustExec("alter user u1 identified with 'authentication_test_plugin' as 'randompassword'")
	tk3.MustExec("set password for 'u1'='random2password'")
	tk3.MustExec("drop user u1")
	// Should not even call the plugin to verify privilege of u1 at all.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 0)
	p.AssertNumberOfCalls(t, "VerifyDynamicPrivilege", 0)

	tk4 := testkit.NewTestKit(t, store)
	tk4.Session().SetExtensions(ext.NewSessionExtensions())
	require.NoError(t, tk4.Session().Auth(&auth.UserIdentity{Username: "u4", Hostname: "localhost"}, []byte("1"), nil, nil))
	// Should be able to run these admin operations on u3.
	tk4.MustExec("alter user u3 identified by 'blah'")
	tk4.MustExec("set password for 'u3'='blahblah'")
	tk4.MustExec("drop user u3")
	// Should have called the plugin to verify privilege of u4 since it's in-session for CREATE_USER/SUPER privileges.
	p.AssertNumberOfCalls(t, "VerifyPrivilege", 3)
	// Dynamic privilege should be checked for u4. for SYSTEM_USER and RESTRICTED_USER_ADMIN.
	p.AssertNumberOfCalls(t, "VerifyDynamicPrivilege", 4)
}

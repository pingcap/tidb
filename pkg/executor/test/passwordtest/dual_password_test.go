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

package passwordtest

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// The tests in this file mirror MySQL 8.0's own dual-password coverage
// (mysql-test/suite/auth_sec/t/multiple_passwords_ddl.test for DDL and
// include/multiple_passwords.inc for login), plus TiDB-specific regressions
// found during review. Scenario map (MySQL DDL test number -> tests here):
//
//	1/2  RETAIN/DISCARD storage      TestDualPasswordRetainAndDiscard, TestDualPasswordChainedRetain
//	3    SET PASSWORD ... RETAIN     TestDualPasswordSetPasswordRetain, TestDualPasswordSelfSetPasswordRetainAcceptsMysqlUpdate
//	4/5  RENAME / DROP USER          TestDualPasswordRenameUserPreservesSecondary, TestDualPasswordDropUserRemovesSecondary
//	6    multi-user mix-n-match      TestDualPasswordMultiUserAlter
//	7    privilege model             TestDualPasswordCrossUserRequiresCreateUser, TestDualPasswordSetPasswordSelfByExplicitName,
//	                                 TestDualPasswordSelfServiceDiscardWithExtraOptionsStillGated
//	9/14 plugin change               TestDualPasswordRejectsPluginChange, TestDualPasswordPluginChangeSilentlyDiscardsSecondary,
//	                                 TestDualPasswordAlterWithoutRetainPreservesSecondary
//	10   empty current password      TestDualPasswordRejectsEmptyPrimary
//	13   ALTER USER USER()           TestDualPasswordAlterUserUserRetainAndDiscard,
//	                                 TestDualPasswordAlterUserUserResolvesAuthUsername (#68937)
//	15   empty new password          TestDualPasswordRejectsEmptyNew
//
// TiDB-specific:
//
//	CREATE USER grammar rejection    TestDualPasswordCreateUserRejectsRetain
//	SHOW CREATE USER redaction       TestDualPasswordShowCreateUserHidesSecondary
//	secondary login / empty primary  TestDualPasswordSecondaryLoginWithEmptyPrimary
//	caching_sha2 storage format      TestDualPasswordCachingSha2PasswordStorage
//	legacy empty-plugin resolution   TestDualPasswordLegacyEmptyPlugin{AcceptsNative,HonorsDefaultPlugin,RejectsLDAPDefault}
//	DISCARD edge cases               TestDualPasswordDiscardNoopOnIncapablePlugin, TestDualPasswordDiscardCollapsesEmptyAttributesToNull

// rootTK returns a testkit authenticated as root, ready to manage users.
func rootTK(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	return tk
}

// authAs attempts to authenticate a fresh session as user@host with the given plaintext password.
// Returns nil on success.
func authAs(t *testing.T, tk *testkit.TestKit, user, host, password string) error {
	sub := testkit.NewTestKit(t, tk.Session().GetStore())
	return sub.Session().Auth(&auth.UserIdentity{Username: user, Hostname: host}, sha1Password(password), nil, nil)
}

func TestDualPasswordRetainAndDiscard(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpuser")
	tk.MustExec("CREATE USER dpuser IDENTIFIED BY 'old'")

	// Baseline: old password works.
	require.NoError(t, authAs(t, tk, "dpuser", "%", "old"))

	// Rotate with RETAIN CURRENT PASSWORD.
	tk.MustExec("ALTER USER dpuser IDENTIFIED BY 'new' RETAIN CURRENT PASSWORD")

	// Both passwords authenticate.
	require.NoError(t, authAs(t, tk, "dpuser", "%", "new"))
	require.NoError(t, authAs(t, tk, "dpuser", "%", "old"))

	// user_attributes should contain additional_password with the old hash.
	rows := tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User = 'dpuser'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "1", rows[0][0])

	// DISCARD OLD PASSWORD drops the secondary.
	tk.MustExec("ALTER USER dpuser DISCARD OLD PASSWORD")
	require.NoError(t, authAs(t, tk, "dpuser", "%", "new"))
	require.Error(t, authAs(t, tk, "dpuser", "%", "old"))

	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') FROM mysql.user WHERE User = 'dpuser'").Check(testkit.Rows("<nil>"))
}

func TestDualPasswordSetPasswordRetain(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpsetu")
	tk.MustExec("CREATE USER dpsetu IDENTIFIED BY 'p1'")

	// SET PASSWORD FOR … RETAIN CURRENT PASSWORD.
	tk.MustExec("SET PASSWORD FOR dpsetu = 'p2' RETAIN CURRENT PASSWORD")

	require.NoError(t, authAs(t, tk, "dpsetu", "%", "p1"))
	require.NoError(t, authAs(t, tk, "dpsetu", "%", "p2"))
}

func TestDualPasswordCreateUserRejectsRetain(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpcre")
	err := tk.ExecToErr("CREATE USER dpcre IDENTIFIED BY 'x' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "RETAIN CURRENT PASSWORD")
}

func TestDualPasswordRejectsEmptyNew(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpempty")
	tk.MustExec("CREATE USER dpempty IDENTIFIED BY 'p1'")
	err := tk.ExecToErr("ALTER USER dpempty IDENTIFIED BY '' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "new password is empty")
}

func TestDualPasswordRejectsPluginChange(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpplug")
	tk.MustExec("CREATE USER dpplug IDENTIFIED WITH mysql_native_password BY 'p1'")
	err := tk.ExecToErr("ALTER USER dpplug IDENTIFIED WITH caching_sha2_password BY 'p2' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "authentication plugin is being changed")
}

// TestDualPasswordLegacyEmptyPluginAcceptsNative guards the legacy-row case:
// a mysql.user record whose `plugin` column is empty is resolved by the
// privilege cache via the `default_authentication_plugin` session variable.
// With the default (mysql_native_password), an explicit
// `IDENTIFIED WITH mysql_native_password ... RETAIN CURRENT PASSWORD` against
// such a row must not be misclassified as a plugin switch.
func TestDualPasswordLegacyEmptyPluginAcceptsNative(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dplegacy")
	tk.MustExec("CREATE USER dplegacy IDENTIFIED BY 'p1'")
	// Simulate a legacy row by clearing the plugin column.
	tk.MustExec("UPDATE mysql.user SET plugin = '' WHERE User = 'dplegacy'")
	tk.MustExec("FLUSH PRIVILEGES")

	// Explicit native plugin with RETAIN must succeed on the legacy row.
	tk.MustExec("ALTER USER dplegacy IDENTIFIED WITH mysql_native_password BY 'p2' RETAIN CURRENT PASSWORD")

	// Both passwords authenticate.
	require.NoError(t, authAs(t, tk, "dplegacy", "%", "p1"))
	require.NoError(t, authAs(t, tk, "dplegacy", "%", "p2"))

	// The secondary slot is actually populated (the legacy-row case
	// previously took the plugin-change branch and dropped it).
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User = 'dplegacy'").Check(testkit.Rows("1"))
}

// TestDualPasswordLegacyEmptyPluginHonorsDefaultPlugin guards the non-native
// default_authentication_plugin case: when an operator sets the default to
// caching_sha2_password, a legacy empty-plugin row resolves to that plugin,
// and an explicit `IDENTIFIED WITH mysql_native_password ... RETAIN CURRENT
// PASSWORD` IS a plugin switch and must be rejected.
func TestDualPasswordLegacyEmptyPluginHonorsDefaultPlugin(t *testing.T) {
	tk := rootTK(t)
	prevDefault := tk.MustQuery("SELECT @@global.default_authentication_plugin").Rows()[0][0]
	defer tk.MustExec(fmt.Sprintf("SET GLOBAL default_authentication_plugin = '%s'", prevDefault))

	tk.MustExec("SET GLOBAL default_authentication_plugin = 'caching_sha2_password'")

	tk.MustExec("DROP USER IF EXISTS dplegacy_sha2")
	tk.MustExec("CREATE USER dplegacy_sha2 IDENTIFIED WITH caching_sha2_password BY 'p1'")
	// Simulate a legacy row by clearing the plugin column: with the new
	// default the row resolves to caching_sha2_password.
	tk.MustExec("UPDATE mysql.user SET plugin = '' WHERE User = 'dplegacy_sha2'")
	tk.MustExec("FLUSH PRIVILEGES")

	// Now an explicit native plugin IS a plugin switch — RETAIN must fail.
	err := tk.ExecToErr("ALTER USER dplegacy_sha2 IDENTIFIED WITH mysql_native_password BY 'p2' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "authentication plugin is being changed")
}

func TestDualPasswordPluginChangeSilentlyDiscardsSecondary(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpdrop")
	tk.MustExec("CREATE USER dpdrop IDENTIFIED WITH mysql_native_password BY 'p1'")
	tk.MustExec("ALTER USER dpdrop IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")

	// secondary is set
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User = 'dpdrop'").Check(testkit.Rows("1"))

	// Changing plugin without RETAIN silently drops the secondary.
	tk.MustExec("ALTER USER dpdrop IDENTIFIED WITH caching_sha2_password BY 'p3'")
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') FROM mysql.user WHERE User = 'dpdrop'").Check(testkit.Rows("<nil>"))
}

func TestDualPasswordCrossUserRequiresCreateUser(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpcreate, dpaponly, dpvictim_create, dpvictim_ap")
	tk.MustExec("CREATE USER dpvictim_create IDENTIFIED BY 'v1'")
	tk.MustExec("CREATE USER dpvictim_ap IDENTIFIED BY 'v1'")
	tk.MustExec("CREATE USER dpcreate IDENTIFIED BY 'a1'")
	tk.MustExec("CREATE USER dpaponly IDENTIFIED BY 'a1'")
	tk.MustExec("GRANT CREATE USER ON *.* TO dpcreate")
	// dpaponly gets APPLICATION_PASSWORD_ADMIN but no authority over other
	// accounts; ALL on test.* ensures the cross-user denial comes from the
	// account-authority check, not an unrelated missing privilege.
	tk.MustExec("GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO dpaponly")
	tk.MustExec("GRANT ALL ON test.* TO dpaponly")

	// MySQL-compatible rule: cross-user secondary-password operations require the
	// normal ALTER USER authority (CREATE USER). APPLICATION_PASSWORD_ADMIN is
	// NOT a substitute for authority over other accounts.
	createTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, createTK.Session().Auth(&auth.UserIdentity{Username: "dpcreate", Hostname: "%"}, sha1Password("a1"), nil, nil))
	createTK.MustExec("ALTER USER dpvictim_create IDENTIFIED BY 'v2' RETAIN CURRENT PASSWORD")

	require.NoError(t, authAs(t, tk, "dpvictim_create", "%", "v1"))
	require.NoError(t, authAs(t, tk, "dpvictim_create", "%", "v2"))

	createTK.MustExec("ALTER USER dpvictim_create DISCARD OLD PASSWORD")
	require.Error(t, authAs(t, tk, "dpvictim_create", "%", "v1"))
	require.NoError(t, authAs(t, tk, "dpvictim_create", "%", "v2"))

	// APPLICATION_PASSWORD_ADMIN alone is NOT sufficient for cross-user RETAIN
	// or DISCARD — that would be a privilege escalation (resetting another
	// account's password without CREATE USER).
	apOnlyTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, apOnlyTK.Session().Auth(&auth.UserIdentity{Username: "dpaponly", Hostname: "%"}, sha1Password("a1"), nil, nil))
	err := apOnlyTK.ExecToErr("ALTER USER dpvictim_ap IDENTIFIED BY 'v2' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE USER")
	err = apOnlyTK.ExecToErr("ALTER USER dpvictim_ap DISCARD OLD PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE USER")
	// The victim's password was not changed by the denied statements.
	require.NoError(t, authAs(t, tk, "dpvictim_ap", "%", "v1"))
	require.Error(t, authAs(t, tk, "dpvictim_ap", "%", "v2"))
}

// TestDualPasswordRejectsEmptyPrimary guards the MySQL-compatible
// ER_SECOND_PASSWORD_CANNOT_BE_EMPTY (3878) path: a user with an empty current
// primary password must not be allowed to promote it to the secondary slot.
func TestDualPasswordRejectsEmptyPrimary(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpemptyprim")
	tk.MustExec("CREATE USER dpemptyprim IDENTIFIED BY ''")

	err := tk.ExecToErr("ALTER USER dpemptyprim IDENTIFIED BY 'new' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Empty password can not be retained as second password")
}

func TestDualPasswordShowCreateUserHidesSecondary(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpshow")
	tk.MustExec("CREATE USER dpshow IDENTIFIED BY 'p1'")
	tk.MustExec("ALTER USER dpshow IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")

	// Confirm the secondary is actually stored.
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User = 'dpshow'").Check(testkit.Rows("1"))

	// SHOW CREATE USER must not leak the additional_password hash.
	rows := tk.MustQuery("SHOW CREATE USER dpshow").Rows()
	require.Len(t, rows, 1)
	showOut := fmt.Sprint(rows[0][0])
	require.NotContains(t, showOut, "additional_password", "SHOW CREATE USER must not expose the secondary password key")
	require.NotContains(t, showOut, "RETAIN CURRENT PASSWORD", "SHOW CREATE USER must not surface the RETAIN clause")
}

func TestDualPasswordSetPasswordSelfByExplicitName(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpself")
	tk.MustExec("CREATE USER dpself IDENTIFIED BY 's1'")
	tk.MustExec("GRANT USAGE ON *.* TO dpself")

	selfTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, selfTK.Session().Auth(&auth.UserIdentity{Username: "dpself", Hostname: "%"}, sha1Password("s1"), nil, nil))

	// MySQL: own-account RETAIN CURRENT PASSWORD requires APPLICATION_PASSWORD_ADMIN.
	// Without it, even the explicit-self form is denied.
	err := selfTK.ExecToErr("SET PASSWORD FOR 'dpself'@'%' = 's2' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "APPLICATION_PASSWORD_ADMIN")
	// The denied statement must not have changed the password.
	require.NoError(t, authAs(t, tk, "dpself", "%", "s1"))
	require.Error(t, authAs(t, tk, "dpself", "%", "s2"))

	// With APPLICATION_PASSWORD_ADMIN, self-service RETAIN succeeds.
	tk.MustExec("GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO dpself")
	selfTK = testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, selfTK.Session().Auth(&auth.UserIdentity{Username: "dpself", Hostname: "%"}, sha1Password("s1"), nil, nil))
	selfTK.MustExec("SET PASSWORD FOR 'dpself'@'%' = 's2' RETAIN CURRENT PASSWORD")

	// Both passwords authenticate after the rotation.
	require.NoError(t, authAs(t, tk, "dpself", "%", "s1"))
	require.NoError(t, authAs(t, tk, "dpself", "%", "s2"))
}

// TestDualPasswordCachingSha2PasswordStorage is a storage-only check for
// caching_sha2_password: it verifies that RETAIN CURRENT PASSWORD writes a
// secondary-password hash in the plugin's own format, and that primary/secondary
// differ. Login-path coverage for this plugin needs a real client-side
// SHA256-scramble and is out of scope here; mysql_native_password login
// (exercised in TestDualPasswordRetainAndDiscard) provides the auth-path
// regression guard.
func TestDualPasswordCachingSha2PasswordStorage(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpsha2")
	tk.MustExec("CREATE USER dpsha2 IDENTIFIED WITH caching_sha2_password BY 'p1'")
	tk.MustExec("ALTER USER dpsha2 IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")

	rows := tk.MustQuery(`SELECT authentication_string, JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.additional_password'))
		FROM mysql.user WHERE User = 'dpsha2'`).Rows()
	require.Len(t, rows, 1)
	primary := fmt.Sprint(rows[0][0])
	secondary := fmt.Sprint(rows[0][1])
	require.NotEmpty(t, primary)
	require.NotEmpty(t, secondary)
	require.NotEqual(t, primary, secondary)
}

// TestDualPasswordChainedRetain mirrors MySQL's DDL Test 1: two consecutive
// RETAIN CURRENT PASSWORD statements should overwrite the secondary with the
// previous primary each time.
func TestDualPasswordChainedRetain(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpchain")
	tk.MustExec("CREATE USER dpchain IDENTIFIED BY 'p1'")

	tk.MustExec("ALTER USER dpchain IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")
	sec1 := tk.MustQuery("SELECT JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.additional_password')) FROM mysql.user WHERE User='dpchain'").Rows()[0][0]

	tk.MustExec("ALTER USER dpchain IDENTIFIED BY 'p3' RETAIN CURRENT PASSWORD")
	sec2 := tk.MustQuery("SELECT JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.additional_password')) FROM mysql.user WHERE User='dpchain'").Rows()[0][0]

	// Secondary must differ between the two RETAINs (primary just before each call).
	require.NotEqual(t, sec1, sec2)

	// p2 (primary just before the second RETAIN) must now authenticate.
	require.NoError(t, authAs(t, tk, "dpchain", "%", "p2"))
	require.NoError(t, authAs(t, tk, "dpchain", "%", "p3"))
	// p1 (pre-first-RETAIN) must not — it was displaced from the secondary slot.
	require.Error(t, authAs(t, tk, "dpchain", "%", "p1"))
}

// TestDualPasswordAlterWithoutRetainPreservesSecondary mirrors MySQL's DDL
// Test 1 tail: an ALTER USER that does not specify RETAIN (and does not change
// the plugin) must leave any existing secondary password alone.
func TestDualPasswordAlterWithoutRetainPreservesSecondary(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpkeep")
	tk.MustExec("CREATE USER dpkeep IDENTIFIED BY 'p1'")
	tk.MustExec("ALTER USER dpkeep IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")

	before := tk.MustQuery("SELECT JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.additional_password')) FROM mysql.user WHERE User='dpkeep'").Rows()[0][0]
	require.NotEmpty(t, before)

	// ALTER without RETAIN, same plugin — secondary must survive.
	tk.MustExec("ALTER USER dpkeep IDENTIFIED BY 'p3'")
	after := tk.MustQuery("SELECT JSON_UNQUOTE(JSON_EXTRACT(user_attributes, '$.additional_password')) FROM mysql.user WHERE User='dpkeep'").Rows()[0][0]
	require.Equal(t, before, after, "secondary password must be preserved when ALTER does not specify RETAIN and keeps the same plugin")

	// p1 (the original secondary) still authenticates; p3 is the new primary.
	require.NoError(t, authAs(t, tk, "dpkeep", "%", "p1"))
	require.NoError(t, authAs(t, tk, "dpkeep", "%", "p3"))
}

// TestDualPasswordRenameUserPreservesSecondary mirrors MySQL's DDL Test 4.
// user_attributes travel with the row during RENAME USER; the renamed user
// should still be able to log in with either password.
func TestDualPasswordRenameUserPreservesSecondary(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dprename_a, dprename_b")
	tk.MustExec("CREATE USER dprename_a IDENTIFIED BY 'p1'")
	tk.MustExec("ALTER USER dprename_a IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")

	tk.MustExec("RENAME USER dprename_a TO dprename_b")

	// Secondary must still be present on the renamed user.
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User='dprename_b'").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT count(*) FROM mysql.user WHERE User='dprename_a'").Check(testkit.Rows("0"))

	// Both passwords still authenticate under the new name.
	require.NoError(t, authAs(t, tk, "dprename_b", "%", "p1"))
	require.NoError(t, authAs(t, tk, "dprename_b", "%", "p2"))
}

// TestDualPasswordDropUserRemovesSecondary mirrors MySQL's DDL Test 5. Dropping
// a user removes the entire row, taking user_attributes with it; this test
// simply pins that invariant against any future soft-delete refactor.
func TestDualPasswordDropUserRemovesSecondary(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpdropme")
	tk.MustExec("CREATE USER dpdropme IDENTIFIED BY 'p1'")
	tk.MustExec("ALTER USER dpdropme IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")

	// Sanity: secondary was set.
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User='dpdropme'").Check(testkit.Rows("1"))

	tk.MustExec("DROP USER dpdropme")

	// No row left at all.
	tk.MustQuery("SELECT count(*) FROM mysql.user WHERE User='dpdropme'").Check(testkit.Rows("0"))
}

// TestDualPasswordMultiUserAlter mirrors MySQL's per-user auth option
// semantics: RETAIN CURRENT PASSWORD / DISCARD OLD PASSWORD applies only to
// the user spec it follows, not to every spec in the ALTER USER statement.
func TestDualPasswordMultiUserAlter(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpm1, dpm2, dpm3")
	tk.MustExec("CREATE USER dpm1 IDENTIFIED BY 'p1', dpm2 IDENTIFIED BY 'q1', dpm3 IDENTIFIED BY 'r1'")

	// RETAIN follows only dpm3 here. dpm1 changes primary password without
	// retaining p1 as a secondary.
	tk.MustExec("ALTER USER dpm1 IDENTIFIED BY 'p2', dpm3 IDENTIFIED BY 'r2' RETAIN CURRENT PASSWORD")
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User IN ('dpm1', 'dpm3') ORDER BY User").Check(testkit.Rows("0", "1"))
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') FROM mysql.user WHERE User='dpm2'").Check(testkit.Rows("<nil>"))
	require.Error(t, authAs(t, tk, "dpm1", "%", "p1"))
	require.NoError(t, authAs(t, tk, "dpm1", "%", "p2"))
	require.NoError(t, authAs(t, tk, "dpm3", "%", "r1"))
	require.NoError(t, authAs(t, tk, "dpm3", "%", "r2"))

	// RETAIN follows only dpm1 here. dpm3 changes primary without RETAIN, so its
	// existing secondary r1 is preserved and old primary r2 is not retained.
	tk.MustExec("ALTER USER dpm1 IDENTIFIED BY 'p3' RETAIN CURRENT PASSWORD, dpm3 IDENTIFIED BY 'r3'")
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User IN ('dpm1', 'dpm3') ORDER BY User").Check(testkit.Rows("1", "1"))
	require.NoError(t, authAs(t, tk, "dpm1", "%", "p2"))
	require.NoError(t, authAs(t, tk, "dpm1", "%", "p3"))
	require.Error(t, authAs(t, tk, "dpm1", "%", "p1"))
	require.NoError(t, authAs(t, tk, "dpm3", "%", "r1"))
	require.NoError(t, authAs(t, tk, "dpm3", "%", "r3"))
	require.Error(t, authAs(t, tk, "dpm3", "%", "r2"))

	// DISCARD follows only dpm3 here. dpm1 keeps its secondary.
	tk.MustExec("ALTER USER dpm1, dpm3 DISCARD OLD PASSWORD")
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User IN ('dpm1', 'dpm3') ORDER BY User").Check(testkit.Rows("1", "0"))
	require.NoError(t, authAs(t, tk, "dpm1", "%", "p2"))
	require.Error(t, authAs(t, tk, "dpm3", "%", "r1"))

	// DISCARD can also appear before the comma and applies only to dpm1.
	tk.MustExec("ALTER USER dpm1 DISCARD OLD PASSWORD, dpm3")
	tk.MustQuery("SELECT count(*) FROM mysql.user WHERE User IN ('dpm1', 'dpm3') AND JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL").Check(testkit.Rows("0"))
	require.Error(t, authAs(t, tk, "dpm1", "%", "p2"))
}

// TestDualPasswordSelfServiceDiscardWithExtraOptionsStillGated guards the
// self-service-bypass surface: a caller may discard their own secondary
// password without extra privilege, but appending other ALTER USER options
// (REQUIRE / RESOURCE / PASSWORD EXPIRE / ATTRIBUTE / RESOURCE GROUP) must
// re-impose the standard CREATE USER check. Otherwise low-priv users could
// piggy-back metadata/TLS/resource changes onto a DISCARD.
func TestDualPasswordSelfServiceDiscardWithExtraOptionsStillGated(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpguard")
	tk.MustExec("CREATE USER dpguard IDENTIFIED BY 'p1'")
	tk.MustExec("GRANT USAGE ON *.* TO dpguard")
	// dpguard holds APPLICATION_PASSWORD_ADMIN (so self-service dual-password is
	// allowed) but NOT CREATE USER (so any extra privileged option must be
	// denied).
	tk.MustExec("GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO dpguard")
	// Plant a secondary password so DISCARD has something to remove.
	tk.MustExec("ALTER USER dpguard IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")

	selfTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, selfTK.Session().Auth(&auth.UserIdentity{Username: "dpguard", Hostname: "%"}, sha1Password("p2"), nil, nil))

	// Standalone self-service DISCARD: allowed with APPLICATION_PASSWORD_ADMIN.
	selfTK.MustExec("ALTER USER 'dpguard'@'%' DISCARD OLD PASSWORD")

	// Plant another secondary so the next DISCARD has something to remove.
	tk.MustExec("ALTER USER dpguard IDENTIFIED BY 'p3' RETAIN CURRENT PASSWORD")
	selfTK = testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, selfTK.Session().Auth(&auth.UserIdentity{Username: "dpguard", Hostname: "%"}, sha1Password("p3"), nil, nil))

	// DISCARD combined with ACCOUNT LOCK must still require CREATE USER —
	// APPLICATION_PASSWORD_ADMIN does not authorize the extra option.
	err := selfTK.ExecToErr("ALTER USER 'dpguard'@'%' DISCARD OLD PASSWORD ACCOUNT LOCK")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE USER")

	// DISCARD with REQUIRE NONE must still require CREATE USER.
	err = selfTK.ExecToErr("ALTER USER 'dpguard'@'%' DISCARD OLD PASSWORD REQUIRE NONE")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE USER")

	// DISCARD with PASSWORD EXPIRE must still require CREATE USER.
	err = selfTK.ExecToErr("ALTER USER 'dpguard'@'%' DISCARD OLD PASSWORD PASSWORD EXPIRE")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE USER")
}

// TestDualPasswordLegacyEmptyPluginRejectsLDAPDefault guards the capability
// check: a legacy mysql.user row whose `plugin` column is empty resolves to
// `default_authentication_plugin`. When that default is an LDAP plugin (not
// dual-password capable), an explicit ALTER USER ... RETAIN CURRENT PASSWORD
// must be rejected before the plugin-change comparison sees both sides as
// equal.
func TestDualPasswordLegacyEmptyPluginRejectsLDAPDefault(t *testing.T) {
	tk := rootTK(t)
	prevDefault := tk.MustQuery("SELECT @@global.default_authentication_plugin").Rows()[0][0]
	defer tk.MustExec(fmt.Sprintf("SET GLOBAL default_authentication_plugin = '%s'", prevDefault))

	tk.MustExec("SET GLOBAL default_authentication_plugin = 'authentication_ldap_simple'")

	tk.MustExec("DROP USER IF EXISTS dplegacy_ldap")
	tk.MustExec("CREATE USER dplegacy_ldap IDENTIFIED BY 'p1'")
	// Simulate a legacy row by clearing the plugin column; with the LDAP
	// default in place the row now resolves to authentication_ldap_simple.
	tk.MustExec("UPDATE mysql.user SET plugin = '' WHERE User = 'dplegacy_ldap'")
	tk.MustExec("FLUSH PRIVILEGES")

	// RETAIN with the default-plugin form must be rejected since the row's
	// effective plugin is LDAP (not dual-password capable). Before this
	// fix, the raw empty-plugin string slipped through
	// isDualPasswordCapablePlugin's `""` case and RETAIN was wrongly
	// accepted.
	err := tk.ExecToErr("ALTER USER dplegacy_ldap IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Dual password is not supported")
}

// TestDualPasswordSecondaryLoginWithEmptyPrimary guards the auth-path fix where
// the dual-password fallback was unreachable when the primary
// authentication_string is empty. After RETAIN then blanking the primary, the
// retained secondary must still authenticate.
func TestDualPasswordSecondaryLoginWithEmptyPrimary(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpemptyfb")
	tk.MustExec("CREATE USER dpemptyfb IDENTIFIED BY 'primary1'")
	// RETAIN: primary='primary2', secondary='primary1'.
	tk.MustExec("ALTER USER dpemptyfb IDENTIFIED BY 'primary2' RETAIN CURRENT PASSWORD")
	// Blank the primary (same plugin, no DISCARD) — secondary is preserved.
	tk.MustExec("ALTER USER dpemptyfb IDENTIFIED BY ''")
	tk.MustQuery("SELECT authentication_string = '' FROM mysql.user WHERE User = 'dpemptyfb'").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User = 'dpemptyfb'").Check(testkit.Rows("1"))

	// The retained secondary still authenticates despite the empty primary.
	require.NoError(t, authAs(t, tk, "dpemptyfb", "%", "primary1"))
	// The overwritten primary no longer authenticates.
	require.Error(t, authAs(t, tk, "dpemptyfb", "%", "primary2"))
	// Passwordless login (empty primary, empty client auth) must still succeed
	// even though a retained secondary is present — the dual-password fallback
	// must not hijack the no-password success path. A real passwordless client
	// sends empty authentication bytes, so pass nil here (sha1Password("") would
	// instead send a non-empty scramble of the empty string).
	sub := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, sub.Session().Auth(&auth.UserIdentity{Username: "dpemptyfb", Hostname: "%"}, nil, nil, nil))
}

// TestDualPasswordDiscardNoopOnIncapablePlugin guards that DISCARD OLD PASSWORD
// is a harmless no-op (no error) on a non-password plugin, matching MySQL,
// rather than being rejected by the dual-password capability gate (which now
// only applies to RETAIN).
func TestDualPasswordDiscardNoopOnIncapablePlugin(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpldap")
	tk.MustExec("CREATE USER dpldap IDENTIFIED WITH authentication_ldap_simple AS 'uid=x,ou=People,dc=example,dc=com'")
	// DISCARD on an LDAP account is accepted and changes nothing (no secondary).
	tk.MustExec("ALTER USER dpldap DISCARD OLD PASSWORD")
	tk.MustQuery("SELECT user_attributes IS NULL FROM mysql.user WHERE User = 'dpldap'").Check(testkit.Rows("1"))
}

// TestDualPasswordDiscardCollapsesEmptyAttributesToNull guards the {} -> NULL
// fix: after DISCARD removes the only attribute, user_attributes must collapse
// to NULL rather than being left as a literal empty object '{}'.
func TestDualPasswordDiscardCollapsesEmptyAttributesToNull(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpnull")
	tk.MustExec("CREATE USER dpnull IDENTIFIED BY 'p1'")
	// DISCARD with no secondary present must not leave a literal '{}'.
	tk.MustExec("ALTER USER dpnull DISCARD OLD PASSWORD")
	tk.MustQuery("SELECT user_attributes IS NULL FROM mysql.user WHERE User = 'dpnull'").Check(testkit.Rows("1"))

	// RETAIN then DISCARD on a row whose only attribute was additional_password
	// also collapses back to NULL rather than '{}'.
	tk.MustExec("ALTER USER dpnull IDENTIFIED BY 'p2' RETAIN CURRENT PASSWORD")
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User = 'dpnull'").Check(testkit.Rows("1"))
	tk.MustExec("ALTER USER dpnull DISCARD OLD PASSWORD")
	tk.MustQuery("SELECT user_attributes IS NULL FROM mysql.user WHERE User = 'dpnull'").Check(testkit.Rows("1"))
}

// TestDualPasswordSelfSetPasswordRetainAcceptsMysqlUpdate guards that the self
// SET PASSWORD ... RETAIN CURRENT PASSWORD gate accepts the UPDATE privilege on
// the mysql schema as a superset authority, matching executeAlterUser's
// needAdminPrivCheck (a user with UPDATE on mysql.* but no CREATE USER /
// APPLICATION_PASSWORD_ADMIN must not be wrongly denied).
func TestDualPasswordSelfSetPasswordRetainAcceptsMysqlUpdate(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpupd")
	tk.MustExec("CREATE USER dpupd IDENTIFIED BY 'u1'")
	tk.MustExec("GRANT UPDATE ON mysql.* TO dpupd")

	selfTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, selfTK.Session().Auth(&auth.UserIdentity{Username: "dpupd", Hostname: "%"}, sha1Password("u1"), nil, nil))
	selfTK.MustExec("SET PASSWORD FOR 'dpupd'@'%' = 'u2' RETAIN CURRENT PASSWORD")

	require.NoError(t, authAs(t, tk, "dpupd", "%", "u1"))
	require.NoError(t, authAs(t, tk, "dpupd", "%", "u2"))
}

// TestDualPasswordAlterUserUserResolvesAuthUsername guards pingcap/tidb#68937:
// ALTER USER USER() must operate on the AUTHENTICATED account
// (AuthUsername/AuthHostname), not the claimed Username, so a proxy/mapped
// login cannot target the wrong mysql.user row.
func TestDualPasswordAlterUserUserResolvesAuthUsername(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dplogin, dpauth")
	tk.MustExec("CREATE USER dplogin IDENTIFIED BY 'loginpw'")
	tk.MustExec("CREATE USER dpauth IDENTIFIED BY 'authpw'")

	// Authenticate as dpauth (sets AuthUsername=dpauth), then simulate a
	// proxy/mapped login by overriding only the claimed Username to dplogin.
	sub := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, sub.Session().Auth(&auth.UserIdentity{Username: "dpauth", Hostname: "%"}, sha1Password("authpw"), nil, nil))
	sub.Session().GetSessionVars().User.Username = "dplogin"

	// USER() must resolve to the authenticated account (dpauth), not dplogin.
	sub.MustExec("ALTER USER USER() IDENTIFIED BY 'newauthpw'")

	// dpauth's password changed; dplogin is untouched.
	require.NoError(t, authAs(t, tk, "dpauth", "%", "newauthpw"))
	require.Error(t, authAs(t, tk, "dpauth", "%", "authpw"))
	require.NoError(t, authAs(t, tk, "dplogin", "%", "loginpw"))
}

// TestDualPasswordAlterUserUserRetainAndDiscard mirrors MySQL's
// multiple_passwords_ddl.test Test 13: the USER() form of ALTER USER accepts
// RETAIN CURRENT PASSWORD and DISCARD OLD PASSWORD, is gated by
// APPLICATION_PASSWORD_ADMIN like any other self-service dual-password
// change, and operates on the caller's own account.
func TestDualPasswordAlterUserUserRetainAndDiscard(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpuserfn")
	tk.MustExec("CREATE USER dpuserfn IDENTIFIED BY 'u1'")

	selfTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, selfTK.Session().Auth(&auth.UserIdentity{Username: "dpuserfn", Hostname: "%"}, sha1Password("u1"), nil, nil))

	// Without APPLICATION_PASSWORD_ADMIN, both USER() dual-password forms are denied.
	err := selfTK.ExecToErr("ALTER USER USER() IDENTIFIED BY 'u2' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "APPLICATION_PASSWORD_ADMIN")
	err = selfTK.ExecToErr("ALTER USER USER() DISCARD OLD PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "APPLICATION_PASSWORD_ADMIN")
	// The denied statements must not have changed the password.
	require.NoError(t, authAs(t, tk, "dpuserfn", "%", "u1"))
	require.Error(t, authAs(t, tk, "dpuserfn", "%", "u2"))

	// With APPLICATION_PASSWORD_ADMIN, USER() ... RETAIN rotates and keeps the old password.
	tk.MustExec("GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO dpuserfn")
	selfTK = testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, selfTK.Session().Auth(&auth.UserIdentity{Username: "dpuserfn", Hostname: "%"}, sha1Password("u1"), nil, nil))
	selfTK.MustExec("ALTER USER USER() IDENTIFIED BY 'u2' RETAIN CURRENT PASSWORD")

	require.NoError(t, authAs(t, tk, "dpuserfn", "%", "u2"))
	require.NoError(t, authAs(t, tk, "dpuserfn", "%", "u1"))
	rows := tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') IS NOT NULL FROM mysql.user WHERE User = 'dpuserfn'").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "1", rows[0][0])

	// USER() ... DISCARD OLD PASSWORD drops the secondary.
	selfTK.MustExec("ALTER USER USER() DISCARD OLD PASSWORD")
	require.NoError(t, authAs(t, tk, "dpuserfn", "%", "u2"))
	require.Error(t, authAs(t, tk, "dpuserfn", "%", "u1"))
	tk.MustQuery("SELECT JSON_EXTRACT(user_attributes, '$.additional_password') FROM mysql.user WHERE User = 'dpuserfn'").Check(testkit.Rows("<nil>"))
}

// TestDualPasswordSelfRetainWithExplicitSamePlugin covers the reviewer-raised
// case that equivalent self-service syntax must not require different
// privileges: an APPLICATION_PASSWORD_ADMIN-only user spelling the account's
// CURRENT plugin explicitly (`IDENTIFIED WITH mysql_native_password BY ...
// RETAIN CURRENT PASSWORD`) is still self-service. A real plugin change stays
// admin-gated.
func TestDualPasswordSelfRetainWithExplicitSamePlugin(t *testing.T) {
	tk := rootTK(t)

	tk.MustExec("DROP USER IF EXISTS dpsameplug")
	tk.MustExec("CREATE USER dpsameplug IDENTIFIED WITH mysql_native_password BY 'p1'")
	tk.MustExec("GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO dpsameplug")

	selfTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, selfTK.Session().Auth(&auth.UserIdentity{Username: "dpsameplug", Hostname: "%"}, sha1Password("p1"), nil, nil))

	// Explicit same plugin == no plugin change: allowed with
	// APPLICATION_PASSWORD_ADMIN only (no CREATE USER).
	selfTK.MustExec("ALTER USER 'dpsameplug'@'%' IDENTIFIED WITH mysql_native_password BY 'p2' RETAIN CURRENT PASSWORD")
	require.NoError(t, authAs(t, tk, "dpsameplug", "%", "p1"))
	require.NoError(t, authAs(t, tk, "dpsameplug", "%", "p2"))

	// Explicit same plugin DISCARD is self-service too.
	selfTK.MustExec("ALTER USER 'dpsameplug'@'%' IDENTIFIED WITH mysql_native_password BY 'p3' RETAIN CURRENT PASSWORD")
	selfTK.MustExec("ALTER USER 'dpsameplug'@'%' DISCARD OLD PASSWORD")
	require.Error(t, authAs(t, tk, "dpsameplug", "%", "p2"))
	require.NoError(t, authAs(t, tk, "dpsameplug", "%", "p3"))

	// A REAL plugin change is not self-service: without CREATE USER the
	// admin gate rejects it before any dual-password validation.
	err := selfTK.ExecToErr("ALTER USER 'dpsameplug'@'%' IDENTIFIED WITH caching_sha2_password BY 'p4' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE USER")
	// Nothing changed.
	require.NoError(t, authAs(t, tk, "dpsameplug", "%", "p3"))
}

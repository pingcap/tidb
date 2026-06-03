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
	tk.MustExec("GRANT APPLICATION_PASSWORD_ADMIN ON *.* TO dpaponly")

	// MySQL-compatible rule: cross-user secondary-password operations require
	// CREATE USER. APPLICATION_PASSWORD_ADMIN alone is for own-account use.
	createTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, createTK.Session().Auth(&auth.UserIdentity{Username: "dpcreate", Hostname: "%"}, sha1Password("a1"), nil, nil))
	createTK.MustExec("ALTER USER dpvictim_create IDENTIFIED BY 'v2' RETAIN CURRENT PASSWORD")

	require.NoError(t, authAs(t, tk, "dpvictim_create", "%", "v1"))
	require.NoError(t, authAs(t, tk, "dpvictim_create", "%", "v2"))

	createTK.MustExec("ALTER USER dpvictim_create DISCARD OLD PASSWORD")
	require.Error(t, authAs(t, tk, "dpvictim_create", "%", "v1"))
	require.NoError(t, authAs(t, tk, "dpvictim_create", "%", "v2"))

	apOnlyTK := testkit.NewTestKit(t, tk.Session().GetStore())
	require.NoError(t, apOnlyTK.Session().Auth(&auth.UserIdentity{Username: "dpaponly", Hostname: "%"}, sha1Password("a1"), nil, nil))
	err := apOnlyTK.ExecToErr("ALTER USER dpvictim_ap IDENTIFIED BY 'v2' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE USER")
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

	// MySQL-compatible rule: own-account secondary-password operations require
	// APPLICATION_PASSWORD_ADMIN or CREATE USER.
	err := selfTK.ExecToErr("SET PASSWORD FOR 'dpself'@'%' = 's2' RETAIN CURRENT PASSWORD")
	require.Error(t, err)
	require.Contains(t, err.Error(), "CREATE USER or APPLICATION_PASSWORD_ADMIN")

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

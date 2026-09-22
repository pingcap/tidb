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

package privileges_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestSelectConstantRequiresTablePrivilege verifies that reading a table in a
// SELECT requires a SELECT privilege on that table even when the statement does
// not reference any of its columns. Otherwise `SELECT 1 FROM t` would leak the
// row count and act as a table-existence oracle for a user without any grant.
func TestSelectConstantRequiresTablePrivilege(t *testing.T) {
	store := createStoreAndPrepareDB(t)
	rootTk := testkit.NewTestKit(t, store)
	rootTk.MustExec("CREATE DATABASE leakdb")
	rootTk.MustExec("CREATE TABLE leakdb.t (a int, b int)")
	rootTk.MustExec("INSERT INTO leakdb.t VALUES (1, 1), (2, 2), (3, 3)")
	rootTk.MustExec("CREATE USER 'nopriv'@'%'")
	// Only the default USAGE global privilege; nothing on leakdb.t.
	rootTk.MustExec("GRANT USAGE ON *.* TO 'nopriv'@'%'")

	userTk := testkit.NewTestKit(t, store)
	require.NoError(t, userTk.Session().Auth(&auth.UserIdentity{
		Username: "nopriv", Hostname: "%", AuthUsername: "nopriv", AuthHostname: "%",
	}, nil, nil, nil))

	// No column is referenced at all. These must be denied with a table-level
	// error, otherwise the row count leaks.
	userTk.MustGetErrCode("SELECT 1 FROM leakdb.t", errno.ErrTableaccessDenied)
	userTk.MustGetErrCode("SELECT 1 FROM leakdb.t LIMIT 1", errno.ErrTableaccessDenied)
	userTk.MustGetErrCode("SELECT COUNT(*) FROM leakdb.t", errno.ErrTableaccessDenied)
	userTk.MustGetErrCode("SELECT * FROM leakdb.t", errno.ErrTableaccessDenied)
	// The fallback is checked first when the user has no SELECT privilege.
	userTk.MustGetErrCode("SELECT a FROM leakdb.t", errno.ErrTableaccessDenied)
	userTk.MustGetErrCode("SELECT 1 FROM leakdb.t WHERE a > 0", errno.ErrTableaccessDenied)
	// The keyword case must not matter for COUNT, and the point-get fast path
	// must not bypass the check either.
	userTk.MustGetErrCode("select count(*) from leakdb.t", errno.ErrTableaccessDenied)
	rootTk.MustExec("CREATE TABLE leakdb.tpk (id int primary key, v int)")
	rootTk.MustExec("INSERT INTO leakdb.tpk VALUES (1, 10)")
	require.Error(t, userTk.ExecToErr("SELECT 1 FROM leakdb.tpk WHERE id = 1"))
	require.Error(t, userTk.ExecToErr("SELECT COUNT(*) FROM leakdb.tpk"))
	// Unknown schemas/tables are still denied.
	userTk.MustGetErrCode("SELECT 1 FROM leakdb.nonexistent", errno.ErrTableaccessDenied)
	userTk.MustGetErrCode("SELECT 1 FROM nodb.t", errno.ErrTableaccessDenied)

	// A column-level SELECT privilege on a single column must still be enough to
	// read that column, and (like MySQL) `SELECT <constant>` / `COUNT(*)` must
	// be satisfiable by any column privilege.
	rootTk.MustExec("GRANT SELECT(a) ON leakdb.t TO 'nopriv'@'%'")
	userTk.MustQuery("SELECT a FROM leakdb.t").Check(testkit.Rows("1", "2", "3"))
	userTk.MustQuery("SELECT 1 FROM leakdb.t").Check(testkit.Rows("1", "1", "1"))
	userTk.MustQuery("SELECT COUNT(*) FROM leakdb.t").Check(testkit.Rows("3"))
	userTk.MustGetErrCode("SELECT b FROM leakdb.t", errno.ErrColumnaccessDenied)

	// A write target does not need SELECT for a constant update/delete, but a
	// second table in multi-table DML is still a read source and needs SELECT.
	rootTk.MustExec("CREATE TABLE leakdb.mt1 (a int)")
	rootTk.MustExec("CREATE TABLE leakdb.mt2 (a int)")
	rootTk.MustExec("INSERT INTO leakdb.mt1 VALUES (1)")
	rootTk.MustExec("INSERT INTO leakdb.mt2 VALUES (1)")
	rootTk.MustExec("GRANT UPDATE(a), DELETE ON leakdb.mt1 TO 'nopriv'@'%'")
	userTk.MustExec("UPDATE leakdb.mt1 SET a = 2")
	userTk.MustGetErrCode("UPDATE leakdb.mt1, leakdb.mt2 SET mt1.a = 1", errno.ErrTableaccessDenied)
	userTk.MustGetErrCode("DELETE mt1 FROM leakdb.mt1, leakdb.mt2", errno.ErrTableaccessDenied)
	userTk.MustGetErrCode("DELETE FROM mt1 USING leakdb.mt1, leakdb.mt2", errno.ErrTableaccessDenied)
}

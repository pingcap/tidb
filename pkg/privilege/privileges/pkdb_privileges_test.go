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

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/stretchr/testify/require"
)

func TestAdminViewPrivilege(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	rootTk := testkit.NewTestKit(t, store)
	tk := testkit.NewTestKit(t, store)
	rootTk.MustExec("use test")
	rootTk.MustExec(`DROP USER IF EXISTS av_none, av_direct, av_role_user`)
	rootTk.MustExec(`DROP ROLE IF EXISTS av_role`)
	rootTk.MustExec(`CREATE USER av_none, av_direct, av_role_user`)
	rootTk.MustExec(`CREATE ROLE av_role`)
	rootTk.MustExec(`GRANT ADMIN_VIEW ON *.* TO av_direct`)
	rootTk.MustExec(`GRANT ADMIN_VIEW ON *.* TO av_role`)
	rootTk.MustExec(`GRANT av_role TO av_role_user`)
	rootTk.MustExec(`CREATE TABLE IF NOT EXISTS t_admin_view (a int)`)

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "av_none", Hostname: "localhost", AuthUsername: "av_none", AuthHostname: "%"}, nil, nil, nil))
	err := tk.ExecToErr("ADMIN SHOW DDL")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied))
	require.Regexp(t, "you need \\(at least one of\\) the SUPER or ADMIN_VIEW privilege\\(s\\) for this operation$", err.Error())
	err = tk.ExecToErr("ADMIN SHOW DDL JOBS 20")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied))
	err = tk.ExecToErr("ADMIN SHOW DDL JOB QUERIES LIMIT 1")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied))

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "av_direct", Hostname: "localhost", AuthUsername: "av_direct", AuthHostname: "%"}, nil, nil, nil))
	tk.MustQuery("ADMIN SHOW DDL")
	tk.MustQuery("ADMIN SHOW DDL JOBS 20")
	tk.MustQuery("ADMIN SHOW DDL JOB QUERIES 1")
	tk.MustQuery("ADMIN SHOW DDL JOB QUERIES LIMIT 1")
	err = tk.ExecToErr("ADMIN CANCEL DDL JOBS 1")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPrivilegeCheckFail))
	err = tk.ExecToErr("ADMIN PAUSE DDL JOBS 1")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPrivilegeCheckFail))
	err = tk.ExecToErr("ADMIN RESUME DDL JOBS 1")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrPrivilegeCheckFail))
	err = tk.ExecToErr("ALTER TABLE t_admin_view ADD INDEX idx_a(a)")
	require.Error(t, err)
	tk.MustQuery(`SHOW GRANTS`).Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'av_direct'@'%'",
		"GRANT ADMIN_VIEW ON *.* TO 'av_direct'@'%'",
	))

	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "av_role_user", Hostname: "localhost", AuthUsername: "av_role_user", AuthHostname: "%"}, nil, nil, nil))
	err = tk.ExecToErr("ADMIN SHOW DDL")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied))
	tk.MustExec("SET ROLE av_role")
	tk.MustQuery("ADMIN SHOW DDL")
	tk.MustQuery("ADMIN SHOW DDL JOB QUERIES LIMIT 1")

	rootTk.MustExec(`REVOKE ADMIN_VIEW ON *.* FROM av_direct`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "av_direct", Hostname: "localhost", AuthUsername: "av_direct", AuthHostname: "%"}, nil, nil, nil))
	err = tk.ExecToErr("ADMIN SHOW DDL")
	require.True(t, terror.ErrorEqual(err, plannererrors.ErrSpecificAccessDenied))

	rootTk.MustExec(`DROP TABLE t_admin_view`)
	rootTk.MustExec(`DROP USER av_none, av_direct, av_role_user`)
	rootTk.MustExec(`DROP ROLE av_role`)
}

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

func TestImportJobViewPrivilege(t *testing.T) {
	store := createStoreAndPrepareDB(t)

	rootTk := testkit.NewTestKit(t, store)
	require.NoError(t, rootTk.Session().Auth(&auth.UserIdentity{
		Username:     "root",
		Hostname:     "localhost",
		AuthUsername: "root",
		AuthHostname: "%",
	}, nil, nil, nil))
	rootTk.MustExec(`SET @@time_zone = '+00:00'`)
	rootTk.MustExec(`CREATE USER job_owner1, job_owner2, import_view_user, import_view_role_user`)
	rootTk.MustExec(`CREATE ROLE import_view_role`)
	rootTk.MustExec(`GRANT IMPORT_JOB_VIEW ON *.* TO import_view_user`)
	rootTk.MustExec(`GRANT IMPORT_JOB_VIEW ON *.* TO import_view_role`)
	rootTk.MustExec(`GRANT import_view_role TO import_view_role_user`)
	rootTk.MustExec(`
		INSERT INTO mysql.tidb_import_jobs
			(id, create_time, start_time, update_time, end_time, table_schema, table_name, table_id,
			 created_by, parameters, source_file_size, status, step, summary, error_message)
		VALUES
			(900001, '2024-01-02 03:04:05.123456', '2024-01-02 03:04:06.123456', '2024-01-02 03:04:07.123456',
			 '2024-01-02 03:04:08.123456', 'test', 't1', 101, 'job_owner1@%', '{"file-location":"s3://bucket/job1.csv","format":"csv"}',
			 128, 'finished', '', '{"row-count":2}', ''),
			(900002, '2024-01-03 04:05:06.123456', '2024-01-03 04:05:07.123456', '2024-01-03 04:05:08.123456',
			 '2024-01-03 04:05:09.123456', 'test', 't2', 102, 'job_owner2@%', '{"file-location":"s3://bucket/job2.csv","format":"csv"}',
			 256, 'failed', '', NULL, 'import failed')
	`)

	tk := testkit.NewTestKit(t, store)
	authAs := func(user string) {
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{
			Username:     user,
			Hostname:     "localhost",
			AuthUsername: user,
			AuthHostname: "%",
		}, nil, nil, nil))
		tk.MustExec(`SET @@time_zone = '+00:00'`)
	}

	// privilege metadata
	privRows := rootTk.MustQuery(`SHOW PRIVILEGES`).Rows()
	var foundImportJobView bool
	for _, row := range privRows {
		if row[0] == "IMPORT_JOB_VIEW" {
			foundImportJobView = true
			require.Equal(t, "Server Admin", row[1])
			require.Equal(t, "", row[2])
		}
	}
	require.True(t, foundImportJobView)
	rootTk.MustQuery(`SHOW GRANTS FOR import_view_user`).Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'import_view_user'@'%'",
		"GRANT IMPORT_JOB_VIEW ON *.* TO 'import_view_user'@'%'",
	))

	// owner only
	authAs("job_owner1")
	rows := tk.MustQuery(`SHOW IMPORT JOBS`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "900001", rows[0][0])
	require.Equal(t, "job_owner1@%", rows[0][12])
	err := tk.QueryToErr(`SHOW IMPORT JOB 900002`)
	require.ErrorIs(t, err, plannererrors.ErrSpecificAccessDenied)
	require.Regexp(t, "SUPER or IMPORT_JOB_VIEW", err.Error())

	// direct grant
	authAs("import_view_user")
	rows = tk.MustQuery(`SHOW IMPORT JOBS`).Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "900001", rows[0][0])
	require.Equal(t, "900002", rows[1][0])
	rows = tk.MustQuery(`SHOW IMPORT JOB 900002`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "900002", rows[0][0])
	require.Equal(t, "job_owner2@%", rows[0][12])
	err = tk.ExecToErr(`CANCEL IMPORT JOB 900002`)
	require.ErrorIs(t, err, plannererrors.ErrSpecificAccessDenied)

	// role grant
	authAs("import_view_role_user")
	err = tk.QueryToErr(`SHOW IMPORT JOB 900002`)
	require.ErrorIs(t, err, plannererrors.ErrSpecificAccessDenied)
	tk.MustExec(`SET ROLE import_view_role`)
	rows = tk.MustQuery(`SHOW IMPORT JOB 900002`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "900002", rows[0][0])

	// revoke grant
	rootTk.MustExec(`REVOKE IMPORT_JOB_VIEW ON *.* FROM import_view_user`)
	rootTk.MustQuery(`SHOW GRANTS FOR import_view_user`).Check(testkit.Rows(
		"GRANT USAGE ON *.* TO 'import_view_user'@'%'",
	))
	authAs("import_view_user")
	rows = tk.MustQuery(`SHOW IMPORT JOBS`).Rows()
	require.Len(t, rows, 0)
}

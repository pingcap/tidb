// Copyright 2025 PingCAP, Inc.
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

package importintotest

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func (s *mockGCSSuite) TestImportSDK() {
	ctx := context.Background()

	// 1. Prepare source data (local files)
	tmpDir := s.T().TempDir()

	// Create schema files for CreateSchemasAndTables
	// DB schema
	err := os.WriteFile(filepath.Join(tmpDir, "importsdk_test-schema-create.sql"), []byte("CREATE DATABASE importsdk_test;"), 0644)
	s.NoError(err)
	// Table schema
	err = os.WriteFile(filepath.Join(tmpDir, "importsdk_test.t-schema.sql"), []byte("CREATE TABLE t (id int, v varchar(255));"), 0644)
	s.NoError(err)

	// Data file: mydumper format: {schema}.{table}.{seq}.csv
	fileName := "importsdk_test.t.001.csv"
	content := []byte("1,test1\n2,test2")
	err = os.WriteFile(filepath.Join(tmpDir, fileName), content, 0644)
	s.NoError(err)

	// 2. Prepare DB connection
	// Ensure clean state
	s.tk.MustExec("DROP DATABASE IF EXISTS importsdk_test")

	// Create a *sql.DB using the testkit driver
	db := testkit.CreateMockDB(s.tk)
	defer db.Close()

	// 3. Initialize SDK
	// Use local file path.
	sdk, err := importsdk.NewImportSDK(ctx, "file://"+tmpDir, db)
	s.NoError(err)
	defer sdk.Close()

	// 4. Test FileScanner.CreateSchemasAndTables
	err = sdk.CreateSchemasAndTables(ctx)
	s.NoError(err)
	// Verify DB and Table exist
	s.tk.MustExec("USE importsdk_test")
	s.tk.MustExec("SHOW CREATE TABLE t")

	// 5. Test FileScanner.GetTableMetas
	metas, err := sdk.GetTableMetas(ctx)
	s.NoError(err)
	s.Len(metas, 1)
	s.Equal("importsdk_test", metas[0].Database)
	s.Equal("t", metas[0].Table)
	s.Equal(int64(len(content)), metas[0].TotalSize)

	// 6. Test FileScanner.GetTableMetaByName
	meta, err := sdk.GetTableMetaByName(ctx, "importsdk_test", "t")
	s.NoError(err)
	s.Equal("importsdk_test", meta.Database)
	s.Equal("t", meta.Table)

	// 7. Test FileScanner.GetTotalSize
	totalSize := sdk.GetTotalSize(ctx)
	s.Equal(int64(len(content)), totalSize)

	// 8. Test SQLGenerator.GenerateImportSQL
	opts := &importsdk.ImportOptions{
		Thread:   4,
		Detached: true,
	}
	sql, err := sdk.GenerateImportSQL(meta, opts)
	s.NoError(err)
	s.Contains(sql, "IMPORT INTO `importsdk_test`.`t` FROM")
	s.Contains(sql, "THREAD=4")
	s.Contains(sql, "DETACHED")

	// 9. Test JobManager.SubmitJob
	jobID, err := sdk.SubmitJob(ctx, sql)
	s.NoError(err)
	s.Greater(jobID, int64(0))

	// 10. Test JobManager.GetJobStatus
	status, err := sdk.GetJobStatus(ctx, jobID)
	s.NoError(err)
	s.Equal(jobID, status.JobID)
	s.Equal("`importsdk_test`.`t`", status.TargetTable)

	// Wait for job to finish
	s.Eventually(func() bool {
		status, err := sdk.GetJobStatus(ctx, jobID)
		s.NoError(err)
		return status.Status == "finished"
	}, 30*time.Second, 500*time.Millisecond)

	// Verify data
	s.tk.MustQuery("SELECT * FROM importsdk_test.t").Check(testkit.Rows("1 test1", "2 test2"))

	// 11. Test JobManager.CancelJob with failpoint
	s.tk.MustExec("TRUNCATE TABLE t")
	require.NoError(s.T(), failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/importinto/syncBeforeJobStarted", "pause"))
	defer func() {
		require.NoError(s.T(), failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/syncBeforeJobStarted"))
	}()

	jobID2, err := sdk.SubmitJob(ctx, sql)
	s.NoError(err)

	// Cancel the job
	err = sdk.CancelJob(ctx, jobID2)
	s.NoError(err)

	// Unpause
	require.NoError(s.T(), failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/importinto/syncBeforeJobStarted"))

	// Verify status is cancelled
	s.Eventually(func() bool {
		status, err := sdk.GetJobStatus(ctx, jobID2)
		s.NoError(err)
		return status.Status == "cancelled"
	}, 30*time.Second, 500*time.Millisecond)

	// 12. Test JobManager.GetGroupSummary
	// Submit a job with GroupKey
	groupKey := "test_group_key"
	optsWithGroup := &importsdk.ImportOptions{
		Thread:   4,
		Detached: true,
		GroupKey: groupKey,
	}
	sqlWithGroup, err := sdk.GenerateImportSQL(meta, optsWithGroup)
	s.NoError(err)
	jobID3, err := sdk.SubmitJob(ctx, sqlWithGroup)
	s.NoError(err)
	s.Greater(jobID3, int64(0))

	// Wait for job to finish
	s.Eventually(func() bool {
		status, err := sdk.GetJobStatus(ctx, jobID3)
		s.NoError(err)
		return status.Status == "finished"
	}, 30*time.Second, 500*time.Millisecond)

	// Get group summary
	groupSummary, err := sdk.GetGroupSummary(ctx, groupKey)
	s.NoError(err)
	s.Equal(groupKey, groupSummary.GroupKey)
	s.Equal(int64(1), groupSummary.TotalJobs)
	s.Equal(int64(1), groupSummary.Completed)

	// 13. Test JobManager.GetJobsByGroup
	jobs, err := sdk.GetJobsByGroup(ctx, groupKey)
	s.NoError(err)
	s.Len(jobs, 1)
	s.Equal(jobID3, jobs[0].JobID)
	s.Equal("finished", jobs[0].Status)
}

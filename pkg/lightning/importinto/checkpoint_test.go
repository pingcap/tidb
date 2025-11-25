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

package importinto

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/stretchr/testify/require"
)

func TestFileCheckpointManager(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "checkpoints.json")

	mgr := NewFileCheckpointManager(filePath)
	ctx := context.Background()

	// Test Initialize
	err := mgr.Initialize(ctx)
	require.NoError(t, err)

	// Test Get - Empty
	cp, err := mgr.Get("db", "t1")
	require.NoError(t, err)
	require.Nil(t, cp)

	// Test Update
	cp1 := &TableCheckpoint{
		DBName:    "db",
		TableName: "t1",
		JobID:     1,
		Status:    CheckpointStatusRunning,
		GroupKey:  "g1",
	}
	err = mgr.Update(ctx, cp1)
	require.NoError(t, err)

	// Test Get - Found
	cp, err = mgr.Get("db", "t1")
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, cp1.JobID, cp.JobID)
	require.Equal(t, cp1.Status, cp.Status)

	// Test Persistence (New Manager)
	mgr2 := NewFileCheckpointManager(filePath)
	err = mgr2.Initialize(ctx)
	require.NoError(t, err)
	cp, err = mgr2.Get("db", "t1")
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, cp1.JobID, cp.JobID)

	// Test IgnoreError
	cp1.Status = CheckpointStatusFailed
	err = mgr.Update(ctx, cp1)
	require.NoError(t, err)

	err = mgr.IgnoreError(ctx, "db", "t1")
	require.NoError(t, err)
	cp, err = mgr.Get("db", "t1")
	require.NoError(t, err)
	require.Equal(t, CheckpointStatusPending, cp.Status)
	require.Equal(t, int64(0), cp.JobID)

	// Test DestroyError
	cp1.Status = CheckpointStatusFailed
	err = mgr.Update(ctx, cp1)
	require.NoError(t, err)

	err = mgr.DestroyError(ctx, "db", "t1")
	require.NoError(t, err)
	cp, err = mgr.Get("db", "t1")
	require.NoError(t, err)
	require.Nil(t, cp)

	// Test DumpTables
	var buf bytes.Buffer
	cp1.Status = CheckpointStatusRunning
	cp1.JobID = 1
	err = mgr.Update(ctx, cp1)
	require.NoError(t, err)
	err = mgr.DumpTables(ctx, &buf)
	require.NoError(t, err)
	require.Contains(t, buf.String(), "db,t1,1,1,,g1")

	require.NoError(t, mgr.DumpEngines(ctx, nil))
	require.NoError(t, mgr.DumpChunks(ctx, nil))

	// Test Remove
	err = mgr.Update(ctx, cp1)
	require.NoError(t, err)
	err = mgr.Remove(ctx, "db", "t1")
	require.NoError(t, err)
	cp, err = mgr.Get("db", "t1")
	require.NoError(t, err)
	require.Nil(t, cp)

	// Test Remove All
	err = mgr.Update(ctx, cp1)
	require.NoError(t, err)
	err = mgr.Remove(ctx, "all", "all")
	require.NoError(t, err)
	// File should be removed
	_, err = os.Stat(filePath)
	require.True(t, os.IsNotExist(err))
}

func TestParseTable(t *testing.T) {
	tests := []struct {
		input     string
		wantDB    string
		wantTable string
		wantErr   bool
	}{
		{"db.table", "db", "table", false},
		{"`db`.`table`", "db", "table", false},
		{"all", "", "all", false},
		{"invalid", "", "", true},
		{"db.table.extra", "", "", true},
	}

	for _, tt := range tests {
		db, tbl, err := ParseTable(tt.input)
		if tt.wantErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tt.wantDB, db)
			require.Equal(t, tt.wantTable, tbl)
		}
	}
}

func TestCheckpointStatus_String(t *testing.T) {
	tests := []struct {
		status CheckpointStatus
		want   string
	}{
		{CheckpointStatusPending, "pending"},
		{CheckpointStatusRunning, "running"},
		{CheckpointStatusFinished, "finished"},
		{CheckpointStatusFailed, "failed"},
		{CheckpointStatus(999), "unknown"},
	}

	for _, tt := range tests {
		require.Equal(t, tt.want, tt.status.String())
	}
}

func TestEnsureCheckpointDir(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "subdir", "checkpoints.json")
	err := EnsureCheckpointDir(filePath)
	require.NoError(t, err)
	info, err := os.Stat(filepath.Dir(filePath))
	require.NoError(t, err)
	require.True(t, info.IsDir())
}

func TestFileCheckpointManager_GetCheckpoints(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "checkpoints.json")
	mgr := NewFileCheckpointManager(filePath)
	ctx := context.Background()
	require.NoError(t, mgr.Initialize(ctx))

	cp1 := &TableCheckpoint{DBName: "db", TableName: "t1", JobID: 1, Status: CheckpointStatusRunning}
	cp2 := &TableCheckpoint{DBName: "db", TableName: "t2", JobID: 2, Status: CheckpointStatusFinished}

	require.NoError(t, mgr.Update(ctx, cp1))
	require.NoError(t, mgr.Update(ctx, cp2))

	cps, err := mgr.GetCheckpoints(ctx)
	require.NoError(t, err)
	require.Len(t, cps, 2)

	// Verify contents (order is not guaranteed in map iteration)
	m := make(map[string]*TableCheckpoint)
	for _, cp := range cps {
		m[common.UniqueTable(cp.DBName, cp.TableName)] = cp
	}
	require.Contains(t, m, common.UniqueTable("db", "t1"))
	require.Contains(t, m, common.UniqueTable("db", "t2"))
}

func TestNoopCheckpointManager(t *testing.T) {
	mgr := &NoopCheckpointManager{}
	ctx := context.Background()

	require.NoError(t, mgr.Initialize(ctx))
	cp, err := mgr.Get("db", "t1")
	require.NoError(t, err)
	require.Nil(t, cp)

	require.NoError(t, mgr.Update(ctx, nil))
	require.NoError(t, mgr.Remove(ctx, "db", "t1"))
	require.NoError(t, mgr.IgnoreError(ctx, "db", "t1"))
	require.NoError(t, mgr.DestroyError(ctx, "db", "t1"))
	var buf bytes.Buffer
	require.NoError(t, mgr.DumpTables(ctx, &buf))
	require.Empty(t, buf.String())
	require.NoError(t, mgr.DumpEngines(ctx, nil))
	require.NoError(t, mgr.DumpChunks(ctx, nil))

	cps, err := mgr.GetCheckpoints(ctx)
	require.NoError(t, err)
	require.Nil(t, cps)

	require.NoError(t, mgr.Close())
}

func TestMySQLCheckpointManager(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	schemaName := "test_schema"
	tableName := "import_into_checkpoints"
	mgr := &MySQLCheckpointManager{
		db:         db,
		schemaName: schemaName,
		tableName:  tableName,
	}
	ctx := context.Background()

	// Test Initialize
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `test_schema`").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test_schema`.`import_into_checkpoints` .*").WillReturnResult(sqlmock.NewResult(0, 0))
	require.NoError(t, mgr.Initialize(ctx))

	// Test Get - Not Found
	mock.ExpectQuery("SELECT job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\?").
		WithArgs("db", "t1").
		WillReturnError(sql.ErrNoRows)
	cp, err := mgr.Get("db", "t1")
	require.NoError(t, err)
	require.Nil(t, cp)

	// Test Get - Found
	mock.ExpectQuery("SELECT job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\?").
		WithArgs("db", "t1").
		WillReturnRows(sqlmock.NewRows([]string{"job_id", "status", "message", "group_key"}).
			AddRow(123, CheckpointStatusRunning, "msg", "g1"))
	cp, err = mgr.Get("db", "t1")
	require.NoError(t, err)
	require.NotNil(t, cp)
	require.Equal(t, int64(123), cp.JobID)
	require.Equal(t, CheckpointStatusRunning, cp.Status)
	require.Equal(t, "msg", cp.Message)
	require.Equal(t, "g1", cp.GroupKey)

	// Test Update
	cp1 := &TableCheckpoint{
		DBName:    "db",
		TableName: "t1",
		JobID:     123,
		Status:    CheckpointStatusRunning,
		Message:   "msg",
		GroupKey:  "g1",
	}
	mock.ExpectExec("INSERT INTO `test_schema`.`import_into_checkpoints` .* ON DUPLICATE KEY UPDATE .*").
		WithArgs("db", "t1", 123, CheckpointStatusRunning, "msg", "g1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, mgr.Update(ctx, cp1))

	// Test Remove
	mock.ExpectExec("DELETE FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\?").
		WithArgs("db", "t1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, mgr.Remove(ctx, "db", "t1"))

	// Test Remove All
	mock.ExpectExec("DELETE FROM `test_schema`.`import_into_checkpoints`").
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, mgr.Remove(ctx, "all", "all"))

	// Test IgnoreError
	mock.ExpectExec("UPDATE `test_schema`.`import_into_checkpoints` SET status = \\?, message = '', job_id = 0 WHERE db_name = \\? AND table_name = \\? AND status = \\?").
		WithArgs(CheckpointStatusPending, "db", "t1", CheckpointStatusFailed).
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, mgr.IgnoreError(ctx, "db", "t1"))

	// Test IgnoreError All
	mock.ExpectExec("UPDATE `test_schema`.`import_into_checkpoints` SET status = \\?, message = '', job_id = 0 WHERE status = \\?").
		WithArgs(CheckpointStatusPending, CheckpointStatusFailed).
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, mgr.IgnoreError(ctx, "all", "all"))

	// Test DestroyError
	mock.ExpectExec("DELETE FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\? AND status = \\?").
		WithArgs("db", "t1", CheckpointStatusFailed).
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, mgr.DestroyError(ctx, "db", "t1"))

	// Test DestroyError All
	mock.ExpectExec("DELETE FROM `test_schema`.`import_into_checkpoints` WHERE status = \\?").
		WithArgs(CheckpointStatusFailed).
		WillReturnResult(sqlmock.NewResult(0, 1))
	require.NoError(t, mgr.DestroyError(ctx, "all", "all"))

	// Test GetCheckpoints
	mock.ExpectQuery("SELECT db_name, table_name, job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints`").
		WillReturnRows(sqlmock.NewRows([]string{"db_name", "table_name", "job_id", "status", "message", "group_key"}).
			AddRow("db", "t1", 123, CheckpointStatusRunning, "msg", "g1"))
	cps, err := mgr.GetCheckpoints(ctx)
	require.NoError(t, err)
	require.Len(t, cps, 1)
	require.Equal(t, "db", cps[0].DBName)
	require.Equal(t, "t1", cps[0].TableName)

	// Test DumpTables
	mock.ExpectQuery("SELECT db_name, table_name, job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints`").
		WillReturnRows(sqlmock.NewRows([]string{"db_name", "table_name", "job_id", "status", "message", "group_key"}).
			AddRow("db", "t1", 123, CheckpointStatusRunning, "msg", "g1"))

	var buf bytes.Buffer
	require.NoError(t, mgr.DumpTables(ctx, &buf))
	require.Contains(t, buf.String(), "db,t1,123,1,msg,g1")

	require.NoError(t, mgr.DumpEngines(ctx, nil))
	require.NoError(t, mgr.DumpChunks(ctx, nil))

	require.NoError(t, mock.ExpectationsWereMet())
}

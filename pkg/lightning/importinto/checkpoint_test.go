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
	tests := []struct {
		name      string
		setup     func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context)
		operation func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string)
	}{
		{
			name:  "Initialize and Get empty",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				cp, err := mgr.Get("db", "t1")
				require.NoError(t, err)
				require.Nil(t, cp)
			},
		},
		{
			name:  "Update and Get",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				cp1 := &TableCheckpoint{
					DBName:    "db",
					TableName: "t1",
					JobID:     1,
					Status:    CheckpointStatusRunning,
					GroupKey:  "g1",
				}
				require.NoError(t, mgr.Update(ctx, cp1))

				cp, err := mgr.Get("db", "t1")
				require.NoError(t, err)
				require.NotNil(t, cp)
				require.Equal(t, cp1.JobID, cp.JobID)
				require.Equal(t, cp1.Status, cp.Status)
			},
		},
		{
			name: "Persistence across manager instances",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {
				cp1 := &TableCheckpoint{
					DBName:    "db",
					TableName: "t1",
					JobID:     1,
					Status:    CheckpointStatusRunning,
				}
				require.NoError(t, mgr.Update(ctx, cp1))
			},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				// Create a new manager instance and verify data persisted
				mgr2 := NewFileCheckpointManager(filePath)
				require.NoError(t, mgr2.Initialize(ctx))
				cp, err := mgr2.Get("db", "t1")
				require.NoError(t, err)
				require.NotNil(t, cp)
				require.Equal(t, int64(1), cp.JobID)
			},
		},
		{
			name: "IgnoreError resets status",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {
				cp1 := &TableCheckpoint{
					DBName:    "db",
					TableName: "t1",
					JobID:     1,
					Status:    CheckpointStatusFailed,
				}
				require.NoError(t, mgr.Update(ctx, cp1))
			},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				require.NoError(t, mgr.IgnoreError(ctx, "db", "t1"))
				cp, err := mgr.Get("db", "t1")
				require.NoError(t, err)
				require.Equal(t, CheckpointStatusPending, cp.Status)
				require.Equal(t, int64(0), cp.JobID)
			},
		},
		{
			name: "DestroyError removes checkpoint",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {
				cp1 := &TableCheckpoint{
					DBName:    "db",
					TableName: "t1",
					JobID:     1,
					Status:    CheckpointStatusFailed,
				}
				require.NoError(t, mgr.Update(ctx, cp1))
			},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				destroyed, err := mgr.DestroyError(ctx, "db", "t1")
				require.NoError(t, err)
				require.Len(t, destroyed, 1)
				require.Equal(t, "db", destroyed[0].DBName)
				require.Equal(t, "t1", destroyed[0].TableName)

				cp, err := mgr.Get("db", "t1")
				require.NoError(t, err)
				require.Nil(t, cp)
			},
		},
		{
			name: "DumpTables writes CSV",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {
				cp1 := &TableCheckpoint{
					DBName:    "db",
					TableName: "t1",
					JobID:     1,
					Status:    CheckpointStatusRunning,
					GroupKey:  "g1",
				}
				require.NoError(t, mgr.Update(ctx, cp1))
			},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				var buf bytes.Buffer
				require.NoError(t, mgr.DumpTables(ctx, &buf))
				require.Contains(t, buf.String(), "db,t1,1,1,,g1")
			},
		},
		{
			name:  "DumpEngines and DumpChunks are no-ops",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				require.NoError(t, mgr.DumpEngines(ctx, nil))
				require.NoError(t, mgr.DumpChunks(ctx, nil))
			},
		},
		{
			name: "Remove single checkpoint",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {
				cp1 := &TableCheckpoint{DBName: "db", TableName: "t1", JobID: 1, Status: CheckpointStatusRunning}
				require.NoError(t, mgr.Update(ctx, cp1))
			},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				require.NoError(t, mgr.Remove(ctx, "db", "t1"))
				cp, err := mgr.Get("db", "t1")
				require.NoError(t, err)
				require.Nil(t, cp)
			},
		},
		{
			name: "Remove all checkpoints",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {
				cp1 := &TableCheckpoint{DBName: "db", TableName: "t1", JobID: 1, Status: CheckpointStatusRunning}
				require.NoError(t, mgr.Update(ctx, cp1))
			},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				require.NoError(t, mgr.Remove(ctx, "all", "all"))
				_, err := os.Stat(filePath)
				require.True(t, os.IsNotExist(err))
			},
		},
		{
			name: "GetCheckpoints returns all",
			setup: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context) {
				cp1 := &TableCheckpoint{DBName: "db", TableName: "t1", JobID: 1, Status: CheckpointStatusRunning}
				cp2 := &TableCheckpoint{DBName: "db", TableName: "t2", JobID: 2, Status: CheckpointStatusFinished}
				require.NoError(t, mgr.Update(ctx, cp1))
				require.NoError(t, mgr.Update(ctx, cp2))
			},
			operation: func(t *testing.T, mgr *FileCheckpointManager, ctx context.Context, filePath string) {
				cps, err := mgr.GetCheckpoints(ctx)
				require.NoError(t, err)
				require.Len(t, cps, 2)

				m := make(map[string]*TableCheckpoint)
				for _, cp := range cps {
					m[common.UniqueTable(cp.DBName, cp.TableName)] = cp
				}
				require.Contains(t, m, common.UniqueTable("db", "t1"))
				require.Contains(t, m, common.UniqueTable("db", "t2"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			filePath := filepath.Join(tempDir, "checkpoints.json")
			mgr := NewFileCheckpointManager(filePath)
			ctx := context.Background()

			require.NoError(t, mgr.Initialize(ctx))
			tt.setup(t, mgr, ctx)
			tt.operation(t, mgr, ctx, filePath)
		})
	}
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
	destroyed, err := mgr.DestroyError(ctx, "db", "t1")
	require.NoError(t, err)
	require.Nil(t, destroyed)
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
	schemaName := "test_schema"
	tableName := "import_into_checkpoints"

	tests := []struct {
		name      string
		setup     func(mock sqlmock.Sqlmock)
		operation func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context)
	}{
		{
			name: "Initialize creates schema and table",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `test_schema`").WillReturnResult(sqlmock.NewResult(0, 0))
				mock.ExpectExec("CREATE TABLE IF NOT EXISTS `test_schema`.`import_into_checkpoints` .*").WillReturnResult(sqlmock.NewResult(0, 0))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				require.NoError(t, mgr.Initialize(ctx))
			},
		},
		{
			name: "Get not found returns nil",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\?").
					WithArgs("db", "t1").
					WillReturnError(sql.ErrNoRows)
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				cp, err := mgr.Get("db", "t1")
				require.NoError(t, err)
				require.Nil(t, cp)
			},
		},
		{
			name: "Get found returns checkpoint",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\?").
					WithArgs("db", "t1").
					WillReturnRows(sqlmock.NewRows([]string{"job_id", "status", "message", "group_key"}).
						AddRow(123, CheckpointStatusRunning, "msg", "g1"))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				cp, err := mgr.Get("db", "t1")
				require.NoError(t, err)
				require.NotNil(t, cp)
				require.Equal(t, int64(123), cp.JobID)
				require.Equal(t, CheckpointStatusRunning, cp.Status)
				require.Equal(t, "msg", cp.Message)
				require.Equal(t, "g1", cp.GroupKey)
			},
		},
		{
			name: "Update inserts or updates checkpoint",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("INSERT INTO `test_schema`.`import_into_checkpoints` .* ON DUPLICATE KEY UPDATE .*").
					WithArgs("db", "t1", 123, CheckpointStatusRunning, "msg", "g1").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				cp := &TableCheckpoint{
					DBName:    "db",
					TableName: "t1",
					JobID:     123,
					Status:    CheckpointStatusRunning,
					Message:   "msg",
					GroupKey:  "g1",
				}
				require.NoError(t, mgr.Update(ctx, cp))
			},
		},
		{
			name: "Remove single checkpoint",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\?").
					WithArgs("db", "t1").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				require.NoError(t, mgr.Remove(ctx, "db", "t1"))
			},
		},
		{
			name: "Remove all checkpoints",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("DELETE FROM `test_schema`.`import_into_checkpoints`").
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				require.NoError(t, mgr.Remove(ctx, "all", "all"))
			},
		},
		{
			name: "IgnoreError single checkpoint",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE `test_schema`.`import_into_checkpoints` SET status = \\?, message = '', job_id = 0 WHERE db_name = \\? AND table_name = \\? AND status = \\?").
					WithArgs(CheckpointStatusPending, "db", "t1", CheckpointStatusFailed).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				require.NoError(t, mgr.IgnoreError(ctx, "db", "t1"))
			},
		},
		{
			name: "IgnoreError all checkpoints",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE `test_schema`.`import_into_checkpoints` SET status = \\?, message = '', job_id = 0 WHERE status = \\?").
					WithArgs(CheckpointStatusPending, CheckpointStatusFailed).
					WillReturnResult(sqlmock.NewResult(0, 1))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				require.NoError(t, mgr.IgnoreError(ctx, "all", "all"))
			},
		},
		{
			name: "DestroyError single checkpoint",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT db_name, table_name, job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\? AND status = \\?").
					WithArgs("db", "t1", CheckpointStatusFailed).
					WillReturnRows(sqlmock.NewRows([]string{"db_name", "table_name", "job_id", "status", "message", "group_key"}).
						AddRow("db", "t1", 123, CheckpointStatusFailed, "msg", "g1"))
				mock.ExpectExec("DELETE FROM `test_schema`.`import_into_checkpoints` WHERE db_name = \\? AND table_name = \\? AND status = \\?").
					WithArgs("db", "t1", CheckpointStatusFailed).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				destroyed, err := mgr.DestroyError(ctx, "db", "t1")
				require.NoError(t, err)
				require.Len(t, destroyed, 1)
				require.Equal(t, "db", destroyed[0].DBName)
				require.Equal(t, "t1", destroyed[0].TableName)
			},
		},
		{
			name: "DestroyError all checkpoints",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT db_name, table_name, job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints` WHERE status = \\?").
					WithArgs(CheckpointStatusFailed).
					WillReturnRows(sqlmock.NewRows([]string{"db_name", "table_name", "job_id", "status", "message", "group_key"}).
						AddRow("db", "t1", 123, CheckpointStatusFailed, "msg", "g1"))
				mock.ExpectExec("DELETE FROM `test_schema`.`import_into_checkpoints` WHERE status = \\?").
					WithArgs(CheckpointStatusFailed).
					WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit()
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				destroyed, err := mgr.DestroyError(ctx, "all", "all")
				require.NoError(t, err)
				require.Len(t, destroyed, 1)
			},
		},
		{
			name: "GetCheckpoints returns all",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT db_name, table_name, job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints`").
					WillReturnRows(sqlmock.NewRows([]string{"db_name", "table_name", "job_id", "status", "message", "group_key"}).
						AddRow("db", "t1", 123, CheckpointStatusRunning, "msg", "g1"))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				cps, err := mgr.GetCheckpoints(ctx)
				require.NoError(t, err)
				require.Len(t, cps, 1)
				require.Equal(t, "db", cps[0].DBName)
				require.Equal(t, "t1", cps[0].TableName)
			},
		},
		{
			name: "DumpTables writes CSV",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT db_name, table_name, job_id, status, message, group_key FROM `test_schema`.`import_into_checkpoints`").
					WillReturnRows(sqlmock.NewRows([]string{"db_name", "table_name", "job_id", "status", "message", "group_key"}).
						AddRow("db", "t1", 123, CheckpointStatusRunning, "msg", "g1"))
			},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				var buf bytes.Buffer
				require.NoError(t, mgr.DumpTables(ctx, &buf))
				require.Contains(t, buf.String(), "db,t1,123,1,msg,g1")
			},
		},
		{
			name:  "DumpEngines and DumpChunks are no-ops",
			setup: func(mock sqlmock.Sqlmock) {},
			operation: func(t *testing.T, mgr *MySQLCheckpointManager, ctx context.Context) {
				require.NoError(t, mgr.DumpEngines(ctx, nil))
				require.NoError(t, mgr.DumpChunks(ctx, nil))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			mgr := &MySQLCheckpointManager{
				db:         db,
				schemaName: schemaName,
				tableName:  tableName,
			}
			ctx := context.Background()

			tt.setup(mock)
			tt.operation(t, mgr, ctx)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
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
		t.Run(tt.input, func(t *testing.T) {
			db, tbl, err := ParseTable(tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantDB, db)
				require.Equal(t, tt.wantTable, tbl)
			}
		})
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
		t.Run(tt.want, func(t *testing.T) {
			require.Equal(t, tt.want, tt.status.String())
		})
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

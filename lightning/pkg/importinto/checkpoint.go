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
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/joho/sqltocsv"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
)

// CheckpointStatus represents the status of a table import job.
type CheckpointStatus int

const (
	// CheckpointStatusPending indicates the job has not started yet.
	CheckpointStatusPending CheckpointStatus = iota
	// CheckpointStatusRunning indicates the job is currently running.
	CheckpointStatusRunning
	// CheckpointStatusFinished indicates the job has finished successfully.
	CheckpointStatusFinished
	// CheckpointStatusFailed indicates the job has failed.
	CheckpointStatusFailed
)

func (s CheckpointStatus) String() string {
	switch s {
	case CheckpointStatusPending:
		return "pending"
	case CheckpointStatusRunning:
		return "running"
	case CheckpointStatusFinished:
		return "finished"
	case CheckpointStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// TableCheckpoint represents the checkpoint state for a single table.
type TableCheckpoint struct {
	TableName string           `json:"table_name"`
	JobID     int64            `json:"job_id"`
	Status    CheckpointStatus `json:"status"`
	Message   string           `json:"message,omitempty"`
	GroupKey  string           `json:"group_key"`
}

// CheckpointManager defines the interface for managing checkpoints.
type CheckpointManager interface {
	// Initialize loads existing checkpoints.
	Initialize(ctx context.Context) error
	// Get returns the checkpoint for a specific table. Returns nil if not found.
	Get(ctx context.Context, tableName string) (*TableCheckpoint, error)
	// Update updates the checkpoint for a specific table.
	Update(ctx context.Context, cp *TableCheckpoint) error
	// Remove removes the checkpoint for a specific table.
	Remove(ctx context.Context, tableName string) error
	// IgnoreError resets the status of a failed checkpoint to Pending.
	IgnoreError(ctx context.Context, tableName string) error
	// DestroyError removes the checkpoint for a specific table if it is in Failed state.
	// It returns the list of checkpoints that were removed.
	DestroyError(ctx context.Context, tableName string) ([]*TableCheckpoint, error)
	// DumpTables dumps the table checkpoints to a writer.
	DumpTables(ctx context.Context, writer io.Writer) error
	// DumpEngines dumps the engine checkpoints to a writer.
	DumpEngines(ctx context.Context, writer io.Writer) error
	// DumpChunks dumps the chunk checkpoints to a writer.
	DumpChunks(ctx context.Context, writer io.Writer) error
	// GetCheckpoints returns all checkpoints.
	GetCheckpoints(ctx context.Context) ([]*TableCheckpoint, error)
	// Close closes the manager.
	Close() error
}

// NewCheckpointManager creates a new CheckpointManager based on the configuration.
func NewCheckpointManager(cfg *config.Config) (CheckpointManager, error) {
	if !cfg.Checkpoint.Enable {
		return &NoopCheckpointManager{}, nil
	}

	switch cfg.Checkpoint.Driver {
	case config.CheckpointDriverFile:
		return NewFileCheckpointManager(cfg.Checkpoint.DSN), nil
	case config.CheckpointDriverMySQL:
		return NewMySQLCheckpointManager(cfg.Checkpoint.MySQLParam, cfg.Checkpoint.Schema)
	default:
		return nil, errors.Errorf("unknown checkpoint driver: %s", cfg.Checkpoint.Driver)
	}
}

// NoopCheckpointManager is a dummy implementation when checkpoint is disabled.
type NoopCheckpointManager struct{}

// Initialize implements CheckpointManager.
func (*NoopCheckpointManager) Initialize(_ context.Context) error { return nil }

// Get implements CheckpointManager.
func (*NoopCheckpointManager) Get(_ context.Context, _ string) (*TableCheckpoint, error) {
	return nil, nil
}

// Update implements CheckpointManager.
func (*NoopCheckpointManager) Update(_ context.Context, _ *TableCheckpoint) error { return nil }

// Remove implements CheckpointManager.
func (*NoopCheckpointManager) Remove(_ context.Context, _ string) error {
	return nil
}

// IgnoreError implements CheckpointManager.
func (*NoopCheckpointManager) IgnoreError(_ context.Context, _ string) error {
	return nil
}

// DestroyError implements CheckpointManager.
func (*NoopCheckpointManager) DestroyError(_ context.Context, _ string) ([]*TableCheckpoint, error) {
	return nil, nil
}

// DumpTables implements CheckpointManager.
func (*NoopCheckpointManager) DumpTables(context.Context, io.Writer) error { return nil }

// DumpEngines implements CheckpointManager.
func (*NoopCheckpointManager) DumpEngines(context.Context, io.Writer) error { return nil }

// DumpChunks implements CheckpointManager.
func (*NoopCheckpointManager) DumpChunks(context.Context, io.Writer) error { return nil }

// GetCheckpoints implements CheckpointManager.
func (*NoopCheckpointManager) GetCheckpoints(context.Context) ([]*TableCheckpoint, error) {
	return nil, nil
}

// Close implements CheckpointManager.
func (*NoopCheckpointManager) Close() error { return nil }

// FileCheckpointManager implements CheckpointManager using a local file.
type FileCheckpointManager struct {
	filePath    string
	storage     *storage.LocalStorage
	checkpoints map[string]*TableCheckpoint
	mu          sync.RWMutex
}

// NewFileCheckpointManager creates a new FileCheckpointManager.
func NewFileCheckpointManager(filePath string) *FileCheckpointManager {
	return &FileCheckpointManager{
		filePath:    filePath,
		checkpoints: make(map[string]*TableCheckpoint),
	}
}

// Initialize loads checkpoints from the backing file.
func (m *FileCheckpointManager) Initialize(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	dir := filepath.Dir(m.filePath)
	st, err := storage.NewLocalStorage(dir)
	if err != nil {
		return errors.Trace(err)
	}
	m.storage = st
	m.storage.IgnoreEnoentForDelete = true

	content, err := m.storage.ReadFile(ctx, filepath.Base(m.filePath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Trace(err)
	}

	if len(content) == 0 {
		return nil
	}

	if err := json.Unmarshal(content, &m.checkpoints); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Get retrieves a checkpoint by database and table name.
func (m *FileCheckpointManager) Get(_ context.Context, tableName string) (*TableCheckpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if cp, ok := m.checkpoints[tableName]; ok {
		// Return a copy to avoid race conditions if the caller modifies it
		cpCopy := *cp
		return &cpCopy, nil
	}
	return nil, nil
}

// Update upserts a checkpoint entry in memory before persisting it to disk.
func (m *FileCheckpointManager) Update(ctx context.Context, cp *TableCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.checkpoints[cp.TableName] = cp

	return m.save(ctx)
}

// Remove deletes checkpoints for specific tables or all tables.
func (m *FileCheckpointManager) Remove(ctx context.Context, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tableName == common.AllTables {
		m.checkpoints = make(map[string]*TableCheckpoint)
		// remove file
		if err := m.storage.DeleteFile(ctx, filepath.Base(m.filePath)); err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	delete(m.checkpoints, tableName)
	return m.save(ctx)
}

// IgnoreError resets failed checkpoints back to the pending state.
func (m *FileCheckpointManager) IgnoreError(ctx context.Context, tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tableName == common.AllTables {
		for _, cp := range m.checkpoints {
			if cp.Status == CheckpointStatusFailed {
				cp.Status = CheckpointStatusPending
				cp.Message = ""
				cp.JobID = 0
			}
		}
	} else {
		if cp, ok := m.checkpoints[tableName]; ok && cp.Status == CheckpointStatusFailed {
			cp.Status = CheckpointStatusPending
			cp.Message = ""
			cp.JobID = 0
		}
	}
	return m.save(ctx)
}

// DestroyError removes failed checkpoints entirely.
func (m *FileCheckpointManager) DestroyError(ctx context.Context, tableName string) ([]*TableCheckpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var destroyed []*TableCheckpoint
	if tableName == common.AllTables {
		for key, cp := range m.checkpoints {
			if cp.Status == CheckpointStatusFailed {
				destroyed = append(destroyed, cp)
				delete(m.checkpoints, key)
			}
		}
	} else {
		if cp, ok := m.checkpoints[tableName]; ok && cp.Status == CheckpointStatusFailed {
			destroyed = append(destroyed, cp)
			delete(m.checkpoints, tableName)
		}
	}
	if err := m.save(ctx); err != nil {
		return nil, err
	}
	return destroyed, nil
}

// DumpTables writes human-readable checkpoint information for tables.
func (m *FileCheckpointManager) DumpTables(_ context.Context, writer io.Writer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	w := csv.NewWriter(writer)

	// Write header
	if err := w.Write([]string{"table_name", "job_id", "status", "message", "group_key"}); err != nil {
		return errors.Trace(err)
	}

	for _, cp := range m.checkpoints {
		record := []string{
			cp.TableName,
			fmt.Sprintf("%d", cp.JobID),
			fmt.Sprintf("%d", cp.Status),
			cp.Message,
			cp.GroupKey,
		}
		if err := w.Write(record); err != nil {
			return errors.Trace(err)
		}
	}
	w.Flush()
	return errors.Trace(w.Error())
}

// DumpEngines is a stub implementation for interface compatibility.
func (*FileCheckpointManager) DumpEngines(context.Context, io.Writer) error {
	return nil
}

// DumpChunks is a stub implementation for interface compatibility.
func (*FileCheckpointManager) DumpChunks(context.Context, io.Writer) error {
	return nil
}

// GetCheckpoints returns all tracked checkpoints.
func (m *FileCheckpointManager) GetCheckpoints(_ context.Context) ([]*TableCheckpoint, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cps := make([]*TableCheckpoint, 0, len(m.checkpoints))
	for _, cp := range m.checkpoints {
		cpCopy := *cp
		cps = append(cps, &cpCopy)
	}
	return cps, nil
}

func (m *FileCheckpointManager) save(ctx context.Context) error {
	content, err := json.MarshalIndent(m.checkpoints, "", "  ")
	if err != nil {
		return errors.Trace(err)
	}

	return m.storage.WriteFile(ctx, filepath.Base(m.filePath), content)
}

// Close closes the checkpoint manager; it is a no-op for file checkpoints.
func (*FileCheckpointManager) Close() error {
	return nil
}

// MySQLCheckpointManager implements CheckpointManager using a MySQL database.
type MySQLCheckpointManager struct {
	db         *sql.DB
	schemaName string
	tableName  string
}

// NewMySQLCheckpointManager creates a new MySQLCheckpointManager.
func NewMySQLCheckpointManager(param *common.MySQLConnectParam, schemaName string) (*MySQLCheckpointManager, error) {
	db, err := param.Connect()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &MySQLCheckpointManager{
		db:         db,
		schemaName: schemaName,
		tableName:  "import_into_checkpoints",
	}, nil
}

// Initialize ensures the schema and checkpoint table exist.
func (m *MySQLCheckpointManager) Initialize(ctx context.Context) error {
	// Create schema if not exists
	if _, err := m.db.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", common.EscapeIdentifier(m.schemaName))); err != nil {
		return errors.Trace(err)
	}

	// Create table if not exists
	createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
		db_name VARCHAR(64) NOT NULL,
		table_name VARCHAR(64) NOT NULL,
		job_id BIGINT NOT NULL,
		status TINYINT NOT NULL,
		message TEXT,
		group_key VARCHAR(128),
		update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (db_name, table_name)
	)`, common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))

	if _, err := m.db.ExecContext(ctx, createTableSQL); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Get fetches a checkpoint from the MySQL checkpoint table.
func (m *MySQLCheckpointManager) Get(ctx context.Context, tableName string) (*TableCheckpoint, error) {
	query := fmt.Sprintf("SELECT job_id, status, message, group_key FROM %s.%s WHERE table_name = ?",
		common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))

	var cp TableCheckpoint
	cp.TableName = tableName
	var msg sql.NullString
	var groupKey sql.NullString

	err := m.db.QueryRowContext(ctx, query, tableName).Scan(&cp.JobID, &cp.Status, &msg, &groupKey)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}
	cp.Message = msg.String
	cp.GroupKey = groupKey.String
	return &cp, nil
}

// Update inserts or updates a checkpoint row.
func (m *MySQLCheckpointManager) Update(ctx context.Context, cp *TableCheckpoint) error {
	query := fmt.Sprintf(`INSERT INTO %s.%s (table_name, job_id, status, message, group_key)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
		job_id = VALUES(job_id),
		status = VALUES(status),
		message = VALUES(message),
		group_key = VALUES(group_key)`,
		common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))

	_, err := m.db.ExecContext(ctx, query, cp.TableName, cp.JobID, cp.Status, cp.Message, cp.GroupKey)
	return errors.Trace(err)
}

// Remove deletes checkpoints for one table or all tables.
func (m *MySQLCheckpointManager) Remove(ctx context.Context, tableName string) error {
	if tableName == common.AllTables {
		query := fmt.Sprintf("DELETE FROM %s.%s", common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
		_, err := m.db.ExecContext(ctx, query)
		return errors.Trace(err)
	}
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE table_name = ?",
		common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
	_, err := m.db.ExecContext(ctx, query, tableName)
	return errors.Trace(err)
}

// IgnoreError resets failed checkpoints back to pending.
func (m *MySQLCheckpointManager) IgnoreError(ctx context.Context, tableName string) error {
	if tableName == common.AllTables {
		query := fmt.Sprintf("UPDATE %s.%s SET status = ?, message = '', job_id = 0 WHERE status = ?",
			common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
		_, err := m.db.ExecContext(ctx, query, CheckpointStatusPending, CheckpointStatusFailed)
		return errors.Trace(err)
	}
	query := fmt.Sprintf("UPDATE %s.%s SET status = ?, message = '', job_id = 0 WHERE table_name = ? AND status = ?",
		common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
	_, err := m.db.ExecContext(ctx, query, CheckpointStatusPending, tableName, CheckpointStatusFailed)
	return errors.Trace(err)
}

// DestroyError deletes checkpoints that are stuck in failed state.
func (m *MySQLCheckpointManager) DestroyError(ctx context.Context, tableName string) ([]*TableCheckpoint, error) {
	var (
		selectQuery string
		deleteQuery string
		args        []any
	)
	if tableName == common.AllTables {
		selectQuery = fmt.Sprintf("SELECT table_name, job_id, status, message, group_key FROM %s.%s WHERE status = ?",
			common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
		deleteQuery = fmt.Sprintf("DELETE FROM %s.%s WHERE status = ?",
			common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
		args = []any{CheckpointStatusFailed}
	} else {
		selectQuery = fmt.Sprintf("SELECT table_name, job_id, status, message, group_key FROM %s.%s WHERE table_name = ? AND status = ?",
			common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
		deleteQuery = fmt.Sprintf("DELETE FROM %s.%s WHERE table_name = ? AND status = ?",
			common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
		args = []any{tableName, CheckpointStatusFailed}
	}

	var destroyed []*TableCheckpoint
	err := common.SQLWithRetry{
		DB:     m.db,
		Logger: log.L(),
	}.Transact(ctx, "destroy error checkpoints", func(c context.Context, tx *sql.Tx) error {
		rows, e := tx.QueryContext(c, selectQuery, args...)
		if e != nil {
			return errors.Trace(e)
		}
		defer rows.Close()
		for rows.Next() {
			var cp TableCheckpoint
			var msg sql.NullString
			var groupKey sql.NullString
			if e := rows.Scan(&cp.TableName, &cp.JobID, &cp.Status, &msg, &groupKey); e != nil {
				return errors.Trace(e)
			}
			cp.Message = msg.String
			cp.GroupKey = groupKey.String
			destroyed = append(destroyed, &cp)
		}
		if e := rows.Err(); e != nil {
			return errors.Trace(e)
		}

		if _, e := tx.ExecContext(c, deleteQuery, args...); e != nil {
			return errors.Trace(e)
		}
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}
	return destroyed, nil
}

// DumpTables exports all checkpoint rows in CSV format.
func (m *MySQLCheckpointManager) DumpTables(ctx context.Context, writer io.Writer) error {
	query := fmt.Sprintf("SELECT table_name, job_id, status, message, group_key FROM %s.%s",
		common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()
	if err := sqltocsv.Write(writer, rows); err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(rows.Err())
}

// DumpEngines is not supported for the import-into backend.
func (*MySQLCheckpointManager) DumpEngines(context.Context, io.Writer) error {
	return nil
}

// DumpChunks is not supported for the import-into backend.
func (*MySQLCheckpointManager) DumpChunks(context.Context, io.Writer) error {
	return nil
}

// GetCheckpoints loads all checkpoints from MySQL.
func (m *MySQLCheckpointManager) GetCheckpoints(ctx context.Context) ([]*TableCheckpoint, error) {
	query := fmt.Sprintf("SELECT table_name, job_id, status, message, group_key FROM %s.%s",
		common.EscapeIdentifier(m.schemaName), common.EscapeIdentifier(m.tableName))
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var cps []*TableCheckpoint
	for rows.Next() {
		var cp TableCheckpoint
		var msg sql.NullString
		var groupKey sql.NullString
		if err := rows.Scan(&cp.TableName, &cp.JobID, &cp.Status, &msg, &groupKey); err != nil {
			return nil, errors.Trace(err)
		}
		cp.Message = msg.String
		cp.GroupKey = groupKey.String
		cps = append(cps, &cp)
	}
	return cps, errors.Trace(rows.Err())
}

// Close releases the underlying MySQL connection.
func (m *MySQLCheckpointManager) Close() error {
	return m.db.Close()
}

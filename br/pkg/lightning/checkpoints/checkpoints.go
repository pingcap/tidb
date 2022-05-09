// Copyright 2019 PingCAP, Inc.
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

package checkpoints

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/joho/sqltocsv"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints/checkpointspb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

type CheckpointStatus uint8

const (
	CheckpointStatusMissing         CheckpointStatus = 0
	CheckpointStatusMaxInvalid      CheckpointStatus = 25
	CheckpointStatusLoaded          CheckpointStatus = 30
	CheckpointStatusAllWritten      CheckpointStatus = 60
	CheckpointStatusClosed          CheckpointStatus = 90
	CheckpointStatusImported        CheckpointStatus = 120
	CheckpointStatusIndexImported   CheckpointStatus = 140
	CheckpointStatusAlteredAutoInc  CheckpointStatus = 150
	CheckpointStatusChecksumSkipped CheckpointStatus = 170
	CheckpointStatusChecksummed     CheckpointStatus = 180
	CheckpointStatusAnalyzeSkipped  CheckpointStatus = 200
	CheckpointStatusAnalyzed        CheckpointStatus = 210
)

const WholeTableEngineID = math.MaxInt32

const (
	// the table names to store each kind of checkpoint in the checkpoint database
	// remember to increase the version number in case of incompatible change.
	CheckpointTableNameTask   = "task_v2"
	CheckpointTableNameTable  = "table_v7"
	CheckpointTableNameEngine = "engine_v5"
	CheckpointTableNameChunk  = "chunk_v5"

	// Some frequently used table name or constants.
	allTables       = "all"
	stringLitAll    = "'all'"
	columnTableName = "table_name"
)

const (
	// shared by MySQLCheckpointsDB and GlueCheckpointsDB
	CreateDBTemplate        = "CREATE DATABASE IF NOT EXISTS %s;"
	CreateTaskTableTemplate = `
		CREATE TABLE IF NOT EXISTS %s.%s (
			id tinyint(1) PRIMARY KEY,
			task_id bigint NOT NULL,
			source_dir varchar(256) NOT NULL,
			backend varchar(16) NOT NULL,
			importer_addr varchar(256),
			tidb_host varchar(128) NOT NULL,
			tidb_port int NOT NULL,
			pd_addr varchar(128) NOT NULL,
			sorted_kv_dir varchar(256) NOT NULL,
			lightning_ver varchar(48) NOT NULL
		);`
	CreateTableTableTemplate = `
		CREATE TABLE IF NOT EXISTS %s.%s (
			task_id bigint NOT NULL,
			table_name varchar(261) NOT NULL PRIMARY KEY,
			hash binary(32) NOT NULL,
			status tinyint unsigned DEFAULT 30,
			alloc_base bigint NOT NULL DEFAULT 0,
			table_id bigint NOT NULL DEFAULT 0,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			kv_bytes bigint unsigned NOT NULL DEFAULT 0,
			kv_kvs bigint unsigned NOT NULL DEFAULT 0,
			kv_checksum bigint unsigned NOT NULL DEFAULT 0,
			INDEX(task_id)
		);`
	CreateEngineTableTemplate = `
		CREATE TABLE IF NOT EXISTS %s.%s (
			table_name varchar(261) NOT NULL,
			engine_id int NOT NULL,
			status tinyint unsigned DEFAULT 30,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY(table_name, engine_id DESC)
		);`
	CreateChunkTableTemplate = `
		CREATE TABLE IF NOT EXISTS %s.%s (
			table_name varchar(261) NOT NULL,
			engine_id int unsigned NOT NULL,
			path varchar(2048) NOT NULL,
			offset bigint NOT NULL,
			type int NOT NULL,
			compression int NOT NULL,
			sort_key varchar(256) NOT NULL,
			file_size bigint NOT NULL,
			columns text NULL,
			should_include_row_id BOOL NOT NULL,
			end_offset bigint NOT NULL,
			pos bigint NOT NULL,
			prev_rowid_max bigint NOT NULL,
			rowid_max bigint NOT NULL,
			kvc_bytes bigint unsigned NOT NULL DEFAULT 0,
			kvc_kvs bigint unsigned NOT NULL DEFAULT 0,
			kvc_checksum bigint unsigned NOT NULL DEFAULT 0,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			PRIMARY KEY(table_name, engine_id, path(500), offset)
		);`
	InitTaskTemplate = `
		REPLACE INTO %s.%s (id, task_id, source_dir, backend, importer_addr, tidb_host, tidb_port, pd_addr, sorted_kv_dir, lightning_ver)
			VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?);`
	InitTableTemplate = `
		INSERT INTO %s.%s (task_id, table_name, hash, table_id) VALUES (?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE task_id = CASE
				WHEN hash = VALUES(hash)
				THEN VALUES(task_id)
			END;`
	ReadTaskTemplate = `
		SELECT task_id, source_dir, backend, importer_addr, tidb_host, tidb_port, pd_addr, sorted_kv_dir, lightning_ver FROM %s.%s WHERE id = 1;`
	ReadEngineTemplate = `
		SELECT engine_id, status FROM %s.%s WHERE table_name = ? ORDER BY engine_id DESC;`
	ReadChunkTemplate = `
		SELECT
			engine_id, path, offset, type, compression, sort_key, file_size, columns,
			pos, end_offset, prev_rowid_max, rowid_max,
			kvc_bytes, kvc_kvs, kvc_checksum, unix_timestamp(create_time)
		FROM %s.%s WHERE table_name = ?
		ORDER BY engine_id, path, offset;`
	ReadTableRemainTemplate = `
		SELECT status, alloc_base, table_id, kv_bytes, kv_kvs, kv_checksum FROM %s.%s WHERE table_name = ?;`
	ReplaceEngineTemplate = `
		REPLACE INTO %s.%s (table_name, engine_id, status) VALUES (?, ?, ?);`
	ReplaceChunkTemplate = `
		REPLACE INTO %s.%s (
				table_name, engine_id,
				path, offset, type, compression, sort_key, file_size, columns, should_include_row_id,
				pos, end_offset, prev_rowid_max, rowid_max,
				kvc_bytes, kvc_kvs, kvc_checksum, create_time
			) VALUES (
				?, ?,
				?, ?, ?, ?, ?, ?, ?, FALSE,
				?, ?, ?, ?,
				0, 0, 0, from_unixtime(?)
			);`
	UpdateChunkTemplate = `
		UPDATE %s.%s SET pos = ?, prev_rowid_max = ?, kvc_bytes = ?, kvc_kvs = ?, kvc_checksum = ?, columns = ?
		WHERE (table_name, engine_id, path, offset) = (?, ?, ?, ?);`
	UpdateTableRebaseTemplate = `
		UPDATE %s.%s SET alloc_base = GREATEST(?, alloc_base) WHERE table_name = ?;`
	UpdateTableStatusTemplate = `
		UPDATE %s.%s SET status = ? WHERE table_name = ?;`
	UpdateTableChecksumTemplate = `UPDATE %s.%s SET kv_bytes = ?, kv_kvs = ?, kv_checksum = ? WHERE table_name = ?;`
	UpdateEngineTemplate        = `
		UPDATE %s.%s SET status = ? WHERE (table_name, engine_id) = (?, ?);`
	DeleteCheckpointRecordTemplate = "DELETE FROM %s.%s WHERE table_name = ?;"
)

func IsCheckpointTable(name string) bool {
	switch name {
	case CheckpointTableNameTask, CheckpointTableNameTable, CheckpointTableNameEngine, CheckpointTableNameChunk:
		return true
	default:
		return false
	}
}

func (status CheckpointStatus) MetricName() string {
	switch status {
	case CheckpointStatusLoaded:
		return "pending"
	case CheckpointStatusAllWritten:
		return "written"
	case CheckpointStatusClosed:
		return "closed"
	case CheckpointStatusImported:
		return "imported"
	case CheckpointStatusIndexImported:
		return "index_imported"
	case CheckpointStatusAlteredAutoInc:
		return "altered_auto_inc"
	case CheckpointStatusChecksummed, CheckpointStatusChecksumSkipped:
		return "checksum"
	case CheckpointStatusAnalyzed, CheckpointStatusAnalyzeSkipped:
		return "analyzed"
	case CheckpointStatusMissing:
		return "missing"
	default:
		return "invalid"
	}
}

type ChunkCheckpointKey struct {
	Path   string
	Offset int64
}

func (key *ChunkCheckpointKey) String() string {
	return fmt.Sprintf("%s:%d", key.Path, key.Offset)
}

func (key *ChunkCheckpointKey) less(other *ChunkCheckpointKey) bool {
	switch {
	case key.Path < other.Path:
		return true
	case key.Path > other.Path:
		return false
	default:
		return key.Offset < other.Offset
	}
}

type ChunkCheckpoint struct {
	Key               ChunkCheckpointKey
	FileMeta          mydump.SourceFileMeta
	ColumnPermutation []int
	Chunk             mydump.Chunk
	Checksum          verify.KVChecksum
	Timestamp         int64
}

func (ccp *ChunkCheckpoint) DeepCopy() *ChunkCheckpoint {
	colPerm := make([]int, 0, len(ccp.ColumnPermutation))
	colPerm = append(colPerm, ccp.ColumnPermutation...)
	return &ChunkCheckpoint{
		Key:               ccp.Key,
		FileMeta:          ccp.FileMeta,
		ColumnPermutation: colPerm,
		Chunk:             ccp.Chunk,
		Checksum:          ccp.Checksum,
		Timestamp:         ccp.Timestamp,
	}
}

type EngineCheckpoint struct {
	Status CheckpointStatus
	Chunks []*ChunkCheckpoint // a sorted array
}

func (engine *EngineCheckpoint) DeepCopy() *EngineCheckpoint {
	chunks := make([]*ChunkCheckpoint, 0, len(engine.Chunks))
	for _, chunk := range engine.Chunks {
		chunks = append(chunks, chunk.DeepCopy())
	}
	return &EngineCheckpoint{
		Status: engine.Status,
		Chunks: chunks,
	}
}

type TableCheckpoint struct {
	Status    CheckpointStatus
	AllocBase int64
	Engines   map[int32]*EngineCheckpoint
	TableID   int64
	// remote checksum before restore
	Checksum verify.KVChecksum
}

func (cp *TableCheckpoint) DeepCopy() *TableCheckpoint {
	engines := make(map[int32]*EngineCheckpoint, len(cp.Engines))
	for engineID, engine := range cp.Engines {
		engines[engineID] = engine.DeepCopy()
	}
	return &TableCheckpoint{
		Status:    cp.Status,
		AllocBase: cp.AllocBase,
		Engines:   engines,
		TableID:   cp.TableID,
		Checksum:  cp.Checksum,
	}
}

func (cp *TableCheckpoint) CountChunks() int {
	result := 0
	for _, engine := range cp.Engines {
		result += len(engine.Chunks)
	}
	return result
}

type chunkCheckpointDiff struct {
	pos               int64
	rowID             int64
	checksum          verify.KVChecksum
	columnPermutation []int
}

type engineCheckpointDiff struct {
	hasStatus bool
	status    CheckpointStatus
	chunks    map[ChunkCheckpointKey]chunkCheckpointDiff
}

type TableCheckpointDiff struct {
	hasStatus   bool
	hasRebase   bool
	hasChecksum bool
	status      CheckpointStatus
	allocBase   int64
	engines     map[int32]engineCheckpointDiff
	checksum    verify.KVChecksum
}

func NewTableCheckpointDiff() *TableCheckpointDiff {
	return &TableCheckpointDiff{
		engines: make(map[int32]engineCheckpointDiff),
	}
}

func (cpd *TableCheckpointDiff) insertEngineCheckpointDiff(engineID int32, newDiff engineCheckpointDiff) {
	if oldDiff, ok := cpd.engines[engineID]; ok {
		if newDiff.hasStatus {
			oldDiff.hasStatus = true
			oldDiff.status = newDiff.status
		}
		for key, chunkDiff := range newDiff.chunks {
			oldDiff.chunks[key] = chunkDiff
		}
		newDiff = oldDiff
	}
	cpd.engines[engineID] = newDiff
}

func (cpd *TableCheckpointDiff) String() string {
	return fmt.Sprintf(
		"{hasStatus:%v, hasRebase:%v, status:%d, allocBase:%d, engines:[%d]}",
		cpd.hasStatus, cpd.hasRebase, cpd.status, cpd.allocBase, len(cpd.engines),
	)
}

// Apply the diff to the existing chunk and engine checkpoints in `cp`.
func (cp *TableCheckpoint) Apply(cpd *TableCheckpointDiff) {
	if cpd.hasStatus {
		cp.Status = cpd.status
	}
	if cpd.hasRebase {
		cp.AllocBase = cpd.allocBase
	}
	for engineID, engineDiff := range cpd.engines {
		engine := cp.Engines[engineID]
		if engine == nil {
			continue
		}
		if engineDiff.hasStatus {
			engine.Status = engineDiff.status
		}
		for key, diff := range engineDiff.chunks {
			checkpointKey := key
			index := sort.Search(len(engine.Chunks), func(i int) bool {
				return !engine.Chunks[i].Key.less(&checkpointKey)
			})
			if index >= len(engine.Chunks) {
				continue
			}
			chunk := engine.Chunks[index]
			if chunk.Key != checkpointKey {
				continue
			}
			chunk.Chunk.Offset = diff.pos
			chunk.Chunk.PrevRowIDMax = diff.rowID
			chunk.Checksum = diff.checksum
		}
	}
}

type TableCheckpointMerger interface {
	// MergeInto the table checkpoint diff from a status update or chunk update.
	// If there are multiple updates to the same table, only the last one will
	// take effect. Therefore, the caller must ensure events for the same table
	// are properly ordered by the global time (an old event must be merged
	// before the new one).
	MergeInto(cpd *TableCheckpointDiff)
}

type StatusCheckpointMerger struct {
	EngineID int32 // WholeTableEngineID == apply to whole table.
	Status   CheckpointStatus
}

func (merger *StatusCheckpointMerger) SetInvalid() {
	merger.Status /= 10
}

func (merger *StatusCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	if merger.EngineID == WholeTableEngineID || merger.Status <= CheckpointStatusMaxInvalid {
		cpd.status = merger.Status
		cpd.hasStatus = true
	}
	if merger.EngineID != WholeTableEngineID {
		cpd.insertEngineCheckpointDiff(merger.EngineID, engineCheckpointDiff{
			hasStatus: true,
			status:    merger.Status,
			chunks:    make(map[ChunkCheckpointKey]chunkCheckpointDiff),
		})
	}
}

type ChunkCheckpointMerger struct {
	EngineID          int32
	Key               ChunkCheckpointKey
	Checksum          verify.KVChecksum
	Pos               int64
	RowID             int64
	ColumnPermutation []int
	EndOffset         int64 // For test only.
}

func (merger *ChunkCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	cpd.insertEngineCheckpointDiff(merger.EngineID, engineCheckpointDiff{
		chunks: map[ChunkCheckpointKey]chunkCheckpointDiff{
			merger.Key: {
				pos:               merger.Pos,
				rowID:             merger.RowID,
				checksum:          merger.Checksum,
				columnPermutation: merger.ColumnPermutation,
			},
		},
	})
}

type TableChecksumMerger struct {
	Checksum verify.KVChecksum
}

func (m *TableChecksumMerger) MergeInto(cpd *TableCheckpointDiff) {
	cpd.hasChecksum = true
	cpd.checksum = m.Checksum
}

type RebaseCheckpointMerger struct {
	AllocBase int64
}

func (merger *RebaseCheckpointMerger) MergeInto(cpd *TableCheckpointDiff) {
	cpd.hasRebase = true
	cpd.allocBase = mathutil.Max(cpd.allocBase, merger.AllocBase)
}

type DestroyedTableCheckpoint struct {
	TableName   string
	MinEngineID int32
	MaxEngineID int32
}

type TaskCheckpoint struct {
	TaskID       int64
	SourceDir    string
	Backend      string
	ImporterAddr string
	TiDBHost     string
	TiDBPort     int
	PdAddr       string
	SortedKVDir  string
	LightningVer string
}

type DB interface {
	Initialize(ctx context.Context, cfg *config.Config, dbInfo map[string]*TidbDBInfo) error
	TaskCheckpoint(ctx context.Context) (*TaskCheckpoint, error)
	Get(ctx context.Context, tableName string) (*TableCheckpoint, error)
	Close() error
	// InsertEngineCheckpoints initializes the checkpoints related to a table.
	// It assumes the entire table has not been imported before and will fill in
	// default values for the column permutations and checksums.
	InsertEngineCheckpoints(ctx context.Context, tableName string, checkpoints map[int32]*EngineCheckpoint) error
	Update(checkpointDiffs map[string]*TableCheckpointDiff) error

	RemoveCheckpoint(ctx context.Context, tableName string) error
	// MoveCheckpoints renames the checkpoint schema to include a suffix
	// including the taskID (e.g. `tidb_lightning_checkpoints.1234567890.bak`).
	MoveCheckpoints(ctx context.Context, taskID int64) error
	// GetLocalStoringTables returns a map containing tables have engine files stored on local disk.
	// currently only meaningful for local backend
	GetLocalStoringTables(ctx context.Context) (map[string][]int32, error)
	IgnoreErrorCheckpoint(ctx context.Context, tableName string) error
	DestroyErrorCheckpoint(ctx context.Context, tableName string) ([]DestroyedTableCheckpoint, error)
	DumpTables(ctx context.Context, csv io.Writer) error
	DumpEngines(ctx context.Context, csv io.Writer) error
	DumpChunks(ctx context.Context, csv io.Writer) error
}

func OpenCheckpointsDB(ctx context.Context, cfg *config.Config) (DB, error) {
	if !cfg.Checkpoint.Enable {
		return NewNullCheckpointsDB(), nil
	}

	switch cfg.Checkpoint.Driver {
	case config.CheckpointDriverMySQL:
		db, err := common.ConnectMySQL(cfg.Checkpoint.DSN)
		if err != nil {
			return nil, errors.Trace(err)
		}
		cpdb, err := NewMySQLCheckpointsDB(ctx, db, cfg.Checkpoint.Schema)
		if err != nil {
			db.Close()
			return nil, errors.Trace(err)
		}
		return cpdb, nil

	case config.CheckpointDriverFile:
		cpdb, err := NewFileCheckpointsDB(ctx, cfg.Checkpoint.DSN)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return cpdb, nil

	default:
		return nil, common.ErrUnknownCheckpointDriver.GenWithStackByArgs(cfg.Checkpoint.Driver)
	}
}

func IsCheckpointsDBExists(ctx context.Context, cfg *config.Config) (bool, error) {
	if !cfg.Checkpoint.Enable {
		return false, nil
	}
	switch cfg.Checkpoint.Driver {
	case config.CheckpointDriverMySQL:
		db, err := sql.Open("mysql", cfg.Checkpoint.DSN)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer db.Close()
		checkSQL := "SHOW DATABASES WHERE `DATABASE` = ?"
		rows, err := db.QueryContext(ctx, checkSQL, cfg.Checkpoint.Schema)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer rows.Close()
		result := rows.Next()
		if err := rows.Err(); err != nil {
			return false, errors.Trace(err)
		}
		return result, nil

	case config.CheckpointDriverFile:
		s, fileName, err := createExstorageByCompletePath(ctx, cfg.Checkpoint.DSN)
		if err != nil {
			return false, errors.Trace(err)
		}
		result, err := s.FileExists(ctx, fileName)
		if err != nil {
			return false, errors.Trace(err)
		}
		return result, nil

	default:
		return false, common.ErrUnknownCheckpointDriver.GenWithStackByArgs(cfg.Checkpoint.Driver)
	}
}

// NullCheckpointsDB is a checkpoints database with no checkpoints.
type NullCheckpointsDB struct{}

func NewNullCheckpointsDB() *NullCheckpointsDB {
	return &NullCheckpointsDB{}
}

func (*NullCheckpointsDB) Initialize(context.Context, *config.Config, map[string]*TidbDBInfo) error {
	return nil
}

func (*NullCheckpointsDB) TaskCheckpoint(ctx context.Context) (*TaskCheckpoint, error) {
	return nil, nil
}

func (*NullCheckpointsDB) Close() error {
	return nil
}

func (*NullCheckpointsDB) Get(_ context.Context, _ string) (*TableCheckpoint, error) {
	return &TableCheckpoint{
		Status:  CheckpointStatusLoaded,
		Engines: map[int32]*EngineCheckpoint{},
	}, nil
}

func (*NullCheckpointsDB) InsertEngineCheckpoints(_ context.Context, _ string, _ map[int32]*EngineCheckpoint) error {
	return nil
}

func (*NullCheckpointsDB) Update(map[string]*TableCheckpointDiff) error {
	return nil
}

type MySQLCheckpointsDB struct {
	db     *sql.DB
	schema string
}

func NewMySQLCheckpointsDB(ctx context.Context, db *sql.DB, schemaName string) (*MySQLCheckpointsDB, error) {
	schema := common.EscapeIdentifier(schemaName)
	sql := common.SQLWithRetry{
		DB:           db,
		Logger:       log.With(zap.String("schema", schemaName)),
		HideQueryLog: true,
	}
	err := sql.Exec(ctx, "create checkpoints database", fmt.Sprintf(CreateDBTemplate, schema))
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = sql.Exec(ctx, "create task checkpoints table", fmt.Sprintf(CreateTaskTableTemplate, schema, CheckpointTableNameTask))
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = sql.Exec(ctx, "create table checkpoints table", fmt.Sprintf(CreateTableTableTemplate, schema, CheckpointTableNameTable))
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = sql.Exec(ctx, "create engine checkpoints table", fmt.Sprintf(CreateEngineTableTemplate, schema, CheckpointTableNameEngine))
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = sql.Exec(ctx, "create chunks checkpoints table", fmt.Sprintf(CreateChunkTableTemplate, schema, CheckpointTableNameChunk))
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &MySQLCheckpointsDB{
		db:     db,
		schema: schema,
	}, nil
}

func (cpdb *MySQLCheckpointsDB) Initialize(ctx context.Context, cfg *config.Config, dbInfo map[string]*TidbDBInfo) error {
	// We can have at most 65535 placeholders https://stackoverflow.com/q/4922345/
	// Since this step is not performance critical, we just insert the rows one-by-one.
	s := common.SQLWithRetry{DB: cpdb.db, Logger: log.L()}
	err := s.Transact(ctx, "insert checkpoints", func(c context.Context, tx *sql.Tx) error {
		taskStmt, err := tx.PrepareContext(c, fmt.Sprintf(InitTaskTemplate, cpdb.schema, CheckpointTableNameTask))
		if err != nil {
			return errors.Trace(err)
		}
		defer taskStmt.Close()
		_, err = taskStmt.ExecContext(ctx, cfg.TaskID, cfg.Mydumper.SourceDir, cfg.TikvImporter.Backend,
			cfg.TikvImporter.Addr, cfg.TiDB.Host, cfg.TiDB.Port, cfg.TiDB.PdAddr, cfg.TikvImporter.SortedKVDir,
			build.ReleaseVersion)
		if err != nil {
			return errors.Trace(err)
		}

		// If `hash` is not the same but the `table_name` duplicates,
		// the CASE expression will return NULL, which can be used to violate
		// the NOT NULL requirement of `task_id` column, and caused this INSERT
		// statement to fail with an irrecoverable error.
		// We do need to capture the error is display a user friendly message
		// (multiple nodes cannot import the same table) though.
		stmt, err := tx.PrepareContext(c, fmt.Sprintf(InitTableTemplate, cpdb.schema, CheckpointTableNameTable))
		if err != nil {
			return errors.Trace(err)
		}
		defer stmt.Close()

		for _, db := range dbInfo {
			for _, table := range db.Tables {
				tableName := common.UniqueTable(db.Name, table.Name)
				_, err = stmt.ExecContext(c, cfg.TaskID, tableName, CheckpointStatusLoaded, table.ID)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (cpdb *MySQLCheckpointsDB) TaskCheckpoint(ctx context.Context) (*TaskCheckpoint, error) {
	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.L(),
	}

	taskQuery := fmt.Sprintf(ReadTaskTemplate, cpdb.schema, CheckpointTableNameTask)
	taskCp := &TaskCheckpoint{}
	err := s.QueryRow(ctx, "fetch task checkpoint", taskQuery, &taskCp.TaskID, &taskCp.SourceDir, &taskCp.Backend,
		&taskCp.ImporterAddr, &taskCp.TiDBHost, &taskCp.TiDBPort, &taskCp.PdAddr, &taskCp.SortedKVDir, &taskCp.LightningVer)
	if err != nil {
		// if task checkpoint is empty, return nil
		if errors.Cause(err) == sql.ErrNoRows {
			return nil, nil
		}
		return nil, errors.Trace(err)
	}

	return taskCp, nil
}

func (cpdb *MySQLCheckpointsDB) Close() error {
	return errors.Trace(cpdb.db.Close())
}

func (cpdb *MySQLCheckpointsDB) Get(ctx context.Context, tableName string) (*TableCheckpoint, error) {
	cp := &TableCheckpoint{
		Engines: map[int32]*EngineCheckpoint{},
	}

	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	err := s.Transact(ctx, "read checkpoint", func(c context.Context, tx *sql.Tx) error {
		// 1. Populate the engines.

		engineQuery := fmt.Sprintf(ReadEngineTemplate, cpdb.schema, CheckpointTableNameEngine)
		engineRows, err := tx.QueryContext(c, engineQuery, tableName)
		if err != nil {
			return errors.Trace(err)
		}
		defer engineRows.Close()
		for engineRows.Next() {
			var (
				engineID int32
				status   uint8
			)
			if err := engineRows.Scan(&engineID, &status); err != nil {
				return errors.Trace(err)
			}
			cp.Engines[engineID] = &EngineCheckpoint{
				Status: CheckpointStatus(status),
			}
		}
		if err := engineRows.Err(); err != nil {
			return errors.Trace(err)
		}

		// 2. Populate the chunks.

		chunkQuery := fmt.Sprintf(ReadChunkTemplate, cpdb.schema, CheckpointTableNameChunk)
		chunkRows, err := tx.QueryContext(c, chunkQuery, tableName)
		if err != nil {
			return errors.Trace(err)
		}
		defer chunkRows.Close()
		for chunkRows.Next() {
			var (
				value       = &ChunkCheckpoint{}
				colPerm     []byte
				engineID    int32
				kvcBytes    uint64
				kvcKVs      uint64
				kvcChecksum uint64
			)
			if err := chunkRows.Scan(
				&engineID, &value.Key.Path, &value.Key.Offset, &value.FileMeta.Type, &value.FileMeta.Compression,
				&value.FileMeta.SortKey, &value.FileMeta.FileSize, &colPerm, &value.Chunk.Offset, &value.Chunk.EndOffset,
				&value.Chunk.PrevRowIDMax, &value.Chunk.RowIDMax, &kvcBytes, &kvcKVs, &kvcChecksum,
				&value.Timestamp,
			); err != nil {
				return errors.Trace(err)
			}
			value.FileMeta.Path = value.Key.Path
			value.Checksum = verify.MakeKVChecksum(kvcBytes, kvcKVs, kvcChecksum)
			if err := json.Unmarshal(colPerm, &value.ColumnPermutation); err != nil {
				return errors.Trace(err)
			}
			cp.Engines[engineID].Chunks = append(cp.Engines[engineID].Chunks, value)
		}
		if err := chunkRows.Err(); err != nil {
			return errors.Trace(err)
		}

		// 3. Fill in the remaining table info

		tableQuery := fmt.Sprintf(ReadTableRemainTemplate, cpdb.schema, CheckpointTableNameTable)
		tableRow := tx.QueryRowContext(c, tableQuery, tableName)

		var status uint8
		var kvs, bytes, checksum uint64
		if err := tableRow.Scan(&status, &cp.AllocBase, &cp.TableID, &bytes, &kvs, &checksum); err != nil {
			if err == sql.ErrNoRows {
				return errors.NotFoundf("checkpoint for table %s", tableName)
			}
		}
		cp.Checksum = verify.MakeKVChecksum(bytes, kvs, checksum)
		cp.Status = CheckpointStatus(status)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return cp, nil
}

func (cpdb *MySQLCheckpointsDB) InsertEngineCheckpoints(ctx context.Context, tableName string, checkpoints map[int32]*EngineCheckpoint) error {
	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	err := s.Transact(ctx, "update engine checkpoints", func(c context.Context, tx *sql.Tx) error {
		engineStmt, err := tx.PrepareContext(c, fmt.Sprintf(ReplaceEngineTemplate, cpdb.schema, CheckpointTableNameEngine))
		if err != nil {
			return errors.Trace(err)
		}
		defer engineStmt.Close()

		chunkStmt, err := tx.PrepareContext(c, fmt.Sprintf(ReplaceChunkTemplate, cpdb.schema, CheckpointTableNameChunk))
		if err != nil {
			return errors.Trace(err)
		}
		defer chunkStmt.Close()

		for engineID, engine := range checkpoints {
			_, err = engineStmt.ExecContext(c, tableName, engineID, engine.Status)
			if err != nil {
				return errors.Trace(err)
			}
			for _, value := range engine.Chunks {
				columnPerm, err := json.Marshal(value.ColumnPermutation)
				if err != nil {
					return errors.Trace(err)
				}
				_, err = chunkStmt.ExecContext(
					c, tableName, engineID,
					value.Key.Path, value.Key.Offset, value.FileMeta.Type, value.FileMeta.Compression,
					value.FileMeta.SortKey, value.FileMeta.FileSize, columnPerm, value.Chunk.Offset, value.Chunk.EndOffset,
					value.Chunk.PrevRowIDMax, value.Chunk.RowIDMax, value.Timestamp,
				)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (cpdb *MySQLCheckpointsDB) Update(checkpointDiffs map[string]*TableCheckpointDiff) error {
	chunkQuery := fmt.Sprintf(UpdateChunkTemplate, cpdb.schema, CheckpointTableNameChunk)
	rebaseQuery := fmt.Sprintf(UpdateTableRebaseTemplate, cpdb.schema, CheckpointTableNameTable)
	tableStatusQuery := fmt.Sprintf(UpdateTableStatusTemplate, cpdb.schema, CheckpointTableNameTable)
	tableChecksumQuery := fmt.Sprintf(UpdateTableChecksumTemplate, cpdb.schema, CheckpointTableNameTable)
	engineStatusQuery := fmt.Sprintf(UpdateEngineTemplate, cpdb.schema, CheckpointTableNameEngine)

	s := common.SQLWithRetry{DB: cpdb.db, Logger: log.L()}
	return s.Transact(context.Background(), "update checkpoints", func(c context.Context, tx *sql.Tx) error {
		chunkStmt, e := tx.PrepareContext(c, chunkQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer chunkStmt.Close()
		rebaseStmt, e := tx.PrepareContext(c, rebaseQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer rebaseStmt.Close()
		tableStatusStmt, e := tx.PrepareContext(c, tableStatusQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer tableStatusStmt.Close()
		tableChecksumStmt, e := tx.PrepareContext(c, tableChecksumQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer tableChecksumStmt.Close()
		engineStatusStmt, e := tx.PrepareContext(c, engineStatusQuery)
		if e != nil {
			return errors.Trace(e)
		}
		defer engineStatusStmt.Close()
		for tableName, cpd := range checkpointDiffs {
			if cpd.hasStatus {
				if _, e := tableStatusStmt.ExecContext(c, cpd.status, tableName); e != nil {
					return errors.Trace(e)
				}
			}
			if cpd.hasRebase {
				if _, e := rebaseStmt.ExecContext(c, cpd.allocBase, tableName); e != nil {
					return errors.Trace(e)
				}
			}
			if cpd.hasChecksum {
				if _, e := tableChecksumStmt.ExecContext(c, cpd.checksum.SumSize(), cpd.checksum.SumKVS(), cpd.checksum.Sum(), tableName); e != nil {
					return errors.Trace(e)
				}
			}
			for engineID, engineDiff := range cpd.engines {
				if engineDiff.hasStatus {
					if _, e := engineStatusStmt.ExecContext(c, engineDiff.status, tableName, engineID); e != nil {
						return errors.Trace(e)
					}
				}
				for key, diff := range engineDiff.chunks {
					columnPerm, err := json.Marshal(diff.columnPermutation)
					if err != nil {
						return errors.Trace(err)
					}
					if _, e := chunkStmt.ExecContext(
						c,
						diff.pos, diff.rowID, diff.checksum.SumSize(), diff.checksum.SumKVS(), diff.checksum.Sum(),
						columnPerm, tableName, engineID, key.Path, key.Offset,
					); e != nil {
						return errors.Trace(e)
					}
				}
			}
		}

		return nil
	})
}

type FileCheckpointsDB struct {
	lock        sync.Mutex // we need to ensure only a thread can access to `checkpoints` at a time
	checkpoints checkpointspb.CheckpointsModel
	ctx         context.Context
	path        string
	fileName    string
	exStorage   storage.ExternalStorage
}

func newFileCheckpointsDB(
	ctx context.Context,
	path string,
	exStorage storage.ExternalStorage,
	fileName string,
) (*FileCheckpointsDB, error) {
	cpdb := &FileCheckpointsDB{
		checkpoints: checkpointspb.CheckpointsModel{
			TaskCheckpoint: &checkpointspb.TaskCheckpointModel{},
			Checkpoints:    map[string]*checkpointspb.TableCheckpointModel{},
		},
		ctx:       ctx,
		path:      path,
		fileName:  fileName,
		exStorage: exStorage,
	}

	if cpdb.fileName == "" {
		return nil, errors.Errorf("the checkpoint DSN '%s' must not be a directory", path)
	}

	exist, err := cpdb.exStorage.FileExists(ctx, cpdb.fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exist {
		log.L().Info("open checkpoint file failed, going to create a new one",
			zap.String("path", path),
			log.ShortError(err),
		)
		return cpdb, nil
	}
	content, err := cpdb.exStorage.ReadFile(ctx, cpdb.fileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = cpdb.checkpoints.Unmarshal(content)
	if err != nil {
		log.L().Error("checkpoint file is broken", zap.String("path", path), zap.Error(err))
	}
	// FIXME: patch for empty map may need initialize manually, because currently
	// FIXME: a map of zero size -> marshall -> unmarshall -> become nil, see checkpoint_test.go
	if cpdb.checkpoints.Checkpoints == nil {
		cpdb.checkpoints.Checkpoints = map[string]*checkpointspb.TableCheckpointModel{}
	}
	for _, table := range cpdb.checkpoints.Checkpoints {
		if table.Engines == nil {
			table.Engines = map[int32]*checkpointspb.EngineCheckpointModel{}
		}
		for _, engine := range table.Engines {
			if engine.Chunks == nil {
				engine.Chunks = map[string]*checkpointspb.ChunkCheckpointModel{}
			}
		}
	}
	return cpdb, nil
}

func NewFileCheckpointsDB(ctx context.Context, path string) (*FileCheckpointsDB, error) {
	// init ExternalStorage
	s, fileName, err := createExstorageByCompletePath(ctx, path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return newFileCheckpointsDB(ctx, path, s, fileName)
}

func NewFileCheckpointsDBWithExstorageFileName(
	ctx context.Context,
	path string,
	s storage.ExternalStorage,
	fileName string,
) (*FileCheckpointsDB, error) {
	return newFileCheckpointsDB(ctx, path, s, fileName)
}

// createExstorageByCompletePath create ExternalStorage by completePath and return fileName.
func createExstorageByCompletePath(ctx context.Context, completePath string) (storage.ExternalStorage, string, error) {
	if completePath == "" {
		return nil, "", nil
	}
	fileName, newPath, err := separateCompletePath(completePath)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	u, err := storage.ParseBackend(newPath, nil)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	s, err := storage.New(ctx, u, &storage.ExternalStorageOptions{})
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	return s, fileName, nil
}

// separateCompletePath separates fileName from completePath, returns fileName and newPath.
func separateCompletePath(completePath string) (string, string, error) {
	if completePath == "" {
		return "", "", nil
	}
	var fileName, newPath string
	purl, err := storage.ParseRawURL(completePath)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	// not url format, we don't use url library to avoid being escaped or unescaped
	if purl.Scheme == "" {
		// no fileName, just path
		if strings.HasSuffix(completePath, "/") {
			return "", completePath, nil
		}
		fileName = path.Base(completePath)
		newPath = path.Dir(completePath)
	} else {
		if strings.HasSuffix(purl.Path, "/") {
			return "", completePath, nil
		}
		fileName = path.Base(purl.Path)
		purl.Path = path.Dir(purl.Path)
		newPath = purl.String()
	}
	return fileName, newPath, nil
}

func (cpdb *FileCheckpointsDB) save() error {
	serialized, err := cpdb.checkpoints.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	return cpdb.exStorage.WriteFile(cpdb.ctx, cpdb.fileName, serialized)
}

func (cpdb *FileCheckpointsDB) Initialize(ctx context.Context, cfg *config.Config, dbInfo map[string]*TidbDBInfo) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	cpdb.checkpoints.TaskCheckpoint = &checkpointspb.TaskCheckpointModel{
		TaskId:       cfg.TaskID,
		SourceDir:    cfg.Mydumper.SourceDir,
		Backend:      cfg.TikvImporter.Backend,
		ImporterAddr: cfg.TikvImporter.Addr,
		TidbHost:     cfg.TiDB.Host,
		TidbPort:     int32(cfg.TiDB.Port),
		PdAddr:       cfg.TiDB.PdAddr,
		SortedKvDir:  cfg.TikvImporter.SortedKVDir,
		LightningVer: build.ReleaseVersion,
	}

	if cpdb.checkpoints.Checkpoints == nil {
		cpdb.checkpoints.Checkpoints = make(map[string]*checkpointspb.TableCheckpointModel)
	}

	for _, db := range dbInfo {
		for _, table := range db.Tables {
			tableName := common.UniqueTable(db.Name, table.Name)
			if _, ok := cpdb.checkpoints.Checkpoints[tableName]; !ok {
				cpdb.checkpoints.Checkpoints[tableName] = &checkpointspb.TableCheckpointModel{
					Status:  uint32(CheckpointStatusLoaded),
					Engines: map[int32]*checkpointspb.EngineCheckpointModel{},
					TableID: table.ID,
				}
			}
			// TODO check if hash matches
		}
	}

	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) TaskCheckpoint(_ context.Context) (*TaskCheckpoint, error) {
	// this method is always called in lock
	cp := cpdb.checkpoints.TaskCheckpoint
	if cp == nil || cp.TaskId == 0 {
		return nil, nil
	}

	return &TaskCheckpoint{
		TaskID:       cp.TaskId,
		SourceDir:    cp.SourceDir,
		Backend:      cp.Backend,
		ImporterAddr: cp.ImporterAddr,
		TiDBHost:     cp.TidbHost,
		TiDBPort:     int(cp.TidbPort),
		PdAddr:       cp.PdAddr,
		SortedKVDir:  cp.SortedKvDir,
		LightningVer: cp.LightningVer,
	}, nil
}

func (cpdb *FileCheckpointsDB) Close() error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) Get(_ context.Context, tableName string) (*TableCheckpoint, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	tableModel, ok := cpdb.checkpoints.Checkpoints[tableName]
	if !ok {
		return nil, errors.NotFoundf("checkpoint for table %s", tableName)
	}

	cp := &TableCheckpoint{
		Status:    CheckpointStatus(tableModel.Status),
		AllocBase: tableModel.AllocBase,
		Engines:   make(map[int32]*EngineCheckpoint, len(tableModel.Engines)),
		TableID:   tableModel.TableID,
		Checksum:  verify.MakeKVChecksum(tableModel.KvBytes, tableModel.KvKvs, tableModel.KvChecksum),
	}

	for engineID, engineModel := range tableModel.Engines {
		engine := &EngineCheckpoint{
			Status: CheckpointStatus(engineModel.Status),
			Chunks: make([]*ChunkCheckpoint, 0, len(engineModel.Chunks)),
		}

		for _, chunkModel := range engineModel.Chunks {
			colPerm := make([]int, 0, len(chunkModel.ColumnPermutation))
			for _, c := range chunkModel.ColumnPermutation {
				colPerm = append(colPerm, int(c))
			}
			engine.Chunks = append(engine.Chunks, &ChunkCheckpoint{
				Key: ChunkCheckpointKey{
					Path:   chunkModel.Path,
					Offset: chunkModel.Offset,
				},
				FileMeta: mydump.SourceFileMeta{
					Path:        chunkModel.Path,
					Type:        mydump.SourceType(chunkModel.Type),
					Compression: mydump.Compression(chunkModel.Compression),
					SortKey:     chunkModel.SortKey,
					FileSize:    chunkModel.FileSize,
				},
				ColumnPermutation: colPerm,
				Chunk: mydump.Chunk{
					Offset:       chunkModel.Pos,
					EndOffset:    chunkModel.EndOffset,
					PrevRowIDMax: chunkModel.PrevRowidMax,
					RowIDMax:     chunkModel.RowidMax,
				},
				Checksum:  verify.MakeKVChecksum(chunkModel.KvcBytes, chunkModel.KvcKvs, chunkModel.KvcChecksum),
				Timestamp: chunkModel.Timestamp,
			})
		}

		sort.Slice(engine.Chunks, func(i, j int) bool {
			return engine.Chunks[i].Key.less(&engine.Chunks[j].Key)
		})

		cp.Engines[engineID] = engine
	}

	return cp, nil
}

func (cpdb *FileCheckpointsDB) InsertEngineCheckpoints(_ context.Context, tableName string, checkpoints map[int32]*EngineCheckpoint) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	tableModel := cpdb.checkpoints.Checkpoints[tableName]
	for engineID, engine := range checkpoints {
		engineModel := &checkpointspb.EngineCheckpointModel{
			Status: uint32(CheckpointStatusLoaded),
			Chunks: make(map[string]*checkpointspb.ChunkCheckpointModel),
		}
		for _, value := range engine.Chunks {
			key := value.Key.String()
			chunk, ok := engineModel.Chunks[key]
			if !ok {
				chunk = &checkpointspb.ChunkCheckpointModel{
					Path:   value.Key.Path,
					Offset: value.Key.Offset,
				}
				engineModel.Chunks[key] = chunk
			}
			chunk.Type = int32(value.FileMeta.Type)
			chunk.Compression = int32(value.FileMeta.Compression)
			chunk.SortKey = value.FileMeta.SortKey
			chunk.FileSize = value.FileMeta.FileSize
			chunk.Pos = value.Chunk.Offset
			chunk.EndOffset = value.Chunk.EndOffset
			chunk.PrevRowidMax = value.Chunk.PrevRowIDMax
			chunk.RowidMax = value.Chunk.RowIDMax
			chunk.Timestamp = value.Timestamp
			if len(value.ColumnPermutation) > 0 {
				chunk.ColumnPermutation = intSlice2Int32Slice(value.ColumnPermutation)
			}
		}
		tableModel.Engines[engineID] = engineModel
	}

	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) Update(checkpointDiffs map[string]*TableCheckpointDiff) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	for tableName, cpd := range checkpointDiffs {
		tableModel := cpdb.checkpoints.Checkpoints[tableName]
		if cpd.hasStatus {
			tableModel.Status = uint32(cpd.status)
		}
		if cpd.hasRebase {
			tableModel.AllocBase = cpd.allocBase
		}
		if cpd.hasChecksum {
			tableModel.KvBytes = cpd.checksum.SumSize()
			tableModel.KvKvs = cpd.checksum.SumKVS()
			tableModel.KvChecksum = cpd.checksum.Sum()
		}
		for engineID, engineDiff := range cpd.engines {
			engineModel := tableModel.Engines[engineID]
			if engineDiff.hasStatus {
				engineModel.Status = uint32(engineDiff.status)
			}

			for key, diff := range engineDiff.chunks {
				chunkModel := engineModel.Chunks[key.String()]
				chunkModel.Pos = diff.pos
				chunkModel.PrevRowidMax = diff.rowID
				chunkModel.KvcBytes = diff.checksum.SumSize()
				chunkModel.KvcKvs = diff.checksum.SumKVS()
				chunkModel.KvcChecksum = diff.checksum.Sum()
				chunkModel.ColumnPermutation = intSlice2Int32Slice(diff.columnPermutation)
			}
		}
	}

	return cpdb.save()
}

// Management functions ----------------------------------------------------------------------------

var errCannotManageNullDB = errors.New("cannot perform this function while checkpoints is disabled")

func (*NullCheckpointsDB) RemoveCheckpoint(context.Context, string) error {
	return errors.Trace(errCannotManageNullDB)
}

func (*NullCheckpointsDB) MoveCheckpoints(context.Context, int64) error {
	return errors.Trace(errCannotManageNullDB)
}

func (*NullCheckpointsDB) GetLocalStoringTables(context.Context) (map[string][]int32, error) {
	return nil, nil
}

func (*NullCheckpointsDB) IgnoreErrorCheckpoint(context.Context, string) error {
	return errors.Trace(errCannotManageNullDB)
}

func (*NullCheckpointsDB) DestroyErrorCheckpoint(context.Context, string) ([]DestroyedTableCheckpoint, error) {
	return nil, errors.Trace(errCannotManageNullDB)
}

func (*NullCheckpointsDB) DumpTables(context.Context, io.Writer) error {
	return errors.Trace(errCannotManageNullDB)
}

func (*NullCheckpointsDB) DumpEngines(context.Context, io.Writer) error {
	return errors.Trace(errCannotManageNullDB)
}

func (*NullCheckpointsDB) DumpChunks(context.Context, io.Writer) error {
	return errors.Trace(errCannotManageNullDB)
}

func (cpdb *MySQLCheckpointsDB) RemoveCheckpoint(ctx context.Context, tableName string) error {
	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.With(zap.String("table", tableName)),
	}

	if tableName == allTables {
		return s.Exec(ctx, "remove all checkpoints", "DROP SCHEMA "+cpdb.schema)
	}

	deleteChunkQuery := fmt.Sprintf(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameChunk)
	deleteEngineQuery := fmt.Sprintf(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameEngine)
	deleteTableQuery := fmt.Sprintf(DeleteCheckpointRecordTemplate, cpdb.schema, CheckpointTableNameTable)

	return s.Transact(ctx, "remove checkpoints", func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, deleteChunkQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteEngineQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteTableQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
}

func (cpdb *MySQLCheckpointsDB) MoveCheckpoints(ctx context.Context, taskID int64) error {
	// The "cpdb.schema" is an escaped schema name of the form "`foo`".
	// We use "x[1:len(x)-1]" instead of unescaping it to keep the
	// double-backquotes (if any) intact.
	newSchema := fmt.Sprintf("`%s.%d.bak`", cpdb.schema[1:len(cpdb.schema)-1], taskID)
	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.With(zap.Int64("taskID", taskID)),
	}

	createSchemaQuery := "CREATE SCHEMA IF NOT EXISTS " + newSchema
	if e := s.Exec(ctx, "create backup checkpoints schema", createSchemaQuery); e != nil {
		return e
	}
	for _, tbl := range []string{
		CheckpointTableNameChunk, CheckpointTableNameEngine,
		CheckpointTableNameTable, CheckpointTableNameTask,
	} {
		query := fmt.Sprintf("RENAME TABLE %[1]s.%[3]s TO %[2]s.%[3]s", cpdb.schema, newSchema, tbl)
		if e := s.Exec(ctx, fmt.Sprintf("move %s checkpoints table", tbl), query); e != nil {
			return e
		}
	}

	return nil
}

func (cpdb *MySQLCheckpointsDB) GetLocalStoringTables(ctx context.Context) (map[string][]int32, error) {
	var targetTables map[string][]int32

	// lightning didn't check CheckpointStatusMaxInvalid before this function is called, so we skip invalid ones
	// engines should exist if
	// 1. table status is earlier than CheckpointStatusIndexImported, and
	// 2. engine status is earlier than CheckpointStatusImported, and
	// 3. chunk has been read

	query := fmt.Sprintf(`
		SELECT DISTINCT t.table_name, c.engine_id
		FROM %s.%s t, %s.%s c, %s.%s e
		WHERE t.table_name = c.table_name AND t.table_name = e.table_name AND c.engine_id = e.engine_id
			AND %d < t.status AND t.status < %d
			AND %d < e.status AND e.status < %d
			AND c.pos > c.offset;`,
		cpdb.schema, CheckpointTableNameTable, cpdb.schema, CheckpointTableNameChunk, cpdb.schema, CheckpointTableNameEngine,
		CheckpointStatusMaxInvalid, CheckpointStatusIndexImported,
		CheckpointStatusMaxInvalid, CheckpointStatusImported)

	err := common.Retry("get local storing tables", log.L(), func() error {
		targetTables = make(map[string][]int32)
		rows, err := cpdb.db.QueryContext(ctx, query) // #nosec G201
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		for rows.Next() {
			var (
				tableName string
				engineID  int32
			)
			if err := rows.Scan(&tableName, &engineID); err != nil {
				return errors.Trace(err)
			}
			targetTables[tableName] = append(targetTables[tableName], engineID)
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, err
}

func (cpdb *MySQLCheckpointsDB) IgnoreErrorCheckpoint(ctx context.Context, tableName string) error {
	var colName string
	if tableName == allTables {
		// This will expand to `WHERE 'all' = 'all'` and effectively allowing
		// all tables to be included.
		colName = stringLitAll
	} else {
		colName = columnTableName
	}

	// nolint:gosec
	engineQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = %d WHERE %s = ? AND status <= %d;
	`, cpdb.schema, CheckpointTableNameEngine, CheckpointStatusLoaded, colName, CheckpointStatusMaxInvalid)

	// nolint:gosec
	tableQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = %d WHERE %s = ? AND status <= %d;
	`, cpdb.schema, CheckpointTableNameTable, CheckpointStatusLoaded, colName, CheckpointStatusMaxInvalid)

	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	err := s.Transact(ctx, "ignore error checkpoints", func(c context.Context, tx *sql.Tx) error {
		if _, e := tx.ExecContext(c, engineQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, tableQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	return errors.Trace(err)
}

func (cpdb *MySQLCheckpointsDB) DestroyErrorCheckpoint(ctx context.Context, tableName string) ([]DestroyedTableCheckpoint, error) {
	var colName, aliasedColName string

	if tableName == allTables {
		// These will expand to `WHERE 'all' = 'all'` and effectively allowing
		// all tables to be included.
		colName = stringLitAll
		aliasedColName = stringLitAll
	} else {
		colName = columnTableName
		aliasedColName = "t.table_name"
	}

	selectQuery := fmt.Sprintf(`
		SELECT
			t.table_name,
			COALESCE(MIN(e.engine_id), 0),
			COALESCE(MAX(e.engine_id), -1)
		FROM %[1]s.%[4]s t
		LEFT JOIN %[1]s.%[5]s e ON t.table_name = e.table_name
		WHERE %[2]s = ? AND t.status <= %[3]d
		GROUP BY t.table_name;
	`, cpdb.schema, aliasedColName, CheckpointStatusMaxInvalid, CheckpointTableNameTable, CheckpointTableNameEngine)

	// nolint:gosec
	deleteChunkQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[4]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[5]s WHERE %[2]s = ? AND status <= %[3]d)
	`, cpdb.schema, colName, CheckpointStatusMaxInvalid, CheckpointTableNameChunk, CheckpointTableNameTable)

	// nolint:gosec
	deleteEngineQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[4]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[5]s WHERE %[2]s = ? AND status <= %[3]d)
	`, cpdb.schema, colName, CheckpointStatusMaxInvalid, CheckpointTableNameEngine, CheckpointTableNameTable)

	// nolint:gosec
	deleteTableQuery := fmt.Sprintf(`
		DELETE FROM %s.%s WHERE %s = ? AND status <= %d
	`, cpdb.schema, CheckpointTableNameTable, colName, CheckpointStatusMaxInvalid)

	var targetTables []DestroyedTableCheckpoint

	s := common.SQLWithRetry{
		DB:     cpdb.db,
		Logger: log.With(zap.String("table", tableName)),
	}
	err := s.Transact(ctx, "destroy error checkpoints", func(c context.Context, tx *sql.Tx) error {
		// Obtain the list of tables
		targetTables = nil
		rows, e := tx.QueryContext(c, selectQuery, tableName) // #nosec G201
		if e != nil {
			return errors.Trace(e)
		}
		defer rows.Close()
		for rows.Next() {
			var dtc DestroyedTableCheckpoint
			if e := rows.Scan(&dtc.TableName, &dtc.MinEngineID, &dtc.MaxEngineID); e != nil {
				return errors.Trace(e)
			}
			targetTables = append(targetTables, dtc)
		}
		if e := rows.Err(); e != nil {
			return errors.Trace(e)
		}

		// Delete the checkpoints
		if _, e := tx.ExecContext(c, deleteChunkQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteEngineQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		if _, e := tx.ExecContext(c, deleteTableQuery, tableName); e != nil {
			return errors.Trace(e)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpTables(ctx context.Context, writer io.Writer) error {
	rows, err := cpdb.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			task_id,
			table_name,
			hex(hash) AS hash,
			status,
			alloc_base,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameTable))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpEngines(ctx context.Context, writer io.Writer) error {
	rows, err := cpdb.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			table_name,
			engine_id,
			status,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameEngine))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

//nolint:rowserrcheck // sqltocsv.Write will check this.
func (cpdb *MySQLCheckpointsDB) DumpChunks(ctx context.Context, writer io.Writer) error {
	rows, err := cpdb.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			table_name,
			path,
			offset,
			type,
			compression,
			sort_key,
			file_size,
			columns,
			pos,
			end_offset,
			prev_rowid_max,
			rowid_max,
			kvc_bytes,
			kvc_kvs,
			kvc_checksum,
			create_time,
			update_time
		FROM %s.%s;
	`, cpdb.schema, CheckpointTableNameChunk))
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	return errors.Trace(sqltocsv.Write(writer, rows))
}

func (cpdb *FileCheckpointsDB) RemoveCheckpoint(_ context.Context, tableName string) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	if tableName == allTables {
		cpdb.checkpoints.Reset()
		return errors.Trace(cpdb.exStorage.DeleteFile(cpdb.ctx, cpdb.fileName))
	}

	delete(cpdb.checkpoints.Checkpoints, tableName)
	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) MoveCheckpoints(ctx context.Context, taskID int64) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	newFileName := fmt.Sprintf("%s.%d.bak", cpdb.fileName, taskID)
	return cpdb.exStorage.Rename(cpdb.ctx, cpdb.fileName, newFileName)
}

func (cpdb *FileCheckpointsDB) GetLocalStoringTables(_ context.Context) (map[string][]int32, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	targetTables := make(map[string][]int32)

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) ||
			tableModel.Status >= uint32(CheckpointStatusIndexImported) {
			continue
		}
		for engineID, engineModel := range tableModel.Engines {
			if engineModel.Status <= uint32(CheckpointStatusMaxInvalid) ||
				engineModel.Status >= uint32(CheckpointStatusImported) {
				continue
			}

			for _, chunkModel := range engineModel.Chunks {
				if chunkModel.Pos > chunkModel.Offset {
					targetTables[tableName] = append(targetTables[tableName], engineID)
					break
				}
			}
		}
	}

	return targetTables, nil
}

func (cpdb *FileCheckpointsDB) IgnoreErrorCheckpoint(_ context.Context, targetTableName string) error {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		if !(targetTableName == allTables || targetTableName == tableName) {
			continue
		}
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) {
			tableModel.Status = uint32(CheckpointStatusLoaded)
		}
		for _, engineModel := range tableModel.Engines {
			if engineModel.Status <= uint32(CheckpointStatusMaxInvalid) {
				engineModel.Status = uint32(CheckpointStatusLoaded)
			}
		}
	}
	return errors.Trace(cpdb.save())
}

func (cpdb *FileCheckpointsDB) DestroyErrorCheckpoint(_ context.Context, targetTableName string) ([]DestroyedTableCheckpoint, error) {
	cpdb.lock.Lock()
	defer cpdb.lock.Unlock()

	var targetTables []DestroyedTableCheckpoint

	for tableName, tableModel := range cpdb.checkpoints.Checkpoints {
		// Obtain the list of tables
		if !(targetTableName == allTables || targetTableName == tableName) {
			continue
		}
		if tableModel.Status <= uint32(CheckpointStatusMaxInvalid) {
			var minEngineID, maxEngineID int32 = math.MaxInt32, math.MinInt32
			for engineID := range tableModel.Engines {
				if engineID < minEngineID {
					minEngineID = engineID
				}
				if engineID > maxEngineID {
					maxEngineID = engineID
				}
			}

			targetTables = append(targetTables, DestroyedTableCheckpoint{
				TableName:   tableName,
				MinEngineID: minEngineID,
				MaxEngineID: maxEngineID,
			})
		}
	}

	// Delete the checkpoints
	for _, dtcp := range targetTables {
		delete(cpdb.checkpoints.Checkpoints, dtcp.TableName)
	}
	if err := cpdb.save(); err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

func (cpdb *FileCheckpointsDB) DumpTables(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

func (cpdb *FileCheckpointsDB) DumpEngines(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

func (cpdb *FileCheckpointsDB) DumpChunks(context.Context, io.Writer) error {
	return errors.Errorf("dumping file checkpoint into CSV not unsupported, you may copy %s instead", cpdb.path)
}

func intSlice2Int32Slice(s []int) []int32 {
	res := make([]int32, 0, len(s))
	for _, i := range s {
		res = append(res, int32(i))
	}
	return res
}

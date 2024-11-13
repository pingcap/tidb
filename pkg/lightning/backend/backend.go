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

package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

const (
	importMaxRetryTimes = 3 // tikv-importer has done retry internally. so we don't retry many times.
)

func makeTag(tableName string, engineID int64) string {
	return fmt.Sprintf("%s:%d", tableName, engineID)
}

func makeLogger(logger log.Logger, tag string, engineUUID uuid.UUID) log.Logger {
	return logger.With(
		zap.String("engineTag", tag),
		zap.Stringer("engineUUID", engineUUID),
	)
}

// MakeUUID generates a UUID for the engine and a tag for the engine.
func MakeUUID(tableName string, engineID int64) (string, uuid.UUID) {
	tag := makeTag(tableName, engineID)
	engineUUID := uuid.NewSHA1(engineNamespace, []byte(tag))
	return tag, engineUUID
}

var engineNamespace = uuid.MustParse("d68d6abe-c59e-45d6-ade8-e2b0ceb7bedf")

// EngineFileSize represents the size of an engine on disk and in memory.
type EngineFileSize struct {
	// UUID is the engine's UUID.
	UUID uuid.UUID
	// DiskSize is the estimated total file size on disk right now.
	DiskSize int64
	// MemSize is the total memory size used by the engine. This is the
	// estimated additional size saved onto disk after calling Flush().
	MemSize int64
	// IsImporting indicates whether the engine performing Import().
	IsImporting bool
}

// LocalWriterConfig defines the configuration to open a LocalWriter
type LocalWriterConfig struct {
	// is the chunk KV written to this LocalWriter sent in order
	// only needed for local backend, can omit for tidb backend
	IsKVSorted bool
	// only needed for tidb backend, can omit for local backend
	TableName string
}

// EngineConfig defines configuration used for open engine
type EngineConfig struct {
	// TableInfo is the corresponding tidb table info
	TableInfo *checkpoints.TidbTableInfo
	// local backend specified configuration
	Local LocalEngineConfig
	// local backend external engine specified configuration
	External *ExternalEngineConfig
	// KeepSortDir indicates whether to keep the temporary sort directory
	// when opening the engine, instead of removing it.
	KeepSortDir bool
	// TS is the preset timestamp of data in the engine. When it's 0, the used TS
	// will be set lazily.
	TS uint64
}

// LocalEngineConfig is the configuration used for local backend in OpenEngine.
type LocalEngineConfig struct {
	// compact small SSTs before ingest into pebble
	Compact bool
	// raw kvs size threshold to trigger compact
	CompactThreshold int64
	// compact routine concurrency
	CompactConcurrency int

	// blocksize
	BlockSize int
}

// ExternalEngineConfig is the configuration used for local backend external engine.
type ExternalEngineConfig struct {
	StorageURI      string
	DataFiles       []string
	StatFiles       []string
	StartKey        []byte
	EndKey          []byte
	SplitKeys       [][]byte
	RegionSplitSize int64
	// TotalFileSize can be an estimated value.
	TotalFileSize int64
	// TotalKVCount can be an estimated value.
	TotalKVCount int64
	CheckHotspot bool
}

// CheckCtx contains all parameters used in CheckRequirements
type CheckCtx struct {
	DBMetas []*mydump.MDDatabaseMeta
}

// TargetInfoGetter defines the interfaces to get target information.
type TargetInfoGetter interface {
	// FetchRemoteDBModels obtains the models of all databases. Currently, only
	// the database name is filled.
	FetchRemoteDBModels(ctx context.Context) ([]*model.DBInfo, error)

	// FetchRemoteTableModels obtains the models of all tables given the schema
	// name. The returned table info does not need to be precise if the encoder,
	// is not requiring them, but must at least fill in the following fields for
	// TablesFromMeta to succeed:
	//  - Name
	//  - State (must be model.StatePublic)
	//  - ID
	//  - Columns
	//     * Name
	//     * State (must be model.StatePublic)
	//     * Offset (must be 0, 1, 2, ...)
	//  - PKIsHandle (true = do not generate _tidb_rowid)
	FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error)

	// CheckRequirements performs the check whether the backend satisfies the version requirements
	CheckRequirements(ctx context.Context, checkCtx *CheckCtx) error
}

// Backend defines the interface for a backend.
// Implementations of this interface must be goroutine safe: you can share an
// instance and execute any method anywhere.
// Usual workflow:
//  1. Create a `Backend` for the whole process.
//  2. For each table,
//     i. Split into multiple "batches" consisting of data files with roughly equal total size.
//     ii. For each batch,
//     a. Create an `OpenedEngine` via `backend.OpenEngine()`
//     b. For each chunk, deliver data into the engine via `engine.WriteRows()`
//     c. When all chunks are written, obtain a `ClosedEngine` via `engine.Close()`
//     d. Import data via `engine.Import()`
//     e. Cleanup via `engine.Cleanup()`
//  3. Close the connection via `backend.Close()`
type Backend interface {
	// Close the connection to the backend.
	Close()

	// RetryImportDelay returns the duration to sleep when retrying an import
	RetryImportDelay() time.Duration

	// ShouldPostProcess returns whether KV-specific post-processing should be
	// performed for this backend. Post-processing includes checksum and analyze.
	ShouldPostProcess() bool

	OpenEngine(ctx context.Context, config *EngineConfig, engineUUID uuid.UUID) error

	CloseEngine(ctx context.Context, config *EngineConfig, engineUUID uuid.UUID) error

	// ImportEngine imports engine data to the backend. If it returns ErrDuplicateDetected,
	// it means there is duplicate detected. For this situation, all data in the engine must be imported.
	// It's safe to reset or cleanup this engine.
	ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error

	CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error

	// FlushEngine ensures all KV pairs written to an open engine has been
	// synchronized, such that kill-9'ing Lightning afterwards and resuming from
	// checkpoint can recover the exact same content.
	//
	// This method is only relevant for local backend, and is no-op for all
	// other backends.
	FlushEngine(ctx context.Context, engineUUID uuid.UUID) error

	// FlushAllEngines performs FlushEngine on all opened engines. This is a
	// very expensive operation and should only be used in some rare situation
	// (e.g. preparing to resolve a disk quota violation).
	FlushAllEngines(ctx context.Context) error

	// ResetEngine clears all written KV pairs in this opened engine.
	ResetEngine(ctx context.Context, engineUUID uuid.UUID) error

	// LocalWriter obtains a thread-local EngineWriter for writing rows into the given engine.
	LocalWriter(ctx context.Context, cfg *LocalWriterConfig, engineUUID uuid.UUID) (EngineWriter, error)
}

// EngineManager is the manager of engines.
// this is a wrapper of Backend, which provides some common methods for managing engines.
// and it has no states, can be created on demand
type EngineManager struct {
	backend Backend
}

type engine struct {
	backend Backend
	logger  log.Logger
	uuid    uuid.UUID
	// id of the engine, used to generate uuid and stored in checkpoint
	// for index engine it's -1
	id int32
}

// OpenedEngine is an opened engine, allowing data to be written via WriteRows.
// This type is goroutine safe: you can share an instance and execute any method
// anywhere.
type OpenedEngine struct {
	engine
	tableName string
	config    *EngineConfig
}

// MakeEngineManager creates a new Backend from an Backend.
func MakeEngineManager(ab Backend) EngineManager {
	return EngineManager{backend: ab}
}

// OpenEngine opens an engine with the given table name and engine ID.
func (be EngineManager) OpenEngine(
	ctx context.Context,
	config *EngineConfig,
	tableName string,
	engineID int32,
) (*OpenedEngine, error) {
	tag, engineUUID := MakeUUID(tableName, int64(engineID))
	logger := makeLogger(log.FromContext(ctx), tag, engineUUID)

	if err := be.backend.OpenEngine(ctx, config, engineUUID); err != nil {
		return nil, err
	}

	if m, ok := metric.FromContext(ctx); ok {
		openCounter := m.ImporterEngineCounter.WithLabelValues("open")
		openCounter.Inc()
	}

	logger.Info("open engine")

	failpoint.Inject("FailIfEngineCountExceeds", func(val failpoint.Value) {
		if m, ok := metric.FromContext(ctx); ok {
			closedCounter := m.ImporterEngineCounter.WithLabelValues("closed")
			openCounter := m.ImporterEngineCounter.WithLabelValues("open")
			openCount := metric.ReadCounter(openCounter)

			closedCount := metric.ReadCounter(closedCounter)
			if injectValue := val.(int); openCount-closedCount > float64(injectValue) {
				panic(fmt.Sprintf(
					"forcing failure due to FailIfEngineCountExceeds: %v - %v >= %d",
					openCount, closedCount, injectValue))
			}
		}
	})

	return &OpenedEngine{
		engine: engine{
			backend: be.backend,
			logger:  logger,
			uuid:    engineUUID,
			id:      engineID,
		},
		tableName: tableName,
		config:    config,
	}, nil
}

// Close the opened engine to prepare it for importing.
func (engine *OpenedEngine) Close(ctx context.Context) (*ClosedEngine, error) {
	closedEngine, err := engine.unsafeClose(ctx, engine.config)
	if err == nil {
		if m, ok := metric.FromContext(ctx); ok {
			m.ImporterEngineCounter.WithLabelValues("closed").Inc()
		}
	}
	return closedEngine, err
}

// Flush current written data for local backend
func (engine *OpenedEngine) Flush(ctx context.Context) error {
	return engine.backend.FlushEngine(ctx, engine.uuid)
}

// LocalWriter returns a writer that writes to the local backend.
func (engine *OpenedEngine) LocalWriter(ctx context.Context, cfg *LocalWriterConfig) (EngineWriter, error) {
	return engine.backend.LocalWriter(ctx, cfg, engine.uuid)
}

// SetTS sets the TS of the engine. In most cases if the caller wants to specify
// TS it should use the TS field in EngineConfig. This method is only used after
// a ResetEngine.
func (engine *OpenedEngine) SetTS(ts uint64) {
	engine.config.TS = ts
}

// UnsafeCloseEngine closes the engine without first opening it.
// This method is "unsafe" as it does not follow the normal operation sequence
// (Open -> Write -> Close -> Import). This method should only be used when one
// knows via other ways that the engine has already been opened, e.g. when
// resuming from a checkpoint.
func (be EngineManager) UnsafeCloseEngine(ctx context.Context, cfg *EngineConfig,
	tableName string, engineID int32) (*ClosedEngine, error) {
	tag, engineUUID := MakeUUID(tableName, int64(engineID))
	return be.UnsafeCloseEngineWithUUID(ctx, cfg, tag, engineUUID, engineID)
}

// UnsafeCloseEngineWithUUID closes the engine without first opening it.
// This method is "unsafe" as it does not follow the normal operation sequence
// (Open -> Write -> Close -> Import). This method should only be used when one
// knows via other ways that the engine has already been opened, e.g. when
// resuming from a checkpoint.
func (be EngineManager) UnsafeCloseEngineWithUUID(ctx context.Context, cfg *EngineConfig, tag string,
	engineUUID uuid.UUID, id int32) (*ClosedEngine, error) {
	return engine{
		backend: be.backend,
		logger:  makeLogger(log.FromContext(ctx), tag, engineUUID),
		uuid:    engineUUID,
		id:      id,
	}.unsafeClose(ctx, cfg)
}

func (en engine) unsafeClose(ctx context.Context, cfg *EngineConfig) (*ClosedEngine, error) {
	task := en.logger.Begin(zap.InfoLevel, "engine close")
	err := en.backend.CloseEngine(ctx, cfg, en.uuid)
	task.End(zap.ErrorLevel, err)
	if err != nil {
		return nil, err
	}
	return &ClosedEngine{engine: en}, nil
}

// GetID get engine id.
func (en engine) GetID() int32 {
	return en.id
}

func (en engine) GetUUID() uuid.UUID {
	return en.uuid
}

// ClosedEngine represents a closed engine, allowing ingestion into the target.
// This type is goroutine safe: you can share an instance and execute any method
// anywhere.
type ClosedEngine struct {
	engine
}

// NewClosedEngine creates a new ClosedEngine.
func NewClosedEngine(backend Backend, logger log.Logger, uuid uuid.UUID, id int32) *ClosedEngine {
	return &ClosedEngine{
		engine: engine{
			backend: backend,
			logger:  logger,
			uuid:    uuid,
			id:      id,
		},
	}
}

// Import the data written to the engine into the target.
func (engine *ClosedEngine) Import(ctx context.Context, regionSplitSize, regionSplitKeys int64) error {
	var err error

	for i := 0; i < importMaxRetryTimes; i++ {
		task := engine.logger.With(zap.Int("retryCnt", i)).Begin(zap.InfoLevel, "import")
		err = engine.backend.ImportEngine(ctx, engine.uuid, regionSplitSize, regionSplitKeys)
		if !common.IsRetryableError(err) {
			if common.ErrFoundDuplicateKeys.Equal(err) {
				task.End(zap.WarnLevel, err)
			} else {
				task.End(zap.ErrorLevel, err)
			}
			return err
		}
		task.Warn("import spuriously failed, going to retry again", log.ShortError(err))
		time.Sleep(engine.backend.RetryImportDelay())
	}

	return errors.Annotatef(err, "[%s] import reach max retry %d and still failed", engine.uuid, importMaxRetryTimes)
}

// Cleanup deletes the intermediate data from target.
func (engine *ClosedEngine) Cleanup(ctx context.Context) error {
	task := engine.logger.Begin(zap.InfoLevel, "cleanup")
	err := engine.backend.CleanupEngine(ctx, engine.uuid)
	task.End(zap.WarnLevel, err)
	return err
}

// Logger returns the logger for the engine.
func (engine *ClosedEngine) Logger() log.Logger {
	return engine.logger
}

// ChunkFlushStatus is the status of a chunk flush.
type ChunkFlushStatus interface {
	Flushed() bool
}

// EngineWriter is the interface for writing data to an engine.
type EngineWriter interface {
	AppendRows(ctx context.Context, columnNames []string, rows encode.Rows) error
	IsSynced() bool
	Close(ctx context.Context) (ChunkFlushStatus, error)
}

// GetEngineUUID returns the engine UUID.
func (engine *OpenedEngine) GetEngineUUID() uuid.UUID {
	return engine.uuid
}

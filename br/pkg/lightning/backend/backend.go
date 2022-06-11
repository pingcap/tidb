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
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"go.uber.org/zap"
)

const (
	importMaxRetryTimes = 3 // tikv-importer has done retry internally. so we don't retry many times.
)

/*

Usual workflow:

1. Create a `Backend` for the whole process.

2. For each table,

	i. Split into multiple "batches" consisting of data files with roughly equal total size.

	ii. For each batch,

		a. Create an `OpenedEngine` via `backend.OpenEngine()`

		b. For each chunk, deliver data into the engine via `engine.WriteRows()`

		c. When all chunks are written, obtain a `ClosedEngine` via `engine.Close()`

		d. Import data via `engine.Import()`

		e. Cleanup via `engine.Cleanup()`

3. Close the connection via `backend.Close()`

*/

func makeTag(tableName string, engineID int32) string {
	return fmt.Sprintf("%s:%d", tableName, engineID)
}

func makeLogger(tag string, engineUUID uuid.UUID) log.Logger {
	return log.With(
		zap.String("engineTag", tag),
		zap.Stringer("engineUUID", engineUUID),
	)
}

func MakeUUID(tableName string, engineID int32) (string, uuid.UUID) {
	tag := makeTag(tableName, engineID)
	engineUUID := uuid.NewSHA1(engineNamespace, []byte(tag))
	return tag, engineUUID
}

var engineNamespace = uuid.MustParse("d68d6abe-c59e-45d6-ade8-e2b0ceb7bedf")

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
	IsKVSorted bool
}

// EngineConfig defines configuration used for open engine
type EngineConfig struct {
	// TableInfo is the corresponding tidb table info
	TableInfo *checkpoints.TidbTableInfo
	// local backend specified configuration
	Local *LocalEngineConfig
}

// LocalEngineConfig is the configuration used for local backend in OpenEngine.
type LocalEngineConfig struct {
	// compact small SSTs before ingest into pebble
	Compact bool
	// raw kvs size threshold to trigger compact
	CompactThreshold int64
	// compact routine concurrency
	CompactConcurrency int
}

// CheckCtx contains all parameters used in CheckRequirements
type CheckCtx struct {
	DBMetas []*mydump.MDDatabaseMeta
}

// AbstractBackend is the abstract interface behind Backend.
// Implementations of this interface must be goroutine safe: you can share an
// instance and execute any method anywhere.
type AbstractBackend interface {
	// Close the connection to the backend.
	Close()

	// MakeEmptyRows creates an empty collection of encoded rows.
	MakeEmptyRows() kv.Rows

	// RetryImportDelay returns the duration to sleep when retrying an import
	RetryImportDelay() time.Duration

	// ShouldPostProcess returns whether KV-specific post-processing should be
	// performed for this backend. Post-processing includes checksum and analyze.
	ShouldPostProcess() bool

	// NewEncoder creates an encoder of a TiDB table.
	NewEncoder(tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error)

	OpenEngine(ctx context.Context, config *EngineConfig, engineUUID uuid.UUID) error

	CloseEngine(ctx context.Context, config *EngineConfig, engineUUID uuid.UUID) error

	// ImportEngine imports engine data to the backend. If it returns ErrDuplicateDetected,
	// it means there is duplicate detected. For this situation, all data in the engine must be imported.
	// It's safe to reset or cleanup this engine.
	ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error

	CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error

	// CheckRequirements performs the check whether the backend satisfies the
	// version requirements
	CheckRequirements(ctx context.Context, checkCtx *CheckCtx) error

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

	// EngineFileSizes obtains the size occupied locally of all engines managed
	// by this backend. This method is used to compute disk quota.
	// It can return nil if the content are all stored remotely.
	EngineFileSizes() []EngineFileSize

	// ResetEngine clears all written KV pairs in this opened engine.
	ResetEngine(ctx context.Context, engineUUID uuid.UUID) error

	// LocalWriter obtains a thread-local EngineWriter for writing rows into the given engine.
	LocalWriter(ctx context.Context, cfg *LocalWriterConfig, engineUUID uuid.UUID) (EngineWriter, error)

	// CollectLocalDuplicateRows collect duplicate keys from local db. We will store the duplicate keys which
	//  may be repeated with other keys in local data source.
	CollectLocalDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (hasDupe bool, err error)

	// CollectRemoteDuplicateRows collect duplicate keys from remote TiKV storage. This keys may be duplicate with
	//  the data import by other lightning.
	CollectRemoteDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (hasDupe bool, err error)

	// ResolveDuplicateRows resolves duplicated rows by deleting/inserting data
	// according to the required algorithm.
	ResolveDuplicateRows(ctx context.Context, tbl table.Table, tableName string, algorithm config.DuplicateResolutionAlgorithm) error
}

// Backend is the delivery target for Lightning
type Backend struct {
	abstract AbstractBackend
}

type engine struct {
	backend AbstractBackend
	logger  log.Logger
	uuid    uuid.UUID
}

// OpenedEngine is an opened engine, allowing data to be written via WriteRows.
// This type is goroutine safe: you can share an instance and execute any method
// anywhere.
type OpenedEngine struct {
	engine
	tableName string
}

// // import_ the data written to the engine into the target.
// import_(ctx context.Context) error

// // cleanup deletes the imported data.
// cleanup(ctx context.Context) error

// ClosedEngine represents a closed engine, allowing ingestion into the target.
// This type is goroutine safe: you can share an instance and execute any method
// anywhere.
type ClosedEngine struct {
	engine
}

type LocalEngineWriter struct {
	writer    EngineWriter
	tableName string
}

func MakeBackend(ab AbstractBackend) Backend {
	return Backend{abstract: ab}
}

func (be Backend) Close() {
	be.abstract.Close()
}

func (be Backend) MakeEmptyRows() kv.Rows {
	return be.abstract.MakeEmptyRows()
}

func (be Backend) NewEncoder(tbl table.Table, options *kv.SessionOptions) (kv.Encoder, error) {
	return be.abstract.NewEncoder(tbl, options)
}

func (be Backend) ShouldPostProcess() bool {
	return be.abstract.ShouldPostProcess()
}

func (be Backend) CheckRequirements(ctx context.Context, checkCtx *CheckCtx) error {
	return be.abstract.CheckRequirements(ctx, checkCtx)
}

func (be Backend) FetchRemoteTableModels(ctx context.Context, schemaName string) ([]*model.TableInfo, error) {
	return be.abstract.FetchRemoteTableModels(ctx, schemaName)
}

func (be Backend) FlushAll(ctx context.Context) error {
	return be.abstract.FlushAllEngines(ctx)
}

// CheckDiskQuota verifies if the total engine file size is below the given
// quota. If the quota is exceeded, this method returns an array of engines,
// which after importing can decrease the total size below quota.
func (be Backend) CheckDiskQuota(quota int64) (
	largeEngines []uuid.UUID,
	inProgressLargeEngines int,
	totalDiskSize int64,
	totalMemSize int64,
) {
	sizes := be.abstract.EngineFileSizes()
	sort.Slice(sizes, func(i, j int) bool {
		a, b := &sizes[i], &sizes[j]
		if a.IsImporting != b.IsImporting {
			return a.IsImporting
		}
		return a.DiskSize+a.MemSize < b.DiskSize+b.MemSize
	})
	for _, size := range sizes {
		totalDiskSize += size.DiskSize
		totalMemSize += size.MemSize
		if totalDiskSize+totalMemSize > quota {
			if size.IsImporting {
				inProgressLargeEngines++
			} else {
				largeEngines = append(largeEngines, size.UUID)
			}
		}
	}
	return
}

// UnsafeImportAndReset forces the backend to import the content of an engine
// into the target and then reset the engine to empty. This method will not
// close the engine. Make sure the engine is flushed manually before calling
// this method.
func (be Backend) UnsafeImportAndReset(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	// DO NOT call be.abstract.CloseEngine()! The engine should still be writable after
	// calling UnsafeImportAndReset().
	closedEngine := ClosedEngine{
		engine: engine{
			backend: be.abstract,
			logger:  makeLogger("<import-and-reset>", engineUUID),
			uuid:    engineUUID,
		},
	}
	if err := closedEngine.Import(ctx, regionSplitSize, regionSplitKeys); err != nil {
		return err
	}
	return be.abstract.ResetEngine(ctx, engineUUID)
}

// OpenEngine opens an engine with the given table name and engine ID.
func (be Backend) OpenEngine(ctx context.Context, config *EngineConfig, tableName string, engineID int32) (*OpenedEngine, error) {
	tag, engineUUID := MakeUUID(tableName, engineID)
	logger := makeLogger(tag, engineUUID)

	if err := be.abstract.OpenEngine(ctx, config, engineUUID); err != nil {
		return nil, err
	}

	openCounter := metric.ImporterEngineCounter.WithLabelValues("open")
	openCounter.Inc()

	logger.Info("open engine")

	failpoint.Inject("FailIfEngineCountExceeds", func(val failpoint.Value) {
		closedCounter := metric.ImporterEngineCounter.WithLabelValues("closed")
		openCount := metric.ReadCounter(openCounter)
		closedCount := metric.ReadCounter(closedCounter)
		if injectValue := val.(int); openCount-closedCount > float64(injectValue) {
			panic(fmt.Sprintf("forcing failure due to FailIfEngineCountExceeds: %v - %v >= %d", openCount, closedCount, injectValue))
		}
	})

	return &OpenedEngine{
		engine: engine{
			backend: be.abstract,
			logger:  logger,
			uuid:    engineUUID,
		},
		tableName: tableName,
	}, nil
}

func (be Backend) CollectLocalDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (bool, error) {
	return be.abstract.CollectLocalDuplicateRows(ctx, tbl, tableName, opts)
}

func (be Backend) CollectRemoteDuplicateRows(ctx context.Context, tbl table.Table, tableName string, opts *kv.SessionOptions) (bool, error) {
	return be.abstract.CollectRemoteDuplicateRows(ctx, tbl, tableName, opts)
}

func (be Backend) ResolveDuplicateRows(ctx context.Context, tbl table.Table, tableName string, algorithm config.DuplicateResolutionAlgorithm) error {
	return be.abstract.ResolveDuplicateRows(ctx, tbl, tableName, algorithm)
}

// Close the opened engine to prepare it for importing.
func (engine *OpenedEngine) Close(ctx context.Context, cfg *EngineConfig) (*ClosedEngine, error) {
	closedEngine, err := engine.unsafeClose(ctx, cfg)
	if err == nil {
		metric.ImporterEngineCounter.WithLabelValues("closed").Inc()
	}
	return closedEngine, err
}

// Flush current written data for local backend
func (engine *OpenedEngine) Flush(ctx context.Context) error {
	return engine.backend.FlushEngine(ctx, engine.uuid)
}

func (engine *OpenedEngine) LocalWriter(ctx context.Context, cfg *LocalWriterConfig) (*LocalEngineWriter, error) {
	w, err := engine.backend.LocalWriter(ctx, cfg, engine.uuid)
	if err != nil {
		return nil, err
	}
	return &LocalEngineWriter{writer: w, tableName: engine.tableName}, nil
}

// WriteRows writes a collection of encoded rows into the engine.
func (w *LocalEngineWriter) WriteRows(ctx context.Context, columnNames []string, rows kv.Rows) error {
	return w.writer.AppendRows(ctx, w.tableName, columnNames, rows)
}

func (w *LocalEngineWriter) Close(ctx context.Context) (ChunkFlushStatus, error) {
	return w.writer.Close(ctx)
}

func (w *LocalEngineWriter) IsSynced() bool {
	return w.writer.IsSynced()
}

// UnsafeCloseEngine closes the engine without first opening it.
// This method is "unsafe" as it does not follow the normal operation sequence
// (Open -> Write -> Close -> Import). This method should only be used when one
// knows via other ways that the engine has already been opened, e.g. when
// resuming from a checkpoint.
func (be Backend) UnsafeCloseEngine(ctx context.Context, cfg *EngineConfig, tableName string, engineID int32) (*ClosedEngine, error) {
	tag, engineUUID := MakeUUID(tableName, engineID)
	return be.UnsafeCloseEngineWithUUID(ctx, cfg, tag, engineUUID)
}

// UnsafeCloseEngineWithUUID closes the engine without first opening it.
// This method is "unsafe" as it does not follow the normal operation sequence
// (Open -> Write -> Close -> Import). This method should only be used when one
// knows via other ways that the engine has already been opened, e.g. when
// resuming from a checkpoint.
func (be Backend) UnsafeCloseEngineWithUUID(ctx context.Context, cfg *EngineConfig, tag string, engineUUID uuid.UUID) (*ClosedEngine, error) {
	return engine{
		backend: be.abstract,
		logger:  makeLogger(tag, engineUUID),
		uuid:    engineUUID,
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

// Import the data written to the engine into the target.
func (engine *ClosedEngine) Import(ctx context.Context, regionSplitSize, regionSplitKeys int64) error {
	var err error

	for i := 0; i < importMaxRetryTimes; i++ {
		task := engine.logger.With(zap.Int("retryCnt", i)).Begin(zap.InfoLevel, "import")
		err = engine.backend.ImportEngine(ctx, engine.uuid, regionSplitSize, regionSplitKeys)
		if !common.IsRetryableError(err) {
			task.End(zap.ErrorLevel, err)
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

func (engine *ClosedEngine) Logger() log.Logger {
	return engine.logger
}

type ChunkFlushStatus interface {
	Flushed() bool
}

type EngineWriter interface {
	AppendRows(
		ctx context.Context,
		tableName string,
		columnNames []string,
		rows kv.Rows,
	) error
	IsSynced() bool
	Close(ctx context.Context) (ChunkFlushStatus, error)
}

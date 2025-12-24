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

package server

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/lightning/pkg/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/importinto"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
)

// CheckpointControl defines the interface for checkpoint management operations.
type CheckpointControl interface {
	Remove(ctx context.Context, tableName string) error
	IgnoreError(ctx context.Context, tableName string) error
	DestroyError(ctx context.Context, tableName string) error
	Dump(ctx context.Context, dumpFolder string) error
	GetLocalStoringTables(ctx context.Context) (map[string][]int32, error)
}

// NewCheckpointControl creates a new CheckpointControl based on the configuration.
func NewCheckpointControl(cfg *config.Config, tls *common.TLS) (CheckpointControl, error) {
	if cfg.TikvImporter.Backend == config.BackendImportInto {
		return NewImportIntoCheckpointControl(cfg, tls)
	}
	return NewLegacyCheckpointControl(cfg, tls)
}

// LegacyCheckpointControl implements CheckpointControl for legacy checkpoints.
type LegacyCheckpointControl struct {
	cfg *config.Config
	tls *common.TLS
}

// NewLegacyCheckpointControl creates a new LegacyCheckpointControl.
func NewLegacyCheckpointControl(cfg *config.Config, tls *common.TLS) (*LegacyCheckpointControl, error) {
	return &LegacyCheckpointControl{cfg: cfg, tls: tls}, nil
}

func (c *LegacyCheckpointControl) withDB(ctx context.Context, fn func(checkpoints.DB) error) error {
	cpdb, err := checkpoints.OpenCheckpointsDB(ctx, c.cfg)
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if closeErr := cpdb.Close(); closeErr != nil {
			log.L().Warn("failed to close checkpoint db", zap.Error(closeErr))
		}
	}()
	return fn(cpdb)
}

// Remove drops checkpoints as well as any meta leftovers for legacy backend tables.
func (c *LegacyCheckpointControl) Remove(ctx context.Context, tableName string) error {
	return c.withDB(ctx, func(cpdb checkpoints.DB) error {
		// try to remove the metadata first.
		taskCp, err := cpdb.TaskCheckpoint(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// a empty id means this task is not inited, we needn't further check metas.
		if taskCp != nil && taskCp.TaskID != 0 {
			// try to clean up table metas if exists
			if err = CleanupMetas(ctx, c.cfg, tableName); err != nil {
				return errors.Trace(err)
			}
		}
		return errors.Trace(cpdb.RemoveCheckpoint(ctx, tableName))
	})
}

// IgnoreError marks failed checkpoints as pending so they can be resumed.
func (c *LegacyCheckpointControl) IgnoreError(ctx context.Context, tableName string) error {
	return c.withDB(ctx, func(cpdb checkpoints.DB) error {
		return errors.Trace(cpdb.IgnoreErrorCheckpoint(ctx, tableName))
	})
}

// DestroyError removes failed checkpoints and associated temporary data.
func (c *LegacyCheckpointControl) DestroyError(ctx context.Context, tableName string) error {
	return c.withDB(ctx, func(cpdb checkpoints.DB) error {
		target, err := importer.NewTiDBManager(ctx, c.cfg.TiDB, c.tls)
		if err != nil {
			return errors.Trace(err)
		}
		defer target.Close()

		targetTables, err := cpdb.DestroyErrorCheckpoint(ctx, tableName)
		if err != nil {
			return errors.Trace(err)
		}

		var errs []error

		for _, table := range targetTables {
			log.L().Info("Dropping table", zap.String("table", table.TableName))
			err := target.DropTable(ctx, table.TableName)
			if err != nil {
				log.L().Error("Encountered error while dropping table", zap.Error(err))
				errs = append(errs, err)
			}
		}

		if c.cfg.TikvImporter.Backend == config.BackendLocal {
			for _, table := range targetTables {
				for engineID := table.MinEngineID; engineID <= table.MaxEngineID; engineID++ {
					log.L().Info("Closing and cleaning up engine", zap.String("table", table.TableName), zap.Int32("engineID", engineID))
					_, eID := backend.MakeUUID(table.TableName, int64(engineID))
					engine := local.Engine{UUID: eID}
					err := engine.Cleanup(c.cfg.TikvImporter.SortedKVDir)
					if err != nil {
						log.L().Error("Encountered error while cleanup engine", zap.Error(err))
						errs = append(errs, err)
					}
				}
			}
		}

		// try clean up metas
		if len(errs) == 0 {
			errs = append(errs, CleanupMetas(ctx, c.cfg, tableName))
		}

		return errors.Trace(errors.Join(errs...))
	})
}

// Dump exports checkpoint information to CSV files for inspection.
func (c *LegacyCheckpointControl) Dump(ctx context.Context, dumpFolder string) error {
	return c.withDB(ctx, func(cpdb checkpoints.DB) error {
		if err := os.MkdirAll(dumpFolder, 0o750); err != nil {
			return errors.Trace(err)
		}

		tablesFileName := filepath.Join(dumpFolder, "tables.csv")
		tablesFile, err := os.Create(tablesFileName)
		if err != nil {
			return errors.Annotatef(err, "failed to create %s", tablesFileName)
		}
		defer tablesFile.Close()

		enginesFileName := filepath.Join(dumpFolder, "engines.csv")
		enginesFile, err := os.Create(enginesFileName)
		if err != nil {
			return errors.Annotatef(err, "failed to create %s", enginesFileName)
		}
		defer enginesFile.Close()

		chunksFileName := filepath.Join(dumpFolder, "chunks.csv")
		chunksFile, err := os.Create(chunksFileName)
		if err != nil {
			return errors.Annotatef(err, "failed to create %s", chunksFileName)
		}
		defer chunksFile.Close()

		if err := cpdb.DumpTables(ctx, tablesFile); err != nil {
			return errors.Trace(err)
		}
		if err := cpdb.DumpEngines(ctx, enginesFile); err != nil {
			return errors.Trace(err)
		}
		if err := cpdb.DumpChunks(ctx, chunksFile); err != nil {
			return errors.Trace(err)
		}
		return nil
	})
}

// GetLocalStoringTables returns engines that are still being persisted locally.
func (c *LegacyCheckpointControl) GetLocalStoringTables(ctx context.Context) (map[string][]int32, error) {
	var result map[string][]int32
	err := c.withDB(ctx, func(cpdb checkpoints.DB) error {
		var err error
		result, err = cpdb.GetLocalStoringTables(ctx)
		return err
	})
	return result, err
}

// ImportIntoCheckpointControl implements CheckpointControl for import into checkpoints.
type ImportIntoCheckpointControl struct {
	cfg *config.Config
	mgr importinto.CheckpointManager
	tls *common.TLS
}

func (c *ImportIntoCheckpointControl) closeManager() {
	if c.mgr == nil {
		return
	}
	if err := c.mgr.Close(); err != nil {
		log.L().Warn("failed to close import-into checkpoint manager", zap.Error(err))
	}
}

// NewImportIntoCheckpointControl creates a new ImportIntoCheckpointControl.
func NewImportIntoCheckpointControl(cfg *config.Config, tls *common.TLS) (*ImportIntoCheckpointControl, error) {
	mgr, err := importinto.NewCheckpointManager(cfg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &ImportIntoCheckpointControl{cfg: cfg, mgr: mgr, tls: tls}, nil
}

// Remove deletes checkpoints in the import-into backend.
func (c *ImportIntoCheckpointControl) Remove(ctx context.Context, tableName string) error {
	defer c.closeManager()
	db, tbl, err := importinto.ParseTable(tableName)
	if err != nil {
		return errors.Trace(err)
	}
	return c.mgr.Remove(ctx, db, tbl)
}

// IgnoreError resets failed checkpoints to allow resuming the import.
func (c *ImportIntoCheckpointControl) IgnoreError(ctx context.Context, tableName string) error {
	defer c.closeManager()
	db, tbl, err := importinto.ParseTable(tableName)
	if err != nil {
		return errors.Trace(err)
	}
	return c.mgr.IgnoreError(ctx, db, tbl)
}

// DestroyError removes failed checkpoints completely.
func (c *ImportIntoCheckpointControl) DestroyError(ctx context.Context, tableName string) error {
	defer c.closeManager()
	db, tbl, err := importinto.ParseTable(tableName)
	if err != nil {
		return errors.Trace(err)
	}

	target, err := importer.NewTiDBManager(ctx, c.cfg.TiDB, c.tls)
	if err != nil {
		return errors.Trace(err)
	}
	defer target.Close()

	destroyed, err := c.mgr.DestroyError(ctx, db, tbl)
	if err != nil {
		return errors.Trace(err)
	}

	var errs []error
	for _, cp := range destroyed {
		fullTableName := common.UniqueTable(cp.DBName, cp.TableName)
		log.L().Info("Dropping table", zap.String("table", fullTableName))
		if err := target.DropTable(ctx, fullTableName); err != nil {
			log.L().Error("Encountered error while dropping table", zap.Error(err))
			errs = append(errs, err)
		}
	}

	// try clean up metas
	if len(errs) == 0 {
		errs = append(errs, CleanupMetas(ctx, c.cfg, tableName))
	}

	return errors.Trace(errors.Join(errs...))
}

// Dump exports checkpoint data for import-into backend tables.
func (c *ImportIntoCheckpointControl) Dump(ctx context.Context, dumpFolder string) error {
	defer c.closeManager()
	if err := os.MkdirAll(dumpFolder, 0o750); err != nil {
		return errors.Trace(err)
	}

	tablesFileName := filepath.Join(dumpFolder, "tables.csv")
	tablesFile, err := os.Create(tablesFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", tablesFileName)
	}
	defer tablesFile.Close()

	// We also create engines and chunks files to be consistent, even if empty
	enginesFileName := filepath.Join(dumpFolder, "engines.csv")
	enginesFile, err := os.Create(enginesFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", enginesFileName)
	}
	defer enginesFile.Close()

	chunksFileName := filepath.Join(dumpFolder, "chunks.csv")
	chunksFile, err := os.Create(chunksFileName)
	if err != nil {
		return errors.Annotatef(err, "failed to create %s", chunksFileName)
	}
	defer chunksFile.Close()

	if err := c.mgr.DumpTables(ctx, tablesFile); err != nil {
		return errors.Trace(err)
	}
	if err := c.mgr.DumpEngines(ctx, enginesFile); err != nil {
		return errors.Trace(err)
	}
	if err := c.mgr.DumpChunks(ctx, chunksFile); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetLocalStoringTables returns nil because import-into does not keep local engines.
func (*ImportIntoCheckpointControl) GetLocalStoringTables(context.Context) (map[string][]int32, error) {
	return nil, nil
}

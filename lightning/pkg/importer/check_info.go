// Copyright 2020 PingCAP, Inc.
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

package importer

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cznic/mathutil"
	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultCSVSize    = 10 * units.GiB
	maxSampleDataSize = 10 * 1024 * 1024
	maxSampleRowCount = 10 * 1024

	warnEmptyRegionCntPerStore  = 500
	errorEmptyRegionCntPerStore = 1000
	warnRegionCntMinMaxRatio    = 0.75
	errorRegionCntMinMaxRatio   = 0.5

	// We only check RegionCntMaxMinRatio when the maximum region count of all stores is larger than this threshold.
	checkRegionCntRatioThreshold = 1000
)

func (rc *Controller) isSourceInLocal() bool {
	return strings.HasPrefix(rc.store.URI(), storage.LocalURIPrefix)
}

func (rc *Controller) checkSoureDataSize(ctx context.Context) error {
	if rc.cfg.Mydumper.MaxSourceDataSize == 0 {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckSourceDataSize)
}

func (rc *Controller) doPreCheckOnItem(ctx context.Context, checkItemID precheck.CheckItemID) error {
	theChecker, err := rc.precheckItemBuilder.BuildPrecheckItem(checkItemID)
	if err != nil {
		return errors.Trace(err)
	}
	result, err := theChecker.Check(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if result != nil {
		rc.checkTemplate.Collect(result.Severity, result.Passed, result.Message)
	}
	return nil
}

// clusterResource check cluster has enough resource to import data. this test can be skipped.
func (rc *Controller) clusterResource(ctx context.Context) error {
	checkCtx := ctx
	if rc.taskMgr != nil {
		checkCtx = WithPrecheckKey(ctx, taskManagerKey, rc.taskMgr)
	}
	return rc.doPreCheckOnItem(checkCtx, precheck.CheckTargetClusterSize)
}

// ClusterIsAvailable check cluster is available to import data. this test can be skipped.
func (rc *Controller) ClusterIsAvailable(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetClusterVersion)
}

func (rc *Controller) checkEmptyRegion(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetClusterEmptyRegion)
}

// checkRegionDistribution checks if regions distribution is unbalanced.
func (rc *Controller) checkRegionDistribution(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetClusterRegionDist)
}

// checkClusterRegion checks cluster if there are too many empty regions or region distribution is unbalanced.
func (rc *Controller) checkClusterRegion(ctx context.Context) error {
	err := rc.taskMgr.CheckTasksExclusively(ctx, func(tasks []taskMeta) ([]taskMeta, error) {
		restoreStarted := false
		for _, task := range tasks {
			if task.status > taskMetaStatusInitial {
				restoreStarted = true
				break
			}
		}
		if restoreStarted {
			return nil, nil
		}
		if err := rc.checkEmptyRegion(ctx); err != nil {
			return nil, errors.Trace(err)
		}
		if err := rc.checkRegionDistribution(ctx); err != nil {
			return nil, errors.Trace(err)
		}
		return nil, nil
	})
	return errors.Trace(err)
}

// StoragePermission checks whether Lightning has enough permission to storage.
func (rc *Controller) StoragePermission(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckSourcePermission)
}

// HasLargeCSV checks whether input csvs is fit for Lightning import.
// If strictFormat is false, and csv file is large. Lightning will have performance issue.
// this test cannot be skipped.
func (rc *Controller) HasLargeCSV(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckLargeDataFile)
}

// localResource checks the local node has enough resources for this import when local backend enabled;
func (rc *Controller) localResource(ctx context.Context) error {
	if rc.isSourceInLocal() {
		if err := rc.doPreCheckOnItem(ctx, precheck.CheckLocalDiskPlacement); err != nil {
			return errors.Trace(err)
		}
	}

	return rc.doPreCheckOnItem(ctx, precheck.CheckLocalTempKVDir)
}

func (rc *Controller) checkCSVHeader(ctx context.Context) error {
	return rc.doPreCheckOnItem(ctx, precheck.CheckCSVHeader)
}

func (rc *Controller) checkTableEmpty(ctx context.Context) error {
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB || rc.cfg.TikvImporter.ParallelImport {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetTableEmpty)
}

func (rc *Controller) checkCheckpoints(ctx context.Context) error {
	if !rc.cfg.Checkpoint.Enable {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckCheckpoints)
}

func (rc *Controller) checkSourceSchema(ctx context.Context) error {
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckSourceSchemaValid)
}

func (rc *Controller) checkCDCPiTR(ctx context.Context) error {
	if rc.cfg.TikvImporter.Backend == config.BackendTiDB {
		return nil
	}
	return rc.doPreCheckOnItem(ctx, precheck.CheckTargetUsingCDCPITR)
}

// TruncateTable Deleting all records from the table and then importing into the remote backend may cause data overlap.
// To prevent this, truncate the table in advance.
// Truncating the table assigns a new table ID, ensuring no overlap occurs.
func (rc *Controller) TruncateTable(ctx context.Context) error {
	if !rc.shouldTruncateTables() {
		return nil
	}

	logger := log.FromContext(ctx)
	if rc.cfg.Checkpoint.Enable {
		shouldSkip, err := rc.shouldSkipTruncateByCheckpoint(ctx, logger)
		if err != nil {
			return err
		}
		if shouldSkip {
			return nil
		}
	}

	task := logger.Begin(zap.InfoLevel, "truncating tables to avoid data overlap")
	truncatedTables, err := rc.performTableTruncation(ctx)
	if err != nil {
		task.End(zap.ErrorLevel, err)
		return errors.Annotate(err, "failed to truncate table")
	}

	if len(truncatedTables) == 0 {
		task.End(zap.InfoLevel, nil, zap.Int("tables", len(truncatedTables)))
		return nil
	}

	if err := rc.updateTruncatedTableIDs(ctx, truncatedTables); err != nil {
		task.End(zap.ErrorLevel, err)
		return err
	}

	task.End(zap.InfoLevel, nil, zap.Int("tables", len(truncatedTables)))
	return nil
}

// shouldTruncateTables checks if table truncation should be performed based on configuration
func (rc *Controller) shouldTruncateTables() bool {
	return rc.cfg.TikvImporter.Backend == config.BackendRemote && !rc.cfg.TikvImporter.ParallelImport
}

// shouldSkipTruncateByCheckpoint checks if truncation should be skipped due to existing checkpoints
func (rc *Controller) shouldSkipTruncateByCheckpoint(ctx context.Context, logger log.Logger) (bool, error) {
	task, err := rc.checkpointsDB.TaskCheckpoint(ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if task != nil {
		// If there are existing checkpoints, the table has already been truncated.
		logger.Info("skipping truncating tables due to existing checkpoints")
		return true, nil
	}
	return false, nil
}

// performTableTruncation executes the actual table truncation with concurrent workers
func (rc *Controller) performTableTruncation(ctx context.Context) (map[string]bool, error) {
	tableCount := rc.countTotalTables()
	// Use up to 4 concurrent workers to truncate tables.
	// If there are too many tables, using too many connections to truncate tables may overload the TiDB server.
	concurrency := mathutil.Min(tableCount, 4)

	ch := make(chan string)
	eg, gCtx := errgroup.WithContext(ctx)

	// Record which tables were actually truncated
	truncatedTables := make(map[string]bool)
	var truncatedTablesMutex sync.Mutex

	// Start worker goroutines to consume table names
	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			return rc.truncateWorker(gCtx, ch, truncatedTables, &truncatedTablesMutex)
		})
	}

	// Start a separate goroutine to send table names to avoid blocking at sending when all workers return error.
	eg.Go(func() error {
		return rc.sendTableNamesForTruncation(gCtx, ch)
	})

	return truncatedTables, eg.Wait()
}

// countTotalTables counts the total number of tables across all databases
func (rc *Controller) countTotalTables() int {
	tableCount := 0
	for _, db := range rc.dbMetas {
		tableCount += len(db.Tables)
	}
	return tableCount
}

// truncateWorker handles the actual truncation of tables in a worker goroutine
func (rc *Controller) truncateWorker(ctx context.Context, ch <-chan string, truncatedTables map[string]bool, mutex *sync.Mutex) error {
	logger := log.FromContext(ctx)
	for {
		select {
		case tblName, ok := <-ch:
			if !ok {
				return nil // channel closed
			}
			_, err := rc.db.Exec(fmt.Sprintf("TRUNCATE TABLE %s", tblName))
			if err != nil {
				logger.Error("failed to truncate table", zap.String("table", tblName), zap.Error(err))
				return errors.Trace(err)
			}
			// Record the truncated table
			mutex.Lock()
			truncatedTables[tblName] = true
			mutex.Unlock()

			logger.Info("truncated table", zap.String("table", tblName))
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "context canceled when truncating tables")
		}
	}
}

// sendTableNamesForTruncation sends table names to the channel for worker goroutines to process
func (rc *Controller) sendTableNamesForTruncation(ctx context.Context, ch chan<- string) error {
	defer close(ch)
	for _, dbMeta := range rc.dbMetas {
		for _, tableMeta := range dbMeta.Tables {
			tableName := common.UniqueTable(dbMeta.Name, tableMeta.Name)
			select {
			case ch <- tableName:
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "context canceled when truncating tables")
			}
		}
	}
	return nil
}

// updateTruncatedTableIDs updates the table IDs for tables that were actually truncated
func (rc *Controller) updateTruncatedTableIDs(ctx context.Context, truncatedTables map[string]bool) error {
	for _, dbInfo := range rc.dbInfos {
		if len(dbInfo.Tables) == 0 {
			continue
		}

		tables, err := rc.preInfoGetter.FetchRemoteTableModels(ctx, dbInfo.Name)
		if err != nil {
			return errors.Trace(err)
		}

		tableMap := make(map[string]*model.TableInfo, len(tables))
		for _, table := range tables {
			tableMap[table.Name.L] = table
		}

		for _, tableInfo := range dbInfo.Tables {
			// Only update ID if this table was actually truncated
			tableName := common.UniqueTable(dbInfo.Name, tableInfo.Name)
			if !truncatedTables[tableName] {
				continue
			}

			tInfo, exist := tableMap[strings.ToLower(tableInfo.Name)]
			if !exist {
				return errors.NotFoundf("table %s.%s", dbInfo.Name, tableInfo.Name)
			}
			tableInfo.ID = tInfo.ID
			tableInfo.Desired = tInfo
			tableInfo.Core = tInfo
		}
	}
	return nil
}

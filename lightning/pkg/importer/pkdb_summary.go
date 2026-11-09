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

package importer

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/lightning/pkdbsummary"
	"go.uber.org/zap"
)

func summaryChunkKey(table string, chunk *checkpoints.ChunkCheckpoint) string {
	return fmt.Sprintf("%s/%s", table, chunk.Key.String())
}

func (rc *Controller) initializeImportSummary(ctx context.Context) error {
	// Prechecks can fail before checkpoint access is allowed. Initialize optional
	// persistence only after those checks; earlier failures use an empty ledger.
	rc.openImportSummary(ctx)
	return nil
}

func (rc *Controller) openImportSummary(ctx context.Context) {
	old, err := rc.checkpointsDB.TaskCheckpoint(ctx)
	rc.summary = pkdbsummary.Open(ctx, checkpoints.ImportStatisticsStore(rc.checkpointsDB), old != nil || err != nil)
	if old != nil {
		for _, db := range rc.dbInfos {
			for _, table := range db.Tables {
				name := common.UniqueTable(db.Name, table.Name)
				if cp, e := rc.checkpointsDB.Get(ctx, name); e == nil {
					rc.registerSummaryChunks(name, cp)
				}
			}
		}
	}
}

func (rc *Controller) registerSummaryChunks(table string, cp *checkpoints.TableCheckpoint) {
	for id, engine := range cp.Engines {
		if id < 0 {
			continue
		}
		for _, chunk := range engine.Chunks {
			rc.summary.Ensure(summaryChunkKey(table, chunk), table, chunk.Key.Offset, chunk.Chunk.Offset)
		}
	}
}

func (rc *Controller) freezeImportSummary(context.Context) error {
	if rc.summaryFrozen {
		return nil
	}
	rc.summaryFrozen = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rc.summary.Close(ctx); err != nil {
		log.L().Warn("import statistics are incomplete", zap.Error(err))
	}
	// Statistics errors must not change task success, write retries or checkpoint cleanup.
	return nil
}

func (rc *Controller) emitImportSummary(err error) {
	if rc.summary == nil {
		return
	}
	_ = rc.freezeImportSummary(context.Background())
	tables, task := rc.summary.Snapshot()
	status := "Success"
	if err != nil {
		status = "Failed"
	}
	for name, result := range tables {
		logSummary("table", name, status, result, rc.cfg.TaskID)
	}
	logSummary("task", "", status, task, rc.cfg.TaskID)
}

func logSummary(scope, table, status string, r pkdbsummary.Result, taskID int64) {
	log.L().Info("Import Summary",
		zap.String("scope", scope), zap.String("table", table), zap.String("status", status), zap.Int64("task-id", taskID),
		zap.Any("total-records", r.Total), zap.Uint64("imported-records", r.Imported),
		zap.Uint64("replaced-records", r.Replaced), zap.Uint64("skipped-records", r.Skipped),
		zap.Uint64("error-records", r.Error), zap.Any("unresolved-records", r.Unresolved),
		zap.Bool("statistics-complete", r.Complete))
}

func (rc *Controller) confirmLocalSummary(ctx context.Context, tr *TableImporter, hasDuplicates, checkedRemoteDuplicates bool) {
	// A nonfinal parallel-import task delegates remote duplicate detection to
	// another task. Its own staged rows remain unresolved until remote checks
	// have actually completed; absence of local duplicates is not proof.
	if rc.summary == nil || !checkedRemoteDuplicates {
		return
	}
	tables, _ := rc.summary.Snapshot()
	if isLocalBackend(rc.cfg) && rc.cfg.Conflict.Strategy != config.NoneOnDup && !hasDuplicates && (tables[tr.tableName].Skipped == 0 || tr.summaryDuplicates.complete()) {
		rc.summary.ConfirmTable(ctx, tr.tableName)
	}
}

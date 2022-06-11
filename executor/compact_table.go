// Copyright 2022 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/driver/backoff"
	"github.com/pingcap/tidb/util/chunk"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ Executor = &CompactTableTiFlashExec{}

const (
	compactRequestTimeout         = time.Minute * 60 // A single compact request may take at most 1 hour.
	compactMaxBackoffSleepMs      = 5 * 1000         // Backoff at most 5 seconds for each request.
	compactProgressReportInterval = time.Second * 10
)

// TODO: maybe we can cache it.
func getTiFlashStores(ctx sessionctx.Context) ([]infoschema.ServerInfo, error) {
	// TODO: Don't use infoschema, to preserve StoreID information.
	aliveTiFlashStores := make([]infoschema.ServerInfo, 0)
	stores, err := infoschema.GetStoreServerInfo(ctx)
	if err != nil {
		return nil, err
	}
	for _, store := range stores {
		if store.ServerType == kv.TiFlash.Name() {
			aliveTiFlashStores = append(aliveTiFlashStores, store)
		}
	}
	return aliveTiFlashStores, nil
}

// CompactTableTiFlashExec represents an executor for "ALTER TABLE [NAME] COMPACT TIFLASH REPLICA" statement.
type CompactTableTiFlashExec struct {
	baseExecutor

	tableInfo *model.TableInfo
	done      bool

	tikvStore tikv.Storage
}

// Next implements the Executor Next interface.
func (e *CompactTableTiFlashExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true
	return e.doCompact(ctx)
}

func (e *CompactTableTiFlashExec) doCompact(execCtx context.Context) error {
	vars := e.ctx.GetSessionVars()
	if e.tableInfo.TiFlashReplica == nil || e.tableInfo.TiFlashReplica.Count == 0 {
		vars.StmtCtx.AppendWarning(errors.Errorf("compact skipped: no tiflash replica in the table"))
		return nil
	}

	// We will do a TiFlash compact in this way:
	// For each TiFlash instance (in parallel):   <--- This is called "storeCompactTask"
	//     For each partition (in series):
	//         Send a series of compact request for this partition.  <--- Handled by "compactOnePhysicalTable"

	tiFlashStores, err := getTiFlashStores(e.ctx)
	if err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(execCtx)
	// TODO: We may add concurrency control in future.
	for _, store := range tiFlashStores {
		task := &storeCompactTask{
			ctx:         ctx,
			parentExec:  e,
			targetStore: store,
		}
		g.Go(task.work)
	}

	_ = g.Wait() // Errors have been turned into warnings, let's simply discard them.
	return nil
}

// storeCompactTask compacts a logical table described by parentExec in a targetStore.
type storeCompactTask struct {
	ctx         context.Context // Maybe cancelled by other tasks, or parentExec is killed.
	parentExec  *CompactTableTiFlashExec
	targetStore infoschema.ServerInfo

	startAt time.Time

	// Fields below are used to output the progress in the log.
	allPhysicalTables       int
	compactedPhysicalTables int
	lastProgressOutputAt    time.Time
}

func (task *storeCompactTask) work() error {
	// We will :
	// For each partition (in series):
	//     Send a series of compact request for this partition. <--- Handled by "compactOnePhysicalTable"

	var stopAllTasks bool
	var err error

	log.Info("Begin compacting table in a store",
		zap.String("table", task.parentExec.tableInfo.Name.O),
		zap.Int64("table-id", task.parentExec.tableInfo.ID),
		zap.String("store-address", task.targetStore.Address),
	)

	task.startAt = time.Now()
	task.lastProgressOutputAt = task.startAt

	if task.parentExec.tableInfo.Partition != nil {
		// There are partitions, let's do it partition by partition.
		// There is no need for partition-level concurrency, as TiFlash will limit table compaction one at a time.
		allPartitions := task.parentExec.tableInfo.Partition.Definitions
		task.allPhysicalTables = len(allPartitions)
		task.compactedPhysicalTables = 0
		for _, partition := range allPartitions {
			stopAllTasks, err = task.compactOnePhysicalTable(partition.ID)
			task.compactedPhysicalTables++
			if err != nil {
				// Stop remaining partitions when error happens.
				break
			}
		}
		// For partition table, there must be no data in task.parentExec.tableInfo.ID. So no need to compact it.
	} else {
		task.allPhysicalTables = 1
		task.compactedPhysicalTables = 0
		stopAllTasks, err = task.compactOnePhysicalTable(task.parentExec.tableInfo.ID)
		task.compactedPhysicalTables++
	}

	if err == nil {
		log.Info("Compact table finished in a store",
			zap.Duration("elapsed", time.Since(task.startAt)),
			zap.String("table", task.parentExec.tableInfo.Name.O),
			zap.Int64("table-id", task.parentExec.tableInfo.ID),
			zap.String("store-address", task.targetStore.Address),
		)
	}

	if err != nil && stopAllTasks {
		// Propagate the error to the errgroup, to stop tasks for other stores.
		return err
	}

	return nil
}

func (task *storeCompactTask) logFailure(otherFields ...zap.Field) {
	allFields := []zap.Field{
		zap.String("table", task.parentExec.tableInfo.Name.O),
		zap.Int64("table-id", task.parentExec.tableInfo.ID),
		zap.String("store-address", task.targetStore.Address),
	}
	log.Warn("Compact table failed", append(allFields, otherFields...)...)
}

func (task *storeCompactTask) logProgressOptionally() {
	// Output progress every 10 seconds.
	if time.Since(task.lastProgressOutputAt) > compactProgressReportInterval {
		task.lastProgressOutputAt = time.Now()
		log.Info("Compact table in progress",
			zap.Float64("compacted-ratio", float64(task.compactedPhysicalTables)/float64(task.allPhysicalTables)),
			zap.Duration("elapsed", time.Since(task.startAt)),
			zap.String("table", task.parentExec.tableInfo.Name.O),
			zap.Int64("table-id", task.parentExec.tableInfo.ID),
			zap.String("store-address", task.targetStore.Address),
			zap.Int("all-physical-tables", task.allPhysicalTables),
			zap.Int("compacted-physical-tables", task.compactedPhysicalTables),
		)
	}
}

// compactOnePhysicalTable compacts one physical table in the TiFlash store, in an incremental way.
// Returns when compaction is finished. When there are network problems it will retry internally.
//
// There are two kind of errors may be returned:
// A. Error only cancel tasks related with this store, e.g. this store is down even after retry.
// 		The remaining partitions in this store should be cancelled.
// B. Error that should cancel tasks of other stores, e.g. CompactErrorCompactInProgress.
//		The remaining partitions in this store should be cancelled, and tasks of other stores should also be cancelled.
//
// During this function, some "problems" will cause it to early return, e.g. physical table not exist in this
// store any more (maybe caused by DDL). No errors will be produced so that remaining partitions will continue
// being compacted.
// 																	Returns: (stopAllTasks, err)
func (task *storeCompactTask) compactOnePhysicalTable(physicalTableID int64) (bool, error) {
	var startKey []byte = nil
	for { // This loop is to compact incrementally for all data. Each RPC request will only compact a partial of data.
		if task.ctx.Err() != nil {
			return true, task.ctx.Err()
		}

		task.logProgressOptionally()

		resp, err := task.sendRequestWithRetry(&tikvrpc.Request{
			Type:    tikvrpc.CmdCompact,
			StoreTp: tikvrpc.TiFlash,
			Req: &kvrpcpb.CompactRequest{
				LogicalTableId:  task.parentExec.tableInfo.ID,
				PhysicalTableId: physicalTableID,
				StartKey:        startKey,
			},
		})
		if err != nil {
			// Even after backoff, the request is still failed.., or the request is cancelled or timed out
			// For example, the store is down. Let's simply don't compact other partitions.
			warn := errors.Errorf("compact on store %s failed: %v", task.targetStore.Address, err)
			task.parentExec.ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
			task.logFailure(
				zap.Int64("physical-table-id", physicalTableID),
				zap.Error(err))
			return false, warn
		}
		if resp.GetError() != nil {
			switch resp.GetError().GetError().(type) {
			case *kvrpcpb.CompactError_ErrCompactInProgress:
				warn := errors.Errorf("compact on store %s failed: table is compacting in progress", task.targetStore.Address)
				task.parentExec.ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
				task.logFailure(
					zap.Int64("physical-table-id", physicalTableID),
					zap.Error(warn))
				// TiFlash reported that there are existing compacting for the same table.
				// We should stop the whole SQL execution, including compacting requests to other stores, as repeatedly
				// compacting the same table is a waste of resource.
				return true, warn
			case *kvrpcpb.CompactError_ErrTooManyPendingTasks:
				// The store is already very busy, don't retry and don't compact other partitions.
				warn := errors.Errorf("compact on store %s failed: store is too busy", task.targetStore.Address)
				task.parentExec.ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
				task.logFailure(
					zap.Int64("physical-table-id", physicalTableID),
					zap.Error(warn))
				return false, warn
			case *kvrpcpb.CompactError_ErrPhysicalTableNotExist:
				// The physical table does not exist, don't retry this partition, but other partitions should still be compacted.
				// This may happen when partition or table is dropped during the long compaction.
				log.Info("Compact physical table skipped",
					zap.String("table", task.parentExec.tableInfo.Name.O),
					zap.Int64("table-id", task.parentExec.tableInfo.ID),
					zap.String("store-address", task.targetStore.Address),
					zap.Any("response-error", resp.GetError().GetError()))
				// We don't need to produce any user warnings.
				return false, nil
			default:
				// Others are unexpected errors, don't retry and don't compact other partitions.
				warn := errors.Errorf("compact on store %s failed: internal error (check logs for details)", task.targetStore.Address)
				task.parentExec.ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
				task.logFailure(
					zap.Int64("physical-table-id", physicalTableID),
					zap.Any("response-error", resp.GetError().GetError()))
				return false, warn
			}
		}

		if !resp.HasRemaining {
			return false, nil
		}

		// Let's send more compact requests, as there are remaining data to compact.
		lastEndKey := resp.GetCompactedEndKey()
		if len(lastEndKey) == 0 || bytes.Compare(lastEndKey, startKey) <= 0 {
			// The TiFlash server returned an invalid compacted end key.
			// This is unexpected...
			warn := errors.Errorf("compact on store %s failed: internal error (check logs for details)", task.targetStore.Address)
			task.parentExec.ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
			task.logFailure(
				zap.Int64("physical-table-id", physicalTableID),
				zap.String("compacted-start-key", hex.EncodeToString(resp.GetCompactedStartKey())),
				zap.String("compacted-end-key", hex.EncodeToString(resp.GetCompactedEndKey())),
			)
			return false, warn
		}
		startKey = lastEndKey
	}
}

// sendRequestWithRetry sends one Compact request to the remote.
// It will backoff and retry when encountering network errors.
func (task *storeCompactTask) sendRequestWithRetry(req *tikvrpc.Request) (*kvrpcpb.CompactResponse, error) {
	bo := backoff.NewBackoffer(task.ctx, compactMaxBackoffSleepMs)

	for {
		resp, err := task.parentExec.tikvStore.
			GetTiKVClient().
			SendRequest(task.ctx, task.targetStore.Address, req, compactRequestTimeout)

		if err != nil {
			if errors.Cause(err) == context.Canceled || errors.Cause(err) == context.DeadlineExceeded || status.Code(errors.Cause(err)) == codes.Canceled {
				// The request is timed out, or cancelled because of Killed
				// No need to retry.
				return nil, err
			}
			if bo.Backoff(tikv.BoTiFlashRPC(), err) != nil {
				// Exceeds max sleep time,
				return nil, err
			}
			// Otherwise: let's loop again to retry.
			continue
		}

		if resp.Resp == nil {
			// The response is invalid.. This is unexpected, no need to retry.
			return nil, tikverr.ErrBodyMissing
		}

		return resp.Resp.(*kvrpcpb.CompactResponse), nil
	}
}

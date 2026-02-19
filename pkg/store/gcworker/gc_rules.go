// Copyright 2017 PingCAP, Inc.
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

package gcworker

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	"go.uber.org/zap"
)

// GC placement rules when the partitions are removed by the GC worker.
// Placement rules cannot be removed immediately after drop table / truncate table,
// because the tables can be flashed back or recovered.
func doGCPlacementRules(se sessionapi.Session, _ uint64,
	dr util.DelRangeTask, gcPlacementRuleCache *sync.Map) (err error) {
	// Get the job from the job history
	var historyJob *model.Job
	failpoint.Inject("mockHistoryJobForGC", func(v failpoint.Value) {
		mockJ := &model.Job{
			Version: model.GetJobVerInUse(),
			ID:      dr.JobID,
			Type:    model.ActionDropTable,
			TableID: int64(v.(int)),
		}
		mockJ.FillFinishedArgs(&model.DropTableArgs{
			OldPartitionIDs: []int64{int64(v.(int))},
		})
		bytes, err1 := mockJ.Encode(true)
		if err1 != nil {
			return
		}
		historyJob = &model.Job{}
		err1 = historyJob.Decode(bytes)
		if err1 != nil {
			return
		}
	})
	if historyJob == nil {
		historyJob, err = ddl.GetHistoryJobByID(se, dr.JobID)
		if err != nil {
			return
		}
		if historyJob == nil {
			return dbterror.ErrDDLJobNotFound.GenWithStackByArgs(dr.JobID)
		}
	}

	// Notify PD to drop the placement rules of partition-ids and table-id, even if there may be no placement rules.
	var physicalTableIDs []int64
	switch historyJob.Type {
	case model.ActionDropTable:
		var args *model.DropTableArgs
		args, err = model.GetFinishedDropTableArgs(historyJob)
		if err != nil {
			return
		}
		physicalTableIDs = append(args.OldPartitionIDs, historyJob.TableID)
	case model.ActionTruncateTable, model.ActionTruncateTablePartition:
		var args *model.TruncateTableArgs
		args, err = model.GetFinishedTruncateTableArgs(historyJob)
		if err != nil {
			return
		}
		physicalTableIDs = args.OldPartitionIDs
		if historyJob.Type == model.ActionTruncateTable {
			physicalTableIDs = append(physicalTableIDs, historyJob.TableID)
		}
	case model.ActionDropTablePartition, model.ActionReorganizePartition,
		model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		args, err2 := model.GetFinishedTablePartitionArgs(historyJob)
		if err2 != nil {
			return err2
		}
		physicalTableIDs = args.OldPhysicalTblIDs
	case model.ActionDropSchema:
		args, err2 := model.GetFinishedDropSchemaArgs(historyJob)
		if err2 != nil {
			return err2
		}
		physicalTableIDs = args.AllDroppedTableIDs
	}

	// Skip table ids that's already successfully handled.
	tmp := physicalTableIDs[:0]
	for _, id := range physicalTableIDs {
		if _, ok := gcPlacementRuleCache.Load(id); !ok {
			tmp = append(tmp, id)
		}
	}
	physicalTableIDs = tmp

	if len(physicalTableIDs) == 0 {
		return
	}

	if err := infosync.DeleteTiFlashPlacementRules(context.Background(), physicalTableIDs); err != nil {
		logutil.BgLogger().Error("delete placement rules failed", zap.Error(err), zap.Int64s("tableIDs", physicalTableIDs))
	}
	bundles := make([]*placement.Bundle, 0, len(physicalTableIDs))
	for _, id := range physicalTableIDs {
		bundles = append(bundles, placement.NewBundle(id))
	}
	err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	if err != nil {
		return
	}

	// Cache the table id if its related rule are deleted successfully.
	for _, id := range physicalTableIDs {
		gcPlacementRuleCache.Store(id, struct{}{})
	}
	return nil
}

func (w *GCWorker) doGCLabelRules(dr util.DelRangeTask) (err error) {
	// Get the job from the job history
	var historyJob *model.Job
	failpoint.Inject("mockHistoryJob", func(v failpoint.Value) {
		mockJ := &model.Job{
			Version: model.GetJobVerInUse(),
			ID:      dr.JobID,
			Type:    model.ActionDropTable,
		}
		mockJ.FillFinishedArgs(&model.DropTableArgs{
			OldRuleIDs: []string{v.(string)},
		})
		bytes, err1 := mockJ.Encode(true)
		if err1 != nil {
			return
		}
		historyJob = &model.Job{}
		if err1 = historyJob.Decode(bytes); err1 != nil {
			return
		}
	})
	if historyJob == nil {
		se := createSession(w.store)
		historyJob, err = ddl.GetHistoryJobByID(se, dr.JobID)
		se.Close()
		if err != nil {
			return
		}
		if historyJob == nil {
			return dbterror.ErrDDLJobNotFound.GenWithStackByArgs(dr.JobID)
		}
	}

	if historyJob.Type == model.ActionDropTable {
		var (
			args  *model.DropTableArgs
			rules map[string]*label.Rule
		)
		args, err = model.GetFinishedDropTableArgs(historyJob)
		if err != nil {
			return
		}
		physicalTableIDs, ruleIDs := args.OldPartitionIDs, args.OldRuleIDs
		// TODO: Here we need to get rules from PD and filter the rules which is not elegant. We should find a better way.
		rules, err = infosync.GetLabelRules(context.TODO(), ruleIDs)
		if err != nil {
			return
		}

		ruleIDs = getGCRules(append(physicalTableIDs, historyJob.TableID), rules)
		patch := label.NewRulePatch([]*label.Rule{}, ruleIDs)
		err = infosync.UpdateLabelRules(context.TODO(), patch)
	}
	return
}

func getGCRules(ids []int64, rules map[string]*label.Rule) []string {
	oldRange := make(map[string]struct{})
	for _, id := range ids {
		startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id)))
		endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id+1)))
		oldRange[startKey+endKey] = struct{}{}
	}

	var gcRules []string
	for _, rule := range rules {
		find := false
		for _, d := range rule.Data.([]any) {
			if r, ok := d.(map[string]any); ok {
				nowRange := fmt.Sprintf("%s%s", r["start_key"], r["end_key"])
				if _, ok := oldRange[nowRange]; ok {
					find = true
				}
			}
		}
		if find {
			gcRules = append(gcRules, rule.ID)
		}
	}
	return gcRules
}

// RunGCJob sends GC command to KV. It is exported for kv api, do not use it with GCWorker at the same time.
// only use for test
func RunGCJob(ctx context.Context, regionLockResolver tikv.RegionLockResolver, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	// Centralized GC is no longer available. Redirect to the distributed version silently.
	return RunDistributedGCJob(ctx, regionLockResolver, s, pd, safePoint, identifier, concurrency)
}

// RunDistributedGCJob notifies TiKVs to do GC. It is exported for kv api, do not use it with GCWorker at the same time.
// This function may not finish immediately because it may take some time to do resolveLocks.
// Param concurrency specifies the concurrency of resolveLocks phase.
func RunDistributedGCJob(ctx context.Context, regionLockResolver tikv.RegionLockResolver, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		tikvStore:            s,
		uuid:                 identifier,
		keyspaceID:           constants.NullKeyspaceID,
		pdClient:             pd,
		pdGCControllerClient: pd.GetGCInternalController(constants.NullKeyspaceID),
		regionLockResolver:   regionLockResolver,
	}

	newTxnSafePoint, err := gcWorker.advanceTxnSafePoint(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	// Sync txn safe point
	time.Sleep(txnSafePointSyncWaitTime)

	err = gcWorker.resolveLocks(ctx, newTxnSafePoint, concurrency)
	if err != nil {
		return errors.Trace(err)
	}

	gcSafePoint := newTxnSafePoint

	err = gcWorker.broadcastGCSafePoint(ctx, gcSafePoint)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunResolveLocks resolves all locks before the safePoint.
// It is exported only for test, do not use it in the production environment.
func RunResolveLocks(ctx context.Context, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		tikvStore:          s,
		uuid:               identifier,
		keyspaceID:         constants.NullKeyspaceID,
		pdClient:           pd,
		regionLockResolver: tikv.NewRegionLockResolver("test-resolver", s),
	}
	return gcWorker.resolveLocks(ctx, safePoint, concurrency)
}

// MockGCWorker is for test.
type MockGCWorker struct {
	worker *GCWorker
}

// NewMockGCWorker creates a MockGCWorker instance ONLY for test.
func NewMockGCWorker(store kv.Storage) (*MockGCWorker, error) {
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		keyspaceID:  constants.NullKeyspaceID,
		store:       store,
		tikvStore:   store.(tikv.Storage),
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
		pdClient:    store.(tikv.Storage).GetRegionCache().PDClient(),
	}
	return &MockGCWorker{worker: worker}, nil
}

// DeleteRanges calls deleteRanges internally, just for test.
func (w *MockGCWorker) DeleteRanges(ctx context.Context, safePoint uint64) error {
	logutil.Logger(ctx).Error("deleteRanges is called")
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnGC)
	return w.worker.deleteRanges(ctx, safePoint, gcConcurrency{1, false})
}

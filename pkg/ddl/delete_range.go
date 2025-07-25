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

package ddl

import (
	"context"
	"encoding/hex"
	"math"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"go.uber.org/zap"
)

const (
	insertDeleteRangeSQLPrefix = `INSERT IGNORE INTO mysql.gc_delete_range VALUES `
	insertDeleteRangeSQLValue  = `(%?, %?, %?, %?, %?)`

	delBatchSize = 65536
	delBackLog   = 128
)

// Only used in the BR unit test. Once these const variables modified, please make sure compatible with BR.
const (
	BRInsertDeleteRangeSQLPrefix = insertDeleteRangeSQLPrefix
	BRInsertDeleteRangeSQLValue  = insertDeleteRangeSQLValue
)

var (
	// batchInsertDeleteRangeSize is the maximum size for each batch insert statement in the delete-range.
	batchInsertDeleteRangeSize = 256
)

type delRangeManager interface {
	// addDelRangeJob add a DDL job into gc_delete_range table.
	addDelRangeJob(ctx context.Context, job *model.Job) error
	// removeFromGCDeleteRange removes the deleting table job from gc_delete_range table by jobID and tableID.
	// It's use for recover the table that was mistakenly deleted.
	removeFromGCDeleteRange(ctx context.Context, jobID int64) error
	start()
	clear()
}

type delRange struct {
	store      kv.Storage
	sessPool   *sess.Pool
	emulatorCh chan struct{}
	keys       []kv.Key
	quitCh     chan struct{}

	wait         sync.WaitGroup // wait is only used when storeSupport is false.
	storeSupport bool
}

// newDelRangeManager returns a delRangeManager.
func newDelRangeManager(store kv.Storage, sessPool *sess.Pool) delRangeManager {
	dr := &delRange{
		store:        store,
		sessPool:     sessPool,
		storeSupport: store.SupportDeleteRange(),
		quitCh:       make(chan struct{}),
	}
	if !dr.storeSupport {
		dr.emulatorCh = make(chan struct{}, delBackLog)
		dr.keys = make([]kv.Key, 0, delBatchSize)
	}
	return dr
}

// addDelRangeJob implements delRangeManager interface.
func (dr *delRange) addDelRangeJob(ctx context.Context, job *model.Job) error {
	sctx, err := dr.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.sessPool.Put(sctx)

	// The same Job ID uses the same element ID allocator
	wrapper := newDelRangeExecWrapper(sctx)
	err = AddDelRangeJobInternal(ctx, wrapper, job)
	if err != nil {
		logutil.DDLLogger().Error("add job into delete-range table failed", zap.Int64("jobID", job.ID), zap.String("jobType", job.Type.String()), zap.Error(err))
		return errors.Trace(err)
	}
	if !dr.storeSupport {
		dr.emulatorCh <- struct{}{}
	}
	logutil.DDLLogger().Info("add job into delete-range table", zap.Int64("jobID", job.ID), zap.String("jobType", job.Type.String()))
	return nil
}

// AddDelRangeJobInternal implements the generation the delete ranges for the provided job and consumes the delete ranges through delRangeExecWrapper.
func AddDelRangeJobInternal(ctx context.Context, wrapper DelRangeExecWrapper, job *model.Job) error {
	var err error
	var ea elementIDAlloc
	if job.MultiSchemaInfo != nil {
		err = insertJobIntoDeleteRangeTableMultiSchema(ctx, wrapper, job, &ea)
	} else {
		err = insertJobIntoDeleteRangeTable(ctx, wrapper, job, &ea)
	}
	return errors.Trace(err)
}

func insertJobIntoDeleteRangeTableMultiSchema(ctx context.Context, wrapper DelRangeExecWrapper, job *model.Job, ea *elementIDAlloc) error {
	for i, sub := range job.MultiSchemaInfo.SubJobs {
		proxyJob := sub.ToProxyJob(job, i)
		if JobNeedGC(&proxyJob) {
			err := insertJobIntoDeleteRangeTable(ctx, wrapper, &proxyJob, ea)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (dr *delRange) removeFromGCDeleteRange(ctx context.Context, jobID int64) error {
	sctx, err := dr.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer dr.sessPool.Put(sctx)
	err = util.RemoveMultiFromGCDeleteRange(ctx, sctx, jobID)
	return errors.Trace(err)
}

// start implements delRangeManager interface.
func (dr *delRange) start() {
	if !dr.storeSupport {
		dr.wait.Add(1)
		go dr.startEmulator()
	}
}

// clear implements delRangeManager interface.
func (dr *delRange) clear() {
	logutil.DDLLogger().Info("closing delRange")
	close(dr.quitCh)
	dr.wait.Wait()
}

// startEmulator is only used for those storage engines which don't support
// delete-range. The emulator fetches records from gc_delete_range table and
// deletes all keys in each DelRangeTask.
func (dr *delRange) startEmulator() {
	defer dr.wait.Done()
	logutil.DDLLogger().Info("start delRange emulator")
	for {
		select {
		case <-dr.emulatorCh:
		case <-dr.quitCh:
			return
		}
		if util.IsEmulatorGCEnable() {
			err := dr.doDelRangeWork()
			terror.Log(errors.Trace(err))
		}
	}
}

func (dr *delRange) doDelRangeWork() error {
	sctx, err := dr.sessPool.Get()
	if err != nil {
		logutil.DDLLogger().Error("delRange emulator get session failed", zap.Error(err))
		return errors.Trace(err)
	}
	defer dr.sessPool.Put(sctx)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
	ranges, err := util.LoadDeleteRanges(ctx, sctx, math.MaxInt64)
	if err != nil {
		logutil.DDLLogger().Error("delRange emulator load tasks failed", zap.Error(err))
		return errors.Trace(err)
	}

	for _, r := range ranges {
		if err := dr.doTask(sctx, r); err != nil {
			logutil.DDLLogger().Error("delRange emulator do task failed", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

func (dr *delRange) doTask(sctx sessionctx.Context, r util.DelRangeTask) error {
	var oldStartKey, newStartKey kv.Key
	oldStartKey = r.StartKey
	for {
		finish := true
		dr.keys = dr.keys[:0]
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL)
		err := kv.RunInNewTxn(ctx, dr.store, false, func(_ context.Context, txn kv.Transaction) error {
			if topsqlstate.TopSQLEnabled() {
				// Only when TiDB run without PD(use unistore as storage for test) will run into here, so just set a mock internal resource tagger.
				txn.SetOption(kv.ResourceGroupTagger, util.GetInternalResourceGroupTaggerForTopSQL())
			}
			iter, err := txn.Iter(oldStartKey, r.EndKey)
			if err != nil {
				return errors.Trace(err)
			}
			defer iter.Close()

			txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
			for range delBatchSize {
				if !iter.Valid() {
					break
				}
				finish = false
				dr.keys = append(dr.keys, iter.Key().Clone())
				newStartKey = iter.Key().Next()

				if err := iter.Next(); err != nil {
					return errors.Trace(err)
				}
			}

			for _, key := range dr.keys {
				err := txn.Delete(key)
				if err != nil && !kv.ErrNotExist.Equal(err) {
					return errors.Trace(err)
				}
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		if finish {
			if err := util.CompleteDeleteRange(sctx, r, true); err != nil {
				logutil.DDLLogger().Error("delRange emulator complete task failed", zap.Error(err))
				return errors.Trace(err)
			}
			startKey, endKey := r.Range()
			logutil.DDLLogger().Info("delRange emulator complete task",
				zap.Int64("jobID", r.JobID),
				zap.Int64("elementID", r.ElementID),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey))
			break
		}
		if err := util.UpdateDeleteRange(sctx, r, newStartKey, oldStartKey); err != nil {
			logutil.DDLLogger().Error("delRange emulator update task failed", zap.Error(err))
		}
		oldStartKey = newStartKey
	}
	return nil
}

// insertJobIntoDeleteRangeTable parses the job into delete-range arguments,
// and inserts a new record into gc_delete_range table. The primary key is
// (job ID, element ID), so we ignore key conflict error.
func insertJobIntoDeleteRangeTable(ctx context.Context, wrapper DelRangeExecWrapper, job *model.Job, ea *elementIDAlloc) error {
	if err := wrapper.UpdateTSOForJob(); err != nil {
		return errors.Trace(err)
	}

	ctx = kv.WithInternalSourceType(ctx, getDDLRequestSource(job.Type))
	switch job.Type {
	case model.ActionDropSchema:
		args, err := model.GetFinishedDropSchemaArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		tableIDs := args.AllDroppedTableIDs
		for i := 0; i < len(tableIDs); i += batchInsertDeleteRangeSize {
			batchEnd := min(len(tableIDs), i+batchInsertDeleteRangeSize)
			if err := doBatchDeleteTablesRange(ctx, wrapper, job.ID, tableIDs[i:batchEnd], ea, "drop schema: table IDs"); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionDropTable:
		tableID := job.TableID
		// The startKey here is for compatibility with previous versions, old version did not endKey so don't have to deal with.
		args, err := model.GetFinishedDropTableArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		physicalTableIDs := args.OldPartitionIDs
		if len(physicalTableIDs) > 0 {
			if err := doBatchDeleteTablesRange(ctx, wrapper, job.ID, physicalTableIDs, ea, "drop table: partition table IDs"); err != nil {
				return errors.Trace(err)
			}
			// logical table may contain global index regions, so delete the logical table range.
			return errors.Trace(doBatchDeleteTablesRange(ctx, wrapper, job.ID, []int64{tableID}, ea, "drop table: table ID"))
		}
		return errors.Trace(doBatchDeleteTablesRange(ctx, wrapper, job.ID, []int64{tableID}, ea, "drop table: table ID"))
	case model.ActionTruncateTable:
		tableID := job.TableID
		args, err := model.GetFinishedTruncateTableArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		oldPartitionIDs := args.OldPartitionIDs
		if len(oldPartitionIDs) > 0 {
			if err := doBatchDeleteTablesRange(ctx, wrapper, job.ID, oldPartitionIDs, ea, "truncate table: partition table IDs"); err != nil {
				return errors.Trace(err)
			}
		}
		// always delete the table range, even when it's a partitioned table where
		// it may contain global index regions.
		return errors.Trace(doBatchDeleteTablesRange(ctx, wrapper, job.ID, []int64{tableID}, ea, "truncate table: table ID"))
	case model.ActionDropTablePartition:
		args, err := model.GetFinishedTablePartitionArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(doBatchDeleteTablesRange(ctx, wrapper, job.ID, args.OldPhysicalTblIDs, ea, "drop partition: physical table ID(s)"))
	case model.ActionReorganizePartition, model.ActionRemovePartitioning, model.ActionAlterTablePartitioning:
		// Delete dropped partitions, as well as replaced global indexes.
		args, err := model.GetFinishedTablePartitionArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		for _, idx := range args.OldGlobalIndexes {
			if err := doBatchDeleteIndiceRange(ctx, wrapper, job.ID, idx.TableID, []int64{idx.IndexID}, ea, "reorganize partition, replaced global indexes"); err != nil {
				return errors.Trace(err)
			}
		}
		return errors.Trace(doBatchDeleteTablesRange(ctx, wrapper, job.ID, args.OldPhysicalTblIDs, ea, "reorganize partition: physical table ID(s)"))
	case model.ActionTruncateTablePartition:
		args, err := model.GetTruncateTableArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(doBatchDeleteTablesRange(ctx, wrapper, job.ID, args.OldPartitionIDs, ea, "truncate partition: physical table ID(s)"))
	// ActionAddIndex, ActionAddPrimaryKey needs do it, because it needs to be rolled back when it's canceled.
	case model.ActionAddIndex, model.ActionAddPrimaryKey:
		args, err := model.GetFinishedModifyIndexArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		// Determine the physicalIDs to be added.
		physicalIDs := []int64{job.TableID}
		if len(args.PartitionIDs) > 0 {
			physicalIDs = args.PartitionIDs
		}
		for _, indexArg := range args.IndexArgs {
			// Determine the index IDs to be added.
			tempIdxID := tablecodec.TempIndexPrefix | indexArg.IndexID
			var indexIDs []int64
			if job.State == model.JobStateRollbackDone {
				indexIDs = []int64{indexArg.IndexID, tempIdxID}
			} else {
				indexIDs = []int64{tempIdxID}
			}
			if indexArg.IsGlobal {
				if err := doBatchDeleteIndiceRange(ctx, wrapper, job.ID, job.TableID, indexIDs, ea, "add index: physical table ID(s)"); err != nil {
					return errors.Trace(err)
				}
			} else {
				for _, pid := range physicalIDs {
					if err := doBatchDeleteIndiceRange(ctx, wrapper, job.ID, pid, indexIDs, ea, "add index: physical table ID(s)"); err != nil {
						return errors.Trace(err)
					}
				}
			}
		}
	case model.ActionDropIndex, model.ActionDropPrimaryKey:
		args, err := model.GetFinishedModifyIndexArgs(job)
		if err != nil {
			return errors.Trace(err)
		}

		tableID := job.TableID
		partitionIDs := args.PartitionIDs
		indexIDs := []int64{args.IndexArgs[0].IndexID}

		// partitionIDs len is 0 if the dropped index is a global index, even if it is a partitioned table.
		if len(partitionIDs) == 0 {
			return errors.Trace(doBatchDeleteIndiceRange(ctx, wrapper, job.ID, tableID, indexIDs, ea, "drop index: table ID"))
		}
		failpoint.Inject("checkDropGlobalIndex", func(val failpoint.Value) {
			if val.(bool) {
				panic("drop global index must not delete partition index range")
			}
		})
		for _, pid := range partitionIDs {
			if err := doBatchDeleteIndiceRange(ctx, wrapper, job.ID, pid, indexIDs, ea, "drop index: partition table ID"); err != nil {
				return errors.Trace(err)
			}
		}
	case model.ActionDropColumn:
		args, err := model.GetTableColumnArgs(job)
		if err != nil {
			return errors.Trace(err)
		}

		if len(args.IndexIDs) > 0 {
			if len(args.PartitionIDs) == 0 {
				return errors.Trace(doBatchDeleteIndiceRange(ctx, wrapper, job.ID, job.TableID, args.IndexIDs, ea, "drop column: table ID"))
			}
			for _, pid := range args.PartitionIDs {
				if err := doBatchDeleteIndiceRange(ctx, wrapper, job.ID, pid, args.IndexIDs, ea, "drop column: partition table ID"); err != nil {
					return errors.Trace(err)
				}
			}
		}
	case model.ActionModifyColumn:
		args, err := model.GetFinishedModifyColumnArgs(job)
		if err != nil {
			return errors.Trace(err)
		}
		indexIDs, partitionIDs := args.IndexIDs, args.PartitionIDs
		if len(indexIDs) == 0 {
			return nil
		}
		if len(partitionIDs) == 0 {
			return doBatchDeleteIndiceRange(ctx, wrapper, job.ID, job.TableID, indexIDs, ea, "modify column: table ID")
		}
		for _, pid := range partitionIDs {
			if err := doBatchDeleteIndiceRange(ctx, wrapper, job.ID, pid, indexIDs, ea, "modify column: partition table ID"); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func doBatchDeleteIndiceRange(ctx context.Context, wrapper DelRangeExecWrapper, jobID, tableID int64, indexIDs []int64, ea *elementIDAlloc, comment string) error {
	logutil.DDLLogger().Info("insert into delete-range indices", zap.Int64("jobID", jobID), zap.Int64("tableID", tableID), zap.Int64s("indexIDs", indexIDs), zap.String("comment", comment))
	var buf strings.Builder
	buf.WriteString(insertDeleteRangeSQLPrefix)
	wrapper.PrepareParamsList(len(indexIDs) * 5)
	tableID, ok := wrapper.RewriteTableID(tableID)
	if !ok {
		return nil
	}
	for i, indexID := range indexIDs {
		startKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID)
		endKey := tablecodec.EncodeTableIndexPrefix(tableID, indexID+1)
		startKeyEncoded := hex.EncodeToString(startKey)
		endKeyEncoded := hex.EncodeToString(endKey)
		buf.WriteString(insertDeleteRangeSQLValue)
		if i != len(indexIDs)-1 {
			buf.WriteString(",")
		}
		elemID := ea.allocForIndexID(tableID, indexID)
		wrapper.AppendParamsList(jobID, elemID, startKeyEncoded, endKeyEncoded)
	}

	return errors.Trace(wrapper.ConsumeDeleteRange(ctx, buf.String()))
}

func doBatchDeleteTablesRange(ctx context.Context, wrapper DelRangeExecWrapper, jobID int64, tableIDs []int64, ea *elementIDAlloc, comment string) error {
	logutil.DDLLogger().Info("insert into delete-range table", zap.Int64("jobID", jobID), zap.Int64s("tableIDs", tableIDs), zap.String("comment", comment))
	var buf strings.Builder
	buf.WriteString(insertDeleteRangeSQLPrefix)
	wrapper.PrepareParamsList(len(tableIDs) * 5)
	for i, tableID := range tableIDs {
		tableID, ok := wrapper.RewriteTableID(tableID)
		if !ok {
			continue
		}
		startKey := tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		startKeyEncoded := hex.EncodeToString(startKey)
		endKeyEncoded := hex.EncodeToString(endKey)
		buf.WriteString(insertDeleteRangeSQLValue)
		if i != len(tableIDs)-1 {
			buf.WriteString(",")
		}
		elemID := ea.allocForPhysicalID(tableID)
		wrapper.AppendParamsList(jobID, elemID, startKeyEncoded, endKeyEncoded)
	}

	return errors.Trace(wrapper.ConsumeDeleteRange(ctx, buf.String()))
}

// DelRangeExecWrapper consumes the delete ranges with the provided table ID(s) and index ID(s).
type DelRangeExecWrapper interface {
	// generate a new tso for the next job
	UpdateTSOForJob() error

	// initialize the paramsList
	PrepareParamsList(sz int)

	// rewrite table id if necessary, used for BR
	RewriteTableID(tableID int64) (int64, bool)

	// (job_id, element_id, start_key, end_key, ts)
	// ts is generated by delRangeExecWrapper itself
	AppendParamsList(jobID, elemID int64, startKey, endKey string)

	// consume the delete range. For TiDB Server, it insert rows into mysql.gc_delete_range.
	ConsumeDeleteRange(ctx context.Context, sql string) error
}

// sessionDelRangeExecWrapper is a lightweight wrapper that implements the DelRangeExecWrapper interface and used for TiDB Server.
// It consumes the delete ranges by directly insert rows into mysql.gc_delete_range.
type sessionDelRangeExecWrapper struct {
	sctx sessionctx.Context
	ts   uint64

	// temporary values
	paramsList []any
}

func newDelRangeExecWrapper(sctx sessionctx.Context) DelRangeExecWrapper {
	return &sessionDelRangeExecWrapper{
		sctx:       sctx,
		paramsList: nil,
	}
}

func (sdr *sessionDelRangeExecWrapper) UpdateTSOForJob() error {
	now, err := getNowTSO(sdr.sctx)
	if err != nil {
		return errors.Trace(err)
	}
	sdr.ts = now
	return nil
}

func (sdr *sessionDelRangeExecWrapper) PrepareParamsList(sz int) {
	sdr.paramsList = make([]any, 0, sz)
}

func (*sessionDelRangeExecWrapper) RewriteTableID(tableID int64) (int64, bool) {
	return tableID, true
}

func (sdr *sessionDelRangeExecWrapper) AppendParamsList(jobID, elemID int64, startKey, endKey string) {
	sdr.paramsList = append(sdr.paramsList, jobID, elemID, startKey, endKey, sdr.ts)
}

func (sdr *sessionDelRangeExecWrapper) ConsumeDeleteRange(ctx context.Context, sql string) error {
	_, err := sdr.sctx.GetSQLExecutor().ExecuteInternal(ctx, sql, sdr.paramsList...)
	sdr.paramsList = nil
	return errors.Trace(err)
}

// getNowTS gets the current timestamp, in TSO.
func getNowTSO(ctx sessionctx.Context) (uint64, error) {
	currVer, err := ctx.GetStore().CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return currVer.Ver, nil
}

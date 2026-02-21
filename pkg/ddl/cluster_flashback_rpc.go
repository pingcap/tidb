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

package ddl

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// SendPrepareFlashbackToVersionRPC prepares regions for flashback, the purpose is to put region into flashback state which region stop write
// Function also be called by BR for volume snapshot backup and restore
func SendPrepareFlashbackToVersionRPC(
	ctx context.Context,
	s tikv.Storage,
	flashbackTS, startTS uint64,
	r tikvstore.KeyRange,
) (rangetask.TaskStat, error) {
	startKey, rangeEndKey := r.StartKey, r.EndKey
	var taskStat rangetask.TaskStat
	bo := tikv.NewBackoffer(ctx, flashbackMaxBackoff)
	for {
		select {
		case <-ctx.Done():
			return taskStat, errors.WithStack(ctx.Err())
		default:
		}

		if len(rangeEndKey) > 0 && bytes.Compare(startKey, rangeEndKey) >= 0 {
			break
		}

		loc, err := s.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return taskStat, err
		}

		endKey := loc.EndKey
		isLast := len(endKey) == 0 || (len(rangeEndKey) > 0 && bytes.Compare(endKey, rangeEndKey) >= 0)
		// If it is the last region.
		if isLast {
			endKey = rangeEndKey
		}

		logutil.DDLLogger().Info("send prepare flashback request", zap.Uint64("region_id", loc.Region.GetID()),
			zap.String("start_key", hex.EncodeToString(startKey)), zap.String("end_key", hex.EncodeToString(endKey)))

		req := tikvrpc.NewRequest(tikvrpc.CmdPrepareFlashbackToVersion, &kvrpcpb.PrepareFlashbackToVersionRequest{
			StartKey: startKey,
			EndKey:   endKey,
			StartTs:  startTS,
			Version:  flashbackTS,
		})

		resp, err := s.SendReq(bo, req, loc.Region, flashbackTimeout)
		if err != nil {
			return taskStat, err
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return taskStat, err
		}
		failpoint.Inject("mockPrepareMeetsEpochNotMatch", func(val failpoint.Value) {
			if val.(bool) && bo.ErrorsNum() == 0 {
				regionErr = &errorpb.Error{
					Message:       "stale epoch",
					EpochNotMatch: &errorpb.EpochNotMatch{},
				}
			}
		})
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String()))
			if err != nil {
				return taskStat, err
			}
			continue
		}
		if resp.Resp == nil {
			logutil.DDLLogger().Warn("prepare flashback miss resp body", zap.Uint64("region_id", loc.Region.GetID()))
			err = bo.Backoff(tikv.BoTiKVRPC(), errors.New("prepare flashback rpc miss resp body"))
			if err != nil {
				return taskStat, err
			}
			continue
		}
		prepareFlashbackToVersionResp := resp.Resp.(*kvrpcpb.PrepareFlashbackToVersionResponse)
		if err := prepareFlashbackToVersionResp.GetError(); err != "" {
			boErr := bo.Backoff(tikv.BoTiKVRPC(), errors.New(err))
			if boErr != nil {
				return taskStat, boErr
			}
			continue
		}
		taskStat.CompletedRegions++
		if isLast {
			break
		}
		bo = tikv.NewBackoffer(ctx, flashbackMaxBackoff)
		startKey = endKey
	}
	return taskStat, nil
}

// SendFlashbackToVersionRPC flashback the MVCC key to the version
// Function also be called by BR for volume snapshot backup and restore
func SendFlashbackToVersionRPC(
	ctx context.Context,
	s tikv.Storage,
	version uint64,
	startTS, commitTS uint64,
	r tikvstore.KeyRange,
) (rangetask.TaskStat, error) {
	startKey, rangeEndKey := r.StartKey, r.EndKey
	var taskStat rangetask.TaskStat
	bo := tikv.NewBackoffer(ctx, flashbackMaxBackoff)
	for {
		select {
		case <-ctx.Done():
			return taskStat, errors.WithStack(ctx.Err())
		default:
		}

		if len(rangeEndKey) > 0 && bytes.Compare(startKey, rangeEndKey) >= 0 {
			break
		}

		loc, err := s.GetRegionCache().LocateKey(bo, startKey)
		if err != nil {
			return taskStat, err
		}

		endKey := loc.EndKey
		isLast := len(endKey) == 0 || (len(rangeEndKey) > 0 && bytes.Compare(endKey, rangeEndKey) >= 0)
		// If it is the last region.
		if isLast {
			endKey = rangeEndKey
		}

		logutil.DDLLogger().Info("send flashback request", zap.Uint64("region_id", loc.Region.GetID()),
			zap.String("start_key", hex.EncodeToString(startKey)), zap.String("end_key", hex.EncodeToString(endKey)))

		req := tikvrpc.NewRequest(tikvrpc.CmdFlashbackToVersion, &kvrpcpb.FlashbackToVersionRequest{
			Version:  version,
			StartKey: startKey,
			EndKey:   endKey,
			StartTs:  startTS,
			CommitTs: commitTS,
		})

		resp, err := s.SendReq(bo, req, loc.Region, flashbackTimeout)
		if err != nil {
			logutil.DDLLogger().Warn("send request meets error", zap.Uint64("region_id", loc.Region.GetID()), zap.Error(err))
			if err.Error() != fmt.Sprintf("region %d is not prepared for the flashback", loc.Region.GetID()) {
				return taskStat, err
			}
		} else {
			regionErr, err := resp.GetRegionError()
			if err != nil {
				return taskStat, err
			}
			if regionErr != nil {
				err = bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String()))
				if err != nil {
					return taskStat, err
				}
				continue
			}
			if resp.Resp == nil {
				logutil.DDLLogger().Warn("flashback miss resp body", zap.Uint64("region_id", loc.Region.GetID()))
				err = bo.Backoff(tikv.BoTiKVRPC(), errors.New("flashback rpc miss resp body"))
				if err != nil {
					return taskStat, err
				}
				continue
			}
			flashbackToVersionResp := resp.Resp.(*kvrpcpb.FlashbackToVersionResponse)
			if respErr := flashbackToVersionResp.GetError(); respErr != "" {
				boErr := bo.Backoff(tikv.BoTiKVRPC(), errors.New(respErr))
				if boErr != nil {
					return taskStat, boErr
				}
				continue
			}
		}
		taskStat.CompletedRegions++
		if isLast {
			break
		}
		bo = tikv.NewBackoffer(ctx, flashbackMaxBackoff)
		startKey = endKey
	}
	return taskStat, nil
}

func flashbackToVersion(
	ctx context.Context,
	store kv.Storage,
	handler rangetask.TaskHandler,
	startKey []byte, endKey []byte,
) (err error) {
	return rangetask.NewRangeTaskRunner(
		"flashback-to-version-runner",
		store.(tikv.Storage),
		int(vardef.GetDDLFlashbackConcurrency()),
		handler,
	).RunOnRange(ctx, startKey, endKey)
}

func splitRegionsByKeyRanges(ctx context.Context, store kv.Storage, keyRanges []model.KeyRange) {
	if s, ok := store.(kv.SplittableStore); ok {
		for _, keys := range keyRanges {
			for {
				// tableID is useless when scatter == false
				_, err := s.SplitRegions(ctx, [][]byte{keys.StartKey, keys.EndKey}, false, nil)
				if err == nil {
					break
				}
			}
		}
	}
}

// A Flashback has 4 different stages.
// 1. before lock flashbackClusterJobID, check clusterJobID and lock it.
// 2. before flashback start, check timestamp, disable GC and close PD schedule, get flashback key ranges.
// 3. phase 1, lock flashback key ranges.
// 4. phase 2, send flashback RPC, do flashback jobs.
func (w *worker) onFlashbackCluster(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	inFlashbackTest := false
	failpoint.Inject("mockFlashbackTest", func(val failpoint.Value) {
		if val.(bool) {
			inFlashbackTest = true
		}
	})
	// TODO: Support flashback in unistore.
	if jobCtx.store.Name() != "TiKV" && !inFlashbackTest {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("Not support flashback cluster in non-TiKV env")
	}

	args, err := model.GetFlashbackClusterArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	var totalRegions, completedRegions atomic.Uint64
	totalRegions.Store(args.LockedRegionCnt)

	sess, err := w.sessPool.Get()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	defer w.sessPool.Put(sess)

	switch job.SchemaState {
	// Stage 1, check and set FlashbackClusterJobID, and update job args.
	case model.StateNone:
		if err = savePDSchedule(w.workCtx, args); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		args.EnableGC, err = gcutil.CheckGCEnable(sess)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		args.EnableAutoAnalyze, err = getGlobalSysVarAsBool(sess, vardef.TiDBEnableAutoAnalyze)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		args.SuperReadOnly, err = getGlobalSysVarAsBool(sess, vardef.TiDBSuperReadOnly)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		args.EnableTTLJob, err = getGlobalSysVarAsBool(sess, vardef.TiDBTTLJobEnable)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}

		job.FillArgs(args)
		job.SchemaState = model.StateDeleteOnly
		return ver, nil
	// Stage 2, check flashbackTS, close GC and PD schedule, get flashback key ranges.
	case model.StateDeleteOnly:
		if err = checkAndSetFlashbackClusterInfo(w.workCtx, sess, jobCtx.store, jobCtx.metaMut, job, args.FlashbackTS); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// We should get startTS here to avoid lost startTS when TiDB crashed during send prepare flashback RPC.
		args.StartTS, err = jobCtx.store.GetOracle().GetTimestamp(w.workCtx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		keyRanges, err := getFlashbackKeyRanges(w.workCtx, sess, args.FlashbackTS)
		if err != nil {
			return ver, errors.Trace(err)
		}
		args.FlashbackKeyRanges = make([]model.KeyRange, len(keyRanges))
		for i, keyRange := range keyRanges {
			args.FlashbackKeyRanges[i] = model.KeyRange{
				StartKey: keyRange.StartKey,
				EndKey:   keyRange.EndKey,
			}
		}

		job.FillArgs(args)
		job.SchemaState = model.StateWriteOnly
		return updateSchemaVersion(jobCtx, job)
	// Stage 3, lock related key ranges.
	case model.StateWriteOnly:
		// TODO: Support flashback in unistore.
		if inFlashbackTest {
			job.SchemaState = model.StateWriteReorganization
			return updateSchemaVersion(jobCtx, job)
		}
		// Split region by keyRanges, make sure no unrelated key ranges be locked.
		splitRegionsByKeyRanges(w.workCtx, jobCtx.store, args.FlashbackKeyRanges)
		totalRegions.Store(0)
		for _, r := range args.FlashbackKeyRanges {
			if err = flashbackToVersion(w.workCtx, jobCtx.store,
				func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
					stats, err := SendPrepareFlashbackToVersionRPC(ctx, jobCtx.store.(tikv.Storage), args.FlashbackTS, args.StartTS, r)
					totalRegions.Add(uint64(stats.CompletedRegions))
					return stats, err
				}, r.StartKey, r.EndKey); err != nil {
				logutil.DDLLogger().Warn("Get error when do flashback", zap.Error(err))
				return ver, err
			}
		}
		args.LockedRegionCnt = totalRegions.Load()

		// We should get commitTS here to avoid lost commitTS when TiDB crashed during send flashback RPC.
		args.CommitTS, err = jobCtx.store.GetOracle().GetTimestamp(w.workCtx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FillArgs(args)
		job.SchemaState = model.StateWriteReorganization
		return ver, nil
	// Stage 4, get key ranges and send flashback RPC.
	case model.StateWriteReorganization:
		// TODO: Support flashback in unistore.
		if inFlashbackTest {
			err = asyncNotifyEvent(jobCtx, notifier.NewFlashbackClusterEvent(), job, noSubJob, w.sess)
			if err != nil {
				return ver, errors.Trace(err)
			}
			job.State = model.JobStateDone
			job.SchemaState = model.StatePublic
			return ver, nil
		}

		for _, r := range args.FlashbackKeyRanges {
			if err = flashbackToVersion(w.workCtx, jobCtx.store,
				func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
					// Use same startTS as prepare phase to simulate 1PC txn.
					stats, err := SendFlashbackToVersionRPC(ctx, jobCtx.store.(tikv.Storage), args.FlashbackTS, args.StartTS, args.CommitTS, r)
					completedRegions.Add(uint64(stats.CompletedRegions))
					logutil.DDLLogger().Info("flashback cluster stats",
						zap.Uint64("complete regions", completedRegions.Load()),
						zap.Uint64("total regions", totalRegions.Load()),
						zap.Error(err))
					return stats, err
				}, r.StartKey, r.EndKey); err != nil {
				logutil.DDLLogger().Warn("Get error when do flashback", zap.Error(err))
				return ver, errors.Trace(err)
			}
		}
		err = asyncNotifyEvent(jobCtx, notifier.NewFlashbackClusterEvent(), job, noSubJob, w.sess)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.State = model.JobStateDone
		job.SchemaState = model.StatePublic
		return updateSchemaVersion(jobCtx, job)
	}
	return ver, nil
}

func finishFlashbackCluster(w *worker, job *model.Job) error {
	// Didn't do anything during flashback, return directly
	if job.SchemaState == model.StateNone {
		return nil
	}

	args, err := model.GetFlashbackClusterArgs(job)
	if err != nil {
		return errors.Trace(err)
	}

	sess, err := w.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.Put(sess)

	err = kv.RunInNewTxn(w.workCtx, w.store, true, func(context.Context, kv.Transaction) error {
		if err = recoverPDSchedule(w.ctx, args.PDScheduleValue); err != nil {
			return errors.Trace(err)
		}

		if args.EnableGC {
			if err = gcutil.EnableGC(sess); err != nil {
				return errors.Trace(err)
			}
		}

		if err = setGlobalSysVarFromBool(w.workCtx, sess, vardef.TiDBSuperReadOnly, args.SuperReadOnly); err != nil {
			return errors.Trace(err)
		}

		if job.IsCancelled() {
			// only restore `tidb_ttl_job_enable` when flashback failed
			if err = setGlobalSysVarFromBool(w.workCtx, sess, vardef.TiDBTTLJobEnable, args.EnableTTLJob); err != nil {
				return errors.Trace(err)
			}
		}

		if err := setGlobalSysVarFromBool(w.workCtx, sess, vardef.TiDBEnableAutoAnalyze, args.EnableAutoAnalyze); err != nil {
			return errors.Trace(err)
		}

		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

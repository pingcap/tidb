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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var pdScheduleKey = []string{
	"hot-region-schedule-limit",
	"leader-schedule-limit",
	"merge-schedule-limit",
	"region-schedule-limit",
	"replica-schedule-limit",
}

const (
	flashbackMaxBackoff = 1800000         // 1800s
	flashbackTimeout    = 3 * time.Minute // 3min

	pdScheduleArgsOffset     = 1
	gcEnabledArgsOffset      = 2
	autoAnalyzeOffset        = 3
	maxAutoAnalyzeTimeOffset = 4
)

func closePDSchedule() error {
	closeMap := make(map[string]interface{})
	for _, key := range pdScheduleKey {
		closeMap[key] = 0
	}
	return infosync.SetPDScheduleConfig(context.Background(), closeMap)
}

func savePDSchedule(job *model.Job) error {
	retValue, err := infosync.GetPDScheduleConfig(context.Background())
	if err != nil {
		return err
	}
	saveValue := make(map[string]interface{})
	for _, key := range pdScheduleKey {
		saveValue[key] = retValue[key]
	}
	job.Args[pdScheduleArgsOffset] = &saveValue
	return nil
}

func recoverPDSchedule(pdScheduleParam map[string]interface{}) error {
	if pdScheduleParam == nil {
		return nil
	}
	return infosync.SetPDScheduleConfig(context.Background(), pdScheduleParam)
}

// ValidateFlashbackTS validates that flashBackTS in range [gcSafePoint, currentTS).
func ValidateFlashbackTS(ctx context.Context, sctx sessionctx.Context, flashBackTS uint64) error {
	currentTS, err := sctx.GetStore().GetOracle().GetStaleTimestamp(ctx, oracle.GlobalTxnScope, 0)
	// If we fail to calculate currentTS from local time, fallback to get a timestamp from PD.
	if err != nil {
		metrics.ValidateReadTSFromPDCount.Inc()
		currentVer, err := sctx.GetStore().CurrentVersion(oracle.GlobalTxnScope)
		if err != nil {
			return errors.Errorf("fail to validate flashback timestamp: %v", err)
		}
		currentTS = currentVer.Ver
	}
	if oracle.GetTimeFromTS(flashBackTS).After(oracle.GetTimeFromTS(currentTS)) {
		return errors.Errorf("cannot set flashback timestamp to future time")
	}
	if oracle.GetTimeFromTS(flashBackTS).After(expression.GetMinSafeTime(sctx)) {
		return errors.Errorf("cannot set flashback timestamp to too close to present time")
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(sctx)
	if err != nil {
		return err
	}

	return gcutil.ValidateSnapshotWithGCSafePoint(flashBackTS, gcSafePoint)
}

func setTiDBEnableAutoAnalyze(sess sessionctx.Context, value string) error {
	return sess.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(variable.TiDBEnableAutoAnalyze, value)
}

func getTiDBMaxAutoAnalyzeTime(sess sessionctx.Context) (string, error) {
	val, err := sess.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBMaxAutoAnalyzeTime)
	if err != nil {
		return "", errors.Trace(err)
	}
	return val, nil
}

func setTiDBMaxAutoAnalyzeTime(sess sessionctx.Context, value string) error {
	return sess.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(variable.TiDBMaxAutoAnalyzeTime, value)
}

func getTiDBEnableAutoAnalyze(sess sessionctx.Context) (string, error) {
	val, err := sess.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableAutoAnalyze)
	if err != nil {
		return "", errors.Trace(err)
	}
	return val, nil
}

func checkAndSetFlashbackClusterInfo(sess sessionctx.Context, d *ddlCtx, t *meta.Meta, job *model.Job, flashbackTS uint64) (err error) {
	if err = ValidateFlashbackTS(d.ctx, sess, flashbackTS); err != nil {
		return err
	}

	if err = gcutil.DisableGC(sess); err != nil {
		return err
	}
	if err = closePDSchedule(); err != nil {
		return err
	}
	if err = setTiDBEnableAutoAnalyze(sess, variable.Off); err != nil {
		return err
	}
	if err = setTiDBMaxAutoAnalyzeTime(sess, "1"); err != nil {
		return err
	}

	nowSchemaVersion, err := t.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	flashbackSchemaVersion, err := meta.NewSnapshotMeta(d.store.GetSnapshot(kv.NewVersion(flashbackTS))).GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	// If flashbackSchemaVersion not same as nowSchemaVersion, we've done ddl during [flashbackTs, now).
	for i := flashbackSchemaVersion + 1; i <= nowSchemaVersion; i++ {
		diff, err := t.GetSchemaDiff(i)
		if err != nil {
			return errors.Trace(err)
		}
		if diff != nil && diff.Type != model.ActionFlashbackCluster {
			return errors.Errorf("Had ddl history during [%s, now), can't do flashback", oracle.GetTimeFromTS(flashbackTS))
		}
	}

	jobs, err := GetAllDDLJobs(sess, t)
	if err != nil {
		return errors.Trace(err)
	}
	// Other ddl jobs in queue, return error.
	if len(jobs) != 1 {
		var otherJob *model.Job
		for _, j := range jobs {
			if j.ID != job.ID {
				otherJob = j
				break
			}
		}
		return errors.Errorf("have other ddl jobs(jobID: %d) in queue, can't do flashback", otherJob.ID)
	}
	return nil
}

type flashbackID struct {
	id       int64
	excluded bool
}

func addToSlice(schema string, tableName string, tableID int64, flashbackIDs []flashbackID) []flashbackID {
	var excluded bool
	if filter.IsSystemSchema(schema) && !strings.HasPrefix(tableName, "stats_") {
		excluded = true
	}
	flashbackIDs = append(flashbackIDs, flashbackID{
		id:       tableID,
		excluded: excluded,
	})
	return flashbackIDs
}

// GetFlashbackKeyRanges make keyRanges efficiently for flashback cluster when many tables in cluster,
// The time complexity is O(nlogn).
func GetFlashbackKeyRanges(sess sessionctx.Context, startKey kv.Key) ([]kv.KeyRange, error) {
	schemas := sess.GetDomainInfoSchema().(infoschema.InfoSchema).AllSchemas()

	// The semantic of keyRanges(output).
	var keyRanges []kv.KeyRange

	var flashbackIDs []flashbackID
	for _, db := range schemas {
		for _, table := range db.Tables {
			if !table.IsBaseTable() || table.ID > meta.MaxGlobalID {
				continue
			}
			flashbackIDs = addToSlice(db.Name.L, table.Name.L, table.ID, flashbackIDs)
			if table.Partition != nil {
				for _, partition := range table.Partition.Definitions {
					flashbackIDs = addToSlice(db.Name.L, table.Name.L, partition.ID, flashbackIDs)
				}
			}
		}
	}

	slices.SortFunc(flashbackIDs, func(a, b flashbackID) bool {
		return a.id < b.id
	})

	lastExcludeIdx := -1
	for i, id := range flashbackIDs {
		if id.excluded {
			// Found a range [lastExcludeIdx, i) needs to be added.
			if i > lastExcludeIdx+1 {
				keyRanges = append(keyRanges, kv.KeyRange{
					StartKey: tablecodec.EncodeTablePrefix(flashbackIDs[lastExcludeIdx+1].id),
					EndKey:   tablecodec.EncodeTablePrefix(flashbackIDs[i-1].id + 1),
				})
			}
			lastExcludeIdx = i
		}
	}

	// The last part needs to be added.
	if lastExcludeIdx < len(flashbackIDs)-1 {
		keyRanges = append(keyRanges, kv.KeyRange{
			StartKey: tablecodec.EncodeTablePrefix(flashbackIDs[lastExcludeIdx+1].id),
			EndKey:   tablecodec.EncodeTablePrefix(flashbackIDs[len(flashbackIDs)-1].id + 1),
		})
	}

	for i, ranges := range keyRanges {
		// startKey smaller than ranges.StartKey, ranges begin with [ranges.StartKey, ranges.EndKey)
		if ranges.StartKey.Cmp(startKey) > 0 {
			keyRanges = keyRanges[i:]
			break
		}
		// startKey in [ranges.StartKey, ranges.EndKey), ranges begin with [startKey, ranges.EndKey)
		if ranges.StartKey.Cmp(startKey) <= 0 && ranges.EndKey.Cmp(startKey) > 0 {
			keyRanges = keyRanges[i:]
			keyRanges[0].StartKey = startKey
			break
		}
	}

	return keyRanges, nil
}

func sendFlashbackToVersionRPC(
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
		// If it is the last region
		if isLast {
			endKey = rangeEndKey
		}

		req := tikvrpc.NewRequest(tikvrpc.CmdFlashbackToVersion, &kvrpcpb.FlashbackToVersionRequest{
			Version:  version,
			StartKey: startKey,
			EndKey:   endKey,
			StartTs:  startTS,
			CommitTs: commitTS,
		})

		resp, err := s.SendReq(bo, req, loc.Region, flashbackTimeout)
		if err != nil {
			return taskStat, err
		}
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
			logutil.BgLogger().Warn("flashback missing resp body")
			continue
		}
		flashbackToVersionResp := resp.Resp.(*kvrpcpb.FlashbackToVersionResponse)
		if err := flashbackToVersionResp.GetError(); err != "" {
			logutil.BgLogger().Warn("flashback rpc meets error", zap.String("err", err))
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

func flashbackToVersion(
	ctx context.Context,
	d *ddlCtx,
	version uint64,
	startTS, commitTS uint64,
	startKey []byte, endKey []byte,
) (err error) {
	return rangetask.NewRangeTaskRunner(
		"flashback-to-version-runner",
		d.store.(tikv.Storage),
		int(variable.GetDDLFlashbackConcurrency()),
		func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
			stats, err := sendFlashbackToVersionRPC(ctx, d.store.(tikv.Storage), version, startTS, commitTS, r)
			logutil.BgLogger().Info("flashback cluster stats",
				zap.Int("complete region", stats.CompletedRegions),
				zap.Error(err))
			return stats, err
		},
	).RunOnRange(ctx, startKey, endKey)
}

// A Flashback has 4 different stages.
// 1. before lock flashbackClusterJobID, check clusterJobID and lock it.
// 2. before flashback start, check timestamp, disable GC and close PD schedule.
// 3. phase 1, get key ranges, lock all regions.
// 4. phase 2, send flashback RPC, do flashback jobs.
func (w *worker) onFlashbackCluster(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	inFlashbackTest := false
	failpoint.Inject("mockFlashbackTest", func(val failpoint.Value) {
		if val.(bool) {
			inFlashbackTest = true
		}
	})
	// TODO: Support flashback in unistore.
	if d.store.Name() != "TiKV" && !inFlashbackTest {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("Not support flashback cluster in non-TiKV env")
	}

	var flashbackTS uint64
	var pdScheduleValue map[string]interface{}
	var autoAnalyzeValue, maxAutoAnalyzeTimeValue string
	var gcEnabledValue bool
	if err := job.DecodeArgs(&flashbackTS, &pdScheduleValue, &gcEnabledValue, &autoAnalyzeValue, &maxAutoAnalyzeTimeValue); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	sess, err := w.sessPool.get()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	defer w.sessPool.put(sess)

	switch job.SchemaState {
	// Stage 1, check and set FlashbackClusterJobID, and update job args.
	case model.StateNone:
		flashbackJobID, err := t.GetFlashbackClusterJobID()
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		if flashbackJobID == 0 || flashbackJobID == job.ID {
			err = kv.RunInNewTxn(w.ctx, w.store, true, func(ctx context.Context, txn kv.Transaction) error {
				return meta.NewMeta(txn).SetFlashbackClusterJobID(job.ID)
			})
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			if err = savePDSchedule(job); err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			gcEnableValue, err := gcutil.CheckGCEnable(sess)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			job.Args[gcEnabledArgsOffset] = &gcEnableValue
			autoAnalyzeValue, err = getTiDBEnableAutoAnalyze(sess)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			job.Args[autoAnalyzeOffset] = &autoAnalyzeValue
			maxAutoAnalyzeTimeValue, err = getTiDBMaxAutoAnalyzeTime(sess)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			job.Args[maxAutoAnalyzeTimeOffset] = &maxAutoAnalyzeTimeValue
		} else {
			job.State = model.JobStateCancelled
			return ver, errors.Errorf("Other flashback job(ID: %d) is running", job.ID)
		}
		job.SchemaState = model.StateDeleteOnly
		return ver, nil
	// Stage 2, check flashbackTS, close GC and PD schedule.
	case model.StateDeleteOnly:
		if err = checkAndSetFlashbackClusterInfo(sess, d, t, job, flashbackTS); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
		return ver, nil
	// Stage 3, get key ranges and get locks.
	case model.StateWriteOnly:
		_, err := GetFlashbackKeyRanges(sess, tablecodec.EncodeTablePrefix(0))
		if err != nil {
			return ver, errors.Trace(err)
		}
		// TODO, lock all regions.
		job.SchemaState = model.StateWriteReorganization
		return updateSchemaVersion(d, t, job)
	// Stage 4, get key ranges and send flashback RPC.
	case model.StateWriteReorganization:
		// TODO: Support flashback in unistore.
		if inFlashbackTest {
			asyncNotifyEvent(d, &util.Event{Tp: model.ActionFlashbackCluster})
			job.State = model.JobStateDone
			job.SchemaState = model.StatePublic
			return ver, nil
		}
		keyRanges, err := GetFlashbackKeyRanges(sess, tablecodec.EncodeTablePrefix(0))
		if err != nil {
			return ver, errors.Trace(err)
		}

		commitTS, err := d.store.GetOracle().GetTimestamp(d.ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		if err != nil {
			return ver, errors.Trace(err)
		}
		for _, ranges := range keyRanges {
			if err = flashbackToVersion(d.ctx, d, flashbackTS, t.StartTS, commitTS, ranges.StartKey, ranges.EndKey); err != nil {
				logutil.BgLogger().Warn("[ddl] Get error when do flashback", zap.Error(err))
				return ver, err
			}
		}

		asyncNotifyEvent(d, &util.Event{Tp: model.ActionFlashbackCluster})
		job.State = model.JobStateDone
		job.SchemaState = model.StatePublic
		return ver, nil
	}
	return ver, nil
}

func finishFlashbackCluster(w *worker, job *model.Job) error {
	var flashbackTS uint64
	var pdScheduleValue map[string]interface{}
	var autoAnalyzeValue, maxAutoAnalyzeTime string
	var gcEnabled bool
	var jobID int64

	if err := job.DecodeArgs(&flashbackTS, &pdScheduleValue, &gcEnabled, &autoAnalyzeValue, &maxAutoAnalyzeTime); err != nil {
		return errors.Trace(err)
	}
	sess, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(sess)

	err = kv.RunInNewTxn(w.ctx, w.store, true, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		jobID, err = t.GetFlashbackClusterJobID()
		if err != nil {
			return err
		}
		if jobID == job.ID {
			if err = recoverPDSchedule(pdScheduleValue); err != nil {
				return err
			}
			if err = setTiDBEnableAutoAnalyze(sess, autoAnalyzeValue); err != nil {
				return err
			}
			if err = setTiDBMaxAutoAnalyzeTime(sess, maxAutoAnalyzeTime); err != nil {
				return err
			}
			if gcEnabled {
				if err = gcutil.EnableGC(sess); err != nil {
					return err
				}
			}
			if err = t.SetFlashbackClusterJobID(0); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

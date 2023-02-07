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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
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
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var pdScheduleKey = []string{
	"merge-schedule-limit",
}

const (
	flashbackMaxBackoff = 1800000         // 1800s
	flashbackTimeout    = 3 * time.Minute // 3min
)

const (
	pdScheduleArgsOffset = 1 + iota
	gcEnabledOffset
	autoAnalyzeOffset
	readOnlyOffset
	totalLockedRegionsOffset
	startTSOffset
	commitTSOffset
	ttlJobEnableOffSet
	keyRangesOffset
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

func getStoreGlobalMinSafeTS(s kv.Storage) time.Time {
	minSafeTS := s.GetMinSafeTS(kv.GlobalTxnScope)
	// Inject mocked SafeTS for test.
	failpoint.Inject("injectSafeTS", func(val failpoint.Value) {
		injectTS := val.(int)
		minSafeTS = uint64(injectTS)
	})
	return oracle.GetTimeFromTS(minSafeTS)
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
	oracleFlashbackTS := oracle.GetTimeFromTS(flashBackTS)
	if oracleFlashbackTS.After(oracle.GetTimeFromTS(currentTS)) {
		return errors.Errorf("cannot set flashback timestamp to future time")
	}

	flashbackGetMinSafeTimeTimeout := time.Minute
	failpoint.Inject("changeFlashbackGetMinSafeTimeTimeout", func(val failpoint.Value) {
		t := val.(int)
		flashbackGetMinSafeTimeTimeout = time.Duration(t)
	})

	start := time.Now()
	minSafeTime := getStoreGlobalMinSafeTS(sctx.GetStore())
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for oracleFlashbackTS.After(minSafeTime) {
		if time.Since(start) >= flashbackGetMinSafeTimeTimeout {
			return errors.Errorf("cannot set flashback timestamp after min-resolved-ts(%s)", minSafeTime)
		}
		select {
		case <-ticker.C:
			minSafeTime = getStoreGlobalMinSafeTS(sctx.GetStore())
			break
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	gcSafePoint, err := gcutil.GetGCSafePoint(sctx)
	if err != nil {
		return err
	}

	return gcutil.ValidateSnapshotWithGCSafePoint(flashBackTS, gcSafePoint)
}

func getTiDBTTLJobEnable(sess sessionctx.Context) (string, error) {
	val, err := sess.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBTTLJobEnable)
	if err != nil {
		return "", errors.Trace(err)
	}
	return val, nil
}

func setTiDBTTLJobEnable(ctx context.Context, sess sessionctx.Context, value string) error {
	return sess.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(ctx, variable.TiDBTTLJobEnable, value)
}

func setTiDBEnableAutoAnalyze(ctx context.Context, sess sessionctx.Context, value string) error {
	return sess.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(ctx, variable.TiDBEnableAutoAnalyze, value)
}

func getTiDBEnableAutoAnalyze(sess sessionctx.Context) (string, error) {
	val, err := sess.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBEnableAutoAnalyze)
	if err != nil {
		return "", errors.Trace(err)
	}
	return val, nil
}

func setTiDBSuperReadOnly(ctx context.Context, sess sessionctx.Context, value string) error {
	return sess.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(ctx, variable.TiDBSuperReadOnly, value)
}

func getTiDBSuperReadOnly(sess sessionctx.Context) (string, error) {
	val, err := sess.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(variable.TiDBSuperReadOnly)
	if err != nil {
		return "", errors.Trace(err)
	}
	return val, nil
}

func isFlashbackSupportedDDLAction(action model.ActionType) bool {
	switch action {
	case model.ActionSetTiFlashReplica, model.ActionUpdateTiFlashReplicaStatus, model.ActionAlterPlacementPolicy,
		model.ActionAlterTablePlacement, model.ActionAlterTablePartitionPlacement, model.ActionCreatePlacementPolicy,
		model.ActionDropPlacementPolicy, model.ActionModifySchemaDefaultPlacement,
		model.ActionAlterTableAttributes, model.ActionAlterTablePartitionAttributes:
		return false
	default:
		return true
	}
}

func checkSystemSchemaID(t *meta.Meta, schemaID int64, flashbackTSString string) error {
	if schemaID <= 0 {
		return nil
	}
	DBInfo, err := t.GetDatabase(schemaID)
	if err != nil || DBInfo == nil {
		return errors.Trace(err)
	}
	if filter.IsSystemSchema(DBInfo.Name.L) {
		return errors.Errorf("Detected modified system table during [%s, now), can't do flashback", flashbackTSString)
	}
	return nil
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
	if err = setTiDBEnableAutoAnalyze(d.ctx, sess, variable.Off); err != nil {
		return err
	}
	if err = setTiDBSuperReadOnly(d.ctx, sess, variable.On); err != nil {
		return err
	}
	if err = setTiDBTTLJobEnable(d.ctx, sess, variable.Off); err != nil {
		return err
	}

	nowSchemaVersion, err := t.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	flashbackSnapshotMeta := meta.NewSnapshotMeta(d.store.GetSnapshot(kv.NewVersion(flashbackTS)))
	flashbackSchemaVersion, err := flashbackSnapshotMeta.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	flashbackTSString := oracle.GetTimeFromTS(flashbackTS).String()

	// Check if there is an upgrade during [flashbackTS, now)
	sql := fmt.Sprintf("select VARIABLE_VALUE from mysql.tidb as of timestamp '%s' where VARIABLE_NAME='tidb_server_version'", flashbackTSString)
	rows, err := newSession(sess).execute(d.ctx, sql, "check_tidb_server_version")
	if err != nil || len(rows) == 0 {
		return errors.Errorf("Get history `tidb_server_version` failed, can't do flashback")
	}
	sql = fmt.Sprintf("select 1 from mysql.tidb where VARIABLE_NAME='tidb_server_version' and VARIABLE_VALUE=%s", rows[0].GetString(0))
	rows, err = newSession(sess).execute(d.ctx, sql, "check_tidb_server_version")
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return errors.Errorf("Detected TiDB upgrade during [%s, now), can't do flashback", flashbackTSString)
	}

	// Check is there a DDL task at flashbackTS.
	sql = fmt.Sprintf("select count(*) from mysql.%s as of timestamp '%s'", JobTable, flashbackTSString)
	rows, err = newSession(sess).execute(d.ctx, sql, "check_history_job")
	if err != nil || len(rows) == 0 {
		return errors.Errorf("Get history ddl jobs failed, can't do flashback")
	}
	if rows[0].GetInt64(0) != 0 {
		return errors.Errorf("Detected another DDL job at %s, can't do flashback", flashbackTSString)
	}

	// If flashbackSchemaVersion not same as nowSchemaVersion, we should check all schema diffs during [flashbackTs, now).
	for i := flashbackSchemaVersion + 1; i <= nowSchemaVersion; i++ {
		diff, err := t.GetSchemaDiff(i)
		if err != nil {
			return errors.Trace(err)
		}
		if diff == nil {
			continue
		}
		if !isFlashbackSupportedDDLAction(diff.Type) {
			return errors.Errorf("Detected unsupported DDL job type(%s) during [%s, now), can't do flashback", diff.Type.String(), flashbackTSString)
		}
		err = checkSystemSchemaID(flashbackSnapshotMeta, diff.SchemaID, flashbackTSString)
		if err != nil {
			return errors.Trace(err)
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

func addToSlice(schema string, tableName string, tableID int64, flashbackIDs []int64) []int64 {
	if filter.IsSystemSchema(schema) && !strings.HasPrefix(tableName, "stats_") && tableName != "gc_delete_range" {
		flashbackIDs = append(flashbackIDs, tableID)
	}
	return flashbackIDs
}

// GetTableDataKeyRanges get keyRanges by `flashbackIDs`.
// This func will return all flashback table data key ranges.
func GetTableDataKeyRanges(nonFlashbackTableIDs []int64) []kv.KeyRange {
	var keyRanges []kv.KeyRange

	nonFlashbackTableIDs = append(nonFlashbackTableIDs, -1)

	slices.SortFunc(nonFlashbackTableIDs, func(a, b int64) bool {
		return a < b
	})

	for i := 1; i < len(nonFlashbackTableIDs); i++ {
		keyRanges = append(keyRanges, kv.KeyRange{
			StartKey: tablecodec.EncodeTablePrefix(nonFlashbackTableIDs[i-1] + 1),
			EndKey:   tablecodec.EncodeTablePrefix(nonFlashbackTableIDs[i]),
		})
	}

	// Add all other key ranges.
	keyRanges = append(keyRanges, kv.KeyRange{
		StartKey: tablecodec.EncodeTablePrefix(nonFlashbackTableIDs[len(nonFlashbackTableIDs)-1] + 1),
		EndKey:   tablecodec.EncodeTablePrefix(meta.MaxGlobalID),
	})

	return keyRanges
}

// GetFlashbackKeyRanges get keyRanges for flashback cluster.
// It contains all non system table key ranges and meta data key ranges.
// The time complexity is O(nlogn).
func GetFlashbackKeyRanges(sess sessionctx.Context, flashbackTS uint64) ([]kv.KeyRange, error) {
	schemas := sess.GetDomainInfoSchema().(infoschema.InfoSchema).AllSchemas()

	// The semantic of keyRanges(output).
	keyRanges := make([]kv.KeyRange, 0)

	// get snapshot schema IDs.
	flashbackSnapshotMeta := meta.NewSnapshotMeta(sess.GetStore().GetSnapshot(kv.NewVersion(flashbackTS)))
	snapshotSchemas, err := flashbackSnapshotMeta.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}

	schemaIDs := make(map[int64]struct{})
	for _, schema := range schemas {
		if !filter.IsSystemSchema(schema.Name.L) {
			schemaIDs[schema.ID] = struct{}{}
		}
	}
	for _, schema := range snapshotSchemas {
		if !filter.IsSystemSchema(schema.Name.L) {
			schemaIDs[schema.ID] = struct{}{}
		}
	}

	// The meta data key ranges.
	for schemaID := range schemaIDs {
		metaStartKey := tablecodec.EncodeMetaKeyPrefix(meta.DBkey(schemaID))
		metaEndKey := tablecodec.EncodeMetaKeyPrefix(meta.DBkey(schemaID + 1))
		keyRanges = append(keyRanges, kv.KeyRange{
			StartKey: metaStartKey,
			EndKey:   metaEndKey,
		})
	}

	startKey := tablecodec.EncodeMetaKeyPrefix([]byte("DBs"))
	keyRanges = append(keyRanges, kv.KeyRange{
		StartKey: startKey,
		EndKey:   startKey.PrefixNext(),
	})

	var nonFlashbackTableIDs []int64
	for _, db := range schemas {
		for _, table := range db.Tables {
			if !table.IsBaseTable() || table.ID > meta.MaxGlobalID {
				continue
			}
			nonFlashbackTableIDs = addToSlice(db.Name.L, table.Name.L, table.ID, nonFlashbackTableIDs)
			if table.Partition != nil {
				for _, partition := range table.Partition.Definitions {
					nonFlashbackTableIDs = addToSlice(db.Name.L, table.Name.L, partition.ID, nonFlashbackTableIDs)
				}
			}
		}
	}

	return append(keyRanges, GetTableDataKeyRanges(nonFlashbackTableIDs)...), nil
}

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

		logutil.BgLogger().Info("[ddl] send prepare flashback request", zap.Uint64("region_id", loc.Region.GetID()),
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
			logutil.BgLogger().Warn("prepare flashback miss resp body", zap.Uint64("region_id", loc.Region.GetID()))
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

		logutil.BgLogger().Info("[ddl] send flashback request", zap.Uint64("region_id", loc.Region.GetID()),
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
			logutil.BgLogger().Warn("send request meets error", zap.Uint64("region_id", loc.Region.GetID()), zap.Error(err))
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
				logutil.BgLogger().Warn("flashback miss resp body", zap.Uint64("region_id", loc.Region.GetID()))
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
	d *ddlCtx,
	handler rangetask.TaskHandler,
	startKey []byte, endKey []byte,
) (err error) {
	return rangetask.NewRangeTaskRunner(
		"flashback-to-version-runner",
		d.store.(tikv.Storage),
		int(variable.GetDDLFlashbackConcurrency()),
		handler,
	).RunOnRange(ctx, startKey, endKey)
}

func splitRegionsByKeyRanges(d *ddlCtx, keyRanges []kv.KeyRange) {
	if s, ok := d.store.(kv.SplittableStore); ok {
		for _, keys := range keyRanges {
			for {
				// tableID is useless when scatter == false
				_, err := s.SplitRegions(d.ctx, [][]byte{keys.StartKey, keys.EndKey}, false, nil)
				if err == nil {
					break
				}
			}
		}
	}
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

	var flashbackTS, lockedRegions, startTS, commitTS uint64
	var pdScheduleValue map[string]interface{}
	var autoAnalyzeValue, readOnlyValue, ttlJobEnableValue string
	var gcEnabledValue bool
	var keyRanges []kv.KeyRange
	if err := job.DecodeArgs(&flashbackTS, &pdScheduleValue, &gcEnabledValue, &autoAnalyzeValue, &readOnlyValue, &lockedRegions, &startTS, &commitTS, &ttlJobEnableValue, &keyRanges); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	var totalRegions, completedRegions atomic.Uint64
	totalRegions.Store(lockedRegions)

	sess, err := w.sessPool.get()
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	defer w.sessPool.put(sess)

	switch job.SchemaState {
	// Stage 1, check and set FlashbackClusterJobID, and update job args.
	case model.StateNone:
		if err = savePDSchedule(job); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		gcEnableValue, err := gcutil.CheckGCEnable(sess)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		job.Args[gcEnabledOffset] = &gcEnableValue
		autoAnalyzeValue, err = getTiDBEnableAutoAnalyze(sess)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		job.Args[autoAnalyzeOffset] = &autoAnalyzeValue
		readOnlyValue, err = getTiDBSuperReadOnly(sess)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		job.Args[readOnlyOffset] = &readOnlyValue
		ttlJobEnableValue, err = getTiDBTTLJobEnable(sess)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		job.Args[ttlJobEnableOffSet] = &ttlJobEnableValue
		job.SchemaState = model.StateDeleteOnly
		return ver, nil
	// Stage 2, check flashbackTS, close GC and PD schedule.
	case model.StateDeleteOnly:
		if err = checkAndSetFlashbackClusterInfo(sess, d, t, job, flashbackTS); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// We should get startTS here to avoid lost startTS when TiDB crashed during send prepare flashback RPC.
		startTS, err = d.store.GetOracle().GetTimestamp(d.ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		job.Args[startTSOffset] = startTS
		keyRanges, err = GetFlashbackKeyRanges(sess, flashbackTS)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.Args[keyRangesOffset] = keyRanges
		job.SchemaState = model.StateWriteOnly
		return ver, nil
	// Stage 3, get key ranges and get locks.
	case model.StateWriteOnly:
		// TODO: Support flashback in unistore.
		if inFlashbackTest {
			job.SchemaState = model.StateWriteReorganization
			return updateSchemaVersion(d, t, job)
		}
		// Split region by keyRanges, make sure no unrelated key ranges be locked.
		splitRegionsByKeyRanges(d, keyRanges)
		totalRegions.Store(0)
		for _, r := range keyRanges {
			if err = flashbackToVersion(d.ctx, d,
				func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
					stats, err := SendPrepareFlashbackToVersionRPC(ctx, d.store.(tikv.Storage), flashbackTS, startTS, r)
					totalRegions.Add(uint64(stats.CompletedRegions))
					return stats, err
				}, r.StartKey, r.EndKey); err != nil {
				logutil.BgLogger().Warn("[ddl] Get error when do flashback", zap.Error(err))
				return ver, err
			}
		}
		job.Args[totalLockedRegionsOffset] = totalRegions.Load()

		// We should get commitTS here to avoid lost commitTS when TiDB crashed during send flashback RPC.
		commitTS, err = d.store.GetOracle().GetTimestamp(d.ctx, &oracle.Option{TxnScope: oracle.GlobalTxnScope})
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.Args[commitTSOffset] = commitTS
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

		for _, r := range keyRanges {
			if err = flashbackToVersion(d.ctx, d,
				func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
					// Use same startTS as prepare phase to simulate 1PC txn.
					stats, err := SendFlashbackToVersionRPC(ctx, d.store.(tikv.Storage), flashbackTS, startTS, commitTS, r)
					completedRegions.Add(uint64(stats.CompletedRegions))
					logutil.BgLogger().Info("[ddl] flashback cluster stats",
						zap.Uint64("complete regions", completedRegions.Load()),
						zap.Uint64("total regions", totalRegions.Load()),
						zap.Error(err))
					return stats, err
				}, r.StartKey, r.EndKey); err != nil {
				logutil.BgLogger().Warn("[ddl] Get error when do flashback", zap.Error(err))
				return ver, errors.Trace(err)
			}
		}

		asyncNotifyEvent(d, &util.Event{Tp: model.ActionFlashbackCluster})
		job.State = model.JobStateDone
		job.SchemaState = model.StatePublic
		return updateSchemaVersion(d, t, job)
	}
	return ver, nil
}

func finishFlashbackCluster(w *worker, job *model.Job) error {
	// Didn't do anything during flashback, return directly
	if job.SchemaState == model.StateNone {
		return nil
	}

	var flashbackTS, lockedRegions, startTS, commitTS uint64
	var pdScheduleValue map[string]interface{}
	var autoAnalyzeValue, readOnlyValue, ttlJobEnableValue string
	var gcEnabled bool

	if err := job.DecodeArgs(&flashbackTS, &pdScheduleValue, &gcEnabled, &autoAnalyzeValue, &readOnlyValue, &lockedRegions, &startTS, &commitTS, &ttlJobEnableValue); err != nil {
		return errors.Trace(err)
	}
	sess, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(sess)

	err = kv.RunInNewTxn(w.ctx, w.store, true, func(ctx context.Context, txn kv.Transaction) error {
		if err = recoverPDSchedule(pdScheduleValue); err != nil {
			return err
		}
		if gcEnabled {
			if err = gcutil.EnableGC(sess); err != nil {
				return err
			}
		}
		if err = setTiDBSuperReadOnly(w.ctx, sess, readOnlyValue); err != nil {
			return err
		}

		if job.IsCancelled() {
			// only restore `tidb_ttl_job_enable` when flashback failed
			if err = setTiDBTTLJobEnable(w.ctx, sess, ttlJobEnableValue); err != nil {
				return err
			}
		}

		return setTiDBEnableAutoAnalyze(w.ctx, sess, autoAnalyzeValue)
	})
	if err != nil {
		return err
	}

	return nil
}

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
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/gcutil"
	"github.com/tikv/client-go/v2/oracle"
)

var pdScheduleKey = []string{
	"merge-schedule-limit",
}

const (
	flashbackMaxBackoff = 1800000         // 1800s
	flashbackTimeout    = 3 * time.Minute // 3min
)

func closePDSchedule(ctx context.Context) error {
	closeMap := make(map[string]any)
	for _, key := range pdScheduleKey {
		closeMap[key] = 0
	}
	return infosync.SetPDScheduleConfig(ctx, closeMap)
}

func savePDSchedule(ctx context.Context, args *model.FlashbackClusterArgs) error {
	retValue, err := infosync.GetPDScheduleConfig(ctx)
	if err != nil {
		return err
	}
	saveValue := make(map[string]any)
	for _, key := range pdScheduleKey {
		saveValue[key] = retValue[key]
	}
	args.PDScheduleValue = saveValue
	return nil
}

func recoverPDSchedule(ctx context.Context, pdScheduleParam map[string]any) error {
	if pdScheduleParam == nil {
		return nil
	}
	return infosync.SetPDScheduleConfig(ctx, pdScheduleParam)
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
	currentVer, err := sctx.GetStore().CurrentVersion(oracle.GlobalTxnScope)
	if err != nil {
		return errors.Errorf("fail to validate flashback timestamp: %v", err)
	}
	currentTS := currentVer.Ver

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

func getGlobalSysVarAsBool(sess sessionctx.Context, name string) (bool, error) {
	//nolint:forbidigo
	val, err := sess.GetSessionVars().GlobalVarsAccessor.GetGlobalSysVar(name)
	if err != nil {
		return false, errors.Trace(err)
	}
	return variable.TiDBOptOn(val), nil
}

func setGlobalSysVarFromBool(ctx context.Context, sess sessionctx.Context, name string, value bool) error {
	sv := vardef.On
	if !value {
		sv = vardef.Off
	}

	//nolint:forbidigo
	return sess.GetSessionVars().GlobalVarsAccessor.SetGlobalSysVar(ctx, name, sv)
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

func checkSystemSchemaID(t meta.Reader, schemaID int64, flashbackTSString string) error {
	if schemaID <= 0 {
		return nil
	}
	dbInfo, err := t.GetDatabase(schemaID)
	if err != nil || dbInfo == nil {
		return errors.Trace(err)
	}
	if filter.IsSystemSchema(dbInfo.Name.L) {
		return errors.Errorf("Detected modified system table during [%s, now), can't do flashback", flashbackTSString)
	}
	return nil
}

func checkAndSetFlashbackClusterInfo(ctx context.Context, se sessionctx.Context, store kv.Storage, t *meta.Mutator, job *model.Job, flashbackTS uint64) (err error) {
	if err = ValidateFlashbackTS(ctx, se, flashbackTS); err != nil {
		return err
	}

	if err = gcutil.DisableGC(se); err != nil {
		return err
	}
	if err = closePDSchedule(ctx); err != nil {
		return err
	}
	if err = setGlobalSysVarFromBool(ctx, se, vardef.TiDBEnableAutoAnalyze, false); err != nil {
		return err
	}
	if err = setGlobalSysVarFromBool(ctx, se, vardef.TiDBSuperReadOnly, true); err != nil {
		return err
	}
	if err = setGlobalSysVarFromBool(ctx, se, vardef.TiDBTTLJobEnable, false); err != nil {
		return err
	}

	nowSchemaVersion, err := t.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	flashbackSnapshotMeta := meta.NewReader(store.GetSnapshot(kv.NewVersion(flashbackTS)))
	flashbackSchemaVersion, err := flashbackSnapshotMeta.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	flashbackTSString := oracle.GetTimeFromTS(flashbackTS).Format(types.TimeFSPFormat)

	// Check if there is an upgrade during [flashbackTS, now)
	sql := fmt.Sprintf("select VARIABLE_VALUE from mysql.tidb as of timestamp '%s' where VARIABLE_NAME='tidb_server_version'", flashbackTSString)
	rows, err := sess.NewSession(se).Execute(ctx, sql, "check_tidb_server_version")
	if err != nil || len(rows) == 0 {
		return errors.Errorf("Get history `tidb_server_version` failed, can't do flashback")
	}
	sql = fmt.Sprintf("select 1 from mysql.tidb where VARIABLE_NAME='tidb_server_version' and VARIABLE_VALUE=%s", rows[0].GetString(0))
	rows, err = sess.NewSession(se).Execute(ctx, sql, "check_tidb_server_version")
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return errors.Errorf("Detected TiDB upgrade during [%s, now), can't do flashback", flashbackTSString)
	}

	// Check is there a DDL task at flashbackTS.
	sql = fmt.Sprintf("select count(*) from mysql.tidb_ddl_job as of timestamp '%s'", flashbackTSString)
	rows, err = sess.NewSession(se).Execute(ctx, sql, "check_history_job")
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

	jobs, err := GetAllDDLJobs(ctx, se)
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

// getTableDataKeyRanges get keyRanges by `flashbackIDs`.
// This func will return all flashback table data key ranges.
func getTableDataKeyRanges(nonFlashbackTableIDs []int64) []kv.KeyRange {
	var keyRanges []kv.KeyRange

	nonFlashbackTableIDs = append(nonFlashbackTableIDs, -1)

	slices.SortFunc(nonFlashbackTableIDs, func(a, b int64) int {
		return cmp.Compare(a, b)
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
		EndKey:   tablecodec.EncodeTablePrefix(metadef.MaxUserGlobalID),
	})

	return keyRanges
}

type keyRangeMayExclude struct {
	r       kv.KeyRange
	exclude bool
}

// mergeContinuousKeyRanges merges not exclude continuous key ranges and appends
// to given []kv.KeyRange, assuming the gap between key ranges has no data.
//
// Precondition: schemaKeyRanges is sorted by start key. schemaKeyRanges are
// non-overlapping.
func mergeContinuousKeyRanges(schemaKeyRanges []keyRangeMayExclude) []kv.KeyRange {
	var (
		continuousStart, continuousEnd kv.Key
	)

	result := make([]kv.KeyRange, 0, 1)

	for _, r := range schemaKeyRanges {
		if r.exclude {
			if continuousStart != nil {
				result = append(result, kv.KeyRange{
					StartKey: continuousStart,
					EndKey:   continuousEnd,
				})
				continuousStart = nil
			}
			continue
		}

		if continuousStart == nil {
			continuousStart = r.r.StartKey
		}
		continuousEnd = r.r.EndKey
	}

	if continuousStart != nil {
		result = append(result, kv.KeyRange{
			StartKey: continuousStart,
			EndKey:   continuousEnd,
		})
	}
	return result
}

// getFlashbackKeyRanges get keyRanges for flashback cluster.
// It contains all non system table key ranges and meta data key ranges.
// The time complexity is O(nlogn).
func getFlashbackKeyRanges(ctx context.Context, sess sessionctx.Context, flashbackTS uint64) ([]kv.KeyRange, error) {
	is := sess.GetLatestInfoSchema().(infoschema.InfoSchema)
	schemas := is.AllSchemas()

	// get snapshot schema IDs.
	flashbackSnapshotMeta := meta.NewReader(sess.GetStore().GetSnapshot(kv.NewVersion(flashbackTS)))
	snapshotSchemas, err := flashbackSnapshotMeta.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}

	schemaIDs := make(map[int64]struct{})
	excludeSchemaIDs := make(map[int64]struct{})
	for _, schema := range schemas {
		if filter.IsSystemSchema(schema.Name.L) {
			excludeSchemaIDs[schema.ID] = struct{}{}
		} else {
			schemaIDs[schema.ID] = struct{}{}
		}
	}
	for _, schema := range snapshotSchemas {
		if filter.IsSystemSchema(schema.Name.L) {
			excludeSchemaIDs[schema.ID] = struct{}{}
		} else {
			schemaIDs[schema.ID] = struct{}{}
		}
	}

	schemaKeyRanges := make([]keyRangeMayExclude, 0, len(schemaIDs)+len(excludeSchemaIDs))
	for schemaID := range schemaIDs {
		metaStartKey := tablecodec.EncodeMetaKeyPrefix(meta.DBkey(schemaID))
		metaEndKey := tablecodec.EncodeMetaKeyPrefix(meta.DBkey(schemaID + 1))
		schemaKeyRanges = append(schemaKeyRanges, keyRangeMayExclude{
			r: kv.KeyRange{
				StartKey: metaStartKey,
				EndKey:   metaEndKey,
			},
			exclude: false,
		})
	}
	for schemaID := range excludeSchemaIDs {
		metaStartKey := tablecodec.EncodeMetaKeyPrefix(meta.DBkey(schemaID))
		metaEndKey := tablecodec.EncodeMetaKeyPrefix(meta.DBkey(schemaID + 1))
		schemaKeyRanges = append(schemaKeyRanges, keyRangeMayExclude{
			r: kv.KeyRange{
				StartKey: metaStartKey,
				EndKey:   metaEndKey,
			},
			exclude: true,
		})
	}

	slices.SortFunc(schemaKeyRanges, func(a, b keyRangeMayExclude) int {
		return bytes.Compare(a.r.StartKey, b.r.StartKey)
	})

	keyRanges := mergeContinuousKeyRanges(schemaKeyRanges)

	startKey := tablecodec.EncodeMetaKeyPrefix([]byte("DBs"))
	keyRanges = append(keyRanges, kv.KeyRange{
		StartKey: startKey,
		EndKey:   startKey.PrefixNext(),
	})

	var nonFlashbackTableIDs []int64
	for _, db := range schemas {
		tbls, err2 := is.SchemaTableInfos(ctx, db.Name)
		if err2 != nil {
			return nil, errors.Trace(err2)
		}
		for _, table := range tbls {
			if !table.IsBaseTable() || table.ID > metadef.MaxUserGlobalID {
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

	return append(keyRanges, getTableDataKeyRanges(nonFlashbackTableIDs)...), nil
}


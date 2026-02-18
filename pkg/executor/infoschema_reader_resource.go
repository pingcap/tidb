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

package executor

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	pd "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

func (e *memtableRetriever) setDataForAttributes(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema) error {
	checker := privilege.GetPrivilegeManager(sctx)
	rules, err := infosync.GetAllLabelRules(context.TODO())
	skipValidateTable := false
	failpoint.Inject("mockOutputOfAttributes", func() {
		convert := func(i any) []any {
			return []any{i}
		}
		rules = []*label.Rule{
			{
				ID:       "schema/test/test_label",
				Labels:   []pd.RegionLabel{{Key: "merge_option", Value: "allow"}, {Key: "db", Value: "test"}, {Key: "table", Value: "test_label"}},
				RuleType: "key-range",
				Data: convert(map[string]any{
					"start_key": "7480000000000000ff395f720000000000fa",
					"end_key":   "7480000000000000ff3a5f720000000000fa",
				}),
			},
			{
				ID:       "invalidIDtest",
				Labels:   []pd.RegionLabel{{Key: "merge_option", Value: "allow"}, {Key: "db", Value: "test"}, {Key: "table", Value: "test_label"}},
				RuleType: "key-range",
				Data: convert(map[string]any{
					"start_key": "7480000000000000ff395f720000000000fa",
					"end_key":   "7480000000000000ff3a5f720000000000fa",
				}),
			},
			{
				ID:       "schema/test/test_label",
				Labels:   []pd.RegionLabel{{Key: "merge_option", Value: "allow"}, {Key: "db", Value: "test"}, {Key: "table", Value: "test_label"}},
				RuleType: "key-range",
				Data: convert(map[string]any{
					"start_key": "aaaaa",
					"end_key":   "bbbbb",
				}),
			},
		}
		err = nil
		skipValidateTable = true
	})

	if err != nil {
		return errors.Wrap(err, "get the label rules failed")
	}

	rows := make([][]types.Datum, 0, len(rules))
	for _, rule := range rules {
		skip := true
		dbName, tableName, partitionName, err := checkRule(rule)
		if err != nil {
			logutil.BgLogger().Warn("check table-rule failed", zap.String("ID", rule.ID), zap.Error(err))
			continue
		}
		tableID, err := decodeTableIDFromRule(rule)
		if err != nil {
			logutil.BgLogger().Warn("decode table ID from rule failed", zap.String("ID", rule.ID), zap.Error(err))
			continue
		}

		if !skipValidateTable && tableOrPartitionNotExist(ctx, dbName, tableName, partitionName, is, tableID) {
			continue
		}

		if tableName != "" && dbName != "" && (checker == nil || checker.RequestVerification(sctx.GetSessionVars().ActiveRoles, dbName, tableName, "", mysql.SelectPriv)) {
			skip = false
		}
		if skip {
			continue
		}

		labels := label.RestoreRegionLabels(&rule.Labels)
		var ranges []string
		for _, data := range rule.Data.([]any) {
			if kv, ok := data.(map[string]any); ok {
				startKey := kv["start_key"]
				endKey := kv["end_key"]
				ranges = append(ranges, fmt.Sprintf("[%s, %s]", startKey, endKey))
			}
		}
		kr := strings.Join(ranges, ", ")

		row := types.MakeDatums(
			rule.ID,
			rule.RuleType,
			labels,
			kr,
		)
		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromPlacementPolicies(sctx sessionctx.Context) error {
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	placementPolicies := is.AllPlacementPolicies()
	rows := make([][]types.Datum, 0, len(placementPolicies))
	// Get global PLACEMENT POLICIES
	// Currently no privileges needed for seeing global PLACEMENT POLICIES!
	for _, policy := range placementPolicies {
		// Currently we skip converting syntactic sugar. We might revisit this decision still in the future
		// I.e.: if PrimaryRegion or Regions are set,
		// also convert them to LeaderConstraints and FollowerConstraints
		// for better user experience searching for particular constraints

		// Followers == 0 means not set, so the default value 2 will be used
		followerCnt := policy.PlacementSettings.Followers
		if followerCnt == 0 {
			followerCnt = 2
		}

		row := types.MakeDatums(
			policy.ID,
			infoschema.CatalogVal, // CATALOG
			policy.Name.O,         // Policy Name
			policy.PlacementSettings.PrimaryRegion,
			policy.PlacementSettings.Regions,
			policy.PlacementSettings.Constraints,
			policy.PlacementSettings.LeaderConstraints,
			policy.PlacementSettings.FollowerConstraints,
			policy.PlacementSettings.LearnerConstraints,
			policy.PlacementSettings.Schedule,
			followerCnt,
			policy.PlacementSettings.Learners,
		)
		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromRunawayWatches(sctx sessionctx.Context) error {
	do := domain.GetDomain(sctx)
	err := do.RunawayManager().UpdateNewAndDoneWatch()
	if err != nil {
		logutil.BgLogger().Warn("read runaway watch list", zap.Error(err))
	}
	watches := do.RunawayManager().GetWatchList()
	rows := make([][]types.Datum, 0, len(watches))
	for _, watch := range watches {
		row := types.MakeDatums(
			watch.ID,
			watch.ResourceGroupName,
			watch.StartTime.UTC().Format(time.DateTime),
			watch.EndTime.UTC().Format(time.DateTime),
			watch.Watch.String(),
			watch.WatchText,
			watch.Source,
			watch.GetActionString(),
			watch.GetExceedCause(),
		)
		if watch.EndTime.Equal(runaway.NullTime) {
			row[3].SetString("UNLIMITED", mysql.DefaultCollationName)
		}
		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}
	e.rows = rows
	return nil
}

// used in resource_groups
const (
	burstableModeratedStr = "MODERATED"
	burstableUnlimitedStr = "UNLIMITED"
	burstdisableStr       = "OFF"
	unlimitedFillRate     = "UNLIMITED"
)

func (e *memtableRetriever) setDataFromResourceGroups() error {
	resourceGroups, err := infosync.ListResourceGroups(context.TODO())
	if err != nil {
		return errors.Errorf("failed to access resource group manager, error message is %s", err.Error())
	}
	rows := make([][]types.Datum, 0, len(resourceGroups))
	for _, group := range resourceGroups {
		// mode := ""
		burstable := burstdisableStr
		priority := ast.PriorityValueToName(uint64(group.Priority))
		fillrate := unlimitedFillRate
		// RU_PER_SEC = unlimited like the default group settings.
		isDefaultInReservedSetting := group.RUSettings.RU.Settings.FillRate == math.MaxInt32
		if !isDefaultInReservedSetting {
			fillrate = strconv.FormatUint(group.RUSettings.RU.Settings.FillRate, 10)
		}
		// convert runaway settings
		limitBuilder := new(strings.Builder)
		if setting := group.RunawaySettings; setting != nil {
			if setting.Rule == nil {
				return errors.Errorf("unexpected runaway config in resource group")
			}
			// rule settings
			firstParam := true
			if setting.Rule.ExecElapsedTimeMs > 0 {
				dur := time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond
				fmt.Fprintf(limitBuilder, "EXEC_ELAPSED='%s'", dur.String())
				firstParam = false
			}
			if setting.Rule.ProcessedKeys > 0 {
				if !firstParam {
					fmt.Fprintf(limitBuilder, ", ")
				}
				fmt.Fprintf(limitBuilder, "PROCESSED_KEYS=%d", setting.Rule.ProcessedKeys)
				firstParam = false
			}
			if setting.Rule.RequestUnit > 0 {
				if !firstParam {
					fmt.Fprintf(limitBuilder, ", ")
				}
				fmt.Fprintf(limitBuilder, "RU=%d", setting.Rule.RequestUnit)
			}
			// action settings
			actionType := ast.RunawayActionType(setting.Action)
			switch actionType {
			case ast.RunawayActionDryRun, ast.RunawayActionCooldown, ast.RunawayActionKill:
				fmt.Fprintf(limitBuilder, ", ACTION=%s", actionType.String())
			case ast.RunawayActionSwitchGroup:
				fmt.Fprintf(limitBuilder, ", ACTION=%s(%s)", actionType.String(), setting.SwitchGroupName)
			}
			if setting.Watch != nil {
				if setting.Watch.LastingDurationMs > 0 {
					dur := time.Duration(setting.Watch.LastingDurationMs) * time.Millisecond
					fmt.Fprintf(limitBuilder, ", WATCH=%s DURATION='%s'", ast.RunawayWatchType(setting.Watch.Type).String(), dur.String())
				} else {
					fmt.Fprintf(limitBuilder, ", WATCH=%s DURATION=UNLIMITED", ast.RunawayWatchType(setting.Watch.Type).String())
				}
			}
		}
		queryLimit := limitBuilder.String()

		// convert background settings
		bgBuilder := new(strings.Builder)
		if setting := group.BackgroundSettings; setting != nil {
			first := true
			if len(setting.JobTypes) > 0 {
				fmt.Fprintf(bgBuilder, "TASK_TYPES='%s'", strings.Join(setting.JobTypes, ","))
				first = false
			}
			if setting.UtilizationLimit > 0 {
				if !first {
					bgBuilder.WriteString(", ")
				}
				fmt.Fprintf(bgBuilder, "UTILIZATION_LIMIT=%d", setting.UtilizationLimit)
			}
		}
		background := bgBuilder.String()

		switch group.Mode {
		case rmpb.GroupMode_RUMode:
			// When the burst limit is less than 0, it means burstable or unlimited.
			switch group.RUSettings.RU.Settings.BurstLimit {
			case -1:
				burstable = burstableUnlimitedStr
			case -2:
				burstable = burstableModeratedStr
			}
			row := types.MakeDatums(
				group.Name,
				fillrate,
				priority,
				burstable,
				queryLimit,
				background,
			)
			if len(queryLimit) == 0 {
				row[4].SetNull()
			}
			if len(background) == 0 {
				row[5].SetNull()
			}
			rows = append(rows, row)
			e.recordMemoryConsume(row)
		default:
			// mode = "UNKNOWN_MODE"
			row := types.MakeDatums(
				group.Name,
				nil,
				nil,
				nil,
				nil,
				nil,
			)
			rows = append(rows, row)
			e.recordMemoryConsume(row)
		}
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromKeywords() error {
	rows := make([][]types.Datum, 0, len(parser.Keywords))
	for _, kw := range parser.Keywords {
		row := types.MakeDatums(kw.Word, kw.Reserved)
		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromIndexUsage(ctx context.Context, sctx sessionctx.Context) error {
	dom := domain.GetDomain(sctx)
	rows := make([][]types.Datum, 0, 100)
	checker := privilege.GetPrivilegeManager(sctx)
	ex, ok := e.extractor.(*plannercore.InfoSchemaTiDBIndexUsageExtractor)
	if !ok {
		return errors.Errorf("wrong extractor type: %T, expected InfoSchemaIndexUsageExtractor", e.extractor)
	}
	if ex.SkipRequest {
		return nil
	}

	schemas, tbls, err := ex.ListSchemasAndTables(ctx, e.is)
	if err != nil {
		return errors.Trace(err)
	}
	for i, tbl := range tbls {
		schema := schemas[i]
		if checker != nil && !checker.RequestVerification(
			sctx.GetSessionVars().ActiveRoles,
			schema.L, tbl.Name.L, "", mysql.AllPrivMask) {
			continue
		}

		idxs := ex.ListIndexes(tbl)
		for _, idx := range idxs {
			row := make([]types.Datum, 0, 14)
			usage := dom.StatsHandle().GetIndexUsage(tbl.ID, idx.ID)
			row = append(row, types.NewStringDatum(schema.O))
			row = append(row, types.NewStringDatum(tbl.Name.O))
			row = append(row, types.NewStringDatum(idx.Name))
			row = append(row, types.NewIntDatum(int64(usage.QueryTotal)))
			row = append(row, types.NewIntDatum(int64(usage.KvReqTotal)))
			row = append(row, types.NewIntDatum(int64(usage.RowAccessTotal)))
			for _, percentage := range usage.PercentageAccess {
				row = append(row, types.NewIntDatum(int64(percentage)))
			}
			lastUsedAt := types.Datum{}
			lastUsedAt.SetNull()
			if !usage.LastUsedAt.IsZero() {
				t := types.NewTime(types.FromGoTime(usage.LastUsedAt), mysql.TypeTimestamp, 0)
				lastUsedAt = types.NewTimeDatum(t)
			}
			row = append(row, lastUsedAt)
			rows = append(rows, row)
			e.recordMemoryConsume(row)
		}
	}

	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromClusterIndexUsage(ctx context.Context, sctx sessionctx.Context) error {
	err := e.setDataFromIndexUsage(ctx, sctx)
	if err != nil {
		return errors.Trace(err)
	}
	rows, err := infoschema.AppendHostInfoToRows(sctx, e.rows)
	if err != nil {
		return err
	}
	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataFromPlanCache(_ context.Context, sctx sessionctx.Context, cluster bool) (err error) {
	values := domain.GetDomain(sctx).GetInstancePlanCache().All()
	rows := make([][]types.Datum, 0, len(values))
	for _, v := range values {
		pcv := v.(*plannercore.PlanCacheValue)

		row := make([]types.Datum, 0, 16)
		row = append(row, types.NewStringDatum(pcv.SQLDigest))
		row = append(row, types.NewStringDatum(pcv.SQLText))
		row = append(row, types.NewStringDatum(pcv.StmtType))
		row = append(row, types.NewStringDatum(pcv.ParseUser))
		row = append(row, types.NewStringDatum(pcv.PlanDigest))
		row = append(row, types.NewStringDatum(pcv.BinaryPlan))
		row = append(row, types.NewStringDatum(pcv.Binding))
		row = append(row, types.NewStringDatum(pcv.OptimizerEnvHash))
		row = append(row, types.NewStringDatum(pcv.ParseValues))
		row = append(row, types.NewIntDatum(pcv.Memory))
		exec, procKeys, totKeys, sumLat, lastTime := pcv.RuntimeInfo()
		row = append(row, types.NewIntDatum(exec))
		row = append(row, types.NewIntDatum(procKeys))
		row = append(row, types.NewIntDatum(totKeys))
		row = append(row, types.NewIntDatum(sumLat))
		row = append(row, types.NewTimeDatum(
			types.NewTime(types.FromGoTime(pcv.LoadTime), mysql.TypeTimestamp, types.DefaultFsp)))
		row = append(row, types.NewTimeDatum(
			types.NewTime(types.FromGoTime(lastTime), mysql.TypeTimestamp, types.DefaultFsp)))

		rows = append(rows, row)
		e.recordMemoryConsume(row)
	}

	if cluster {
		if rows, err = infoschema.AppendHostInfoToRows(sctx, rows); err != nil {
			return err
		}
	}

	e.rows = rows
	return nil
}

func (e *memtableRetriever) setDataForKeyspaceMeta(sctx sessionctx.Context) (err error) {
	meta := sctx.GetStore().GetCodec().GetKeyspaceMeta()
	var (
		keyspaceName string
		keyspaceID   string
		keyspaceCfg  []byte
	)

	if meta != nil {
		keyspaceName = meta.Name
		keyspaceID = fmt.Sprintf("%d", meta.Id)
		if len(meta.Config) > 0 {
			keyspaceCfg, err = json.Marshal(meta.Config)
			if err != nil {
				return err
			}
		}
	}

	row := make([]types.Datum, 3)
	// Keyspace name
	row[0] = types.NewStringDatum(keyspaceName)
	// Keyspace ID
	row[1] = types.NewStringDatum(keyspaceID)
	// Keyspace config
	var bj types.BinaryJSON
	if len(keyspaceCfg) > 0 {
		err = bj.UnmarshalJSON(keyspaceCfg)
		if err != nil {
			return err
		}
	}
	row[2] = types.NewJSONDatum(bj)
	e.rows = [][]types.Datum{row}
	return
}

func checkRule(rule *label.Rule) (dbName, tableName string, partitionName string, err error) {
	s := strings.Split(rule.ID, "/")
	if len(s) < 3 {
		err = errors.Errorf("invalid label rule ID: %v", rule.ID)
		return
	}
	if rule.RuleType == "" {
		err = errors.New("empty label rule type")
		return
	}
	if len(rule.Labels) == 0 {
		err = errors.New("the label rule has no label")
		return
	}
	if rule.Data == nil {
		err = errors.New("the label rule has no data")
		return
	}
	dbName = s[1]
	tableName = s[2]
	if len(s) > 3 {
		partitionName = s[3]
	}
	return
}

func decodeTableIDFromRule(rule *label.Rule) (tableID int64, err error) {
	datas := rule.Data.([]any)
	if len(datas) == 0 {
		err = fmt.Errorf("there is no data in rule %s", rule.ID)
		return
	}
	data := datas[0]
	dataMap, ok := data.(map[string]any)
	if !ok {
		err = fmt.Errorf("get the label rules %s failed", rule.ID)
		return
	}
	key, err := hex.DecodeString(fmt.Sprintf("%s", dataMap["start_key"]))
	if err != nil {
		err = fmt.Errorf("decode key from start_key %s in rule %s failed", dataMap["start_key"], rule.ID)
		return
	}
	_, bs, err := codec.DecodeBytes(key, nil)
	if err == nil {
		key = bs
	}
	tableID = tablecodec.DecodeTableID(key)
	if tableID == 0 {
		err = fmt.Errorf("decode tableID from key %s in rule %s failed", key, rule.ID)
		return
	}
	return
}

func tableOrPartitionNotExist(ctx context.Context, dbName string, tableName string, partitionName string, is infoschema.InfoSchema, tableID int64) (tableNotExist bool) {
	if len(partitionName) == 0 {
		curTable, _ := is.TableByName(ctx, ast.NewCIStr(dbName), ast.NewCIStr(tableName))
		if curTable == nil {
			return true
		}
		curTableID := curTable.Meta().ID
		if curTableID != tableID {
			return true
		}
	} else {
		_, _, partInfo := is.FindTableByPartitionID(tableID)
		if partInfo == nil {
			return true
		}
	}
	return false
}

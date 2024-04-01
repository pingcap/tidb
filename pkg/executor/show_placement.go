// Copyright 2021 PingCAP, Inc.
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
	"cmp"
	"context"
	"encoding/hex"
	gjson "encoding/json"
	"fmt"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	pd "github.com/tikv/pd/client/http"
)

type showPlacementLabelsResultBuilder struct {
	labelKey2values map[string]any
}

func (b *showPlacementLabelsResultBuilder) AppendStoreLabels(bj types.BinaryJSON) error {
	if b.labelKey2values == nil {
		b.labelKey2values = make(map[string]any)
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return errors.Trace(err)
	}

	if string(data) == "null" {
		return nil
	}

	if bj.TypeCode != types.JSONTypeCodeArray {
		return errors.New("only array or null type is allowed")
	}

	labels := make([]*pd.StoreLabel, 0, bj.GetElemCount())
	err = gjson.Unmarshal(data, &labels)
	if err != nil {
		return errors.Trace(err)
	}

	for _, label := range labels {
		if values, ok := b.labelKey2values[label.Key]; ok {
			values.(map[string]any)[label.Value] = true
		} else {
			b.labelKey2values[label.Key] = map[string]any{label.Value: true}
		}
	}

	return nil
}

func (b *showPlacementLabelsResultBuilder) BuildRows() ([][]any, error) {
	rows := make([][]any, 0, len(b.labelKey2values))
	for _, key := range b.sortMapKeys(b.labelKey2values) {
		values := b.sortMapKeys(b.labelKey2values[key].(map[string]any))
		d, err := gjson.Marshal(values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		valuesJSON := types.BinaryJSON{}
		err = valuesJSON.UnmarshalJSON(d)
		if err != nil {
			return nil, errors.Trace(err)
		}

		rows = append(rows, []any{key, valuesJSON})
	}

	return rows, nil
}

func (*showPlacementLabelsResultBuilder) sortMapKeys(m map[string]any) []string {
	sorted := make([]string, 0, len(m))
	for key := range m {
		sorted = append(sorted, key)
	}

	slices.Sort(sorted)
	return sorted
}

func (e *ShowExec) fetchShowPlacementLabels(ctx context.Context) error {
	exec := e.Ctx().GetRestrictedSQLExecutor()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, "SELECT DISTINCT LABEL FROM %n.%n", "INFORMATION_SCHEMA", infoschema.TableTiKVStoreStatus)
	if err != nil {
		return errors.Trace(err)
	}

	b := &showPlacementLabelsResultBuilder{}
	for _, row := range rows {
		bj := row.GetJSON(0)
		if err := b.AppendStoreLabels(bj); err != nil {
			return err
		}
	}

	result, err := b.BuildRows()
	if err != nil {
		return err
	}

	for _, row := range result {
		e.appendRow(row)
	}

	return nil
}

func (e *ShowExec) fetchShowPlacementForDB(ctx context.Context) (err error) {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	if checker != nil && e.Ctx().GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.Ctx().GetSessionVars().ActiveRoles, e.DBName.String()) {
			return e.dbAccessDenied()
		}
	}

	dbInfo, ok := e.is.SchemaByName(e.DBName)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(e.DBName.O)
	}

	placement, err := e.getDBPlacement(dbInfo)
	if err != nil {
		return err
	}

	if placement != nil {
		state, err := e.fetchDBScheduleState(ctx, nil, dbInfo)
		if err != nil {
			return err
		}
		e.appendRow([]any{"DATABASE " + dbInfo.Name.String(), placement.String(), state.String()})
	}

	return nil
}

func (e *ShowExec) fetchShowPlacementForTable(ctx context.Context) (err error) {
	tbl, err := e.getTable()
	if err != nil {
		return err
	}

	tblInfo := tbl.Meta()
	placement, err := e.getTablePlacement(tblInfo)
	if err != nil {
		return err
	}

	if placement != nil {
		state, err := fetchTableScheduleState(ctx, nil, tblInfo)
		if err != nil {
			return err
		}
		ident := ast.Ident{Schema: e.Table.DBInfo.Name, Name: tblInfo.Name}
		e.appendRow([]any{"TABLE " + ident.String(), placement.String(), state.String()})
	}

	return nil
}

func (e *ShowExec) fetchShowPlacementForPartition(ctx context.Context) (err error) {
	tbl, err := e.getTable()
	if err != nil {
		return err
	}

	tblInfo := tbl.Meta()
	if tblInfo.Partition == nil {
		return errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs(e.Partition.O, tblInfo.Name.O))
	}

	var partition *model.PartitionDefinition
	for i := range tblInfo.Partition.Definitions {
		par := tblInfo.Partition.Definitions[i]
		if par.Name.L == e.Partition.L {
			partition = &par
			break
		}
	}

	if partition == nil {
		return errors.Trace(table.ErrUnknownPartition.GenWithStackByArgs(e.Partition.O, tblInfo.Name.O))
	}

	placement, err := e.getTablePlacement(tblInfo)
	if err != nil {
		return err
	}

	placement, err = e.getPartitionPlacement(placement, partition)
	if err != nil {
		return err
	}

	if placement != nil {
		state, err := fetchPartitionScheduleState(ctx, nil, partition)
		if err != nil {
			return err
		}
		tableIndent := ast.Ident{Schema: e.Table.DBInfo.Name, Name: tblInfo.Name}
		e.appendRow([]any{
			fmt.Sprintf("TABLE %s PARTITION %s", tableIndent.String(), partition.Name.String()),
			placement.String(),
			state.String(),
		})
	}

	return nil
}

func (e *ShowExec) fetchShowPlacement(ctx context.Context) error {
	if err := e.fetchAllPlacementPolicies(); err != nil {
		return err
	}

	scheduled := make(map[int64]infosync.PlacementScheduleState)

	if err := e.fetchAllDBPlacements(ctx, scheduled); err != nil {
		return err
	}

	if err := e.fetchAllTablePlacements(ctx, scheduled); err != nil {
		return err
	}
	return e.fetchRangesPlacementPlocy(ctx)
}

func (e *ShowExec) fetchAllPlacementPolicies() error {
	policies := e.is.AllPlacementPolicies()
	slices.SortFunc(policies, func(i, j *model.PolicyInfo) int { return cmp.Compare(i.Name.O, j.Name.O) })
	for _, policy := range policies {
		name := policy.Name
		settings := policy.PlacementSettings
		e.appendRow([]any{"POLICY " + name.String(), settings.String(), "NULL"})
	}

	return nil
}

func (e *ShowExec) fetchRangesPlacementPlocy(ctx context.Context) error {
	fetchFn := func(ctx context.Context, rangeBundleID string) error {
		policyName, err := ddl.GetRangePlacementPolicyName(ctx, rangeBundleID)
		if err != nil {
			return err
		}
		if policyName != "" {
			startKeyHex, endKeyHex := placement.GetRangeStartAndEndKeyHex(rangeBundleID)
			startKey, _ := hex.DecodeString(startKeyHex)
			endKey, _ := hex.DecodeString(endKeyHex)
			state, err := infosync.GetReplicationState(ctx, startKey, endKey)
			if err != nil {
				return err
			}
			policy, ok := e.is.PolicyByName(model.NewCIStr(policyName))
			if !ok {
				return errors.Errorf("Policy with name '%s' not found", policyName)
			}
			e.appendRow([]any{"RANGE " + rangeBundleID, policy.PlacementSettings.String(), state.String()})
		}

		return nil
	}
	// try fetch ranges placement policy
	if err := fetchFn(ctx, placement.TiDBBundleRangePrefixForGlobal); err != nil {
		return err
	}
	return fetchFn(ctx, placement.TiDBBundleRangePrefixForMeta)
}

func (e *ShowExec) fetchAllDBPlacements(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState) error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles

	dbs := e.is.AllSchemaNames()
	slices.SortFunc(dbs, func(i, j model.CIStr) int { return cmp.Compare(i.O, j.O) })

	for _, dbName := range dbs {
		if checker != nil && e.Ctx().GetSessionVars().User != nil && !checker.DBIsVisible(activeRoles, dbName.O) {
			continue
		}
		dbInfo, ok := e.is.SchemaByName(dbName)
		if !ok {
			return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbName.O)
		}
		placement, err := e.getDBPlacement(dbInfo)
		if err != nil {
			return err
		}

		if placement != nil {
			state, err := e.fetchDBScheduleState(ctx, scheduleState, dbInfo)
			if err != nil {
				return err
			}
			e.appendRow([]any{"DATABASE " + dbInfo.Name.String(), placement.String(), state.String()})
		}
	}

	return nil
}

func (e *ShowExec) fetchDBScheduleState(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState, db *model.DBInfo) (infosync.PlacementScheduleState, error) {
	state := infosync.PlacementScheduleStateScheduled
	for _, table := range e.is.SchemaTables(db.Name) {
		tbl := table.Meta()
		schedule, err := fetchTableScheduleState(ctx, scheduleState, tbl)
		if err != nil {
			return state, err
		}
		state = accumulateState(state, schedule)
		if state != infosync.PlacementScheduleStateScheduled {
			break
		}
	}
	return state, nil
}

type tableRowSet struct {
	name string
	rows [][]any
}

func (e *ShowExec) fetchAllTablePlacements(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState) error {
	checker := privilege.GetPrivilegeManager(e.Ctx())
	activeRoles := e.Ctx().GetSessionVars().ActiveRoles

	dbs := e.is.AllSchemaNames()
	slices.SortFunc(dbs, func(i, j model.CIStr) int { return cmp.Compare(i.O, j.O) })

	for _, dbName := range dbs {
		tableRowSets := make([]tableRowSet, 0)

		for _, tbl := range e.is.SchemaTables(dbName) {
			tblInfo := tbl.Meta()
			if checker != nil && !checker.RequestVerification(activeRoles, dbName.O, tblInfo.Name.O, "", mysql.AllPrivMask) {
				continue
			}

			var rows [][]any
			ident := ast.Ident{Schema: dbName, Name: tblInfo.Name}
			tblPlacement, err := e.getTablePlacement(tblInfo)
			if err != nil {
				return err
			}

			if tblPlacement != nil {
				state, err := fetchTableScheduleState(ctx, scheduleState, tblInfo)
				if err != nil {
					return err
				}
				rows = append(rows, []any{"TABLE " + ident.String(), tblPlacement.String(), state.String()})
			}

			if tblInfo.Partition != nil {
				for i := range tblInfo.Partition.Definitions {
					partition := tblInfo.Partition.Definitions[i]
					partitionPlacement, err := e.getPartitionPlacement(tblPlacement, &partition)
					if err != nil {
						return err
					}

					if partitionPlacement != nil {
						state, err := fetchPartitionScheduleState(ctx, scheduleState, &partition)
						if err != nil {
							return err
						}
						rows = append(rows, []any{
							fmt.Sprintf("TABLE %s PARTITION %s", ident.String(), partition.Name.String()),
							partitionPlacement.String(),
							state.String(),
						})
					}
				}
			}

			if len(rows) > 0 {
				tableRowSets = append(tableRowSets, struct {
					name string
					rows [][]any
				}{
					name: tblInfo.Name.String(),
					rows: rows,
				})
			}
		}

		slices.SortFunc(tableRowSets, func(i, j tableRowSet) int { return cmp.Compare(i.name, j.name) })
		for _, rowSet := range tableRowSets {
			for _, row := range rowSet.rows {
				e.appendRow(row)
			}
		}
	}

	return nil
}

func (e *ShowExec) getDBPlacement(dbInfo *model.DBInfo) (*model.PlacementSettings, error) {
	return e.getPolicyPlacement(dbInfo.PlacementPolicyRef)
}

func (e *ShowExec) getTablePlacement(tblInfo *model.TableInfo) (*model.PlacementSettings, error) {
	return e.getPolicyPlacement(tblInfo.PlacementPolicyRef)
}

func (e *ShowExec) getPartitionPlacement(tblPlacement *model.PlacementSettings, partition *model.PartitionDefinition) (*model.PlacementSettings, error) {
	placement, err := e.getPolicyPlacement(partition.PlacementPolicyRef)
	if err != nil {
		return nil, err
	}

	if placement != nil {
		return placement, nil
	}

	return tblPlacement, nil
}

func (e *ShowExec) getPolicyPlacement(policyRef *model.PolicyRefInfo) (settings *model.PlacementSettings, err error) {
	if policyRef == nil {
		return nil, nil
	}

	policy, ok := e.is.PolicyByName(policyRef.Name)
	if !ok {
		return nil, errors.Errorf("Policy with name '%s' not found", policyRef.Name)
	}
	return policy.PlacementSettings, nil
}

func fetchScheduleState(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState, id int64) (infosync.PlacementScheduleState, error) {
	if s, ok := scheduleState[id]; ok {
		return s, nil
	}
	startKey := codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id))
	endKey := codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id+1))
	schedule, err := infosync.GetReplicationState(ctx, startKey, endKey)
	if err == nil && scheduleState != nil {
		scheduleState[id] = schedule
	}
	return schedule, err
}

func fetchPartitionScheduleState(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState, part *model.PartitionDefinition) (infosync.PlacementScheduleState, error) {
	return fetchScheduleState(ctx, scheduleState, part.ID)
}

func fetchTableScheduleState(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState, table *model.TableInfo) (infosync.PlacementScheduleState, error) {
	state := infosync.PlacementScheduleStateScheduled

	schedule, err := fetchScheduleState(ctx, scheduleState, table.ID)
	if err != nil {
		return state, err
	}
	state = accumulateState(state, schedule)
	if state != infosync.PlacementScheduleStateScheduled {
		return state, nil
	}

	if table.GetPartitionInfo() != nil {
		for _, part := range table.GetPartitionInfo().Definitions {
			schedule, err = fetchScheduleState(ctx, scheduleState, part.ID)
			if err != nil {
				return infosync.PlacementScheduleStatePending, err
			}
			state = accumulateState(state, schedule)
			if state != infosync.PlacementScheduleStateScheduled {
				break
			}
		}
	}

	return schedule, nil
}

func accumulateState(curr, news infosync.PlacementScheduleState) infosync.PlacementScheduleState {
	a, b := int(curr), int(news)
	if a > b {
		return news
	}
	return curr
}

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
	"context"
	gjson "encoding/json"
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/sqlexec"
)

type showPlacementLabelsResultBuilder struct {
	labelKey2values map[string]interface{}
}

func (b *showPlacementLabelsResultBuilder) AppendStoreLabels(bj json.BinaryJSON) error {
	if b.labelKey2values == nil {
		b.labelKey2values = make(map[string]interface{})
	}

	data, err := bj.MarshalJSON()
	if err != nil {
		return errors.Trace(err)
	}

	if string(data) == "null" {
		return nil
	}

	if bj.TypeCode != json.TypeCodeArray {
		return errors.New("only array or null type is allowed")
	}

	labels := make([]*helper.StoreLabel, 0, bj.GetElemCount())
	err = gjson.Unmarshal(data, &labels)
	if err != nil {
		return errors.Trace(err)
	}

	for _, label := range labels {
		if values, ok := b.labelKey2values[label.Key]; ok {
			values.(map[string]interface{})[label.Value] = true
		} else {
			b.labelKey2values[label.Key] = map[string]interface{}{label.Value: true}
		}
	}

	return nil
}

func (b *showPlacementLabelsResultBuilder) BuildRows() ([][]interface{}, error) {
	rows := make([][]interface{}, 0, len(b.labelKey2values))
	for _, key := range b.sortMapKeys(b.labelKey2values) {
		values := b.sortMapKeys(b.labelKey2values[key].(map[string]interface{}))
		d, err := gjson.Marshal(values)
		if err != nil {
			return nil, errors.Trace(err)
		}

		valuesJSON := json.BinaryJSON{}
		err = valuesJSON.UnmarshalJSON(d)
		if err != nil {
			return nil, errors.Trace(err)
		}

		rows = append(rows, []interface{}{key, valuesJSON})
	}

	return rows, nil
}

func (b *showPlacementLabelsResultBuilder) sortMapKeys(m map[string]interface{}) []string {
	sorted := make([]string, 0, len(m))
	for key := range m {
		sorted = append(sorted, key)
	}

	sort.Strings(sorted)
	return sorted
}

func (e *ShowExec) fetchShowPlacementLabels(ctx context.Context) error {
	exec := e.ctx.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(ctx, true, "SELECT DISTINCT LABEL FROM %n.%n", "INFORMATION_SCHEMA", infoschema.TableTiKVStoreStatus)
	if err != nil {
		return errors.Trace(err)
	}

	rows, _, err := exec.ExecRestrictedStmt(ctx, stmt)
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
	checker := privilege.GetPrivilegeManager(e.ctx)
	if checker != nil && e.ctx.GetSessionVars().User != nil {
		if !checker.DBIsVisible(e.ctx.GetSessionVars().ActiveRoles, e.DBName.String()) {
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
		state, err := fetchDBScheduleState(ctx, nil, dbInfo)
		if err != nil {
			return err
		}
		e.appendRow([]interface{}{"DATABASE " + dbInfo.Name.String(), placement.String(), state.String()})
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
		e.appendRow([]interface{}{"TABLE " + ident.String(), placement.String(), state.String()})
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
		e.appendRow([]interface{}{
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

	return e.fetchAllTablePlacements(ctx, scheduled)
}

func (e *ShowExec) fetchAllPlacementPolicies() error {
	policies := e.is.AllPlacementPolicies()
	sort.Slice(policies, func(i, j int) bool { return policies[i].Name.O < policies[j].Name.O })
	for _, policy := range policies {
		name := policy.Name
		settings := policy.PlacementSettings
		e.appendRow([]interface{}{"POLICY " + name.String(), settings.String(), "NULL"})
	}

	return nil
}

func (e *ShowExec) fetchAllDBPlacements(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState) error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	activeRoles := e.ctx.GetSessionVars().ActiveRoles

	dbs := e.is.AllSchemas()
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name.O < dbs[j].Name.O })

	for _, dbInfo := range dbs {
		if e.ctx.GetSessionVars().User != nil && checker != nil && !checker.DBIsVisible(activeRoles, dbInfo.Name.O) {
			continue
		}

		placement, err := e.getDBPlacement(dbInfo)
		if err != nil {
			return err
		}

		if placement != nil {
			state, err := fetchDBScheduleState(ctx, scheduleState, dbInfo)
			if err != nil {
				return err
			}
			e.appendRow([]interface{}{"DATABASE " + dbInfo.Name.String(), placement.String(), state.String()})
		}
	}

	return nil
}

func (e *ShowExec) fetchAllTablePlacements(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState) error {
	checker := privilege.GetPrivilegeManager(e.ctx)
	activeRoles := e.ctx.GetSessionVars().ActiveRoles

	dbs := e.is.AllSchemas()
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name.O < dbs[j].Name.O })

	for _, dbInfo := range dbs {
		tableRowSets := make([]struct {
			name string
			rows [][]interface{}
		}, 0)

		for _, tbl := range e.is.SchemaTables(dbInfo.Name) {
			tblInfo := tbl.Meta()
			if checker != nil && !checker.RequestVerification(activeRoles, dbInfo.Name.O, tblInfo.Name.O, "", mysql.AllPrivMask) {
				continue
			}

			var rows [][]interface{}
			ident := ast.Ident{Schema: dbInfo.Name, Name: tblInfo.Name}
			tblPlacement, err := e.getTablePlacement(tblInfo)
			if err != nil {
				return err
			}

			if tblPlacement != nil {
				state, err := fetchTableScheduleState(ctx, scheduleState, tblInfo)
				if err != nil {
					return err
				}
				rows = append(rows, []interface{}{"TABLE " + ident.String(), tblPlacement.String(), state.String()})
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
						rows = append(rows, []interface{}{
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
					rows [][]interface{}
				}{
					name: tblInfo.Name.String(),
					rows: rows,
				})
			}
		}

		sort.Slice(tableRowSets, func(i, j int) bool { return tableRowSets[i].name < tableRowSets[j].name })
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

func fetchDBScheduleState(ctx context.Context, scheduleState map[int64]infosync.PlacementScheduleState, db *model.DBInfo) (infosync.PlacementScheduleState, error) {
	state := infosync.PlacementScheduleStateScheduled
	for _, table := range db.Tables {
		schedule, err := fetchTableScheduleState(ctx, scheduleState, table)
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

func accumulateState(curr, news infosync.PlacementScheduleState) infosync.PlacementScheduleState {
	a, b := int(curr), int(news)
	if a > b {
		return news
	}
	return curr
}

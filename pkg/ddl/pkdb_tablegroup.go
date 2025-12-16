// Copyright 2019 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	pdhttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
)

func onCreateTableGroup(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetTableGroupArgs(job)
	if err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	tgInfo := args.TableGroup
	tgInfo.State = model.StateNone
	metaMut := jobCtx.metaMut

	err = checkTableGroupNotExists(jobCtx.infoCache, metaMut, tgInfo.Name)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	if !tgInfo.Valid() {
		job.State = model.JobStateCancelled
		return ver, errors.Errorf("invalid tablegroup: %v", tgInfo)
	}

	switch tgInfo.State {
	case model.StateNone, model.StatePublic:
		// none -> public
		is := jobCtx.infoCache.GetLatest()
		physicalTables := make([][]model.PhysicalTableInfo, 0, len(tgInfo.Tables))
		for _, tn := range tgInfo.Tables {
			tbInfo, err := is.TableInfoByName(pmodel.NewCIStr(tn.DB), pmodel.NewCIStr(tn.Table))
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			physicalTables = append(physicalTables, getPhysicalTableInfos(tn.DB, tbInfo))
		}
		agInfos, affinityGroups := buildAffinityGroups(tgInfo.Name.L, physicalTables)
		state, err := infosync.CreateAffinityGroup(jobCtx.ctx, affinityGroups)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		tgInfo.AffinityGroups = agInfos
		tgInfo.GroupState = state
		tgInfo.State = model.StatePublic
		err = metaMut.CreateTableGroup(tgInfo)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		job.SchemaID = tgInfo.ID
		ver, err = updateSchemaVersion(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
		return ver, nil
	default:
		return ver, dbterror.ErrInvalidDDLState.GenWithStackByArgs("tablegroup", tgInfo.State)
	}
}

func buildAffinityGroups(groupName string, physicalTables [][]model.PhysicalTableInfo) ([]model.AffinityGroupInfo, map[string][]pdhttp.AffinityGroupKeyRange) {
	if len(physicalTables) == 0 {
		return nil, nil
	}
	count := len(physicalTables[0])
	agInfos := make([]model.AffinityGroupInfo, 0, count)
	affinityGroups := make(map[string][]pdhttp.AffinityGroupKeyRange, count)
	for i := 0; i < count; i++ {
		var name string
		if count > 1 {
			name = fmt.Sprintf("_tidb_ptg_%s_p%d", groupName, i)
		} else {
			name = "_tidb_tg_" + groupName
		}
		groupTables := make([]model.PhysicalTableInfo, 0, len(physicalTables))
		for _, physicals := range physicalTables {
			if i < len(physicals) {
				groupTables = append(groupTables, physicals[i])
			}
		}
		ranges := make([]pdhttp.AffinityGroupKeyRange, 0, len(groupTables))
		for _, tb := range groupTables {
			ranges = append(ranges, pdhttp.AffinityGroupKeyRange{
				StartKey: codec.EncodeBytes(nil, tablecodec.GenTablePrefix(tb.PhysicalTableID)),
				EndKey:   codec.EncodeBytes(nil, tablecodec.GenTablePrefix(tb.PhysicalTableID+1)),
			})
		}
		agInfos = append(agInfos, model.AffinityGroupInfo{
			Name:   name,
			Tables: groupTables,
		})
		affinityGroups[name] = ranges
	}
	return agInfos, affinityGroups
}

func buildAffinityGroupsToRemove(tgInfo *model.TableGroupInfo, tables []model.TableName) map[string][]pdhttp.AffinityGroupKeyRange {
	if len(tables) == 0 {
		return nil
	}
	tablesToRemove := make(map[model.TableName]struct{}, len(tables))
	for _, tn := range tables {
		tablesToRemove[tn] = struct{}{}
	}

	affinityGroupsToRemove := make(map[string][]pdhttp.AffinityGroupKeyRange, len(tgInfo.AffinityGroups))
	for i, ag := range tgInfo.AffinityGroups {
		name := ag.Name
		ranges := make([]pdhttp.AffinityGroupKeyRange, 0, len(tables))
		newPhysicalTables := make([]model.PhysicalTableInfo, 0, len(ag.Tables))
		for _, physicalTable := range ag.Tables {
			_, exist := tablesToRemove[physicalTable.TableName]
			if exist {
				ranges = append(ranges, pdhttp.AffinityGroupKeyRange{
					StartKey: codec.EncodeBytes(nil, tablecodec.GenTablePrefix(physicalTable.PhysicalTableID)),
					EndKey:   codec.EncodeBytes(nil, tablecodec.GenTablePrefix(physicalTable.PhysicalTableID+1)),
				})
			} else {
				newPhysicalTables = append(newPhysicalTables, physicalTable)
			}
		}
		affinityGroupsToRemove[name] = ranges
		tgInfo.AffinityGroups[i].Tables = newPhysicalTables
	}
	return affinityGroupsToRemove
}

func getPhysicalTableInfos(dbName string, tbInfo *model.TableInfo) []model.PhysicalTableInfo {
	var physicalTables []model.PhysicalTableInfo
	tn := model.TableName{DB: dbName, Table: tbInfo.Name.L}
	if pi := tbInfo.GetPartitionInfo(); pi != nil {
		physicalTables = make([]model.PhysicalTableInfo, 0, len(pi.Definitions))
		for _, def := range pi.Definitions {
			physicalTables = append(physicalTables, model.PhysicalTableInfo{
				TableName:       tn,
				PartitionName:   def.Name.L,
				PhysicalTableID: def.ID,
			})
		}
	} else {
		physicalTables = []model.PhysicalTableInfo{{
			TableName:       tn,
			PhysicalTableID: tbInfo.ID,
		}}
	}
	return physicalTables
}

func checkTableGroupNotExists(infoCache *infoschema.InfoCache, t *meta.Mutator, name pmodel.CIStr) error {
	existTgInfo, err := getTableGroupByName(infoCache, t, name)
	if err != nil {
		return errors.Trace(err)
	}
	if existTgInfo != nil {
		return infoschema.ErrTableGroupExists.GenWithStackByArgs(existTgInfo.Name)
	}
	return nil
}

func getTableGroupByName(infoCache *infoschema.InfoCache, t *meta.Mutator, name pmodel.CIStr) (*model.TableGroupInfo, error) {
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return nil, err
	}

	is := infoCache.GetLatest()
	if is != nil && is.SchemaMetaVersion() == currVer {
		// Use cached.
		tg, ok := is.TableGroupByName(name)
		if ok {
			return tg, nil
		}
		return nil, nil
	}
	// Check in meta directly.
	tableGroups, err := t.ListTableGroups()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, tg := range tableGroups {
		if tg.Name.L == name.L {
			return tg, nil
		}
	}
	return nil, nil
}

func onDropTableGroup(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	metaMut := jobCtx.metaMut
	tgInfo, err := checkTableGroupExistAndCancelNotExistJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch tgInfo.State {
	case model.StatePublic:
		// public -> write only
		tgInfo.State = model.StateWriteOnly
		err = metaMut.UpdateTableGroup(tgInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		tgInfo.State = model.StateDeleteOnly
		err = metaMut.UpdateTableGroup(tgInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		tgInfo.State = model.StateNone
		err = metaMut.DropTableGroup(tgInfo.ID)
		if err != nil {
			return ver, errors.Trace(err)
		}
		err = infosync.BatchDeleteAffinityGroups(jobCtx.ctx, tgInfo.GetAffinityGroupIDs())
		if err != nil {
			logutil.DDLLogger().Error("DeleteAffinityGroup fails", zap.Error(err))
		}
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, nil)
	default:
		// We can't enter here.
		return ver, errors.Trace(errors.Errorf("invalid db state %v", tgInfo.State))
	}
	job.SchemaState = tgInfo.State
	return ver, errors.Trace(err)
}

func onAlterTableGroup(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	args, err := model.GetAlterTableGroupArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	metaMut := jobCtx.metaMut
	tgInfo, err := checkTableGroupExistAndCancelNotExistJob(metaMut, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	err = checkAlterTableGroup(tgInfo, args.Option, args.Tables)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	switch args.Option {
	case ast.AlterTableGroupOptionAddTables:
		is := jobCtx.infoCache.GetLatest()
		err = handleTableGroupAddTables(jobCtx.ctx, is, tgInfo, args.Tables...)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
	case ast.AlterTableGroupOptionDropTables:
		handleTableGroupDropTables(jobCtx.ctx, tgInfo, args.Tables...)
	}

	err = metaMut.UpdateTableGroup(tgInfo)
	if err != nil {
		return ver, errors.Trace(err)
	}
	ver, err = updateSchemaVersion(jobCtx, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, nil)
	job.SchemaState = model.StatePublic
	return ver, errors.Trace(err)
}

func updateTableGroupWhenDropTable(ctx context.Context, metaMut *meta.Mutator, infoCache *infoschema.InfoCache, tbInfo *model.TableInfo, schemaID int64) error {
	is := infoCache.GetLatest()
	dbInfo, ok := is.SchemaByID(schemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	return updateTableGroupWhenDropTableInDB(ctx, metaMut, infoCache, dbInfo, tbInfo)
}

func updateTableGroupWhenDropTableInDB(ctx context.Context, metaMut *meta.Mutator, infoCache *infoschema.InfoCache, dbInfo *model.DBInfo, tbInfo *model.TableInfo) error {
	is := infoCache.GetLatest()
	tn := model.TableName{DB: dbInfo.Name.L, Table: tbInfo.Name.L}
	tg, ok := is.TableGroupByTableName(tn)
	if !ok {
		return nil
	}
	tgInfo, err := metaMut.GetTableGroup(tg.ID)
	if err != nil {
		return errors.Trace(err)
	}
	handleTableGroupDropTables(ctx, tgInfo, tn)
	return metaMut.UpdateTableGroup(tgInfo)
}

func updateTableGroupWhenTruncateTable(ctx context.Context, metaMut *meta.Mutator, infoCache *infoschema.InfoCache, tbInfo *model.TableInfo, schemaID int64) error {
	is := infoCache.GetLatest()
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	tn := model.TableName{DB: schema.Name.L, Table: tbInfo.Name.L}
	tg, ok := is.TableGroupByTableName(tn)
	if !ok {
		return nil
	}
	tgInfo, err := metaMut.GetTableGroup(tg.ID)
	if err != nil {
		return errors.Trace(err)
	}

	newPhysicalTables := getPhysicalTableInfos(schema.Name.L, tbInfo)
	affinityGroupsToRemove := make(map[string][]pdhttp.AffinityGroupKeyRange, len(tgInfo.AffinityGroups))
	affinityGroupsToAdd := make(map[string][]pdhttp.AffinityGroupKeyRange, len(tgInfo.AffinityGroups))
	for i, ag := range tgInfo.AffinityGroups {
		for j, tableInGroup := range ag.Tables {
			if tableInGroup.TableName == tn {
				affinityGroupsToRemove[ag.Name] = []pdhttp.AffinityGroupKeyRange{
					{
						StartKey: codec.EncodeBytes(nil, tablecodec.GenTablePrefix(tableInGroup.PhysicalTableID)),
						EndKey:   codec.EncodeBytes(nil, tablecodec.GenTablePrefix(tableInGroup.PhysicalTableID+1)),
					},
				}
				if i < len(newPhysicalTables) {
					newPhysicalID := newPhysicalTables[i].PhysicalTableID
					affinityGroupsToAdd[ag.Name] = []pdhttp.AffinityGroupKeyRange{
						{
							StartKey: codec.EncodeBytes(nil, tablecodec.GenTablePrefix(newPhysicalID)),
							EndKey:   codec.EncodeBytes(nil, tablecodec.GenTablePrefix(newPhysicalID+1)),
						},
					}
					tgInfo.AffinityGroups[i].Tables[j].PhysicalTableID = newPhysicalID
				}
				break
			}
		}
	}

	_, err = infosync.RemoveAffinityGroupKeyRanges(ctx, affinityGroupsToRemove)
	if err != nil {
		logutil.DDLLogger().Error("RemoveAffinityGroupKeyRanges fails", zap.Error(err))
	}
	_, err = infosync.AddAffinityGroupKeyRanges(ctx, affinityGroupsToAdd)
	if err != nil {
		logutil.DDLLogger().Error("AddAffinityGroupKeyRanges fails", zap.Error(err))
	}
	return metaMut.UpdateTableGroup(tgInfo)
}

func updateTableGroupWhenTruncatePartitions(ctx context.Context, metaMut *meta.Mutator, infoCache *infoschema.InfoCache, tbInfo *model.TableInfo, schemaID int64, oldIDs, newIDs []int64) error {
	is := infoCache.GetLatest()
	schema, ok := is.SchemaByID(schemaID)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs("")
	}
	tn := model.TableName{DB: schema.Name.L, Table: tbInfo.Name.L}
	tg, ok := is.TableGroupByTableName(tn)
	if !ok {
		return nil
	}
	tgInfo, err := metaMut.GetTableGroup(tg.ID)
	if err != nil {
		return errors.Trace(err)
	}

	affinityGroupsToRemove := make(map[string][]pdhttp.AffinityGroupKeyRange, len(tgInfo.AffinityGroups))
	affinityGroupsToAdd := make(map[string][]pdhttp.AffinityGroupKeyRange, len(tgInfo.AffinityGroups))
	for idx, oldID := range oldIDs {
		for i, ag := range tgInfo.AffinityGroups {
			found := false
			for j, tableInGroup := range ag.Tables {
				if tableInGroup.TableName == tn && tableInGroup.PhysicalTableID == oldID {
					found = true
					affinityGroupsToRemove[ag.Name] = []pdhttp.AffinityGroupKeyRange{
						{
							StartKey: codec.EncodeBytes(nil, tablecodec.GenTablePrefix(tableInGroup.PhysicalTableID)),
							EndKey:   codec.EncodeBytes(nil, tablecodec.GenTablePrefix(tableInGroup.PhysicalTableID+1)),
						},
					}
					newPhysicalID := newIDs[idx]
					affinityGroupsToAdd[ag.Name] = []pdhttp.AffinityGroupKeyRange{
						{
							StartKey: codec.EncodeBytes(nil, tablecodec.GenTablePrefix(newPhysicalID)),
							EndKey:   codec.EncodeBytes(nil, tablecodec.GenTablePrefix(newPhysicalID+1)),
						},
					}
					tgInfo.AffinityGroups[i].Tables[j].PhysicalTableID = newPhysicalID
					break
				}
			}
			if found {
				break
			}
		}
	}

	_, err = infosync.RemoveAffinityGroupKeyRanges(ctx, affinityGroupsToRemove)
	if err != nil {
		logutil.DDLLogger().Error("RemoveAffinityGroupKeyRanges fails", zap.Error(err))
	}
	_, err = infosync.AddAffinityGroupKeyRanges(ctx, affinityGroupsToAdd)
	if err != nil {
		logutil.DDLLogger().Error("AddAffinityGroupKeyRanges fails", zap.Error(err))
	}
	return metaMut.UpdateTableGroup(tgInfo)
}

func handleTableGroupDropTables(ctx context.Context, tgInfo *model.TableGroupInfo, tables ...model.TableName) {
	needDrop := make(map[model.TableName]struct{}, len(tables))
	for _, tn := range tables {
		needDrop[tn] = struct{}{}
	}
	newTables := make([]model.TableName, 0, max(len(tgInfo.Tables)-len(tables), 2))
	tablesNeedRemove := make([]model.TableName, 0, len(tables))
	for _, tn := range tgInfo.Tables {
		_, exist := needDrop[tn]
		if !exist {
			newTables = append(newTables, tn)
		} else {
			tablesNeedRemove = append(tablesNeedRemove, tn)
		}
	}
	affinityGroups := buildAffinityGroupsToRemove(tgInfo, tablesNeedRemove)
	state, err := infosync.RemoveAffinityGroupKeyRanges(ctx, affinityGroups)
	if err != nil {
		logutil.DDLLogger().Error("RemoveAffinityGroupKeyRanges fails", zap.Error(err))
	} else {
		tgInfo.GroupState = state
	}
	tgInfo.Tables = newTables
}

func handleTableGroupAddTables(ctx context.Context, is infoschema.InfoSchema, tgInfo *model.TableGroupInfo, tables ...model.TableName) error {
	newTables := make([]model.TableName, 0, len(tables))
	physicalTables := make([][]model.PhysicalTableInfo, 0, len(tgInfo.Tables))
	for _, tn := range tables {
		tbInfo, err := is.TableInfoByName(pmodel.NewCIStr(tn.DB), pmodel.NewCIStr(tn.Table))
		if err != nil {
			return errors.Trace(err)
		}
		physicalTables = append(physicalTables, getPhysicalTableInfos(tn.DB, tbInfo))
		newTables = append(newTables, tn)
	}
	agInfos, affinityGroups := buildAffinityGroups(tgInfo.Name.L, physicalTables)
	state, err := infosync.AddAffinityGroupKeyRanges(ctx, affinityGroups)
	if err != nil {
		return errors.Trace(err)
	}
	for i := range tgInfo.AffinityGroups {
		if i < len(agInfos) {
			tgInfo.AffinityGroups[i].Tables = append(tgInfo.AffinityGroups[i].Tables, agInfos[i].Tables...)
		}
	}
	tgInfo.GroupState = state
	tgInfo.Tables = append(tgInfo.Tables, newTables...)
	return nil
}

func checkTableGroupExistAndCancelNotExistJob(t *meta.Mutator, job *model.Job, tableGroupID int64) (*model.TableGroupInfo, error) {
	tgInfo, err := t.GetTableGroup(tableGroupID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tgInfo == nil {
		job.State = model.JobStateCancelled
		return nil, infoschema.ErrDatabaseDropExists.GenWithStackByArgs("")
	}
	return tgInfo, nil
}

func checkAlterTableGroup(tgInfo *model.TableGroupInfo, option ast.AlterTableGroupOptionType, tables []model.TableName) error {
	uniqueMap := make(map[model.TableName]struct{}, len(tgInfo.Tables))
	for _, tn := range tgInfo.Tables {
		uniqueMap[tn] = struct{}{}
	}
	for _, tn := range tables {
		if tn.DB == "" {
			return errors.Trace(plannererrors.ErrNoDB)
		}
		_, exist := uniqueMap[tn]
		switch option {
		case ast.AlterTableGroupOptionAddTables:
			if exist {
				return infoschema.ErrTableExists.GenWithStackByArgs(ast.Ident{Schema: pmodel.NewCIStr(tn.DB), Name: pmodel.NewCIStr(tn.Table)})
			}
		case ast.AlterTableGroupOptionDropTables:
			if !exist {
				return infoschema.ErrTableNotExists.GenWithStackByArgs(tn.DB, tn.Table)
			}
		default:
			return errors.New("invalid alter table group option")
		}
	}
	if option == ast.AlterTableGroupOptionDropTables && len(tables) == len(tgInfo.Tables) {
		return errors.New("drop all tables from tablegroup is not allowed, try drop tablegroup instead")
	}
	return nil
}

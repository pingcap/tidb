// Copyright 2026 PingCAP, Inc.

package ddl

import (
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

const pkdbReplacingPrimaryKeyPrefix = "_PkReplace$_"

func pkdbPrepareReplacePrimaryKeyArg(
	originalTblInfo, trackedTblInfo *model.TableInfo,
	arg *model.IndexArg,
	multiSchemaInfo *model.MultiSchemaInfo,
) {
	if arg == nil || originalTblInfo == nil || trackedTblInfo == nil || multiSchemaInfo == nil {
		return
	}
	if originalTblInfo.HasClusteredIndex() || trackedTblInfo.PKIsHandle {
		return
	}
	if trackedTblInfo.FindIndexByName(strings.ToLower(mysql.PrimaryKeyName)) != nil {
		return
	}

	oldPrimaryKey := originalTblInfo.GetPrimaryKey()
	if oldPrimaryKey == nil || !oldPrimaryKey.Primary {
		return
	}

	arg.IndexName = pkdbGenReplacingPrimaryKeyName(trackedTblInfo)
	multiSchemaInfo.PKDBPrimaryKeyReplaceInfo = &model.PKDBPrimaryKeyReplaceInfo{
		OldPrimaryKeyID: oldPrimaryKey.ID,
	}
}

func pkdbOldPrimaryKeyIDForDropColumn(job *model.Job) int64 {
	if job == nil || job.Type != model.ActionDropColumn || job.MultiSchemaInfo == nil ||
		job.MultiSchemaInfo.PKDBPrimaryKeyReplaceInfo == nil {
		return 0
	}
	return job.MultiSchemaInfo.PKDBPrimaryKeyReplaceInfo.OldPrimaryKeyID
}

type dropColumnValidationPolicy struct {
	allowedReplacementOldPrimaryKeyID int64
}

func (policy dropColumnValidationPolicy) allowsReplacementPrimaryKey(
	tblInfo *model.TableInfo,
	indexInfo *model.IndexInfo,
) bool {
	return policy.allowedReplacementOldPrimaryKeyID != 0 &&
		indexInfo.ID == policy.allowedReplacementOldPrimaryKeyID &&
		indexInfo.Primary && !tblInfo.HasClusteredIndex() &&
		indexInfo.State == model.StateWriteOnly
}

func pkdbOldPrimaryKeyID(job *model.Job) int64 {
	if job == nil || job.MultiSchemaInfo == nil || job.MultiSchemaInfo.PKDBPrimaryKeyReplaceInfo == nil {
		return 0
	}
	return job.MultiSchemaInfo.PKDBPrimaryKeyReplaceInfo.OldPrimaryKeyID
}

func pkdbIsReplacePrimaryKeyJob(job *model.Job) bool {
	return job != nil && job.Type == model.ActionAddPrimaryKey && pkdbOldPrimaryKeyID(job) != 0
}

func pkdbValidateNonclusteredPrimaryKeyReplacement(
	clusteredIndexMode variable.ClusteredIndexDefMode,
	indexOption *ast.IndexOption,
	multiSchemaInfo *model.MultiSchemaInfo,
) error {
	if multiSchemaInfo == nil || multiSchemaInfo.PKDBPrimaryKeyReplaceInfo == nil {
		return nil
	}
	if indexOption != nil && indexOption.PrimaryKeyTp == pmodel.PrimaryKeyTypeNonClustered {
		return nil
	}
	if clusteredIndexMode == variable.ClusteredIndexDefModeOff {
		return nil
	}
	return dbterror.ErrUnsupportedModifyPrimaryKey.GenWithStack(
		"Replacing a primary key supports NONCLUSTERED primary keys only. " +
			"Set tidb_enable_clustered_index=OFF or specify NONCLUSTERED",
	)
}

func pkdbShouldDeferReplacementDropColumnValidation(job *model.Job) bool {
	return pkdbOldPrimaryKeyIDForDropColumn(job) != 0 &&
		job.MultiSchemaInfo.Revertible && !job.IsRollingback()
}

func pkdbGenReplacingPrimaryKeyName(tblInfo *model.TableInfo) pmodel.CIStr {
	indexNameMap := make(map[string]struct{}, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		indexNameMap[idx.Name.L] = struct{}{}
	}

	suffix := 0
	name := fmt.Sprintf("%s%s_%d", pkdbReplacingPrimaryKeyPrefix, mysql.PrimaryKeyName, suffix)
	for {
		if _, ok := indexNameMap[strings.ToLower(name)]; !ok {
			return pmodel.NewCIStr(name)
		}
		suffix++
		name = fmt.Sprintf("%s%s_%d", pkdbReplacingPrimaryKeyPrefix, mysql.PrimaryKeyName, suffix)
	}
}

func pkdbCutoverReplacePrimaryKey(tblInfo *model.TableInfo, newPrimaryKey *model.IndexInfo, oldPrimaryKeyID int64) error {
	if tblInfo == nil || newPrimaryKey == nil || oldPrimaryKeyID == 0 {
		return errors.New("invalid replace primary key cutover context")
	}
	if newPrimaryKey.ID == oldPrimaryKeyID || !newPrimaryKey.Primary {
		return errors.Errorf("invalid replacement primary key identity: old=%d, new=%d", oldPrimaryKeyID, newPrimaryKey.ID)
	}

	oldPrimaryKey := tblInfo.FindIndexByID(oldPrimaryKeyID)
	if oldPrimaryKey == nil {
		return errors.Errorf("old primary key %d not found during replace primary key cutover", oldPrimaryKeyID)
	}
	if !oldPrimaryKey.Primary || tblInfo.HasClusteredIndex() || oldPrimaryKey.State != model.StateWriteOnly {
		return errors.Errorf(
			"invalid old primary key %d during cutover: primary=%t, clustered=%t, state=%s",
			oldPrimaryKey.ID, oldPrimaryKey.Primary, tblInfo.HasClusteredIndex(), oldPrimaryKey.State,
		)
	}
	primaryKeyName := strings.ToLower(mysql.PrimaryKeyName)
	if newPrimaryKey.State == model.StatePublic {
		if newPrimaryKey.Name.L != primaryKeyName || tblInfo.FindIndexByName(primaryKeyName) != newPrimaryKey ||
			!oldPrimaryKey.IsRemoving() ||
			pkdbIndexOffset(tblInfo, newPrimaryKey.ID) > pkdbIndexOffset(tblInfo, oldPrimaryKey.ID) {
			return errors.Errorf("inconsistent completed primary key cutover: old=%d, new=%d", oldPrimaryKey.ID, newPrimaryKey.ID)
		}
		pkdbRebuildPublicIndexColumnFlags(tblInfo)
		return nil
	}
	if oldPrimaryKey.Name.L != primaryKeyName || tblInfo.FindIndexByName(primaryKeyName) != oldPrimaryKey {
		return errors.Errorf("old primary key %d does not own the PRIMARY name during cutover", oldPrimaryKey.ID)
	}
	if newPrimaryKey.State != model.StateWriteReorganization {
		return errors.Errorf("replacement primary key %d has invalid cutover state %s", newPrimaryKey.ID, newPrimaryKey.State)
	}

	oldPrimaryKey.Name = pkdbGenRemovingPrimaryKeyName(tblInfo)
	newPrimaryKey.Name = pmodel.NewCIStr(mysql.PrimaryKeyName)
	newPrimaryKey.State = model.StatePublic
	if pkdbIndexOffset(tblInfo, oldPrimaryKey.ID) < pkdbIndexOffset(tblInfo, newPrimaryKey.ID) {
		swapIndexInfoByID(tblInfo, oldPrimaryKey.ID, newPrimaryKey.ID)
	}
	pkdbRebuildPublicIndexColumnFlags(tblInfo)
	return nil
}

func pkdbGenRemovingPrimaryKeyName(tblInfo *model.TableInfo) pmodel.CIStr {
	indexNames := make(map[string]struct{}, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		indexNames[idx.Name.L] = struct{}{}
	}

	for suffix := 0; ; suffix++ {
		name := mysql.PrimaryKeyName
		if suffix > 0 {
			name = fmt.Sprintf("%s_%d", mysql.PrimaryKeyName, suffix)
		}
		removingName := pmodel.NewCIStr(model.GenRemovingObjName(name))
		if _, exists := indexNames[removingName.L]; !exists {
			return removingName
		}
	}
}

func pkdbFindIndexToDrop(tblInfo *model.TableInfo, arg *model.IndexArg) *model.IndexInfo {
	if tblInfo == nil || arg == nil {
		return nil
	}
	if arg.IndexID != 0 {
		return tblInfo.FindIndexByID(arg.IndexID)
	}
	return tblInfo.FindIndexByName(arg.IndexName.L)
}

func pkdbShouldRebuildPublicIndexColumnFlagsAfterDrop(job *model.Job, indexInfo *model.IndexInfo) bool {
	oldPrimaryKeyID := pkdbOldPrimaryKeyID(job)
	if oldPrimaryKeyID == 0 || indexInfo == nil || !indexInfo.Primary {
		return false
	}
	if indexInfo.ID == oldPrimaryKeyID {
		return true
	}
	return job.Type == model.ActionAddPrimaryKey && job.IsRollingback()
}

func pkdbRebuildPublicIndexColumnFlags(tblInfo *model.TableInfo) {
	var pkHandleColID int64
	if tblInfo.PKIsHandle {
		if pkCol := tblInfo.GetPkColInfo(); pkCol != nil {
			pkHandleColID = pkCol.ID
		}
	}

	for _, col := range tblInfo.Columns {
		col.DelFlag(mysql.PriKeyFlag | mysql.UniqueKeyFlag | mysql.MultipleKeyFlag)
	}
	if pkHandleColID != 0 {
		if pkCol := model.FindColumnInfoByID(tblInfo.Columns, pkHandleColID); pkCol != nil {
			pkCol.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		AddIndexColumnFlag(tblInfo, idx)
	}
}

func pkdbIndexOffset(tblInfo *model.TableInfo, indexID int64) int {
	for i, idx := range tblInfo.Indices {
		if idx.ID == indexID {
			return i
		}
	}
	return -1
}

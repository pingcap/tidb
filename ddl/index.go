// Copyright 2015 PingCAP, Inc.
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
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

const (
	// MaxCommentLength is exported for testing.
	MaxCommentLength = 1024
)

func buildIndexColumns(columns []*model.ColumnInfo, indexPartSpecifications []*ast.IndexPartSpecification) ([]*model.IndexColumn, error) {
	// Build offsets.
	idxParts := make([]*model.IndexColumn, 0, len(indexPartSpecifications))
	var col *model.ColumnInfo

	// The sum of length of all index columns.
	sumLength := 0
	for _, ip := range indexPartSpecifications {
		col = model.FindColumnInfo(columns, ip.Column.Name.L)
		if col == nil {
			return nil, dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ip.Column.Name)
		}

		if err := checkIndexColumn(col, ip.Length); err != nil {
			return nil, err
		}

		indexColumnLength, err := getIndexColumnLength(col, ip.Length)
		if err != nil {
			return nil, err
		}
		sumLength += indexColumnLength

		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > config.GetGlobalConfig().MaxIndexLength {
			return nil, dbterror.ErrTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
		}

		idxParts = append(idxParts, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: ip.Length,
		})
	}

	return idxParts, nil
}

func checkPKOnGeneratedColumn(tblInfo *model.TableInfo, indexPartSpecifications []*ast.IndexPartSpecification) (*model.ColumnInfo, error) {
	var lastCol *model.ColumnInfo
	for _, colName := range indexPartSpecifications {
		lastCol = getColumnInfoByName(tblInfo, colName.Column.Name.L)
		if lastCol == nil {
			return nil, dbterror.ErrKeyColumnDoesNotExits.GenWithStackByArgs(colName.Column.Name)
		}
		// Virtual columns cannot be used in primary key.
		if lastCol.IsGenerated() && !lastCol.GeneratedStored {
			if lastCol.Hidden {
				return nil, dbterror.ErrFunctionalIndexPrimaryKey
			}
			return nil, dbterror.ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
		}
	}

	return lastCol, nil
}

func checkIndexPrefixLength(columns []*model.ColumnInfo, idxColumns []*model.IndexColumn) error {
	idxLen, err := indexColumnsLen(columns, idxColumns)
	if err != nil {
		return err
	}
	if idxLen > config.GetGlobalConfig().MaxIndexLength {
		return dbterror.ErrTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
	}
	return nil
}

func checkIndexColumn(col *model.ColumnInfo, indexColumnLen int) error {
	if col.GetFlen() == 0 && (types.IsTypeChar(col.FieldType.GetType()) || types.IsTypeVarchar(col.FieldType.GetType())) {
		if col.Hidden {
			return errors.Trace(dbterror.ErrWrongKeyColumnFunctionalIndex.GenWithStackByArgs(col.GeneratedExprString))
		}
		return errors.Trace(dbterror.ErrWrongKeyColumn.GenWithStackByArgs(col.Name))
	}

	// JSON column cannot index.
	if col.FieldType.GetType() == mysql.TypeJSON {
		if col.Hidden {
			return dbterror.ErrFunctionalIndexOnJSONOrGeometryFunction
		}
		return errors.Trace(dbterror.ErrJSONUsedAsKey.GenWithStackByArgs(col.Name.O))
	}

	// Length must be specified and non-zero for BLOB and TEXT column indexes.
	if types.IsTypeBlob(col.FieldType.GetType()) {
		if indexColumnLen == types.UnspecifiedLength {
			if col.Hidden {
				return dbterror.ErrFunctionalIndexOnBlob
			}
			return errors.Trace(dbterror.ErrBlobKeyWithoutLength.GenWithStackByArgs(col.Name.O))
		}
		if indexColumnLen == types.ErrorLength {
			return errors.Trace(dbterror.ErrKeyPart0.GenWithStackByArgs(col.Name.O))
		}
	}

	// Length can only be specified for specifiable types.
	if indexColumnLen != types.UnspecifiedLength && !types.IsTypePrefixable(col.FieldType.GetType()) {
		return errors.Trace(dbterror.ErrIncorrectPrefixKey)
	}

	// Key length must be shorter or equal to the column length.
	if indexColumnLen != types.UnspecifiedLength &&
		types.IsTypeChar(col.FieldType.GetType()) {
		if col.GetFlen() < indexColumnLen {
			return errors.Trace(dbterror.ErrIncorrectPrefixKey)
		}
		// Length must be non-zero for char.
		if indexColumnLen == types.ErrorLength {
			return errors.Trace(dbterror.ErrKeyPart0.GenWithStackByArgs(col.Name.O))
		}
	}

	if types.IsString(col.FieldType.GetType()) {
		desc, err := charset.GetCharsetInfo(col.GetCharset())
		if err != nil {
			return err
		}
		indexColumnLen *= desc.Maxlen
	}
	// Specified length must be shorter than the max length for prefix.
	if indexColumnLen > config.GetGlobalConfig().MaxIndexLength {
		return dbterror.ErrTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
	}
	return nil
}

// getIndexColumnLength calculate the bytes number required in an index column.
func getIndexColumnLength(col *model.ColumnInfo, colLen int) (int, error) {
	length := types.UnspecifiedLength
	if colLen != types.UnspecifiedLength {
		length = colLen
	} else if col.GetFlen() != types.UnspecifiedLength {
		length = col.GetFlen()
	}

	switch col.GetType() {
	case mysql.TypeBit:
		return (length + 7) >> 3, nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		// Different charsets occupy different numbers of bytes on each character.
		desc, err := charset.GetCharsetInfo(col.GetCharset())
		if err != nil {
			return 0, dbterror.ErrUnsupportedCharset.GenWithStackByArgs(col.GetCharset(), col.GetCollate())
		}
		return desc.Maxlen * length, nil
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeShort:
		return mysql.DefaultLengthOfMysqlTypes[col.GetType()], nil
	case mysql.TypeFloat:
		if length <= mysql.MaxFloatPrecisionLength {
			return mysql.DefaultLengthOfMysqlTypes[mysql.TypeFloat], nil
		}
		return mysql.DefaultLengthOfMysqlTypes[mysql.TypeDouble], nil
	case mysql.TypeNewDecimal:
		return calcBytesLengthForDecimal(length), nil
	case mysql.TypeYear, mysql.TypeDate, mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeTimestamp:
		return mysql.DefaultLengthOfMysqlTypes[col.GetType()], nil
	default:
		return length, nil
	}
}

// decimal using a binary format that packs nine decimal (base 10) digits into four bytes.
func calcBytesLengthForDecimal(m int) int {
	return (m / 9 * 4) + ((m%9)+1)/2
}

func buildIndexInfo(tblInfo *model.TableInfo, indexName model.CIStr, indexPartSpecifications []*ast.IndexPartSpecification, state model.SchemaState) (*model.IndexInfo, error) {
	if err := checkTooLongIndex(indexName); err != nil {
		return nil, errors.Trace(err)
	}

	idxColumns, err := buildIndexColumns(tblInfo.Columns, indexPartSpecifications)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		State:   state,
	}
	return idxInfo, nil
}

func addIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].AddFlag(mysql.PriKeyFlag)
		}
		return
	}

	col := indexInfo.Columns[0]
	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].AddFlag(mysql.UniqueKeyFlag)
	} else {
		tblInfo.Columns[col.Offset].AddFlag(mysql.MultipleKeyFlag)
	}
}

func dropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].DelFlag(mysql.PriKeyFlag)
		}
	} else if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[indexInfo.Columns[0].Offset].DelFlag(mysql.UniqueKeyFlag)
	} else {
		tblInfo.Columns[indexInfo.Columns[0].Offset].DelFlag(mysql.MultipleKeyFlag)
	}

	col := indexInfo.Columns[0]
	// other index may still cover this col
	for _, index := range tblInfo.Indices {
		if index.Name.L == indexInfo.Name.L {
			continue
		}

		if index.Columns[0].Name.L != col.Name.L {
			continue
		}

		addIndexColumnFlag(tblInfo, index)
	}
}

func validateRenameIndex(from, to model.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
	if fromIdx := tbl.FindIndexByName(from.L); fromIdx == nil {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(from.O, tbl.Name))
	}
	// Take case-sensitivity into account, if `FromKey` and  `ToKey` are the same, nothing need to be changed
	if from.O == to.O {
		return true, nil
	}
	// If spec.FromKey.L == spec.ToKey.L, we operate on the same index(case-insensitive) and change its name (case-sensitive)
	// e.g: from `inDex` to `IndEX`. Otherwise, we try to rename an index to another different index which already exists,
	// that's illegal by rule.
	if toIdx := tbl.FindIndexByName(to.L); toIdx != nil && from.L != to.L {
		return false, errors.Trace(infoschema.ErrKeyNameDuplicate.GenWithStackByArgs(toIdx.Name.O))
	}
	return false, nil
}

func onRenameIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, from, to, err := checkRenameIndex(t, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Index"))
	}

	idx := tblInfo.FindIndexByName(from.L)
	idx.Name = to
	if ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func validateAlterIndexVisibility(indexName model.CIStr, invisible bool, tbl *model.TableInfo) (bool, error) {
	if idx := tbl.FindIndexByName(indexName.L); idx == nil {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(indexName.O, tbl.Name))
	} else if idx.Invisible == invisible {
		return true, nil
	}
	return false, nil
}

func onAlterIndexVisibility(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, from, invisible, err := checkAlterIndexVisibility(t, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}
	idx := tblInfo.FindIndexByName(from.L)
	idx.Invisible = invisible
	if ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func getNullColInfos(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) ([]*model.ColumnInfo, error) {
	nullCols := make([]*model.ColumnInfo, 0, len(indexInfo.Columns))
	for _, colName := range indexInfo.Columns {
		col := model.FindColumnInfo(tblInfo.Columns, colName.Name.L)
		if !mysql.HasNotNullFlag(col.GetFlag()) || mysql.HasPreventNullInsertFlag(col.GetFlag()) {
			nullCols = append(nullCols, col)
		}
	}
	return nullCols, nil
}

func checkPrimaryKeyNotNull(d *ddlCtx, w *worker, sqlMode mysql.SQLMode, t *meta.Meta, job *model.Job,
	tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (warnings []string, err error) {
	if !indexInfo.Primary {
		return nil, nil
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return nil, err
	}
	nullCols, err := getNullColInfos(tblInfo, indexInfo)
	if err != nil {
		return nil, err
	}
	if len(nullCols) == 0 {
		return nil, nil
	}

	err = modifyColsFromNull2NotNull(w, dbInfo, tblInfo, nullCols, &model.ColumnInfo{Name: model.NewCIStr("")}, false)
	if err == nil {
		return nil, nil
	}
	_, err = convertAddIdxJob2RollbackJob(d, t, job, tblInfo, indexInfo, err)
	// TODO: Support non-strict mode.
	// warnings = append(warnings, ErrWarnDataTruncated.GenWithStackByArgs(oldCol.Name.L, 0).Error())
	return nil, err
}

func updateHiddenColumns(tblInfo *model.TableInfo, idxInfo *model.IndexInfo, state model.SchemaState) {
	for _, col := range idxInfo.Columns {
		if tblInfo.Columns[col.Offset].Hidden {
			tblInfo.Columns[col.Offset].State = state
		}
	}
}

func (w *worker) onCreateIndex(d *ddlCtx, t *meta.Meta, job *model.Job, isPK bool) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(d, t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}

	var (
		unique                  bool
		global                  bool
		indexName               model.CIStr
		indexPartSpecifications []*ast.IndexPartSpecification
		indexOption             *ast.IndexOption
		sqlMode                 mysql.SQLMode
		warnings                []string
		hiddenCols              []*model.ColumnInfo
	)
	if isPK {
		// Notice: sqlMode and warnings is used to support non-strict mode.
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption, &sqlMode, &warnings, &global)
	} else {
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption, &hiddenCols, &global)
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo != nil && indexInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		err = dbterror.ErrDupKeyName.GenWithStack("index already exist %s", indexName)
		if isPK {
			err = infoschema.ErrMultiplePriKey
		}
		return ver, err
	}

	if indexInfo == nil {
		for _, hiddenCol := range hiddenCols {
			columnInfo := model.FindColumnInfo(tblInfo.Columns, hiddenCol.Name.L)
			if columnInfo != nil && columnInfo.State == model.StatePublic {
				// We already have a column with the same column name.
				job.State = model.JobStateCancelled
				// TODO: refine the error message
				return ver, infoschema.ErrColumnExists.GenWithStackByArgs(hiddenCol.Name)
			}
		}
	}

	if indexInfo == nil {
		if len(hiddenCols) > 0 {
			pos := &ast.ColumnPosition{Tp: ast.ColumnPositionNone}
			for _, hiddenCol := range hiddenCols {
				_, _, _, err = createColumnInfo(tblInfo, hiddenCol, pos)
				if err != nil {
					job.State = model.JobStateCancelled
					return ver, errors.Trace(err)
				}
			}
		}
		if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		indexInfo, err = buildIndexInfo(tblInfo, indexName, indexPartSpecifications, model.StateNone)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		if indexOption != nil {
			indexInfo.Comment = indexOption.Comment
			if indexOption.Visibility == ast.IndexVisibilityInvisible {
				indexInfo.Invisible = true
			}
			if indexOption.Tp == model.IndexTypeInvalid {
				// Use btree as default index type.
				indexInfo.Tp = model.IndexTypeBtree
			} else {
				indexInfo.Tp = indexOption.Tp
			}
		} else {
			// Use btree as default index type.
			indexInfo.Tp = model.IndexTypeBtree
		}
		indexInfo.Primary = false
		if isPK {
			if _, err = checkPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
				job.State = model.JobStateCancelled
				return ver, err
			}
			indexInfo.Primary = true
		}
		indexInfo.Unique = unique
		indexInfo.Global = global
		indexInfo.ID = allocateIndexID(tblInfo)
		tblInfo.Indices = append(tblInfo.Indices, indexInfo)
		if err = checkTooManyIndexes(tblInfo.Indices); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		// Here we need do this check before set state to `DeleteOnly`,
		// because if hidden columns has been set to `DeleteOnly`,
		// the `DeleteOnly` columns are missing when we do this check.
		if err := checkInvisibleIndexOnPK(tblInfo); err != nil {
			job.State = model.JobStateCancelled
			return ver, err
		}
		logutil.BgLogger().Info("[ddl] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		indexInfo.State = model.StateDeleteOnly
		updateHiddenColumns(tblInfo, indexInfo, model.StatePublic)
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
		metrics.GetBackfillProgressByLabel(metrics.LblAddIndex).Set(0)
	case model.StateDeleteOnly:
		// delete only -> write only
		indexInfo.State = model.StateWriteOnly
		_, err = checkPrimaryKeyNotNull(d, w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		indexInfo.State = model.StateWriteReorganization
		_, err = checkPrimaryKeyNotNull(d, w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(d.store, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		var done bool
		done, ver, err = doReorgWorkForCreateIndex(w, d, t, job, tbl, indexInfo)
		if !done {
			return ver, err
		}

		indexInfo.State = model.StatePublic
		// Set column index flag.
		addIndexColumnFlag(tblInfo, indexInfo)
		if isPK {
			if err = updateColsNull2NotNull(tblInfo, indexInfo); err != nil {
				return ver, errors.Trace(err)
			}
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func doReorgWorkForCreateIndex(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job,
	tbl table.Table, indexInfo *model.IndexInfo) (done bool, ver int64, err error) {
	elements := []*meta.Element{{ID: indexInfo.ID, TypeKey: meta.IndexElementKey}}
	reorgInfo, err := getReorgInfo(w.JobContext, d, t, job, tbl, elements)
	if err != nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}

	err = w.runReorgJob(t, reorgInfo, tbl.Meta(), d.lease, func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onCreateIndex",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tbl.Meta().Name, indexInfo.Name)
			}, false)
		return w.addTableIndex(tbl, indexInfo, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if kv.ErrKeyExists.Equal(err) || dbterror.ErrCancelledDDLJob.Equal(err) || dbterror.ErrCantDecodeRecord.Equal(err) {
			logutil.BgLogger().Warn("[ddl] run add index job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), indexInfo, err)
			if err1 := t.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
				logutil.BgLogger().Warn("[ddl] run add index job failed, convert job to rollback, RemoveDDLReorgHandle failed", zap.String("job", job.String()), zap.Error(err1))
			}
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()
		return false, ver, errors.Trace(err)
	}
	// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
	w.reorgCtx.cleanNotifyReorgCancel()
	return true, ver, errors.Trace(err)
}

func onDropIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, indexInfo, err := checkDropIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}

	dependentHiddenCols := make([]*model.ColumnInfo, 0)
	for _, indexColumn := range indexInfo.Columns {
		if tblInfo.Columns[indexColumn.Offset].Hidden {
			dependentHiddenCols = append(dependentHiddenCols, tblInfo.Columns[indexColumn.Offset])
		}
	}

	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StatePublic:
		// public -> write only
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		// delete only -> reorganization
		indexInfo.State = model.StateDeleteReorganization
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		indexInfo.State = model.StateNone
		if len(dependentHiddenCols) > 0 {
			firstHiddenOffset := dependentHiddenCols[0].Offset
			for i := 0; i < len(dependentHiddenCols); i++ {
				// Set this column's offset to the last and reset all following columns' offsets.
				adjustColumnInfoInDropColumn(tblInfo, firstHiddenOffset)
			}
		}
		newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			if idx.Name.L != indexInfo.Name.L {
				newIndices = append(newIndices, idx)
			}
		}
		tblInfo.Indices = newIndices
		// Set column index flag.
		dropIndexColumnFlag(tblInfo, indexInfo)

		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(dependentHiddenCols)]
		failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
			if val.(bool) {
				panic("panic test in cancelling add index")
			}
		})

		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			job.Args[0] = indexInfo.ID
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			job.Args = append(job.Args, indexInfo.ID, getPartitionIDs(tblInfo))
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State))
	}
	job.SchemaState = indexInfo.State
	return ver, errors.Trace(err)
}

func checkDropIndex(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var indexName model.CIStr
	if err = job.DecodeArgs(&indexName); err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		job.State = model.JobStateCancelled
		return nil, nil, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	// Double check for drop index on auto_increment column.
	err = checkDropIndexOnAutoIncrementColumn(tblInfo, indexInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, autoid.ErrWrongAutoKey
	}

	// Check that drop primary index will not cause invisible implicit primary index.
	if err := checkInvisibleIndexesOnPK(tblInfo, []*model.IndexInfo{indexInfo}, job); err != nil {
		return nil, nil, errors.Trace(err)
	}

	return tblInfo, indexInfo, nil
}

func onDropIndexes(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, indexNames, ifExists, err := getSchemaInfos(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Indexes"))
	}

	indexInfos, err := checkDropIndexes(tblInfo, job, indexNames, ifExists)
	if err != nil {
		return ver, errors.Trace(err)
	}

	if len(indexInfos) == 0 {
		job.State = model.JobStateCancelled
		return ver, nil
	}

	dependentHiddenCols := make([]*model.ColumnInfo, 0)
	for _, indexInfo := range indexInfos {
		for _, indexColumn := range indexInfo.Columns {
			if tblInfo.Columns[indexColumn.Offset].Hidden {
				dependentHiddenCols = append(dependentHiddenCols, tblInfo.Columns[indexColumn.Offset])
			}
		}
	}

	originalState := indexInfos[0].State
	switch indexInfos[0].State {
	case model.StatePublic:
		// public -> write only
		setIndicesState(indexInfos, model.StateWriteOnly)
		setColumnsState(dependentHiddenCols, model.StateWriteOnly)
		for _, colInfo := range dependentHiddenCols {
			adjustColumnInfoInDropColumn(tblInfo, colInfo.Offset)
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> delete only
		setIndicesState(indexInfos, model.StateDeleteOnly)
		setColumnsState(dependentHiddenCols, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> reorganization
		setIndicesState(indexInfos, model.StateDeleteReorganization)
		setColumnsState(dependentHiddenCols, model.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfos[0].State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.SchemaState = model.StateDeleteReorganization
	case model.StateDeleteReorganization:
		// reorganization -> absent
		indexIDs := make([]int64, 0, len(indexInfos))
		indexNames := make(map[string]bool, len(indexInfos))
		for _, indexInfo := range indexInfos {
			indexNames[indexInfo.Name.L] = true
			indexIDs = append(indexIDs, indexInfo.ID)
		}

		newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			if _, ok := indexNames[idx.Name.L]; !ok {
				newIndices = append(newIndices, idx)
			}
		}
		tblInfo.Indices = newIndices

		// Set column index flag.
		for _, indexInfo := range indexInfos {
			dropIndexColumnFlag(tblInfo, indexInfo)
		}

		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(dependentHiddenCols)]

		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
		job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfos[0].State)
	}

	return ver, errors.Trace(err)
}

func getSchemaInfos(t *meta.Meta, job *model.Job) (*model.TableInfo, []model.CIStr, []bool, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	var indexNames []model.CIStr
	var ifExists []bool
	if err = job.DecodeArgs(&indexNames, &ifExists); err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	return tblInfo, indexNames, ifExists, nil
}

func checkDropIndexes(tblInfo *model.TableInfo, job *model.Job, indexNames []model.CIStr, ifExists []bool) ([]*model.IndexInfo, error) {
	var warnings []*errors.Error
	indexInfos := make([]*model.IndexInfo, 0, len(indexNames))
	UniqueIndexNames := make(map[model.CIStr]bool, len(indexNames))
	for i, indexName := range indexNames {
		// Double check the index is exists.
		indexInfo := tblInfo.FindIndexByName(indexName.L)
		if indexInfo == nil {
			if ifExists[i] {
				warnings = append(warnings, toTError(dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)))
				continue
			}
			job.State = model.JobStateCancelled
			return nil, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
		}

		// Double check for drop index on auto_increment column.
		if err := checkDropIndexOnAutoIncrementColumn(tblInfo, indexInfo); err != nil {
			job.State = model.JobStateCancelled
			return nil, autoid.ErrWrongAutoKey
		}

		// Check for dropping duplicate indexes.
		if UniqueIndexNames[indexName] {
			if !ifExists[i] {
				job.State = model.JobStateCancelled
				return nil, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
			}
			warnings = append(warnings, toTError(dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)))
		}
		UniqueIndexNames[indexName] = true

		indexInfos = append(indexInfos, indexInfo)
	}

	// Check that drop primary index will not cause invisible implicit primary index.
	if err := checkInvisibleIndexesOnPK(tblInfo, indexInfos, job); err != nil {
		return nil, errors.Trace(err)
	}

	job.MultiSchemaInfo = &model.MultiSchemaInfo{Warnings: warnings}

	return indexInfos, nil
}

func checkInvisibleIndexesOnPK(tblInfo *model.TableInfo, indexInfos []*model.IndexInfo, job *model.Job) error {
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, oidx := range tblInfo.Indices {
		needAppend := true
		for _, idx := range indexInfos {
			if idx.Name.L == oidx.Name.L {
				needAppend = false
				break
			}
		}
		if needAppend {
			newIndices = append(newIndices, oidx)
		}
	}
	newTbl := tblInfo.Clone()
	newTbl.Indices = newIndices
	if err := checkInvisibleIndexOnPK(newTbl); err != nil {
		job.State = model.JobStateCancelled
		return err
	}

	return nil
}

func checkDropIndexOnAutoIncrementColumn(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) error {
	cols := tblInfo.Columns
	for _, idxCol := range indexInfo.Columns {
		flag := cols[idxCol.Offset].GetFlag()
		if !mysql.HasAutoIncrementFlag(flag) {
			continue
		}
		// check the count of index on auto_increment column.
		count := 0
		for _, idx := range tblInfo.Indices {
			for _, c := range idx.Columns {
				if c.Name.L == idxCol.Name.L {
					count++
					break
				}
			}
		}
		if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(flag) {
			count++
		}
		if count < 2 {
			return autoid.ErrWrongAutoKey
		}
	}
	return nil
}

func checkRenameIndex(t *meta.Meta, job *model.Job) (*model.TableInfo, model.CIStr, model.CIStr, error) {
	var from, to model.CIStr
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, from, to, errors.Trace(err)
	}

	if err := job.DecodeArgs(&from, &to); err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}

	// Double check. See function `RenameIndex` in ddl_api.go
	duplicate, err := validateRenameIndex(from, to, tblInfo)
	if duplicate {
		return nil, from, to, nil
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	return tblInfo, from, to, errors.Trace(err)
}

func checkAlterIndexVisibility(t *meta.Meta, job *model.Job) (*model.TableInfo, model.CIStr, bool, error) {
	var (
		indexName model.CIStr
		invisible bool
	)

	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, indexName, invisible, errors.Trace(err)
	}

	if err := job.DecodeArgs(&indexName, &invisible); err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}

	skip, err := validateAlterIndexVisibility(indexName, invisible, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	if skip {
		return nil, indexName, invisible, nil
	}
	return tblInfo, indexName, invisible, nil
}

// indexRecord is the record information of an index.
type indexRecord struct {
	handle kv.Handle
	key    []byte        // It's used to lock a record. Record it to reduce the encoding time.
	vals   []types.Datum // It's the index values.
	rsData []types.Datum // It's the restored data for handle.
	skip   bool          // skip indicates that the index key is already exists, we should not add it.
}

type baseIndexWorker struct {
	*backfillWorker
	indexes []table.Index

	metricCounter prometheus.Counter

	// The following attributes are used to reduce memory allocation.
	defaultVals []types.Datum
	idxRecords  []*indexRecord
	rowMap      map[int64]types.Datum
	rowDecoder  *decoder.RowDecoder

	sqlMode mysql.SQLMode
}

type addIndexWorker struct {
	baseIndexWorker
	index table.Index

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
}

func newAddIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column, sqlMode mysql.SQLMode) *addIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	return &addIndexWorker{
		baseIndexWorker: baseIndexWorker{
			backfillWorker: newBackfillWorker(sessCtx, worker, id, t),
			indexes:        []table.Index{index},
			rowDecoder:     rowDecoder,
			defaultVals:    make([]types.Datum, len(t.WritableCols())),
			rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
			metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("add_idx_rate"),
			sqlMode:        sqlMode,
		},
		index: index,
	}
}

func (w *baseIndexWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

// mockNotOwnerErrOnce uses to make sure `notOwnerErr` only mock error once.
var mockNotOwnerErrOnce uint32

// getIndexRecord gets index columns values use w.rowDecoder, and generate indexRecord.
func (w *baseIndexWorker) getIndexRecord(idxInfo *model.IndexInfo, handle kv.Handle, recordKey []byte) (*indexRecord, error) {
	cols := w.table.WritableCols()
	failpoint.Inject("MockGetIndexRecordErr", func(val failpoint.Value) {
		if valStr, ok := val.(string); ok {
			switch valStr {
			case "cantDecodeRecordErr":
				failpoint.Return(nil, errors.Trace(dbterror.ErrCantDecodeRecord.GenWithStackByArgs("index",
					errors.New("mock can't decode record error"))))
			case "modifyColumnNotOwnerErr":
				if idxInfo.Name.O == "_Idx$_idx" && handle.IntValue() == 7168 && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 0, 1) {
					failpoint.Return(nil, errors.Trace(dbterror.ErrNotOwner))
				}
			case "addIdxNotOwnerErr":
				// For the case of the old TiDB version(do not exist the element information) is upgraded to the new TiDB version.
				// First step, we need to exit "addPhysicalTableIndex".
				if idxInfo.Name.O == "idx2" && handle.IntValue() == 6144 && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 1, 2) {
					failpoint.Return(nil, errors.Trace(dbterror.ErrNotOwner))
				}
			}
		}
	})
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	var err error
	for j, v := range idxInfo.Columns {
		col := cols[v.Offset]
		idxColumnVal, ok := w.rowMap[col.ID]
		if ok {
			idxVal[j] = idxColumnVal
			continue
		}
		idxColumnVal, err = tables.GetColDefaultValue(w.sessCtx, col, w.defaultVals)
		if err != nil {
			return nil, errors.Trace(err)
		}

		idxVal[j] = idxColumnVal
	}

	rsData := tables.TryGetHandleRestoredDataWrapper(w.table, nil, w.rowMap, idxInfo)
	idxRecord := &indexRecord{handle: handle, key: recordKey, vals: idxVal, rsData: rsData}
	return idxRecord, nil
}

func (w *baseIndexWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

// getNextKey gets next key of entry that we are going to process.
func (w *baseIndexWorker) getNextKey(taskRange reorgBackfillTask, taskDone bool) (nextKey kv.Key) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		lastHandle := w.idxRecords[len(w.idxRecords)-1].handle
		recordKey := tablecodec.EncodeRecordKey(w.table.RecordPrefix(), lastHandle)
		return recordKey.Next()
	}
	return taskRange.endKey.Next()
}

func (w *baseIndexWorker) updateRowDecoder(handle kv.Handle, rawRecord []byte) error {
	sysZone := w.sessCtx.GetSessionVars().StmtCtx.TimeZone
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.sessCtx, handle, rawRecord, sysZone, w.rowMap)
	if err != nil {
		return errors.Trace(dbterror.ErrCantDecodeRecord.GenWithStackByArgs("index", err))
	}
	return nil
}

// fetchRowColVals fetch w.batchCnt count records that need to reorganize indices, and build the corresponding indexRecord slice.
// fetchRowColVals returns:
// 1. The corresponding indexRecord slice.
// 2. Next handle of entry that we need to process.
// 3. Boolean indicates whether the task is done.
// 4. error occurs in fetchRowColVals. nil if no error occurs.
func (w *baseIndexWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*indexRecord, kv.Key, bool, error) {
	// TODO: use tableScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the reorged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotRows(w.ddlWorker.JobContext, w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startKey, taskRange.endKey,
		func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in baseIndexWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) > 0

			if taskDone || len(w.idxRecords) >= w.batchCnt {
				return false, nil
			}

			// Decode one row, generate records of this row.
			err := w.updateRowDecoder(handle, rawRow)
			if err != nil {
				return false, err
			}
			for _, index := range w.indexes {
				idxRecord, err1 := w.getIndexRecord(index.Meta(), handle, recordKey)
				if err1 != nil {
					return false, errors.Trace(err1)
				}
				w.idxRecords = append(w.idxRecords, idxRecord)
			}
			// If there are generated column, rowDecoder will use column value that not in idxInfo.Columns to calculate
			// the generated value, so we need to clear up the reusing map.
			w.cleanRowMap()

			if recordKey.Cmp(taskRange.endKey) == 0 {
				// If taskRange.endIncluded == false, we will not reach here when handle == taskRange.endHandle
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.idxRecords) == 0 {
		taskDone = true
	}

	logutil.BgLogger().Debug("[ddl] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextKey(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
}

func (w *addIndexWorker) checkHandleExists(key kv.Key, value []byte, handle kv.Handle) error {
	idxInfo := w.index.Meta()
	tblInfo := w.table.Meta()
	idxColLen := len(idxInfo.Columns)
	h, err := tablecodec.DecodeIndexHandle(key, value, idxColLen)
	if err != nil {
		return errors.Trace(err)
	}
	hasBeenBackFilled := h.Equal(handle)
	if hasBeenBackFilled {
		return nil
	}
	colInfos := tables.BuildRowcodecColInfoForIndexColumns(idxInfo, tblInfo)
	values, err := tablecodec.DecodeIndexKV(key, value, idxColLen, tablecodec.HandleNotNeeded, colInfos)
	if err != nil {
		return err
	}
	indexName := w.index.Meta().Name.String()
	valueStr := make([]string, 0, idxColLen)
	for i, val := range values[:idxColLen] {
		d, err := tablecodec.DecodeColumnValue(val, colInfos[i].Ft, time.Local)
		if err != nil {
			return kv.ErrKeyExists.FastGenByArgs(key.String(), indexName)
		}
		str, err := d.ToString()
		if err != nil {
			str = string(val)
		}
		valueStr = append(valueStr, str)
	}
	return kv.ErrKeyExists.FastGenByArgs(strings.Join(valueStr, "-"), indexName)
}

func (w *addIndexWorker) batchCheckUniqueKey(txn kv.Transaction, idxRecords []*indexRecord) error {
	idxInfo := w.index.Meta()
	if !idxInfo.Unique {
		// non-unique key need not to check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	w.initBatchCheckBufs(len(idxRecords))
	stmtCtx := w.sessCtx.GetSessionVars().StmtCtx
	for i, record := range idxRecords {
		idxKey, distinct, err := w.index.GenIndexKey(stmtCtx, record.vals, record.handle, w.idxKeyBufs[i])
		if err != nil {
			return errors.Trace(err)
		}
		// save the buffer to reduce memory allocations.
		w.idxKeyBufs[i] = idxKey

		w.batchCheckKeys = append(w.batchCheckKeys, idxKey)
		w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
	}

	batchVals, err := txn.BatchGet(context.Background(), w.batchCheckKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key/primary-key is duplicate and the handle is equal, skip it.
	// 2. unique-key/primary-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if val, found := batchVals[string(key)]; found {
			if w.distinctCheckFlags[i] {
				if err := w.checkHandleExists(key, val, idxRecords[i].handle); err != nil {
					return errors.Trace(err)
				}
			}
			idxRecords[i].skip = true
		} else if w.distinctCheckFlags[i] {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			needRsData := tables.NeedRestoredData(w.index.Meta().Columns, w.table.Meta().Columns)
			val, err := tablecodec.GenIndexValuePortal(stmtCtx, w.table.Meta(), w.index.Meta(), needRsData, w.distinctCheckFlags[i], false, idxRecords[i].vals, idxRecords[i].handle, 0, idxRecords[i].rsData)
			if err != nil {
				return errors.Trace(err)
			}
			batchVals[string(key)] = val
		}
	}
	// Constrains is already checked.
	stmtCtx.BatchCheck = true
	return nil
}

// BackfillDataInTxn will backfill table index in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed,
// Note that index columns values may change, and an index is not allowed to be added, so the txn will rollback and retry.
// BackfillDataInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
func (w *addIndexWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(context.Background(), w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.ddlWorker.getResourceGroupTaggerForTopSQL(); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckUniqueKey(txn, idxRecords)
		if err != nil {
			return errors.Trace(err)
		}

		for _, idxRecord := range idxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// We need to add this lock to make sure pessimistic transaction can realize this operation.
			// For the normal pessimistic transaction, it's ok. But if async commmit is used, it may lead to inconsistent data and index.
			err := txn.LockKeys(context.Background(), new(kv.LockCtx), idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			// Create the index.
			handle, err := w.index.Create(w.sessCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData, table.WithIgnoreAssertion)
			if err != nil {
				if kv.ErrKeyExists.Equal(err) && idxRecord.handle.Equal(handle) {
					// Index already exists, skip it.
					continue
				}

				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexBackfillDataInTxn", 3000)

	return
}

func (w *worker) addPhysicalTableIndex(t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[ddl] start to add table index", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writePhysicalTableRecord(t, typeAddIndexWorker, indexInfo, nil, nil, reorgInfo)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			err = w.addPhysicalTableIndex(p, idx, reorgInfo)
			if err != nil {
				break
			}
			finish, err = w.updateReorgInfo(tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		err = w.addPhysicalTableIndex(t.(table.PhysicalTable), idx, reorgInfo)
	}
	return errors.Trace(err)
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateReorgInfo(t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	pid, err := findNextPartitionID(reorg.PhysicalTableID, pi.Definitions)
	if err != nil {
		// Fatal error, should not run here.
		logutil.BgLogger().Error("[ddl] find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}

	failpoint.Inject("mockUpdateCachedSafePoint", func(val failpoint.Value) {
		if val.(bool) {
			ts := oracle.GoTimeToTS(time.Now())
			s := reorg.d.store.(tikv.Storage)
			s.UpdateSPCache(ts, time.Now())
			time.Sleep(time.Millisecond * 3)
		}
	})
	currentVer, err := getValidCurrentVersion(reorg.d.store)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getTableRange(w.JobContext, reorg.d, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(start)
	logutil.BgLogger().Info("[ddl] job update reorgInfo",
		zap.Int64("jobID", reorg.Job.ID),
		zap.ByteString("elementType", reorg.currElement.TypeKey),
		zap.Int64("elementID", reorg.currElement.ID),
		zap.Int64("partitionTableID", pid),
		zap.String("startHandle", tryDecodeToHandleString(start)),
		zap.String("endHandle", tryDecodeToHandleString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

// findNextPartitionID finds the next partition ID in the PartitionDefinition array.
// Returns 0 if current partition is already the last one.
func findNextPartitionID(currentPartition int64, defs []model.PartitionDefinition) (int64, error) {
	for i, def := range defs {
		if currentPartition == def.ID {
			if i == len(defs)-1 {
				return 0, nil
			}
			return defs[i+1].ID, nil
		}
	}
	return 0, errors.Errorf("partition id not found %d", currentPartition)
}

func allocateIndexID(tblInfo *model.TableInfo) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

func getIndexInfoByNameAndColumn(oldTableInfo *model.TableInfo, newOne *model.IndexInfo) *model.IndexInfo {
	for _, oldOne := range oldTableInfo.Indices {
		if newOne.Name.L == oldOne.Name.L && indexColumnSliceEqual(newOne.Columns, oldOne.Columns) {
			return oldOne
		}
	}
	return nil
}

func indexColumnSliceEqual(a, b []*model.IndexColumn) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		logutil.BgLogger().Warn("[ddl] admin repair table : index's columns length equal to 0")
		return true
	}
	// Accelerate the compare by eliminate index bound check.
	b = b[:len(a)]
	for i, v := range a {
		if v.Name.L != b[i].Name.L {
			return false
		}
	}
	return true
}

type cleanUpIndexWorker struct {
	baseIndexWorker
}

func newCleanUpIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, sqlMode mysql.SQLMode) *cleanUpIndexWorker {
	indexes := make([]table.Index, 0, len(t.Indices()))
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	for _, index := range t.Indices() {
		if index.Meta().Global {
			indexes = append(indexes, index)
		}
	}
	return &cleanUpIndexWorker{
		baseIndexWorker: baseIndexWorker{
			backfillWorker: newBackfillWorker(sessCtx, worker, id, t),
			indexes:        indexes,
			rowDecoder:     rowDecoder,
			defaultVals:    make([]types.Datum, len(t.WritableCols())),
			rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
			metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("cleanup_idx_rate"),
			sqlMode:        sqlMode,
		},
	}
}

func (w *cleanUpIndexWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(context.Background(), w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.ddlWorker.getResourceGroupTaggerForTopSQL(); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)

		n := len(w.indexes)
		for i, idxRecord := range idxRecords {
			taskCtx.scanCount++
			// we fetch records row by row, so records will belong to
			// index[0], index[1] ... index[n-1], index[0], index[1] ...
			// respectively. So indexes[i%n] is the index of idxRecords[i].
			err := w.indexes[i%n].Delete(w.sessCtx.GetSessionVars().StmtCtx, txn, idxRecord.vals, idxRecord.handle)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "cleanUpIndexBackfillDataInTxn", 3000)

	return
}

// cleanupPhysicalTableIndex handles the drop partition reorganization state for a non-partitioned table or a partition.
func (w *worker) cleanupPhysicalTableIndex(t table.PhysicalTable, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[ddl] start to clean up index", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writePhysicalTableRecord(t, typeCleanUpIndexWorker, nil, nil, nil, reorgInfo)
}

// cleanupGlobalIndex handles the drop partition reorganization state to clean up index entries of partitions.
func (w *worker) cleanupGlobalIndexes(tbl table.PartitionedTable, partitionIDs []int64, reorgInfo *reorgInfo) error {
	var err error
	var finish bool
	for !finish {
		p := tbl.GetPartition(reorgInfo.PhysicalTableID)
		if p == nil {
			return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, tbl.Meta().ID)
		}
		err = w.cleanupPhysicalTableIndex(p, reorgInfo)
		if err != nil {
			break
		}
		finish, err = w.updateReorgInfoForPartitions(tbl, reorgInfo, partitionIDs)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(err)
}

// updateReorgInfoForPartitions will find the next partition in partitionIDs according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateReorgInfoForPartitions(t table.PartitionedTable, reorg *reorgInfo, partitionIDs []int64) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	var pid int64
	for i, pi := range partitionIDs {
		if pi == reorg.PhysicalTableID {
			if i == len(partitionIDs)-1 {
				return true, nil
			}
		}
		pid = partitionIDs[i+1]
	}

	currentVer, err := getValidCurrentVersion(reorg.d.store)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getTableRange(w.JobContext, reorg.d, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey)
	logutil.BgLogger().Info("[ddl] job update reorgInfo", zap.Int64("jobID", reorg.Job.ID),
		zap.ByteString("elementType", reorg.currElement.TypeKey), zap.Int64("elementID", reorg.currElement.ID),
		zap.Int64("partitionTableID", pid), zap.String("startHandle", tryDecodeToHandleString(start)),
		zap.String("endHandle", tryDecodeToHandleString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

func findIndexesByColName(indexes []*model.IndexInfo, colName string) ([]*model.IndexInfo, []int) {
	idxInfos := make([]*model.IndexInfo, 0, len(indexes))
	offsets := make([]int, 0, len(indexes))
	for _, idxInfo := range indexes {
		for i, c := range idxInfo.Columns {
			if strings.EqualFold(colName, c.Name.L) {
				idxInfos = append(idxInfos, idxInfo)
				offsets = append(offsets, i)
				break
			}
		}
	}
	return idxInfos, offsets
}

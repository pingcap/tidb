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
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/pingcap/tidb/util/timeutil"
	"github.com/prometheus/client_golang/prometheus"
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
			return nil, errKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ip.Column.Name)
		}

		if err := checkIndexColumn(col, ip); err != nil {
			return nil, err
		}

		indexColumnLength, err := getIndexColumnLength(col, ip.Length)
		if err != nil {
			return nil, err
		}
		sumLength += indexColumnLength

		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > config.GetGlobalConfig().MaxIndexLength {
			return nil, errTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
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
			return nil, errKeyColumnDoesNotExits.GenWithStackByArgs(colName.Column.Name)
		}
		// Virtual columns cannot be used in primary key.
		if lastCol.IsGenerated() && !lastCol.GeneratedStored {
			return nil, ErrUnsupportedOnGeneratedColumn.GenWithStackByArgs("Defining a virtual generated column as primary key")
		}
	}

	return lastCol, nil
}

func checkIndexPrefixLength(columns []*model.ColumnInfo, idxColumns []*model.IndexColumn) error {
	// The sum of length of all index columns.
	sumLength := 0
	for _, ic := range idxColumns {
		col := model.FindColumnInfo(columns, ic.Name.L)
		if col == nil {
			return errKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		indexColumnLength, err := getIndexColumnLength(col, ic.Length)
		if err != nil {
			return err
		}
		sumLength += indexColumnLength
		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > config.GetGlobalConfig().MaxIndexLength {
			return errTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
		}
	}
	return nil
}

func checkIndexColumn(col *model.ColumnInfo, ic *ast.IndexPartSpecification) error {
	if col.Flen == 0 && (types.IsTypeChar(col.FieldType.Tp) || types.IsTypeVarchar(col.FieldType.Tp)) {
		return errors.Trace(errWrongKeyColumn.GenWithStackByArgs(ic.Column.Name))
	}

	// JSON column cannot index.
	if col.FieldType.Tp == mysql.TypeJSON {
		return errors.Trace(errJSONUsedAsKey.GenWithStackByArgs(col.Name.O))
	}

	// Length must be specified and non-zero for BLOB and TEXT column indexes.
	if types.IsTypeBlob(col.FieldType.Tp) {
		if ic.Length == types.UnspecifiedLength {
			return errors.Trace(errBlobKeyWithoutLength.GenWithStackByArgs(col.Name.O))
		}
		if ic.Length == types.ErrorLength {
			return errors.Trace(errKeyPart0.GenWithStackByArgs(col.Name.O))
		}
	}

	// Length can only be specified for specifiable types.
	if ic.Length != types.UnspecifiedLength && !types.IsTypePrefixable(col.FieldType.Tp) {
		return errors.Trace(errIncorrectPrefixKey)
	}

	// Key length must be shorter or equal to the column length.
	if ic.Length != types.UnspecifiedLength &&
		types.IsTypeChar(col.FieldType.Tp) {
		if col.Flen < ic.Length {
			return errors.Trace(errIncorrectPrefixKey)
		}
		// Length must be non-zero for char.
		if ic.Length == types.ErrorLength {
			return errors.Trace(errKeyPart0.GenWithStackByArgs(col.Name.O))
		}
	}

	// Specified length must be shorter than the max length for prefix.
	if ic.Length > config.GetGlobalConfig().MaxIndexLength {
		return errTooLongKey.GenWithStackByArgs(config.GetGlobalConfig().MaxIndexLength)
	}
	return nil
}

// getIndexColumnLength calculate the bytes number required in an index column.
func getIndexColumnLength(col *model.ColumnInfo, colLen int) (int, error) {
	length := types.UnspecifiedLength
	if colLen != types.UnspecifiedLength {
		length = colLen
	} else if col.Flen != types.UnspecifiedLength {
		length = col.Flen
	}

	switch col.Tp {
	case mysql.TypeBit:
		return (length + 7) >> 3, nil
	case mysql.TypeVarchar, mysql.TypeString:
		// Different charsets occupy different numbers of bytes on each character.
		desc, err := charset.GetCharsetDesc(col.Charset)
		if err != nil {
			return 0, errUnsupportedCharset.GenWithStackByArgs(col.Charset, col.Collate)
		}
		return desc.Maxlen * length, nil
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return length, nil
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeShort:
		return mysql.DefaultLengthOfMysqlTypes[col.Tp], nil
	case mysql.TypeFloat:
		if length <= mysql.MaxFloatPrecisionLength {
			return mysql.DefaultLengthOfMysqlTypes[mysql.TypeFloat], nil
		}
		return mysql.DefaultLengthOfMysqlTypes[mysql.TypeDouble], nil
	case mysql.TypeNewDecimal:
		return calcBytesLengthForDecimal(length), nil
	case mysql.TypeYear, mysql.TypeDate, mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeTimestamp:
		return mysql.DefaultLengthOfMysqlTypes[col.Tp], nil
	default:
		return length, nil
	}
}

// Decimal using a binary format that packs nine decimal (base 10) digits into four bytes.
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
			tblInfo.Columns[col.Offset].Flag |= mysql.PriKeyFlag
		}
		return
	}

	col := indexInfo.Columns[0]
	if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[col.Offset].Flag |= mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[col.Offset].Flag |= mysql.MultipleKeyFlag
	}
}

func dropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
	if indexInfo.Primary {
		for _, col := range indexInfo.Columns {
			tblInfo.Columns[col.Offset].Flag &= ^mysql.PriKeyFlag
		}
	} else if indexInfo.Unique && len(indexInfo.Columns) == 1 {
		tblInfo.Columns[indexInfo.Columns[0].Offset].Flag &= ^mysql.UniqueKeyFlag
	} else {
		tblInfo.Columns[indexInfo.Columns[0].Offset].Flag &= ^mysql.MultipleKeyFlag
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

func onRenameIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, from, to, err := checkRenameIndex(t, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}

	idx := tblInfo.FindIndexByName(from.L)
	idx.Name = to
	if ver, err = updateVersionAndTableInfo(t, job, tblInfo, true); err != nil {
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

func onAlterIndexVisibility(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, from, invisible, err := checkAlterIndexVisibility(t, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}
	idx := tblInfo.FindIndexByName(from.L)
	idx.Invisible = invisible
	if ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, true); err != nil {
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
		if !mysql.HasNotNullFlag(col.Flag) || mysql.HasPreventNullInsertFlag(col.Flag) {
			nullCols = append(nullCols, col)
		}
	}
	return nullCols, nil
}

func checkPrimaryKeyNotNull(w *worker, sqlMode mysql.SQLMode, t *meta.Meta, job *model.Job,
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

	err = modifyColsFromNull2NotNull(w, dbInfo, tblInfo, nullCols, model.NewCIStr(""), false)
	if err == nil {
		return nil, nil
	}
	_, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
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
		ver, err = onDropIndex(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
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
		err = ErrDupKeyName.GenWithStack("index already exist %s", indexName)
		if isPK {
			err = infoschema.ErrMultiplePriKey
		}
		return ver, err
	}
	for _, hiddenCol := range hiddenCols {
		columnInfo := model.FindColumnInfo(tblInfo.Columns, hiddenCol.Name.L)
		if columnInfo != nil && columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			job.State = model.JobStateCancelled
			// TODO: refine the error message
			return ver, infoschema.ErrColumnExists.GenWithStackByArgs(hiddenCol.Name)
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
		updateHiddenColumns(tblInfo, indexInfo, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
		metrics.AddIndexProgress.Set(0)
	case model.StateDeleteOnly:
		// delete only -> write only
		indexInfo.State = model.StateWriteOnly
		updateHiddenColumns(tblInfo, indexInfo, model.StateWriteOnly)
		_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		indexInfo.State = model.StateWriteReorganization
		updateHiddenColumns(tblInfo, indexInfo, model.StateWriteReorganization)
		_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		updateHiddenColumns(tblInfo, indexInfo, model.StatePublic)
		tbl, err := getTable(d.store, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		reorgInfo, err := getReorgInfo(d, t, job, tbl)
		if err != nil || reorgInfo.first {
			// If we run reorg firstly, we should update the job snapshot version
			// and then run the reorg next time.
			return ver, errors.Trace(err)
		}

		err = w.runReorgJob(t, reorgInfo, tbl.Meta(), d.lease, func() (addIndexErr error) {
			defer util.Recover(metrics.LabelDDL, "onCreateIndex",
				func() {
					addIndexErr = errCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tblInfo.Name, indexInfo.Name)
				}, false)
			return w.addTableIndex(tbl, indexInfo, reorgInfo)
		})
		if err != nil {
			if errWaitReorgTimeout.Equal(err) {
				// if timeout, we should return, check for the owner and re-wait job done.
				return ver, nil
			}
			if kv.ErrKeyExists.Equal(err) || errCancelledDDLJob.Equal(err) || errCantDecodeRecord.Equal(err) {
				logutil.BgLogger().Warn("[ddl] run add index job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
				ver, err = convertAddIdxJob2RollbackJob(t, job, tblInfo, indexInfo, err)
			}
			// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
			w.reorgCtx.cleanNotifyReorgCancel()
			return ver, errors.Trace(err)
		}
		// Clean up the channel of notifyCancelReorgJob. Make sure it can't affect other jobs.
		w.reorgCtx.cleanNotifyReorgCancel()

		indexInfo.State = model.StatePublic
		// Set column index flag.
		addIndexColumnFlag(tblInfo, indexInfo)
		if isPK {
			if err = updateColsNull2NotNull(tblInfo, indexInfo); err != nil {
				return ver, errors.Trace(err)
			}
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("index", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

func onDropIndex(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, indexInfo, err := checkDropIndex(t, job)
	if err != nil {
		return ver, errors.Trace(err)
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
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		if len(dependentHiddenCols) > 0 {
			firstHiddenOffset := dependentHiddenCols[0].Offset
			for i := 0; i < len(dependentHiddenCols); i++ {
				tblInfo.Columns[firstHiddenOffset].State = model.StateWriteOnly
				// Set this column's offset to the last and reset all following columns' offsets.
				adjustColumnInfoInDropColumn(tblInfo, firstHiddenOffset)
			}
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		updateHiddenColumns(tblInfo, indexInfo, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		indexInfo.State = model.StateDeleteReorganization
		updateHiddenColumns(tblInfo, indexInfo, model.StateDeleteReorganization)
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateDeleteReorganization:
		// reorganization -> absent
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

		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != model.StateNone)
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
		err = ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
	}
	return ver, errors.Trace(err)
}

func checkDropIndex(t *meta.Meta, job *model.Job) (*model.TableInfo, *model.IndexInfo, error) {
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
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
		return nil, nil, ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	// Double check for drop index on auto_increment column.
	err = checkDropIndexOnAutoIncrementColumn(tblInfo, indexInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, autoid.ErrWrongAutoKey
	}

	// Check that drop primary index will not cause invisible implicit primary index.
	newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
	for _, idx := range tblInfo.Indices {
		if idx.Name.L != indexInfo.Name.L {
			newIndices = append(newIndices, idx)
		}
	}
	newTbl := tblInfo.Clone()
	newTbl.Indices = newIndices
	err = checkInvisibleIndexOnPK(newTbl)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, errors.Trace(err)
	}

	return tblInfo, indexInfo, nil
}

func checkDropIndexOnAutoIncrementColumn(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) error {
	cols := tblInfo.Columns
	for _, idxCol := range indexInfo.Columns {
		flag := cols[idxCol.Offset].Flag
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
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
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
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
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
	skip   bool          // skip indicates that the index key is already exists, we should not add it.
}

type addIndexWorker struct {
	*backfillWorker
	index         table.Index
	metricCounter prometheus.Counter

	// The following attributes are used to reduce memory allocation.
	defaultVals        []types.Datum
	idxRecords         []*indexRecord
	rowMap             map[int64]types.Datum
	rowDecoder         *decoder.RowDecoder
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
}

func newAddIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column) *addIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	return &addIndexWorker{
		backfillWorker: newBackfillWorker(sessCtx, worker, id, t),
		index:          index,
		metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("add_idx_speed"),
		rowDecoder:     rowDecoder,
		defaultVals:    make([]types.Datum, len(t.WritableCols())),
		rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
	}
}

func (w *addIndexWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

// getIndexRecord gets index columns values from raw binary value row.
func (w *addIndexWorker) getIndexRecord(handle kv.Handle, recordKey []byte, rawRecord []byte) (*indexRecord, error) {
	t := w.table
	cols := t.WritableCols()
	idxInfo := w.index.Meta()
	sysZone := timeutil.SystemLocation()
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.sessCtx, handle, rawRecord, time.UTC, sysZone, w.rowMap)
	if err != nil {
		return nil, errors.Trace(errCantDecodeRecord.GenWithStackByArgs("index", err))
	}
	idxVal := make([]types.Datum, len(idxInfo.Columns))
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

		if idxColumnVal.Kind() == types.KindMysqlTime {
			t := idxColumnVal.GetMysqlTime()
			if t.Type() == mysql.TypeTimestamp && sysZone != time.UTC {
				err := t.ConvertTimeZone(sysZone, time.UTC)
				if err != nil {
					return nil, errors.Trace(err)
				}
				idxColumnVal.SetMysqlTime(t)
			}
		}
		idxVal[j] = idxColumnVal
	}
	// If there are generated column, rowDecoder will use column value that not in idxInfo.Columns to calculate
	// the generated value, so we need to clear up the reusing map.
	w.cleanRowMap()
	idxRecord := &indexRecord{handle: handle, key: recordKey, vals: idxVal}
	return idxRecord, nil
}

func (w *addIndexWorker) cleanRowMap() {
	for id := range w.rowMap {
		delete(w.rowMap, id)
	}
}

// getNextHandle gets next handle of entry that we are going to process.
func (w *addIndexWorker) getNextHandle(taskRange reorgBackfillTask, taskDone bool) (nextHandle kv.Handle) {
	if !taskDone {
		// The task is not done. So we need to pick the last processed entry's handle and add one.
		return w.idxRecords[len(w.idxRecords)-1].handle.Next()
	}

	// The task is done. So we need to choose a handle outside this range.
	// Some corner cases should be considered:
	// - The end of task range is MaxInt64.
	// - The end of the task is excluded in the range.
	if (taskRange.endHandle.IsInt() && taskRange.endHandle.IntValue() == math.MaxInt64) || !taskRange.endIncluded {
		return taskRange.endHandle
	}

	return taskRange.endHandle.Next()
}

// fetchRowColVals fetch w.batchCnt count rows that need to backfill indices, and build the corresponding indexRecord slice.
// fetchRowColVals returns:
// 1. The corresponding indexRecord slice.
// 2. Next handle of entry that we need to process.
// 3. Boolean indicates whether the task is done.
// 4. error occurs in fetchRowColVals. nil if no error occurs.
func (w *addIndexWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*indexRecord, kv.Handle, bool, error) {
	// TODO: use tableScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startHandle, taskRange.endHandle, taskRange.endIncluded,
		func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in addIndexWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			if !taskRange.endIncluded {
				taskDone = handle.Compare(taskRange.endHandle) >= 0
			} else {
				taskDone = handle.Compare(taskRange.endHandle) > 0
			}

			if taskDone || len(w.idxRecords) >= w.batchCnt {
				return false, nil
			}

			idxRecord, err1 := w.getIndexRecord(handle, recordKey, rawRow)
			if err1 != nil {
				return false, errors.Trace(err1)
			}

			w.idxRecords = append(w.idxRecords, idxRecord)
			if handle.Equal(taskRange.endHandle) {
				// If taskRange.endIncluded == false, we will not reach here when handle == taskRange.endHandle
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.idxRecords) == 0 {
		taskDone = true
	}

	logutil.BgLogger().Debug("[ddl] txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()), zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextHandle(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
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
				handle, err1 := tablecodec.DecodeHandleInUniqueIndexValue(val, w.table.Meta().IsCommonHandle)
				if err1 != nil {
					return errors.Trace(err1)
				}

				if handle != idxRecords[i].handle {
					return errors.Trace(kv.ErrKeyExists)
				}
			}
			idxRecords[i].skip = true
		} else {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			if w.distinctCheckFlags[i] {
				batchVals[string(key)] = tablecodec.EncodeHandleInUniqueIndexValue(idxRecords[i].handle, false)
			}
		}
	}
	// Constrains is already checked.
	stmtCtx.BatchCheck = true
	return nil
}

// BackfillDataInTxn will backfill table index in a transaction, lock corresponding rowKey, if the value of rowKey is changed,
// indicate that index columns values may changed, index is not allowed to be added, so the txn will rollback and retry.
// BackfillDataInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
// TODO: make w.batchCnt can be modified by system variable.
func (w *addIndexWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(w.sessCtx.GetStore(), true, func(txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)

		idxRecords, nextHandle, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextHandle = nextHandle
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

			// Lock the row key to notify us that someone delete or update the row,
			// then we should not backfill the index of it, otherwise the adding index is redundant.
			err := txn.LockKeys(context.Background(), new(kv.LockCtx), idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			// Create the index.
			handle, err := w.index.Create(w.sessCtx, txn.GetUnionStore(), idxRecord.vals, idxRecord.handle)
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
	return w.writePhysicalTableRecord(t.(table.PhysicalTable), typeAddIndexWorker, indexInfo, nil, nil, reorgInfo)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return errCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
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
			// 18 is for the logical time.
			ts := oracle.GetPhysical(time.Now()) << 18
			s := reorg.d.store.(tikv.Storage)
			s.UpdateSPCache(uint64(ts), time.Now())
			time.Sleep(time.Millisecond * 3)
		}
	})
	currentVer, err := getValidCurrentVersion(reorg.d.store)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getTableRange(reorg.d, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = kv.RunInNewTxn(reorg.d.store, true, func(txn kv.Transaction) error {
		return errors.Trace(reorg.UpdateReorgMeta(txn, reorg.StartHandle, reorg.EndHandle, reorg.PhysicalTableID))
	})
	logutil.BgLogger().Info("[ddl] job update reorgInfo", zap.Int64("jobID", reorg.Job.ID),
		zap.Int64("partitionTableID", pid), zap.String("startHandle", toString(start)),
		zap.String("endHandle", toString(end)), zap.Error(err))
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

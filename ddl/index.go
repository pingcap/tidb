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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
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
		indexName               model.CIStr
		indexPartSpecifications []*ast.IndexPartSpecification
		indexOption             *ast.IndexOption
		sqlMode                 mysql.SQLMode
		warnings                []string
		hiddenCols              []*model.ColumnInfo
	)
	if isPK {
		// Notice: sqlMode and warnings is used to support non-strict mode.
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption, &sqlMode, &warnings)
	} else {
		err = job.DecodeArgs(&unique, &indexName, &indexPartSpecifications, &indexOption, &hiddenCols)
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
		job.SchemaState = model.StateDeleteOnly
		indexInfo.State = model.StateDeleteOnly
		updateHiddenColumns(tblInfo, indexInfo, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != indexInfo.State)
		metrics.AddIndexProgress.Set(0)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		indexInfo.State = model.StateWriteOnly
		updateHiddenColumns(tblInfo, indexInfo, model.StateWriteOnly)
		_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		indexInfo.State = model.StateWriteReorganization
		updateHiddenColumns(tblInfo, indexInfo, model.StateWriteReorganization)
		_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		if err != nil {
			break
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfo.State)
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
			if kv.ErrKeyExists.Equal(err) || errCancelledDDLJob.Equal(err) || errCantDecodeIndex.Equal(err) {
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

	return doDropIndices(t, job, tblInfo, []*model.IndexInfo{indexInfo}, "onDropIndex")
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
	id        int
	ddlWorker *worker
	batchCnt  int
	sessCtx   sessionctx.Context
	taskCh    chan *reorgIndexTask
	resultCh  chan *addIndexResult
	index     table.Index
	table     table.Table
	closed    bool
	priority  int

	// The following attributes are used to reduce memory allocation.
	defaultVals        []types.Datum
	idxRecords         []*indexRecord
	rowMap             map[int64]types.Datum
	rowDecoder         *decoder.RowDecoder
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
}

type reorgIndexTask struct {
	physicalTableID int64
	startHandle     kv.Handle
	endHandle       kv.Handle
	// endIncluded indicates whether the range include the endHandle.
	// When the last handle is math.MaxInt64, set endIncluded to true to
	// tell worker backfilling index of endHandle.
	endIncluded bool
}

func (r *reorgIndexTask) String() string {
	rightParenthesis := ")"
	if r.endIncluded {
		rightParenthesis = "]"
	}
	return "physicalTableID" + strconv.FormatInt(r.physicalTableID, 10) + "_" + "[" + r.startHandle.String() + "," + r.endHandle.String() + rightParenthesis
}

type addIndexResult struct {
	addedCount int
	scanCount  int
	nextHandle kv.Handle
	err        error
}

// addIndexTaskContext is the context of the batch adding indices.
// After finishing the batch adding indices, result in addIndexTaskContext will be merged into addIndexResult.
type addIndexTaskContext struct {
	nextHandle kv.Handle
	done       bool
	addedCount int
	scanCount  int
}

// mergeAddIndexCtxToResult merge partial result in taskCtx into result.
func mergeAddIndexCtxToResult(taskCtx *addIndexTaskContext, result *addIndexResult) {
	result.nextHandle = taskCtx.nextHandle
	result.addedCount += taskCtx.addedCount
	result.scanCount += taskCtx.scanCount
}

func newAddIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column) *addIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, decodeColMap)
	return &addIndexWorker{
		id:          id,
		ddlWorker:   worker,
		batchCnt:    int(variable.GetDDLReorgBatchSize()),
		sessCtx:     sessCtx,
		taskCh:      make(chan *reorgIndexTask, 1),
		resultCh:    make(chan *addIndexResult, 1),
		index:       index,
		table:       t,
		rowDecoder:  rowDecoder,
		priority:    kv.PriorityLow,
		defaultVals: make([]types.Datum, len(t.Cols())),
		rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
	}
}

func (w *addIndexWorker) close() {
	if !w.closed {
		w.closed = true
		close(w.taskCh)
	}
}

// getIndexRecord gets index columns values from raw binary value row.
func (w *addIndexWorker) getIndexRecord(handle kv.Handle, recordKey []byte, rawRecord []byte) (*indexRecord, error) {
	t := w.table
	cols := t.Cols()
	idxInfo := w.index.Meta()
	sysZone := timeutil.SystemLocation()
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.sessCtx, handle, rawRecord, time.UTC, sysZone, w.rowMap)
	if err != nil {
		return nil, errors.Trace(errCantDecodeIndex.GenWithStackByArgs(err))
	}
	idxVal := make([]types.Datum, len(idxInfo.Columns))
	for j, v := range idxInfo.Columns {
		col := cols[v.Offset]
		idxColumnVal, ok := w.rowMap[col.ID]
		if ok {
			idxVal[j] = idxColumnVal
			// Make sure there is no dirty data.
			delete(w.rowMap, col.ID)
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
func (w *addIndexWorker) getNextHandle(taskRange reorgIndexTask, taskDone bool) (nextHandle kv.Handle) {
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
func (w *addIndexWorker) fetchRowColVals(txn kv.Transaction, taskRange reorgIndexTask) ([]*indexRecord, kv.Handle, bool, error) {
	// TODO: use tableScan to prune columns.
	w.idxRecords = w.idxRecords[:0]
	startTime := time.Now()

	// taskDone means that the added handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotRows(w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startHandle, taskRange.endHandle, taskRange.endIncluded,
		func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			w.logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotRows in fetchRowColVals", 0)
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

func (w *addIndexWorker) logSlowOperations(elapsed time.Duration, slowMsg string, threshold uint32) {
	if threshold == 0 {
		threshold = atomic.LoadUint32(&variable.DDLSlowOprThreshold)
	}

	if elapsed >= time.Duration(threshold)*time.Millisecond {
		logutil.BgLogger().Info("[ddl] slow operations", zap.Duration("takeTimes", elapsed), zap.String("msg", slowMsg))
	}
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

// backfillIndexInTxn will backfill table index in a transaction, lock corresponding rowKey, if the value of rowKey is changed,
// indicate that index columns values may changed, index is not allowed to be added, so the txn will rollback and retry.
// backfillIndexInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
// TODO: make w.batchCnt can be modified by system variable.
func (w *addIndexWorker) backfillIndexInTxn(handleRange reorgIndexTask) (taskCtx addIndexTaskContext, errInTxn error) {
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
	w.logSlowOperations(time.Since(oprStartTime), "backfillIndexInTxn", 3000)

	return
}

var addIndexSpeedCounter = metrics.AddIndexTotalCounter.WithLabelValues("speed")

// handleBackfillTask backfills range [task.startHandle, task.endHandle) handle's index to table.
func (w *addIndexWorker) handleBackfillTask(d *ddlCtx, task *reorgIndexTask) *addIndexResult {
	handleRange := *task
	result := &addIndexResult{addedCount: 0, nextHandle: handleRange.startHandle, err: nil}
	lastLogCount := 0
	lastLogTime := time.Now()
	startTime := lastLogTime

	for {
		// Give job chance to be canceled, if we not check it here,
		// if there is panic in w.backfillIndexInTxn we will never cancel the job.
		// Because reorgIndexTask may run a long time,
		// we should check whether this ddl job is still runnable.
		err := w.ddlWorker.isReorgRunnable(d)
		if err != nil {
			result.err = err
			return result
		}

		taskCtx, err := w.backfillIndexInTxn(handleRange)
		if err != nil {
			result.err = err
			return result
		}

		addIndexSpeedCounter.Add(float64(taskCtx.addedCount))
		mergeAddIndexCtxToResult(&taskCtx, result)
		w.ddlWorker.reorgCtx.increaseRowCount(int64(taskCtx.addedCount))

		if num := result.scanCount - lastLogCount; num >= 30000 {
			lastLogCount = result.scanCount
			logutil.BgLogger().Info("[ddl] add index worker back fill index", zap.Int("workerID", w.id), zap.Int("addedCount", result.addedCount),
				zap.Int("scanCount", result.scanCount), zap.String("nextHandle", toString(taskCtx.nextHandle)), zap.Float64("speed(rows/s)", float64(num)/time.Since(lastLogTime).Seconds()))
			lastLogTime = time.Now()
		}

		handleRange.startHandle = taskCtx.nextHandle
		if taskCtx.done {
			break
		}
	}
	logutil.BgLogger().Info("[ddl] add index worker finish task", zap.Int("workerID", w.id),
		zap.String("task", task.String()), zap.Int("addedCount", result.addedCount),
		zap.Int("scanCount", result.scanCount), zap.String("nextHandle", toString(result.nextHandle)),
		zap.String("takeTime", time.Since(startTime).String()))
	return result
}

func (w *addIndexWorker) run(d *ddlCtx) {
	logutil.BgLogger().Info("[ddl] add index worker start", zap.Int("workerID", w.id))
	defer func() {
		w.resultCh <- &addIndexResult{err: errReorgPanic}
	}()
	defer util.Recover(metrics.LabelDDL, "addIndexWorker.run", nil, false)
	for {
		task, more := <-w.taskCh
		if !more {
			break
		}

		logutil.BgLogger().Debug("[ddl] add index worker got task", zap.Int("workerID", w.id), zap.String("task", task.String()))
		failpoint.Inject("mockAddIndexErr", func() {
			if w.id == 0 {
				result := &addIndexResult{addedCount: 0, nextHandle: nil, err: errors.Errorf("mock add index error")}
				w.resultCh <- result
				failpoint.Continue()
			}
		})

		// Dynamic change batch size.
		w.batchCnt = int(variable.GetDDLReorgBatchSize())
		result := w.handleBackfillTask(d, task)
		w.resultCh <- result
	}
	logutil.BgLogger().Info("[ddl] add index worker exit", zap.Int("workerID", w.id))
}

func makeupDecodeColMap(sessCtx sessionctx.Context, t table.Table) (map[int64]decoder.Column, error) {
	dbName := model.NewCIStr(sessCtx.GetSessionVars().CurrentDB)
	exprCols, _, err := expression.ColumnInfos2ColumnsAndNames(sessCtx, dbName, t.Meta().Name, t.Meta().Columns, t.Meta())
	if err != nil {
		return nil, err
	}
	mockSchema := expression.NewSchema(exprCols...)

	decodeColMap := decoder.BuildFullDecodeColMap(t, mockSchema)

	return decodeColMap, nil
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up adding index in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(t table.PhysicalTable, store kv.Storage, startHandle, endHandle kv.Handle) ([]kv.KeyRange, error) {
	startRecordKey := t.RecordKey(startHandle)
	endRecordKey := t.RecordKey(endHandle)

	logutil.BgLogger().Info("[ddl] split table range from PD", zap.Int64("physicalTableID", t.GetPhysicalID()),
		zap.String("startHandle", toString(startHandle)), zap.String("endHandle", toString(endHandle)))
	kvRange := kv.KeyRange{StartKey: startRecordKey, EndKey: endRecordKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := tikv.NewBackofferWithVars(context.Background(), maxSleep, nil)
	ranges, err := tikv.SplitRegionRanges(bo, s.GetRegionCache(), []kv.KeyRange{kvRange})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ranges) == 0 {
		return nil, errors.Trace(errInvalidSplitRegionRanges)
	}
	return ranges, nil
}

func decodeHandleRange(keyRange kv.KeyRange) (kv.Handle, kv.Handle, error) {
	startHandle, err := tablecodec.DecodeRowKey(keyRange.StartKey)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	endHandle, err := tablecodec.DecodeRowKey(keyRange.EndKey)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return startHandle, endHandle, nil
}

func closeAddIndexWorkers(workers []*addIndexWorker) {
	for _, worker := range workers {
		worker.close()
	}
}

func (w *worker) waitTaskResults(workers []*addIndexWorker, taskCnt int, totalAddedCount *int64, startHandle kv.Handle) (kv.Handle, int64, error) {
	var (
		addedCount int64
		nextHandle = startHandle
		firstErr   error
	)
	for i := 0; i < taskCnt; i++ {
		worker := workers[i]
		result := <-worker.resultCh
		if firstErr == nil && result.err != nil {
			firstErr = result.err
			// We should wait all working workers exits, any way.
			continue
		}

		if result.err != nil {
			logutil.BgLogger().Warn("[ddl] add index worker failed", zap.Int("workerID", worker.id),
				zap.Error(result.err))
		}

		if firstErr == nil {
			*totalAddedCount += int64(result.addedCount)
			addedCount += int64(result.addedCount)
			nextHandle = result.nextHandle
		}
	}

	return nextHandle, addedCount, errors.Trace(firstErr)
}

// handleReorgTasks sends tasks to workers, and waits for all the running workers to return results,
// there are taskCnt running workers.
func (w *worker) handleReorgTasks(reorgInfo *reorgInfo, totalAddedCount *int64, workers []*addIndexWorker, batchTasks []*reorgIndexTask) error {
	for i, task := range batchTasks {
		workers[i].taskCh <- task
	}

	startHandle := batchTasks[0].startHandle
	taskCnt := len(batchTasks)
	startTime := time.Now()
	nextHandle, taskAddedCount, err := w.waitTaskResults(workers, taskCnt, totalAddedCount, startHandle)
	elapsedTime := time.Since(startTime)
	if err == nil {
		err = w.isReorgRunnable(reorgInfo.d)
	}

	if err != nil {
		// update the reorg handle that has been processed.
		err1 := kv.RunInNewTxn(reorgInfo.d.store, true, func(txn kv.Transaction) error {
			return errors.Trace(reorgInfo.UpdateReorgMeta(txn, nextHandle, reorgInfo.EndHandle, reorgInfo.PhysicalTableID))
		})
		metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblError).Observe(elapsedTime.Seconds())
		logutil.BgLogger().Warn("[ddl] add index worker handle batch tasks failed",
			zap.Int64("totalAddedCount", *totalAddedCount), zap.String("startHandle", toString(startHandle)),
			zap.String("nextHandle", toString(nextHandle)), zap.Int64("batchAddedCount", taskAddedCount),
			zap.String("taskFailedError", err.Error()), zap.String("takeTime", elapsedTime.String()),
			zap.NamedError("updateHandleError", err1))
		return errors.Trace(err)
	}

	// nextHandle will be updated periodically in runReorgJob, so no need to update it here.
	w.reorgCtx.setNextHandle(nextHandle)
	metrics.BatchAddIdxHistogram.WithLabelValues(metrics.LblOK).Observe(elapsedTime.Seconds())
	logutil.BgLogger().Info("[ddl] add index worker handle batch tasks successful", zap.Int64("totalAddedCount", *totalAddedCount), zap.String("startHandle", toString(startHandle)),
		zap.String("nextHandle", toString(nextHandle)), zap.Int64("batchAddedCount", taskAddedCount), zap.String("takeTime", elapsedTime.String()))
	return nil
}

// sendRangeTaskToWorkers sends tasks to workers, and returns remaining kvRanges that is not handled.
func (w *worker) sendRangeTaskToWorkers(t table.Table, workers []*addIndexWorker, reorgInfo *reorgInfo,
	totalAddedCount *int64, kvRanges []kv.KeyRange, globalEndHandle kv.Handle) ([]kv.KeyRange, error) {
	batchTasks := make([]*reorgIndexTask, 0, len(workers))
	physicalTableID := reorgInfo.PhysicalTableID

	// Build reorg indices tasks.
	for _, keyRange := range kvRanges {
		startHandle, endHandle, err := decodeHandleRange(keyRange)
		if err != nil {
			return nil, errors.Trace(err)
		}

		endIncluded := false
		if endHandle.Equal(globalEndHandle) {
			endIncluded = true
		}
		task := &reorgIndexTask{physicalTableID, startHandle, endHandle, endIncluded}
		batchTasks = append(batchTasks, task)

		if len(batchTasks) >= len(workers) {
			break
		}
	}

	if len(batchTasks) == 0 {
		return nil, nil
	}

	// Wait tasks finish.
	err := w.handleReorgTasks(reorgInfo, totalAddedCount, workers, batchTasks)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(batchTasks) < len(kvRanges) {
		// there are kvRanges not handled.
		remains := kvRanges[len(batchTasks):]
		return remains, nil
	}

	return nil, nil
}

var (
	// TestCheckWorkerNumCh use for test adjust add index worker.
	TestCheckWorkerNumCh = make(chan struct{})
	// TestCheckWorkerNumber use for test adjust add index worker.
	TestCheckWorkerNumber = int32(16)
)

func loadDDLReorgVars(w *worker) error {
	// Get sessionctx from context resource pool.
	var ctx sessionctx.Context
	ctx, err := w.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer w.sessPool.put(ctx)
	return ddlutil.LoadDDLReorgVars(ctx)
}

// addPhysicalTableIndex handles the add index reorganization state for a non-partitioned table or a partition.
// For a partitioned table, it should be handled partition by partition.
//
// How to add index in reorganization state?
// Concurrently process the @@tidb_ddl_reorg_worker_cnt tasks. Each task deals with a handle range of the index record.
// The handle range is split from PD regions now. Each worker deal with a region table key range one time.
// Each handle range by estimation, concurrent processing needs to perform after the handle range has been acquired.
// The operation flow is as follows:
//	1. Open numbers of defaultWorkers goroutines.
//	2. Split table key range from PD regions.
//	3. Send tasks to running workers by workers's task channel. Each task deals with a region key ranges.
//	4. Wait all these running tasks finished, then continue to step 3, until all tasks is done.
// The above operations are completed in a transaction.
// Finally, update the concurrent processing of the total number of rows, and store the completed handle value.
func (w *worker) addPhysicalTableIndex(t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) error {
	job := reorgInfo.Job
	logutil.BgLogger().Info("[ddl] start to add table index", zap.String("job", job.String()), zap.String("reorgInfo", reorgInfo.String()))
	totalAddedCount := job.GetRowCount()

	startHandle, endHandle := reorgInfo.StartHandle, reorgInfo.EndHandle
	sessCtx := newContext(reorgInfo.d.store)
	decodeColMap, err := makeupDecodeColMap(sessCtx, t)
	if err != nil {
		return errors.Trace(err)
	}

	if err := w.isReorgRunnable(reorgInfo.d); err != nil {
		return errors.Trace(err)
	}
	if startHandle == nil && endHandle == nil {
		return nil
	}

	// variable.ddlReorgWorkerCounter can be modified by system variable "tidb_ddl_reorg_worker_cnt".
	workerCnt := variable.GetDDLReorgWorkerCounter()
	idxWorkers := make([]*addIndexWorker, 0, workerCnt)
	defer func() {
		closeAddIndexWorkers(idxWorkers)
	}()

	for {
		kvRanges, err := splitTableRanges(t, reorgInfo.d.store, startHandle, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		// For dynamic adjust add index worker number.
		if err := loadDDLReorgVars(w); err != nil {
			logutil.BgLogger().Error("[ddl] load DDL reorganization variable failed", zap.Error(err))
		}
		workerCnt = variable.GetDDLReorgWorkerCounter()
		// If only have 1 range, we can only start 1 worker.
		if len(kvRanges) < int(workerCnt) {
			workerCnt = int32(len(kvRanges))
		}
		// Enlarge the worker size.
		for i := len(idxWorkers); i < int(workerCnt); i++ {
			sessCtx := newContext(reorgInfo.d.store)
			idxWorker := newAddIndexWorker(sessCtx, w, i, t, indexInfo, decodeColMap)
			idxWorker.priority = job.Priority
			idxWorkers = append(idxWorkers, idxWorker)
			go idxWorkers[i].run(reorgInfo.d)
		}
		// Shrink the worker size.
		if len(idxWorkers) > int(workerCnt) {
			workers := idxWorkers[workerCnt:]
			idxWorkers = idxWorkers[:workerCnt]
			closeAddIndexWorkers(workers)
		}

		failpoint.Inject("checkIndexWorkerNum", func(val failpoint.Value) {
			if val.(bool) {
				num := int(atomic.LoadInt32(&TestCheckWorkerNumber))
				if num != 0 {
					if num > len(kvRanges) {
						if len(idxWorkers) != len(kvRanges) {
							failpoint.Return(errors.Errorf("check index worker num error, len kv ranges is: %v, check index worker num is: %v, actual index num is: %v", len(kvRanges), num, len(idxWorkers)))
						}
					} else if num != len(idxWorkers) {
						failpoint.Return(errors.Errorf("check index worker num error, len kv ranges is: %v, check index worker num is: %v, actual index num is: %v", len(kvRanges), num, len(idxWorkers)))
					}
					TestCheckWorkerNumCh <- struct{}{}
				}
			}
		})

		logutil.BgLogger().Info("[ddl] start add index workers to reorg index", zap.Int("workerCnt", len(idxWorkers)),
			zap.Int("regionCnt", len(kvRanges)), zap.String("startHandle", toString(startHandle)), zap.String("endHandle", toString(endHandle)))
		remains, err := w.sendRangeTaskToWorkers(t, idxWorkers, reorgInfo, &totalAddedCount, kvRanges, endHandle)
		if err != nil {
			return errors.Trace(err)
		}

		if len(remains) == 0 {
			break
		}
		startHandle, _, err = decodeHandleRange(remains[0])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
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

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(h kv.Handle, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(store kv.Storage, priority int, t table.Table, version uint64, startHandle kv.Handle, endHandle kv.Handle, endIncluded bool, fn recordIterFunc) error {
	var firstKey kv.Key
	if startHandle == nil {
		firstKey = t.RecordPrefix()
	} else {
		firstKey = t.RecordKey(startHandle)
	}

	var upperBound kv.Key
	if endHandle == nil {
		upperBound = t.RecordPrefix().PrefixNext()
	} else {
		if endIncluded {
			if endHandle.IsInt() && endHandle.IntValue() == math.MaxInt64 {
				upperBound = t.RecordKey(endHandle).PrefixNext()
			} else {
				upperBound = t.RecordKey(endHandle.Next())
			}
		} else {
			upperBound = t.RecordKey(endHandle)
		}
	}

	ver := kv.Version{Ver: version}
	snap, err := store.GetSnapshot(ver)
	snap.SetOption(kv.Priority, priority)
	if err != nil {
		return errors.Trace(err)
	}

	it, err := snap.Iter(firstKey, upperBound)
	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	for it.Valid() {
		if !it.Key().HasPrefix(t.RecordPrefix()) {
			break
		}

		var handle kv.Handle
		handle, err = tablecodec.DecodeRowKey(it.Key())
		if err != nil {
			return errors.Trace(err)
		}
		rk := t.RecordKey(handle)

		more, err := fn(handle, rk, it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}

		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rk))
		if err != nil {
			if kv.ErrNotExist.Equal(err) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
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

func doDropIndices(t *meta.Meta, job *model.Job, tblInfo *model.TableInfo, indexInfos []*model.IndexInfo, callFrom string) (ver int64, err error) {
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
		job.SchemaState = model.StateWriteOnly
		setIndicesState(indexInfos, model.StateWriteOnly)
		if len(dependentHiddenCols) > 0 {
			firstHiddenOffset := dependentHiddenCols[0].Offset
			for i := 0; i < len(dependentHiddenCols); i++ {
				tblInfo.Columns[firstHiddenOffset].State = model.StateWriteOnly
				// Set this column's offset to the last and reset all following columns' offsets.
				adjustColumnInfoInDropColumn(tblInfo, firstHiddenOffset)
			}
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfos[0].State)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		setIndicesState(indexInfos, model.StateDeleteOnly)
		for _, indexInfo := range indexInfos {
			updateHiddenColumns(tblInfo, indexInfo, model.StateDeleteOnly)
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfos[0].State)
	case model.StateDeleteOnly:
		// delete only -> reorganization
		job.SchemaState = model.StateDeleteReorganization
		setIndicesState(indexInfos, model.StateDeleteReorganization)
		for _, indexInfo := range indexInfos {
			updateHiddenColumns(tblInfo, indexInfo, model.StateDeleteReorganization)
		}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != indexInfos[0].State)
	case model.StateDeleteReorganization:
		// reorganization -> absent
		newIndices := make([]*model.IndexInfo, 0, len(tblInfo.Indices))
		for _, idx := range tblInfo.Indices {
			if !indexInfoContains(idx.ID, indexInfos) {
				newIndices = append(newIndices, idx)
			}
		}
		tblInfo.Indices = newIndices
		// Set column index flag.
		for _, indexInfo := range indexInfos {
			dropIndexColumnFlag(tblInfo, indexInfo)
		}

		tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(dependentHiddenCols)]

		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		indexIDs := indexInfosToIDList(indexInfos)
		// Finish this job.
		if job.IsRollingback() {
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			switch callFrom {
			case "onDropIndex":
				// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
				// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
				job.Args[0] = indexIDs[0]
			case "onDropColumn", "onDropColumns":
				job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
			}
		} else {
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			switch callFrom {
			case "onDropIndex":
				job.Args = append(job.Args, indexIDs[0], getPartitionIDs(tblInfo))
			case "onDropColumn", "onDropColumns":
				job.Args = append(job.Args, indexIDs, getPartitionIDs(tblInfo))
			}
		}
	default:
		err = ErrInvalidDDLState.GenWithStackByArgs("index", indexInfos[0].State)
	}
	return ver, errors.Trace(err)
}

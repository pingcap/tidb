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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const (
	// MaxCommentLength is exported for testing.
	MaxCommentLength = 1024
)

var (
	telemetryAddIndexIngestUsage = metrics.TelemetryAddIndexIngestCnt
)

func buildIndexColumns(ctx sessionctx.Context, columns []*model.ColumnInfo, indexPartSpecifications []*ast.IndexPartSpecification) ([]*model.IndexColumn, bool, error) {
	// Build offsets.
	idxParts := make([]*model.IndexColumn, 0, len(indexPartSpecifications))
	var col *model.ColumnInfo
	var mvIndex bool
	maxIndexLength := config.GetGlobalConfig().MaxIndexLength
	// The sum of length of all index columns.
	sumLength := 0
	for _, ip := range indexPartSpecifications {
		col = model.FindColumnInfo(columns, ip.Column.Name.L)
		if col == nil {
			return nil, false, dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ip.Column.Name)
		}

		if err := checkIndexColumn(ctx, col, ip.Length); err != nil {
			return nil, false, err
		}
		if col.FieldType.IsArray() {
			if mvIndex {
				return nil, false, dbterror.ErrNotSupportedYet.GenWithStackByArgs("more than one multi-valued key part per index")
			}
			mvIndex = true
		}
		indexColLen := ip.Length
		indexColumnLength, err := getIndexColumnLength(col, ip.Length)
		if err != nil {
			return nil, false, err
		}
		sumLength += indexColumnLength

		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > maxIndexLength {
			// The multiple column index and the unique index in which the length sum exceeds the maximum size
			// will return an error instead produce a warning.
			if ctx == nil || ctx.GetSessionVars().StrictSQLMode || mysql.HasUniKeyFlag(col.GetFlag()) || len(indexPartSpecifications) > 1 {
				return nil, false, dbterror.ErrTooLongKey.GenWithStackByArgs(sumLength, maxIndexLength)
			}
			// truncate index length and produce warning message in non-restrict sql mode.
			colLenPerUint, err := getIndexColumnLength(col, 1)
			if err != nil {
				return nil, false, err
			}
			indexColLen = maxIndexLength / colLenPerUint
			// produce warning message
			ctx.GetSessionVars().StmtCtx.AppendWarning(dbterror.ErrTooLongKey.FastGenByArgs(sumLength, maxIndexLength))
		}

		idxParts = append(idxParts, &model.IndexColumn{
			Name:   col.Name,
			Offset: col.Offset,
			Length: indexColLen,
		})
	}

	return idxParts, mvIndex, nil
}

// CheckPKOnGeneratedColumn checks the specification of PK is valid.
func CheckPKOnGeneratedColumn(tblInfo *model.TableInfo, indexPartSpecifications []*ast.IndexPartSpecification) (*model.ColumnInfo, error) {
	var lastCol *model.ColumnInfo
	for _, colName := range indexPartSpecifications {
		lastCol = tblInfo.FindPublicColumnByName(colName.Column.Name.L)
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
		return dbterror.ErrTooLongKey.GenWithStackByArgs(idxLen, config.GetGlobalConfig().MaxIndexLength)
	}
	return nil
}

func checkIndexColumn(ctx sessionctx.Context, col *model.ColumnInfo, indexColumnLen int) error {
	if col.GetFlen() == 0 && (types.IsTypeChar(col.FieldType.GetType()) || types.IsTypeVarchar(col.FieldType.GetType())) {
		if col.Hidden {
			return errors.Trace(dbterror.ErrWrongKeyColumnFunctionalIndex.GenWithStackByArgs(col.GeneratedExprString))
		}
		return errors.Trace(dbterror.ErrWrongKeyColumn.GenWithStackByArgs(col.Name))
	}

	// JSON column cannot index.
	if col.FieldType.GetType() == mysql.TypeJSON && !col.FieldType.IsArray() {
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
	maxIndexLength := config.GetGlobalConfig().MaxIndexLength
	if indexColumnLen > maxIndexLength && (ctx == nil || ctx.GetSessionVars().StrictSQLMode) {
		// return error in strict sql mode
		return dbterror.ErrTooLongKey.GenWithStackByArgs(indexColumnLen, maxIndexLength)
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
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
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

// BuildIndexInfo builds a new IndexInfo according to the index information.
func BuildIndexInfo(
	ctx sessionctx.Context,
	allTableColumns []*model.ColumnInfo,
	indexName model.CIStr,
	isPrimary bool,
	isUnique bool,
	isGlobal bool,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
	state model.SchemaState,
) (*model.IndexInfo, error) {
	if err := checkTooLongIndex(indexName); err != nil {
		return nil, errors.Trace(err)
	}

	idxColumns, mvIndex, err := buildIndexColumns(ctx, allTableColumns, indexPartSpecifications)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		Columns: idxColumns,
		State:   state,
		Primary: isPrimary,
		Unique:  isUnique,
		Global:  isGlobal,
		MVIndex: mvIndex,
	}

	if indexOption != nil {
		idxInfo.Comment = indexOption.Comment
		if indexOption.Visibility == ast.IndexVisibilityInvisible {
			idxInfo.Invisible = true
		}
		if indexOption.Tp == model.IndexTypeInvalid {
			// Use btree as default index type.
			idxInfo.Tp = model.IndexTypeBtree
		} else {
			idxInfo.Tp = indexOption.Tp
		}
	} else {
		// Use btree as default index type.
		idxInfo.Tp = model.IndexTypeBtree
	}

	return idxInfo, nil
}

// AddIndexColumnFlag aligns the column flags of columns in TableInfo to IndexInfo.
func AddIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
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

// DropIndexColumnFlag drops the column flag of columns in TableInfo according to the IndexInfo.
func DropIndexColumnFlag(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) {
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

		AddIndexColumnFlag(tblInfo, index)
	}
}

// ValidateRenameIndex checks if index name is ok to be renamed.
func ValidateRenameIndex(from, to model.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
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

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, false)
	}

	renameIndexes(tblInfo, from, to)
	if ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func validateAlterIndexVisibility(ctx sessionctx.Context, indexName model.CIStr, invisible bool, tbl *model.TableInfo) (bool, error) {
	var idx *model.IndexInfo
	if idx = tbl.FindIndexByName(indexName.L); idx == nil || idx.State != model.StatePublic {
		return false, errors.Trace(infoschema.ErrKeyNotExists.GenWithStackByArgs(indexName.O, tbl.Name))
	}
	if ctx == nil || ctx.GetSessionVars() == nil || ctx.GetSessionVars().StmtCtx.MultiSchemaInfo == nil {
		// Early return.
		if idx.Invisible == invisible {
			return true, nil
		}
	}
	return false, nil
}

func onAlterIndexVisibility(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, from, invisible, err := checkAlterIndexVisibility(t, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return updateVersionAndTableInfo(d, t, job, tblInfo, false)
	}

	setIndexVisibility(tblInfo, from, invisible)
	if ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func setIndexVisibility(tblInfo *model.TableInfo, name model.CIStr, invisible bool) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == name.L || (isTempIdxInfo(idx, tblInfo) && getChangingIndexOriginName(idx) == name.O) {
			idx.Invisible = invisible
		}
	}
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

func checkPrimaryKeyNotNull(d *ddlCtx, w *worker, t *meta.Meta, job *model.Job,
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

// moveAndUpdateHiddenColumnsToPublic updates the hidden columns to public, and
// moves the hidden columns to proper offsets, so that Table.Columns' states meet the assumption of
// [public, public, ..., public, non-public, non-public, ..., non-public].
func moveAndUpdateHiddenColumnsToPublic(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	hiddenColOffset := make(map[int]struct{}, 0)
	for _, col := range idxInfo.Columns {
		if tblInfo.Columns[col.Offset].Hidden {
			hiddenColOffset[col.Offset] = struct{}{}
		}
	}
	if len(hiddenColOffset) == 0 {
		return
	}
	// Find the first non-public column.
	firstNonPublicPos := len(tblInfo.Columns) - 1
	for i, c := range tblInfo.Columns {
		if c.State != model.StatePublic {
			firstNonPublicPos = i
			break
		}
	}
	for _, col := range idxInfo.Columns {
		tblInfo.Columns[col.Offset].State = model.StatePublic
		if _, needMove := hiddenColOffset[col.Offset]; needMove {
			tblInfo.MoveColumnInfo(col.Offset, firstNonPublicPos)
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
			for _, hiddenCol := range hiddenCols {
				InitAndAddColumnToTable(tblInfo, hiddenCol)
			}
		}
		if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		indexInfo, err = BuildIndexInfo(
			nil,
			tblInfo.Columns,
			indexName,
			isPK,
			unique,
			global,
			indexPartSpecifications,
			indexOption,
			model.StateNone,
		)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		if isPK {
			if _, err = CheckPKOnGeneratedColumn(tblInfo, indexPartSpecifications); err != nil {
				job.State = model.JobStateCancelled
				return ver, err
			}
		}
		indexInfo.ID = AllocateIndexID(tblInfo)
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
		reorgTp := pickBackfillType(w, job)
		if reorgTp.NeedMergeProcess() {
			// Increase telemetryAddIndexIngestUsage
			telemetryAddIndexIngestUsage.Inc()
			indexInfo.BackfillState = model.BackfillStateRunning
		}
		indexInfo.State = model.StateDeleteOnly
		moveAndUpdateHiddenColumnsToPublic(tblInfo, indexInfo)
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		indexInfo.State = model.StateWriteOnly
		_, err = checkPrimaryKeyNotNull(d, w, t, job, tblInfo, indexInfo)
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
		_, err = checkPrimaryKeyNotNull(d, w, t, job, tblInfo, indexInfo)
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

		if job.MultiSchemaInfo == nil {
			initDistReorg(job.ReorgMeta)
		}
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(d.store, schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		var done bool
		if job.MultiSchemaInfo != nil {
			done, ver, err = doReorgWorkForCreateIndexMultiSchema(w, d, t, job, tbl, indexInfo)
		} else {
			if job.ReorgMeta.IsDistReorg {
				done, ver, err = doReorgWorkForCreateIndexWithDistReorg(w, d, t, job, tbl, indexInfo)
			} else {
				done, ver, err = doReorgWorkForCreateIndex(w, d, t, job, tbl, indexInfo)
			}
		}
		if !done {
			return ver, err
		}

		// Set column index flag.
		AddIndexColumnFlag(tblInfo, indexInfo)
		if isPK {
			if err = UpdateColsNull2NotNull(tblInfo, indexInfo); err != nil {
				return ver, errors.Trace(err)
			}
		}
		indexInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.Args = []interface{}{indexInfo.ID, false /*if exists*/, getPartitionIDs(tbl.Meta())}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		if job.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
			ingest.LitBackCtxMgr.Unregister(job.ID)
		}
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

// pickBackfillType determines which backfill process will be used.
func pickBackfillType(w *worker, job *model.Job) model.ReorgType {
	if job.ReorgMeta.ReorgTp != model.ReorgTypeNone {
		// The backfill task has been started.
		// Don't switch the backfill process.
		return job.ReorgMeta.ReorgTp
	}
	if IsEnableFastReorg() {
		var useIngest bool
		if ingest.LitInitialized {
			useIngest = canUseIngest(w)
			if useIngest {
				job.ReorgMeta.ReorgTp = model.ReorgTypeLitMerge
				return model.ReorgTypeLitMerge
			}
		}
		// The lightning environment is unavailable, but we can still use the txn-merge backfill.
		logutil.BgLogger().Info("[ddl] fallback to txn-merge backfill process",
			zap.Bool("lightning env initialized", ingest.LitInitialized),
			zap.Bool("can use ingest", useIngest))
		job.ReorgMeta.ReorgTp = model.ReorgTypeTxnMerge
		return model.ReorgTypeTxnMerge
	}
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
	return model.ReorgTypeTxn
}

// canUseIngest indicates whether it can use ingest way to backfill index.
func canUseIngest(w *worker) bool {
	// We only allow one task to use ingest at the same time, in order to limit the CPU usage.
	activeJobIDs := ingest.LitBackCtxMgr.Keys()
	if len(activeJobIDs) > 0 {
		logutil.BgLogger().Info("[ddl-ingest] ingest backfill is already in use by another DDL job",
			zap.Int64("job ID", activeJobIDs[0]))
		return false
	}
	ctx, err := w.sessPool.get()
	if err != nil {
		return false
	}
	defer w.sessPool.put(ctx)
	failpoint.Inject("EnablePiTR", func() {
		logutil.BgLogger().Info("lightning: mock enable PiTR")
		failpoint.Return(true)
	})
	// Ingest way is not compatible with PiTR.
	return !utils.IsLogBackupInUse(ctx)
}

// IngestJobsNotExisted checks the ddl about `add index` with ingest method not existed.
func IngestJobsNotExisted(ctx sessionctx.Context) bool {
	sess := session{ctx}
	template := "select job_meta from mysql.tidb_ddl_job where reorg and (type = %d or type = %d) and processing;"
	sql := fmt.Sprintf(template, model.ActionAddIndex, model.ActionAddPrimaryKey)
	rows, err := sess.execute(context.Background(), sql, "check-pitr")
	if err != nil {
		logutil.BgLogger().Warn("cannot check ingest job", zap.Error(err))
		return false
	}
	for _, row := range rows {
		jobBinary := row.GetBytes(0)
		runJob := model.Job{}
		err := runJob.Decode(jobBinary)
		if err != nil {
			logutil.BgLogger().Warn("cannot check ingest job", zap.Error(err))
			return false
		}
		// Check whether this add index job is using lightning to do the backfill work.
		if runJob.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
			return false
		}
	}
	return true
}

// tryFallbackToTxnMerge changes the reorg type to txn-merge if the lightning backfill meets something wrong.
func tryFallbackToTxnMerge(job *model.Job, err error) error {
	if job.State != model.JobStateRollingback {
		logutil.BgLogger().Info("[ddl] fallback to txn-merge backfill process", zap.Error(err))
		job.ReorgMeta.ReorgTp = model.ReorgTypeTxnMerge
		job.SnapshotVer = 0
		job.RowCount = 0
		return nil
	}
	return err
}

func doReorgWorkForCreateIndexMultiSchema(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job,
	tbl table.Table, indexInfo *model.IndexInfo) (done bool, ver int64, err error) {
	if job.MultiSchemaInfo.Revertible {
		done, ver, err = doReorgWorkForCreateIndex(w, d, t, job, tbl, indexInfo)
		if done {
			job.MarkNonRevertible()
			if err == nil {
				ver, err = updateVersionAndTableInfo(d, t, job, tbl.Meta(), true)
			}
		}
		// We need another round to wait for all the others sub-jobs to finish.
		return false, ver, err
	}
	return true, ver, err
}

func doReorgWorkForCreateIndex(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job,
	tbl table.Table, indexInfo *model.IndexInfo) (done bool, ver int64, err error) {
	bfProcess := pickBackfillType(w, job)
	if !bfProcess.NeedMergeProcess() {
		return runReorgJobAndHandleErr(w, d, t, job, tbl, indexInfo, false)
	}
	switch indexInfo.BackfillState {
	case model.BackfillStateRunning:
		logutil.BgLogger().Info("[ddl] index backfill state running",
			zap.Int64("job ID", job.ID), zap.String("table", tbl.Meta().Name.O),
			zap.Bool("ingest mode", bfProcess == model.ReorgTypeLitMerge),
			zap.String("index", indexInfo.Name.O))
		switch bfProcess {
		case model.ReorgTypeLitMerge:
			bc, ok := ingest.LitBackCtxMgr.Load(job.ID)
			if ok && bc.Done() {
				break
			}
			if !ok && job.SnapshotVer != 0 {
				// The owner is crashed or changed, we need to restart the backfill.
				job.SnapshotVer = 0
				job.RowCount = 0
				return false, ver, nil
			}
			bc, err = ingest.LitBackCtxMgr.Register(w.ctx, indexInfo.Unique, job.ID, job.ReorgMeta.SQLMode)
			if err != nil {
				err = tryFallbackToTxnMerge(job, err)
				return false, ver, errors.Trace(err)
			}
			done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, indexInfo, false)
			if err != nil {
				ingest.LitBackCtxMgr.Unregister(job.ID)
				err = tryFallbackToTxnMerge(job, err)
				return false, ver, errors.Trace(err)
			}
			if !done {
				return false, ver, nil
			}
			err = bc.FinishImport(indexInfo.ID, indexInfo.Unique, tbl)
			if err != nil {
				if common.ErrFoundDuplicateKeys.Equal(err) {
					err = convertToKeyExistsErr(err, indexInfo, tbl.Meta())
				}
				if kv.ErrKeyExists.Equal(err) {
					logutil.BgLogger().Warn("[ddl] import index duplicate key, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
					ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), indexInfo, err)
				} else {
					logutil.BgLogger().Warn("[ddl] lightning import error", zap.Error(err))
					err = tryFallbackToTxnMerge(job, err)
				}
				ingest.LitBackCtxMgr.Unregister(job.ID)
				return false, ver, errors.Trace(err)
			}
			bc.SetDone()
		case model.ReorgTypeTxnMerge:
			done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, indexInfo, false)
			if err != nil || !done {
				return false, ver, errors.Trace(err)
			}
		}
		indexInfo.BackfillState = model.BackfillStateReadyToMerge
		ver, err = updateVersionAndTableInfo(d, t, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateReadyToMerge:
		logutil.BgLogger().Info("[ddl] index backfill state ready to merge", zap.Int64("job ID", job.ID),
			zap.String("table", tbl.Meta().Name.O), zap.String("index", indexInfo.Name.O))
		indexInfo.BackfillState = model.BackfillStateMerging
		if bfProcess == model.ReorgTypeLitMerge {
			ingest.LitBackCtxMgr.Unregister(job.ID)
		}
		job.SnapshotVer = 0 // Reset the snapshot version for merge index reorg.
		ver, err = updateVersionAndTableInfo(d, t, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateMerging:
		done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, indexInfo, true)
		if !done {
			return false, ver, err
		}
		indexInfo.BackfillState = model.BackfillStateInapplicable // Prevent double-write on this index.
		return true, ver, err
	default:
		return false, 0, dbterror.ErrInvalidDDLState.GenWithStackByArgs("backfill", indexInfo.BackfillState)
	}
}

func doReorgWorkForCreateIndexWithDistReorg(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job,
	tbl table.Table, indexInfo *model.IndexInfo) (done bool, ver int64, err error) {
	bfProcess := pickBackfillType(w, job)
	if !bfProcess.NeedMergeProcess() {
		return runReorgJobAndHandleErr(w, d, t, job, tbl, indexInfo, false)
	}
	switch indexInfo.BackfillState {
	case model.BackfillStateRunning:
		logutil.BgLogger().Info("[ddl] index backfill state running",
			zap.Int64("job ID", job.ID), zap.String("table", tbl.Meta().Name.O),
			zap.Bool("ingest mode", bfProcess == model.ReorgTypeLitMerge),
			zap.String("index", indexInfo.Name.O))
		switch bfProcess {
		case model.ReorgTypeLitMerge:
			done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, indexInfo, false)
			if err != nil {
				logutil.BgLogger().Warn("[ddl] dist lightning import error", zap.Error(err))
				return false, ver, errors.Trace(err)
			}
			if !done {
				return false, ver, nil
			}
		case model.ReorgTypeTxnMerge:
			done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, indexInfo, false)
			if err != nil || !done {
				return false, ver, errors.Trace(err)
			}
		}
		indexInfo.BackfillState = model.BackfillStateReadyToMerge
		ver, err = updateVersionAndTableInfo(d, t, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateReadyToMerge:
		logutil.BgLogger().Info("[ddl] index backfill state ready to merge", zap.Int64("job ID", job.ID),
			zap.String("table", tbl.Meta().Name.O), zap.String("index", indexInfo.Name.O))
		indexInfo.BackfillState = model.BackfillStateMerging
		job.SnapshotVer = 0 // Reset the snapshot version for merge index reorg.
		ver, err = updateVersionAndTableInfo(d, t, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateMerging:
		done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, indexInfo, true)
		if !done {
			return false, ver, err
		}
		indexInfo.BackfillState = model.BackfillStateInapplicable // Prevent double-write on this index.
		return true, ver, nil
	default:
		return false, 0, dbterror.ErrInvalidDDLState.GenWithStackByArgs("backfill", indexInfo.BackfillState)
	}
}

func convertToKeyExistsErr(originErr error, idxInfo *model.IndexInfo, tblInfo *model.TableInfo) error {
	tErr, ok := errors.Cause(originErr).(*terror.Error)
	if !ok {
		return originErr
	}
	if len(tErr.Args()) != 2 {
		return originErr
	}
	key, keyIsByte := tErr.Args()[0].([]byte)
	value, valIsByte := tErr.Args()[1].([]byte)
	if !keyIsByte || !valIsByte {
		return originErr
	}
	return genKeyExistsErr(key, value, idxInfo, tblInfo)
}

func runReorgJobAndHandleErr(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job,
	tbl table.Table, indexInfo *model.IndexInfo, mergingTmpIdx bool) (done bool, ver int64, err error) {
	elements := []*meta.Element{{ID: indexInfo.ID, TypeKey: meta.IndexElementKey}}

	failpoint.Inject("mockDMLExecutionStateMerging", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && indexInfo.BackfillState == model.BackfillStateMerging &&
			MockDMLExecutionStateMerging != nil {
			MockDMLExecutionStateMerging()
		}
	})

	sctx, err1 := w.sessPool.get()
	if err1 != nil {
		err = err1
		return
	}
	defer w.sessPool.put(sctx)
	rh := newReorgHandler(newSession(sctx))
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(d.jobContext(job.ID), d, rh, job, dbInfo, tbl, elements, mergingTmpIdx)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}
	err = w.runReorgJob(rh, reorgInfo, tbl.Meta(), d.lease, func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onCreateIndex",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tbl.Meta().Name, indexInfo.Name)
			}, false)
		return w.addTableIndex(tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if common.ErrFoundDuplicateKeys.Equal(err) {
			err = convertToKeyExistsErr(err, indexInfo, tbl.Meta())
		}
		if kv.ErrKeyExists.Equal(err) || dbterror.ErrCancelledDDLJob.Equal(err) || dbterror.ErrCantDecodeRecord.Equal(err) ||
			// TODO: Remove this check make it can be retry. Related test is TestModifyColumnReorgInfo.
			job.ReorgMeta.IsDistReorg {
			logutil.BgLogger().Warn("[ddl] run add index job failed, convert job to rollback", zap.String("job", job.String()), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), indexInfo, err)
			if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
				logutil.BgLogger().Warn("[ddl] run add index job failed, convert job to rollback, RemoveDDLReorgHandle failed", zap.String("job", job.String()), zap.Error(err1))
			}
		}
		return false, ver, errors.Trace(err)
	}
	return true, ver, nil
}

func onDropIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, indexInfo, ifExists, err := checkDropIndex(d, t, job)
	if err != nil {
		if ifExists && dbterror.ErrCantDropFieldOrKey.Equal(err) {
			job.Warning = toTError(err)
			job.State = model.JobStateDone
			return ver, nil
		}
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Drop Index"))
	}

	if job.MultiSchemaInfo != nil && !job.IsRollingback() && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		job.SchemaState = indexInfo.State
		return updateVersionAndTableInfo(d, t, job, tblInfo, false)
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
		// Set column index flag.
		DropIndexColumnFlag(tblInfo, indexInfo)
		RemoveDependentHiddenColumns(tblInfo, indexInfo)
		removeIndexInfo(tblInfo, indexInfo)

		failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
			//nolint:forcetypeassert
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
			if job.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
				ingest.LitBackCtxMgr.Unregister(job.ID)
			}
			job.Args[0] = indexInfo.ID
		} else {
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			// Global index key has t{tableID}_ prefix.
			// Assign partitionIDs empty to guarantee correct prefix in insertJobIntoDeleteRangeTable.
			if indexInfo.Global {
				job.Args = append(job.Args, indexInfo.ID, []int64{})
			} else {
				job.Args = append(job.Args, indexInfo.ID, getPartitionIDs(tblInfo))
			}
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State))
	}
	job.SchemaState = indexInfo.State
	return ver, errors.Trace(err)
}

// RemoveDependentHiddenColumns removes hidden columns by the indexInfo.
func RemoveDependentHiddenColumns(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	hiddenColOffs := make([]int, 0)
	for _, indexColumn := range idxInfo.Columns {
		col := tblInfo.Columns[indexColumn.Offset]
		if col.Hidden {
			hiddenColOffs = append(hiddenColOffs, col.Offset)
		}
	}
	// Sort the offset in descending order.
	slices.SortFunc(hiddenColOffs, func(a, b int) bool { return a > b })
	// Move all the dependent hidden columns to the end.
	endOffset := len(tblInfo.Columns) - 1
	for _, offset := range hiddenColOffs {
		tblInfo.MoveColumnInfo(offset, endOffset)
	}
	tblInfo.Columns = tblInfo.Columns[:len(tblInfo.Columns)-len(hiddenColOffs)]
}

func removeIndexInfo(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	indices := tblInfo.Indices
	offset := -1
	for i, idx := range indices {
		if idxInfo.ID == idx.ID {
			offset = i
			break
		}
	}
	if offset == -1 {
		// The target index has been removed.
		return
	}
	// Remove the target index.
	tblInfo.Indices = append(tblInfo.Indices[:offset], tblInfo.Indices[offset+1:]...)
}

func checkDropIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (*model.TableInfo, *model.IndexInfo, bool /* ifExists */, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	var indexName model.CIStr
	var ifExists bool
	if err = job.DecodeArgs(&indexName, &ifExists); err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, false, errors.Trace(err)
	}

	indexInfo := tblInfo.FindIndexByName(indexName.L)
	if indexInfo == nil {
		job.State = model.JobStateCancelled
		return nil, nil, ifExists, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", indexName)
	}

	// Double check for drop index on auto_increment column.
	err = CheckDropIndexOnAutoIncrementColumn(tblInfo, indexInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, false, autoid.ErrWrongAutoKey
	}

	// Check that drop primary index will not cause invisible implicit primary index.
	if err := checkInvisibleIndexesOnPK(tblInfo, []*model.IndexInfo{indexInfo}, job); err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, false, errors.Trace(err)
	}

	// Double check for drop index needed in foreign key.
	if err := checkIndexNeededInForeignKeyInOwner(d, t, job, job.SchemaName, tblInfo, indexInfo); err != nil {
		return nil, nil, false, errors.Trace(err)
	}
	return tblInfo, indexInfo, false, nil
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

// CheckDropIndexOnAutoIncrementColumn checks if the index to drop is on auto_increment column.
func CheckDropIndexOnAutoIncrementColumn(tblInfo *model.TableInfo, indexInfo *model.IndexInfo) error {
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
	duplicate, err := ValidateRenameIndex(from, to, tblInfo)
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

	skip, err := validateAlterIndexVisibility(nil, indexName, invisible, tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	if skip {
		job.State = model.JobStateDone
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
	*backfillCtx
	indexes []table.Index

	tp            backfillerType
	metricCounter prometheus.Counter

	// The following attributes are used to reduce memory allocation.
	defaultVals []types.Datum
	idxRecords  []*indexRecord
	rowMap      map[int64]types.Datum
	rowDecoder  *decoder.RowDecoder

	jobContext *JobContext
}

type addIndexWorker struct {
	baseIndexWorker
	index            table.Index
	writerCtx        *ingest.WriterContext
	copReqSenderPool *copReqSenderPool

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	batchCheckValues   [][]byte
	distinctCheckFlags []bool
}

func newAddIndexWorker(decodeColMap map[int64]decoder.Column, t table.PhysicalTable, bfCtx *backfillCtx, jc *JobContext, jobID, eleID int64, eleTypeKey []byte) (*addIndexWorker, error) {
	if !bytes.Equal(eleTypeKey, meta.IndexElementKey) {
		logutil.BgLogger().Error("Element type for addIndexWorker incorrect", zap.String("jobQuery", jc.cacheSQL),
			zap.Int64("job ID", jobID), zap.ByteString("element type", eleTypeKey), zap.Int64("element ID", eleID))
		return nil, errors.Errorf("element type is not index, typeKey: %v", eleTypeKey)
	}
	indexInfo := model.FindIndexInfoByID(t.Meta().Indices, eleID)
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)

	var lwCtx *ingest.WriterContext
	if bfCtx.reorgTp == model.ReorgTypeLitMerge {
		bc, ok := ingest.LitBackCtxMgr.Load(jobID)
		if !ok {
			return nil, errors.Trace(errors.New(ingest.LitErrGetBackendFail))
		}
		ei, err := bc.EngMgr.Register(bc, jobID, eleID, bfCtx.schemaName, t.Meta().Name.O)
		if err != nil {
			return nil, errors.Trace(err)
		}
		lwCtx, err = ei.NewWriterCtx(bfCtx.id, indexInfo.Unique)
		if err != nil {
			return nil, err
		}
	}

	return &addIndexWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx:   bfCtx,
			indexes:       []table.Index{index},
			rowDecoder:    rowDecoder,
			defaultVals:   make([]types.Datum, len(t.WritableCols())),
			rowMap:        make(map[int64]types.Datum, len(decodeColMap)),
			metricCounter: metrics.BackfillTotalCounter.WithLabelValues(metrics.GenerateReorgLabel("add_idx_rate", bfCtx.schemaName, t.Meta().Name.String())),
			jobContext:    jc,
		},
		index:     index,
		writerCtx: lwCtx,
	}, nil
}

func (w *baseIndexWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (*baseIndexWorker) GetTasks() ([]*BackfillJob, error) {
	return nil, nil
}

func (w *baseIndexWorker) String() string {
	return w.tp.String()
}

func (w *baseIndexWorker) UpdateTask(bfJob *BackfillJob) error {
	s := newSession(w.backfillCtx.sessCtx)

	return s.runInTxn(func(se *session) error {
		jobs, err := GetBackfillJobs(se, BackgroundSubtaskTable, fmt.Sprintf("task_key = '%s'", bfJob.keyString()), "update_backfill_task")
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			return dbterror.ErrDDLJobNotFound.FastGen("get zero backfill job")
		}
		if jobs[0].InstanceID != bfJob.InstanceID {
			return dbterror.ErrDDLJobNotFound.FastGenByArgs(fmt.Sprintf("get a backfill job %v, want instance ID %s", jobs[0], bfJob.InstanceID))
		}

		currTime, err := GetOracleTimeWithStartTS(se)
		if err != nil {
			return err
		}
		bfJob.InstanceLease = GetLeaseGoTime(currTime, InstanceLease)
		return updateBackfillJob(se, BackgroundSubtaskTable, bfJob, "update_backfill_task")
	})
}

func (w *baseIndexWorker) FinishTask(bfJob *BackfillJob) error {
	s := newSession(w.backfillCtx.sessCtx)
	return s.runInTxn(func(se *session) error {
		txn, err := se.txn()
		if err != nil {
			return errors.Trace(err)
		}
		bfJob.StateUpdateTS = txn.StartTS()
		err = RemoveBackfillJob(se, false, bfJob)
		if err != nil {
			return err
		}
		return AddBackfillHistoryJob(se, []*BackfillJob{bfJob})
	})
}

func (w *baseIndexWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
}

func newAddIndexWorkerContext(d *ddl, schemaName model.CIStr, tbl table.Table, workerCnt int,
	bfJob *BackfillJob, jobCtx *JobContext) (*backfillWorkerContext, error) {
	//nolint:forcetypeassert
	phyTbl := tbl.(table.PhysicalTable)
	return newBackfillWorkerContext(d, schemaName.O, tbl, workerCnt, bfJob.JobID, bfJob.Meta,
		func(bfCtx *backfillCtx) (backfiller, error) {
			decodeColMap, err := makeupDecodeColMap(bfCtx.sessCtx, schemaName, phyTbl)
			if err != nil {
				logutil.BgLogger().Error("[ddl] make up decode column map failed", zap.Error(err))
				return nil, errors.Trace(err)
			}
			bf, err1 := newAddIndexWorker(decodeColMap, phyTbl, bfCtx, jobCtx, bfJob.JobID, bfJob.EleID, bfJob.EleKey)
			return bf, err1
		})
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
				if idxInfo.Name.O == "_Idx$_idx_0" && handle.IntValue() == 7168 && atomic.CompareAndSwapUint32(&mockNotOwnerErrOnce, 0, 1) {
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

	rsData := tables.TryGetHandleRestoredDataWrapper(w.table.Meta(), nil, w.rowMap, idxInfo)
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
		recordKey := tablecodec.EncodeRecordKey(taskRange.physicalTable.RecordPrefix(), lastHandle)
		return recordKey.Next()
	}
	if taskRange.endInclude {
		return taskRange.endKey.Next()
	}
	return taskRange.endKey
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
	jobID := taskRange.getJobID()
	err := iterateSnapshotKeys(w.GetCtx().jobContext(jobID), w.sessCtx.GetStore(), taskRange.priority, taskRange.physicalTable.RecordPrefix(), txn.StartTS(),
		taskRange.startKey, taskRange.endKey, func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in baseIndexWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			if taskRange.endInclude {
				taskDone = recordKey.Cmp(taskRange.endKey) > 0
			} else {
				taskDone = recordKey.Cmp(taskRange.endKey) >= 0
			}

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
				taskDone = true
				return false, nil
			}
			return true, nil
		})

	if len(w.idxRecords) == 0 {
		taskDone = true
	}

	logutil.BgLogger().Debug("[ddl] txn fetches handle info", zap.Stringer("worker", w), zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextKey(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.batchCheckValues = w.batchCheckValues[:0]
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
	return genKeyExistsErr(key, value, idxInfo, tblInfo)
}

func genKeyExistsErr(key, value []byte, idxInfo *model.IndexInfo, tblInfo *model.TableInfo) error {
	idxColLen := len(idxInfo.Columns)
	indexName := fmt.Sprintf("%s.%s", tblInfo.Name.String(), idxInfo.Name.String())
	colInfos := tables.BuildRowcodecColInfoForIndexColumns(idxInfo, tblInfo)
	values, err := tablecodec.DecodeIndexKV(key, value, idxColLen, tablecodec.HandleNotNeeded, colInfos)
	if err != nil {
		logutil.BgLogger().Warn("decode index key value failed", zap.String("index", indexName),
			zap.String("key", hex.EncodeToString(key)), zap.String("value", hex.EncodeToString(value)), zap.Error(err))
		return kv.ErrKeyExists.FastGenByArgs(key, indexName)
	}
	valueStr := make([]string, 0, idxColLen)
	for i, val := range values[:idxColLen] {
		d, err := tablecodec.DecodeColumnValue(val, colInfos[i].Ft, time.Local)
		if err != nil {
			logutil.BgLogger().Warn("decode column value failed", zap.String("index", indexName),
				zap.String("key", hex.EncodeToString(key)), zap.String("value", hex.EncodeToString(value)), zap.Error(err))
			return kv.ErrKeyExists.FastGenByArgs(key, indexName)
		}
		str, err := d.ToString()
		if err != nil {
			str = string(val)
		}
		if types.IsBinaryStr(colInfos[i].Ft) || types.IsTypeBit(colInfos[i].Ft) {
			str = util.FmtNonASCIIPrintableCharToHex(str)
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
	cnt := 0
	for i, record := range idxRecords {
		iter := w.index.GenIndexKVIter(stmtCtx, record.vals, record.handle, idxRecords[i].rsData)
		for iter.Valid() {
			var buf []byte
			if cnt < len(w.idxKeyBufs) {
				buf = w.idxKeyBufs[cnt]
			}
			key, val, distinct, err := iter.Next(buf)
			if err != nil {
				return errors.Trace(err)
			}
			if cnt < len(w.idxKeyBufs) {
				w.idxKeyBufs[cnt] = key
			} else {
				w.idxKeyBufs = append(w.idxKeyBufs, key)
			}
			cnt++
			w.batchCheckKeys = append(w.batchCheckKeys, key)
			w.batchCheckValues = append(w.batchCheckValues, val)
			w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
		}
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
			batchVals[string(key)] = w.batchCheckValues[i]
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
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	needMergeTmpIdx := w.index.Meta().BackfillState != model.BackfillStateInapplicable

	oprStartTime := time.Now()
	jobID := handleRange.getJobID()
	ctx := kv.WithInternalSourceType(context.Background(), w.jobContext.ddlJobSourceType())
	errInTxn = kv.RunInNewTxn(ctx, w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) (err error) {
		taskCtx.finishTS = txn.StartTS()
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(jobID); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		var (
			idxRecords []*indexRecord
			copChunk   *chunk.Chunk // only used by the coprocessor request sender.
			nextKey    kv.Key
			taskDone   bool
		)
		if w.copReqSenderPool != nil {
			idxRecords, copChunk, nextKey, taskDone, err = w.copReqSenderPool.fetchRowColValsFromCop(handleRange)
			defer w.copReqSenderPool.recycleIdxRecordsAndChunk(idxRecords, copChunk)
		} else {
			idxRecords, nextKey, taskDone, err = w.fetchRowColVals(txn, handleRange)
		}
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

			// When the backfill-merge process is used, the writes from DML are redirected to a temp index.
			// The write-conflict will be handled by the merge worker. Thus, the locks are unnecessary.
			if !needMergeTmpIdx {
				// We need to add this lock to make sure pessimistic transaction can realize this operation.
				// For the normal pessimistic transaction, it's ok. But if async commit is used, it may lead to inconsistent data and index.
				err := txn.LockKeys(context.Background(), new(kv.LockCtx), idxRecord.key)
				if err != nil {
					return errors.Trace(err)
				}
			}

			// Create the index.
			if w.writerCtx == nil {
				handle, err := w.index.Create(w.sessCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData, table.WithIgnoreAssertion, table.FromBackfill)
				if err != nil {
					if kv.ErrKeyExists.Equal(err) && idxRecord.handle.Equal(handle) {
						// Index already exists, skip it.
						continue
					}

					return errors.Trace(err)
				}
			} else { // The lightning environment is ready.
				vars := w.sessCtx.GetSessionVars()
				sCtx, writeBufs := vars.StmtCtx, vars.GetWriteStmtBufs()
				iter := w.index.GenIndexKVIter(sCtx, idxRecord.vals, idxRecord.handle, idxRecord.rsData)
				for iter.Valid() {
					key, idxVal, _, err := iter.Next(writeBufs.IndexKeyBuf)
					if err != nil {
						return errors.Trace(err)
					}
					err = w.writerCtx.WriteRow(key, idxVal)
					if err != nil {
						return errors.Trace(err)
					}
					writeBufs.IndexKeyBuf = key
				}
			}
			taskCtx.addedCount++
		}

		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexBackfillDataInTxn", 3000)
	failpoint.Inject("mockDMLExecution", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecution != nil {
			MockDMLExecution()
		}
	})
	return
}

// MockDMLExecution is only used for test.
var MockDMLExecution func()

// MockDMLExecutionMerging is only used for test.
var MockDMLExecutionMerging func()

// MockDMLExecutionStateMerging is only used for test.
var MockDMLExecutionStateMerging func()

func (w *worker) addPhysicalTableIndex(t table.PhysicalTable, reorgInfo *reorgInfo) error {
	if reorgInfo.mergingTmpIdx {
		logutil.BgLogger().Info("[ddl] start to merge temp index", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
		return w.writePhysicalTableRecord(w.sessPool, t, typeAddIndexMergeTmpWorker, reorgInfo)
	}
	logutil.BgLogger().Info("[ddl] start to add table index", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writePhysicalTableRecord(w.sessPool, t, typeAddIndexWorker, reorgInfo)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(t table.Table, reorgInfo *reorgInfo) error {
	// TODO: Support typeAddIndexMergeTmpWorker.
	if reorgInfo.Job.ReorgMeta.IsDistReorg && !reorgInfo.mergingTmpIdx {
		return w.controlWriteTableRecord(w.sessPool, t, typeAddIndexWorker, reorgInfo)
	}

	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			err = w.addPhysicalTableIndex(p, reorgInfo)
			if err != nil {
				break
			}
			finish, err = updateReorgInfo(w.sessPool, tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		//nolint:forcetypeassert
		phyTbl := t.(table.PhysicalTable)
		err = w.addPhysicalTableIndex(phyTbl, reorgInfo)
	}
	return errors.Trace(err)
}

func getNextPartitionInfo(reorg *reorgInfo, t table.PartitionedTable, currPhysicalTableID int64) (int64, kv.Key, kv.Key, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return 0, nil, nil, nil
	}

	// During data copying, copy data from partitions to be dropped
	nextPartitionDefs := pi.DroppingDefinitions
	if bytes.Equal(reorg.currElement.TypeKey, meta.IndexElementKey) {
		// During index re-creation, process data from partitions to be added
		nextPartitionDefs = pi.AddingDefinitions
	}
	if nextPartitionDefs == nil {
		nextPartitionDefs = pi.Definitions
	}
	pid, err := findNextPartitionID(currPhysicalTableID, nextPartitionDefs)
	if err != nil {
		// Fatal error, should not run here.
		logutil.BgLogger().Error("[ddl] find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
		return 0, nil, nil, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return 0, nil, nil, nil
	}

	failpoint.Inject("mockUpdateCachedSafePoint", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			ts := oracle.GoTimeToTS(time.Now())
			//nolint:forcetypeassert
			s := reorg.d.store.(tikv.Storage)
			s.UpdateSPCache(ts, time.Now())
			time.Sleep(time.Second * 3)
		}
	})

	var startKey, endKey kv.Key
	if reorg.mergingTmpIdx {
		indexID := reorg.currElement.ID
		startKey, endKey = tablecodec.GetTableIndexKeyRange(pid, tablecodec.TempIndexPrefix|indexID)
	} else {
		currentVer, err := getValidCurrentVersion(reorg.d.store)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		startKey, endKey, err = getTableRange(reorg.d.jobContext(reorg.Job.ID), reorg.d, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
	}
	return pid, startKey, endKey, nil
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func updateReorgInfo(sessPool *sessionPool, t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
	pid, startKey, endKey, err := getNextPartitionInfo(reorg, t, reorg.PhysicalTableID)
	if err != nil {
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}
	reorg.PhysicalTableID, reorg.StartKey, reorg.EndKey = pid, startKey, endKey

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, sessPool)
	logutil.BgLogger().Info("[ddl] job update reorgInfo",
		zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partitionTableID", pid),
		zap.String("startKey", hex.EncodeToString(reorg.StartKey)),
		zap.String("endKey", hex.EncodeToString(reorg.EndKey)), zap.Error(err))
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

// AllocateIndexID allocates an index ID from TableInfo.
func AllocateIndexID(tblInfo *model.TableInfo) int64 {
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

func newCleanUpIndexWorker(sessCtx sessionctx.Context, id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *JobContext) *cleanUpIndexWorker {
	indexes := make([]table.Index, 0, len(t.Indices()))
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	for _, index := range t.Indices() {
		if index.Meta().Global {
			indexes = append(indexes, index)
		}
	}
	return &cleanUpIndexWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx:   newBackfillCtx(reorgInfo.d, id, sessCtx, reorgInfo.ReorgMeta.ReorgTp, reorgInfo.SchemaName, t, false),
			indexes:       indexes,
			rowDecoder:    rowDecoder,
			defaultVals:   make([]types.Datum, len(t.WritableCols())),
			rowMap:        make(map[int64]types.Datum, len(decodeColMap)),
			metricCounter: metrics.BackfillTotalCounter.WithLabelValues(metrics.GenerateReorgLabel("cleanup_idx_rate", reorgInfo.SchemaName, t.Meta().Name.String())),
			jobContext:    jc,
		},
	}
}

func (w *cleanUpIndexWorker) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceType(context.Background(), w.jobContext.ddlJobSourceType())
	errInTxn = kv.RunInNewTxn(ctx, w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
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
	return w.writePhysicalTableRecord(w.sessPool, t, typeCleanUpIndexWorker, reorgInfo)
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
	start, end, err := getTableRange(reorg.d.jobContext(reorg.Job.ID), reorg.d, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, w.sessPool)
	logutil.BgLogger().Info("[ddl] job update reorg info", zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partition table ID", pid), zap.String("start key", hex.EncodeToString(start)),
		zap.String("end key", hex.EncodeToString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

func runBackfillJobsWithLightning(d *ddl, sess *session, bfJob *BackfillJob, jobCtx *JobContext) error {
	bc, err := ingest.LitBackCtxMgr.Register(d.ctx, bfJob.Meta.IsUnique, bfJob.JobID, bfJob.Meta.SQLMode)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] lightning register error", zap.Error(err))
		return err
	}

	tbl, err := runBackfillJobs(d, sess, bc, bfJob, jobCtx)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] runBackfillJobs error", zap.Error(err))
		ingest.LitBackCtxMgr.Unregister(bfJob.JobID)
		return err
	}

	bc.EngMgr.ResetWorkers(bc, bfJob.JobID, bfJob.EleID)
	err = bc.FinishImport(bfJob.EleID, bfJob.Meta.IsUnique, tbl)
	if err != nil {
		logutil.BgLogger().Warn("[ddl] lightning import error", zap.String("first backfill job", bfJob.AbbrStr()), zap.Error(err))
		ingest.LitBackCtxMgr.Unregister(bfJob.JobID)
		return err
	}
	ingest.LitBackCtxMgr.Unregister(bfJob.ID)
	bc.SetDone()
	return nil
}

// changingIndex is used to store the index that need to be changed during modifying column.
type changingIndex struct {
	IndexInfo *model.IndexInfo
	// Column offset in idxInfo.Columns.
	Offset int
	// When the modifying column is contained in the index, a temp index is created.
	// isTemp indicates whether the indexInfo is a temp index created by a previous modify column job.
	isTemp bool
}

// FindRelatedIndexesToChange finds the indexes that covering the given column.
// The normal one will be overridden by the temp one.
func FindRelatedIndexesToChange(tblInfo *model.TableInfo, colName model.CIStr) []changingIndex {
	// In multi-schema change jobs that contains several "modify column" sub-jobs, there may be temp indexes for another temp index.
	// To prevent reorganizing too many indexes, we should create the temp indexes that are really necessary.
	var normalIdxInfos, tempIdxInfos []changingIndex
	for _, idxInfo := range tblInfo.Indices {
		if pos := findIdxCol(idxInfo, colName); pos != -1 {
			isTemp := isTempIdxInfo(idxInfo, tblInfo)
			r := changingIndex{IndexInfo: idxInfo, Offset: pos, isTemp: isTemp}
			if isTemp {
				tempIdxInfos = append(tempIdxInfos, r)
			} else {
				normalIdxInfos = append(normalIdxInfos, r)
			}
		}
	}
	// Overwrite if the index has the corresponding temp index. For example,
	// we try to find the indexes that contain the column `b` and there are two indexes, `i(a, b)` and `$i($a, b)`.
	// Note that the symbol `$` means temporary. The index `$i($a, b)` is temporarily created by the previous "modify a" statement.
	// In this case, we would create a temporary index like $$i($a, $b), so the latter should be chosen.
	result := normalIdxInfos
	for _, tmpIdx := range tempIdxInfos {
		origName := getChangingIndexOriginName(tmpIdx.IndexInfo)
		for i, normIdx := range normalIdxInfos {
			if normIdx.IndexInfo.Name.O == origName {
				result[i] = tmpIdx
			}
		}
	}
	return result
}

func isTempIdxInfo(idxInfo *model.IndexInfo, tblInfo *model.TableInfo) bool {
	for _, idxCol := range idxInfo.Columns {
		if tblInfo.Columns[idxCol.Offset].ChangeStateInfo != nil {
			return true
		}
	}
	return false
}

func findIdxCol(idxInfo *model.IndexInfo, colName model.CIStr) int {
	for offset, idxCol := range idxInfo.Columns {
		if idxCol.Name.L == colName.L {
			return offset
		}
	}
	return -1
}

func renameIndexes(tblInfo *model.TableInfo, from, to model.CIStr) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == from.L {
			idx.Name = to
		} else if isTempIdxInfo(idx, tblInfo) && getChangingIndexOriginName(idx) == from.O {
			idx.Name.L = strings.Replace(idx.Name.L, from.L, to.L, 1)
			idx.Name.O = strings.Replace(idx.Name.O, from.O, to.O, 1)
		}
	}
}

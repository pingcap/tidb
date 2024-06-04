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
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	sess "github.com/pingcap/tidb/pkg/ddl/internal/session"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	pdHttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// MaxCommentLength is exported for testing.
	MaxCommentLength = 1024
)

var (
	// SuppressErrorTooLongKeyKey is used by SchemaTracker to suppress err too long key error
	SuppressErrorTooLongKeyKey stringutil.StringerStr = "suppressErrorTooLongKeyKey"
)

func suppressErrorTooLongKeyKey(sctx sessionctx.Context) bool {
	if suppress, ok := sctx.Value(SuppressErrorTooLongKeyKey).(bool); ok && suppress {
		return true
	}
	return false
}

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
		if indexColLen != types.UnspecifiedLength &&
			types.IsTypeChar(col.FieldType.GetType()) &&
			indexColLen == col.FieldType.GetFlen() {
			indexColLen = types.UnspecifiedLength
		}
		indexColumnLength, err := getIndexColumnLength(col, indexColLen)
		if err != nil {
			return nil, false, err
		}
		sumLength += indexColumnLength

		// The sum of all lengths must be shorter than the max length for prefix.
		if sumLength > maxIndexLength {
			// The multiple column index and the unique index in which the length sum exceeds the maximum size
			// will return an error instead produce a warning.
			if ctx == nil || (ctx.GetSessionVars().SQLMode.HasStrictMode() && !suppressErrorTooLongKeyKey(ctx)) || mysql.HasUniKeyFlag(col.GetFlag()) || len(indexPartSpecifications) > 1 {
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
	if indexColumnLen > maxIndexLength {
		if ctx == nil || (ctx.GetSessionVars().SQLMode.HasStrictMode() && !suppressErrorTooLongKeyKey(ctx)) {
			// return error in strict sql mode
			return dbterror.ErrTooLongKey.GenWithStackByArgs(indexColumnLen, maxIndexLength)
		}
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
	renameHiddenColumns(tblInfo, from, to)

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
	_, err = convertAddIdxJob2RollbackJob(d, t, job, tblInfo, []*model.IndexInfo{indexInfo}, err)
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

	unique := make([]bool, 1)
	global := make([]bool, 1)
	indexNames := make([]model.CIStr, 1)
	indexPartSpecifications := make([][]*ast.IndexPartSpecification, 1)
	indexOption := make([]*ast.IndexOption, 1)
	var sqlMode mysql.SQLMode
	var warnings []string
	hiddenCols := make([][]*model.ColumnInfo, 1)

	if isPK {
		// Notice: sqlMode and warnings is used to support non-strict mode.
		err = job.DecodeArgs(&unique[0], &indexNames[0], &indexPartSpecifications[0], &indexOption[0], &sqlMode, &warnings, &global[0])
	} else {
		err = job.DecodeArgs(&unique[0], &indexNames[0], &indexPartSpecifications[0], &indexOption[0], &hiddenCols[0], &global[0])
		if err != nil {
			err = job.DecodeArgs(&unique, &indexNames, &indexPartSpecifications, &indexOption, &hiddenCols, &global)
		}
	}
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	allIndexInfos := make([]*model.IndexInfo, 0, len(indexNames))
	for i, indexName := range indexNames {
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
			for _, hiddenCol := range hiddenCols[i] {
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
				for _, hiddenCol := range hiddenCols[i] {
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
				unique[i],
				global[i],
				indexPartSpecifications[i],
				indexOption[i],
				model.StateNone,
			)
			if err != nil {
				job.State = model.JobStateCancelled
				return ver, errors.Trace(err)
			}
			if isPK {
				if _, err = CheckPKOnGeneratedColumn(tblInfo, indexPartSpecifications[i]); err != nil {
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
			logutil.DDLLogger().Info("run add index job", zap.Stringer("job", job), zap.Reflect("indexInfo", indexInfo))
		}
		allIndexInfos = append(allIndexInfos, indexInfo)
	}

	originalState := allIndexInfos[0].State

SwitchIndexState:
	switch allIndexInfos[0].State {
	case model.StateNone:
		// none -> delete only
		var reorgTp model.ReorgType
		reorgTp, err = pickBackfillType(w.ctx, job)
		if err != nil {
			if !errorIsRetryable(err, job) {
				job.State = model.JobStateCancelled
			}
			return ver, err
		}
		loadCloudStorageURI(w, job)
		if reorgTp.NeedMergeProcess() {
			for _, indexInfo := range allIndexInfos {
				indexInfo.BackfillState = model.BackfillStateRunning
			}
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
			moveAndUpdateHiddenColumnsToPublic(tblInfo, indexInfo)
		}
		ver, err = updateVersionAndTableInfoWithCheck(d, t, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
			_, err = checkPrimaryKeyNotNull(d, w, t, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteReorganization
			_, err = checkPrimaryKeyNotNull(d, w, t, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != model.StateWriteReorganization)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable((*asAutoIDRequirement)(d), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		var done bool
		if job.MultiSchemaInfo != nil {
			done, ver, err = doReorgWorkForCreateIndexMultiSchema(w, d, t, job, tbl, allIndexInfos)
		} else {
			done, ver, err = doReorgWorkForCreateIndex(w, d, t, job, tbl, allIndexInfos)
		}
		if !done {
			return ver, err
		}

		// Set column index flag.
		for _, indexInfo := range allIndexInfos {
			AddIndexColumnFlag(tblInfo, indexInfo)
			if isPK {
				if err = UpdateColsNull2NotNull(tblInfo, indexInfo); err != nil {
					return ver, errors.Trace(err)
				}
			}
			indexInfo.State = model.StatePublic
		}

		// Inject the failpoint to prevent the progress of index creation.
		failpoint.Inject("create-index-stuck-before-public", func(v failpoint.Value) {
			if sigFile, ok := v.(string); ok {
				for {
					time.Sleep(1 * time.Second)
					if _, err := os.Stat(sigFile); err != nil {
						if os.IsNotExist(err) {
							continue
						}
						failpoint.Return(ver, errors.Trace(err))
					}
					break
				}
			}
		})

		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != model.StatePublic)
		if err != nil {
			return ver, errors.Trace(err)
		}

		allIndexIDs := make([]int64, 0, len(allIndexInfos))
		ifExists := make([]bool, 0, len(allIndexInfos))
		for _, indexInfo := range allIndexInfos {
			allIndexIDs = append(allIndexIDs, indexInfo.ID)
			ifExists = append(ifExists, false)
		}
		job.Args = []any{allIndexIDs, ifExists, getPartitionIDs(tbl.Meta())}
		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		if !job.ReorgMeta.IsDistReorg && job.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
			ingest.LitBackCtxMgr.Unregister(job.ID)
		}
		logutil.DDLLogger().Info("run add index job done",
			zap.String("charset", job.Charset),
			zap.String("collation", job.Collate))
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

// pickBackfillType determines which backfill process will be used.
func pickBackfillType(ctx context.Context, job *model.Job) (model.ReorgType, error) {
	if job.ReorgMeta.ReorgTp != model.ReorgTypeNone {
		// The backfill task has been started.
		// Don't change the backfill type.
		return job.ReorgMeta.ReorgTp, nil
	}
	if !job.ReorgMeta.IsFastReorg {
		job.ReorgMeta.ReorgTp = model.ReorgTypeTxn
		return model.ReorgTypeTxn, nil
	}
	if ingest.LitInitialized {
		if job.ReorgMeta.UseCloudStorage {
			job.ReorgMeta.ReorgTp = model.ReorgTypeLitMerge
			return model.ReorgTypeLitMerge, nil
		}
		available, err := ingest.LitBackCtxMgr.CheckMoreTasksAvailable(ctx)
		if err != nil {
			return model.ReorgTypeNone, err
		}
		if available {
			job.ReorgMeta.ReorgTp = model.ReorgTypeLitMerge
			return model.ReorgTypeLitMerge, nil
		}
	}
	// The lightning environment is unavailable, but we can still use the txn-merge backfill.
	logutil.DDLLogger().Info("fallback to txn-merge backfill process",
		zap.Bool("lightning env initialized", ingest.LitInitialized))
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxnMerge
	return model.ReorgTypeTxnMerge, nil
}

func loadCloudStorageURI(w *worker, job *model.Job) {
	jc := w.jobContext(job.ID, job.ReorgMeta)
	jc.cloudStorageURI = variable.CloudStorageURI.Load()
	job.ReorgMeta.UseCloudStorage = len(jc.cloudStorageURI) > 0
}

func doReorgWorkForCreateIndexMultiSchema(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	if job.MultiSchemaInfo.Revertible {
		done, ver, err = doReorgWorkForCreateIndex(w, d, t, job, tbl, allIndexInfos)
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
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	var reorgTp model.ReorgType
	reorgTp, err = pickBackfillType(w.ctx, job)
	if err != nil {
		return false, ver, err
	}
	if !reorgTp.NeedMergeProcess() {
		return runReorgJobAndHandleErr(w, d, t, job, tbl, allIndexInfos, false)
	}
	switch allIndexInfos[0].BackfillState {
	case model.BackfillStateRunning:
		logutil.DDLLogger().Info("index backfill state running",
			zap.Int64("job ID", job.ID), zap.String("table", tbl.Meta().Name.O),
			zap.Bool("ingest mode", reorgTp == model.ReorgTypeLitMerge),
			zap.String("index", allIndexInfos[0].Name.O))
		switch reorgTp {
		case model.ReorgTypeLitMerge:
			if job.ReorgMeta.IsDistReorg {
				done, ver, err = runIngestReorgJobDist(w, d, t, job, tbl, allIndexInfos)
			} else {
				done, ver, err = runIngestReorgJob(w, d, t, job, tbl, allIndexInfos)
			}
		case model.ReorgTypeTxnMerge:
			done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, allIndexInfos, false)
		}
		if err != nil || !done {
			return false, ver, errors.Trace(err)
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateReadyToMerge
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateReadyToMerge:
		failpoint.Inject("mockDMLExecutionStateBeforeMerge", func(_ failpoint.Value) {
			if MockDMLExecutionStateBeforeMerge != nil {
				MockDMLExecutionStateBeforeMerge()
			}
		})
		logutil.DDLLogger().Info("index backfill state ready to merge",
			zap.Int64("job ID", job.ID),
			zap.String("table", tbl.Meta().Name.O),
			zap.String("index", allIndexInfos[0].Name.O))
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateMerging
		}
		if reorgTp == model.ReorgTypeLitMerge {
			ingest.LitBackCtxMgr.Unregister(job.ID)
		}
		job.SnapshotVer = 0 // Reset the snapshot version for merge index reorg.
		ver, err = updateVersionAndTableInfo(d, t, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateMerging:
		done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, allIndexInfos, true)
		if !done {
			return false, ver, err
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateInapplicable // Prevent double-write on this index.
		}
		return true, ver, err
	default:
		return false, 0, dbterror.ErrInvalidDDLState.GenWithStackByArgs("backfill", allIndexInfos[0].BackfillState)
	}
}

func runIngestReorgJobDist(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, allIndexInfos, false)
	if err != nil {
		return false, ver, errors.Trace(err)
	}

	if !done {
		return false, ver, nil
	}

	return true, ver, nil
}

func runIngestReorgJob(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	bc, ok := ingest.LitBackCtxMgr.Load(job.ID)
	if ok && bc.Done() {
		return true, 0, nil
	}
	ctx := tidblogutil.WithCategory(w.ctx, "ddl-ingest")
	var discovery pd.ServiceDiscovery
	if d != nil {
		//nolint:forcetypeassert
		discovery = d.store.(tikv.Storage).GetRegionCache().PDClient().GetServiceDiscovery()
	}
	bc, err = ingest.LitBackCtxMgr.Register(ctx, job.ID, allIndexInfos[0].Unique, nil, discovery, job.ReorgMeta.ResourceGroupName)
	if err != nil {
		ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), allIndexInfos, err)
		return false, ver, errors.Trace(err)
	}
	done, ver, err = runReorgJobAndHandleErr(w, d, t, job, tbl, allIndexInfos, false)
	if err != nil {
		if !errorIsRetryable(err, job) {
			logutil.DDLLogger().Warn("run reorg job failed, convert job to rollback",
				zap.String("job", job.String()), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), allIndexInfos, err)
		}
		return false, ver, errors.Trace(err)
	}
	if !done {
		return false, ver, nil
	}
	for _, indexInfo := range allIndexInfos {
		err = bc.FinishImport(indexInfo.ID, indexInfo.Unique, tbl)
		if err != nil {
			if common.ErrFoundDuplicateKeys.Equal(err) {
				err = convertToKeyExistsErr(err, indexInfo, tbl.Meta())
			}
			if kv.ErrKeyExists.Equal(err) {
				logutil.DDLLogger().Warn("import index duplicate key, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
				ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), allIndexInfos, err)
			} else {
				logutil.DDLLogger().Warn("lightning import error", zap.Error(err))
				if !errorIsRetryable(err, job) {
					ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), allIndexInfos, err)
				}
			}
			return false, ver, errors.Trace(err)
		}
	}
	bc.SetDone()
	return true, ver, nil
}

func errorIsRetryable(err error, job *model.Job) bool {
	if job.ErrorCount+1 >= variable.GetDDLErrorCountLimit() {
		return false
	}
	originErr := errors.Cause(err)
	if tErr, ok := originErr.(*terror.Error); ok {
		sqlErr := terror.ToSQLError(tErr)
		_, ok := dbterror.ReorgRetryableErrCodes[sqlErr.Code]
		return ok
	}
	// For the unknown errors, we should retry.
	return true
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
	tbl table.Table, allIndexInfos []*model.IndexInfo, mergingTmpIdx bool) (done bool, ver int64, err error) {
	elements := make([]*meta.Element, 0, len(allIndexInfos))
	for _, indexInfo := range allIndexInfos {
		elements = append(elements, &meta.Element{ID: indexInfo.ID, TypeKey: meta.IndexElementKey})
	}

	failpoint.Inject("mockDMLExecutionStateMerging", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && allIndexInfos[0].BackfillState == model.BackfillStateMerging &&
			MockDMLExecutionStateMerging != nil {
			MockDMLExecutionStateMerging()
		}
	})

	sctx, err1 := w.sessPool.Get()
	if err1 != nil {
		err = err1
		return
	}
	defer w.sessPool.Put(sctx)
	rh := newReorgHandler(sess.NewSession(sctx))
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(d.jobContext(job.ID, job.ReorgMeta), d, rh, job, dbInfo, tbl, elements, mergingTmpIdx)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}
	err = overwriteReorgInfoFromGlobalCheckpoint(w, rh.s, job, reorgInfo)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	err = w.runReorgJob(reorgInfo, tbl.Meta(), d.lease, func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onCreateIndex",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tbl.Meta().Name, allIndexInfos[0].Name)
			}, false)
		return w.addTableIndex(tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		if common.ErrFoundDuplicateKeys.Equal(err) {
			// TODO(tangenta): get duplicate column and match index.
			err = convertToKeyExistsErr(err, allIndexInfos[0], tbl.Meta())
		}
		if !errorIsRetryable(err, job) {
			logutil.DDLLogger().Warn("run add index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(d, t, job, tbl.Meta(), allIndexInfos, err)
			if err1 := rh.RemoveDDLReorgHandle(job, reorgInfo.elements); err1 != nil {
				logutil.DDLLogger().Warn("run add index job failed, convert job to rollback, RemoveDDLReorgHandle failed", zap.Stringer("job", job), zap.Error(err1))
			}
		}
		return false, ver, errors.Trace(err)
	}
	failpoint.Inject("mockDMLExecutionStateBeforeImport", func(_ failpoint.Value) {
		if MockDMLExecutionStateBeforeImport != nil {
			MockDMLExecutionStateBeforeImport()
		}
	})
	return true, ver, nil
}

func onDropIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	tblInfo, allIndexInfos, ifExists, err := checkDropIndex(d, t, job)
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
		job.SchemaState = allIndexInfos[0].State
		return updateVersionAndTableInfo(d, t, job, tblInfo, false)
	}

	originalState := allIndexInfos[0].State
	switch allIndexInfos[0].State {
	case model.StatePublic:
		// public -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		// delete only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteReorganization
		}
		ver, err = updateVersionAndTableInfo(d, t, job, tblInfo, originalState != model.StateDeleteReorganization)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		idxIDs := make([]int64, 0, len(allIndexInfos))
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateNone
			// Set column index flag.
			DropIndexColumnFlag(tblInfo, indexInfo)
			RemoveDependentHiddenColumns(tblInfo, indexInfo)
			removeIndexInfo(tblInfo, indexInfo)
			idxIDs = append(idxIDs, indexInfo.ID)
		}

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
			job.Args[0] = idxIDs
		} else {
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			// Global index key has t{tableID}_ prefix.
			// Assign partitionIDs empty to guarantee correct prefix in insertJobIntoDeleteRangeTable.
			if allIndexInfos[0].Global {
				job.Args = append(job.Args, idxIDs[0], []int64{})
			} else {
				job.Args = append(job.Args, idxIDs[0], getPartitionIDs(tblInfo))
			}
		}
	default:
		return ver, errors.Trace(dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", allIndexInfos[0].State))
	}
	job.SchemaState = allIndexInfos[0].State
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
	slices.SortFunc(hiddenColOffs, func(a, b int) int { return cmp.Compare(b, a) })
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

func checkDropIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (*model.TableInfo, []*model.IndexInfo, bool /* ifExists */, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	indexNames := make([]model.CIStr, 1)
	ifExists := make([]bool, 1)
	if err = job.DecodeArgs(&indexNames[0], &ifExists[0]); err != nil {
		if err = job.DecodeArgs(&indexNames, &ifExists); err != nil {
			job.State = model.JobStateCancelled
			return nil, nil, false, errors.Trace(err)
		}
	}

	indexInfos := make([]*model.IndexInfo, 0, len(indexNames))
	for i, idxName := range indexNames {
		indexInfo := tblInfo.FindIndexByName(idxName.L)
		if indexInfo == nil {
			job.State = model.JobStateCancelled
			return nil, nil, ifExists[i], dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", idxName)
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
		indexInfos = append(indexInfos, indexInfo)
	}
	return tblInfo, indexInfos, false, nil
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

	tp backfillerType
	// The following attributes are used to reduce memory allocation.
	defaultVals []types.Datum
	idxRecords  []*indexRecord
	rowMap      map[int64]types.Datum
	rowDecoder  *decoder.RowDecoder
}

type addIndexTxnWorker struct {
	baseIndexWorker

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	batchCheckValues   [][]byte
	distinctCheckFlags []bool
	recordIdx          []int
}

func newAddIndexTxnWorker(
	decodeColMap map[int64]decoder.Column,
	t table.PhysicalTable,
	bfCtx *backfillCtx,
	jobID int64,
	elements []*meta.Element,
	eleTypeKey []byte,
) (*addIndexTxnWorker, error) {
	if !bytes.Equal(eleTypeKey, meta.IndexElementKey) {
		logutil.DDLLogger().Error("Element type for addIndexTxnWorker incorrect",
			zap.Int64("job ID", jobID), zap.ByteString("element type", eleTypeKey), zap.Int64("element ID", elements[0].ID))
		return nil, errors.Errorf("element type is not index, typeKey: %v", eleTypeKey)
	}

	allIndexes := make([]table.Index, 0, len(elements))
	for _, elem := range elements {
		if !bytes.Equal(elem.TypeKey, meta.IndexElementKey) {
			continue
		}
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
		allIndexes = append(allIndexes, index)
	}
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)

	return &addIndexTxnWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx: bfCtx,
			indexes:     allIndexes,
			rowDecoder:  rowDecoder,
			defaultVals: make([]types.Datum, len(t.WritableCols())),
			rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
		},
	}, nil
}

func (w *baseIndexWorker) AddMetricInfo(cnt float64) {
	w.metricCounter.Add(cnt)
}

func (w *baseIndexWorker) String() string {
	return w.tp.String()
}

func (w *baseIndexWorker) GetCtx() *backfillCtx {
	return w.backfillCtx
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
		idxColumnVal, err = tables.GetColDefaultValue(w.exprCtx, col, w.defaultVals)
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
	return taskRange.endKey
}

func (w *baseIndexWorker) updateRowDecoder(handle kv.Handle, rawRecord []byte) error {
	sysZone := w.loc
	_, err := w.rowDecoder.DecodeAndEvalRowWithMap(w.exprCtx, handle, rawRecord, sysZone, w.rowMap)
	return errors.Trace(err)
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
	err := iterateSnapshotKeys(w.jobContext, w.ddlCtx.store, taskRange.priority, taskRange.physicalTable.RecordPrefix(), txn.StartTS(),
		taskRange.startKey, taskRange.endKey, func(handle kv.Handle, recordKey kv.Key, rawRow []byte) (bool, error) {
			oprEndTime := time.Now()
			logSlowOperations(oprEndTime.Sub(oprStartTime), "iterateSnapshotKeys in baseIndexWorker fetchRowColVals", 0)
			oprStartTime = oprEndTime

			taskDone = recordKey.Cmp(taskRange.endKey) >= 0

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

	logutil.DDLLogger().Debug("txn fetches handle info", zap.Stringer("worker", w), zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.idxRecords, w.getNextKey(taskRange, taskDone), taskDone, errors.Trace(err)
}

func (w *addIndexTxnWorker) initBatchCheckBufs(batchCount int) {
	if len(w.idxKeyBufs) < batchCount {
		w.idxKeyBufs = make([][]byte, batchCount)
	}

	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.batchCheckValues = w.batchCheckValues[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]
	w.recordIdx = w.recordIdx[:0]
}

func (w *addIndexTxnWorker) checkHandleExists(idxInfo *model.IndexInfo, key kv.Key, value []byte, handle kv.Handle) error {
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
	indexName := fmt.Sprintf("%s.%s", tblInfo.Name.String(), idxInfo.Name.String())
	valueStr, err := tables.GenIndexValueFromIndex(key, value, tblInfo, idxInfo)
	if err != nil {
		logutil.DDLLogger().Warn("decode index key value / column value failed", zap.String("index", indexName),
			zap.String("key", hex.EncodeToString(key)), zap.String("value", hex.EncodeToString(value)), zap.Error(err))
		return errors.Trace(kv.ErrKeyExists.FastGenByArgs(key, indexName))
	}
	return kv.ErrKeyExists.FastGenByArgs(strings.Join(valueStr, "-"), indexName)
}

// batchCheckUniqueKey checks the unique keys in the batch.
// Note that `idxRecords` may belong to multiple indexes.
func (w *addIndexTxnWorker) batchCheckUniqueKey(txn kv.Transaction, idxRecords []*indexRecord) error {
	w.initBatchCheckBufs(len(idxRecords))
	evalCtx := w.exprCtx.GetEvalCtx()
	ec := evalCtx.ErrCtx()
	uniqueBatchKeys := make([]kv.Key, 0, len(idxRecords))
	cnt := 0
	for i, record := range idxRecords {
		idx := w.indexes[i%len(w.indexes)]
		if !idx.Meta().Unique {
			// non-unique key need not to check, use `nil` as a placeholder to keep
			// `idxRecords[i]` belonging to `indexes[i%len(indexes)]`.
			w.batchCheckKeys = append(w.batchCheckKeys, nil)
			w.batchCheckValues = append(w.batchCheckValues, nil)
			w.distinctCheckFlags = append(w.distinctCheckFlags, false)
			w.recordIdx = append(w.recordIdx, 0)
			continue
		}
		// skip by default.
		idxRecords[i].skip = true
		iter := idx.GenIndexKVIter(ec, w.loc, record.vals, record.handle, idxRecords[i].rsData)
		for iter.Valid() {
			var buf []byte
			if cnt < len(w.idxKeyBufs) {
				buf = w.idxKeyBufs[cnt]
			}
			key, val, distinct, err := iter.Next(buf, nil)
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
			w.recordIdx = append(w.recordIdx, i)
			uniqueBatchKeys = append(uniqueBatchKeys, key)
		}
	}

	if len(uniqueBatchKeys) == 0 {
		return nil
	}

	batchVals, err := txn.BatchGet(context.Background(), uniqueBatchKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key/primary-key is duplicate and the handle is equal, skip it.
	// 2. unique-key/primary-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if len(key) == 0 {
			continue
		}
		idx := w.indexes[i%len(w.indexes)]
		val, found := batchVals[string(key)]
		if found {
			if w.distinctCheckFlags[i] {
				if err := w.checkHandleExists(idx.Meta(), key, val, idxRecords[w.recordIdx[i]].handle); err != nil {
					return errors.Trace(err)
				}
			}
		} else if w.distinctCheckFlags[i] {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			batchVals[string(key)] = w.batchCheckValues[i]
		}
		idxRecords[w.recordIdx[i]].skip = found && idxRecords[w.recordIdx[i]].skip
	}
	// Constrains is already checked.
	w.tblCtx.GetSessionVars().StmtCtx.BatchCheck = true
	return nil
}

type addIndexIngestWorker struct {
	ctx           context.Context
	d             *ddlCtx
	metricCounter prometheus.Counter
	writeLoc      *time.Location
	writeErrCtx   errctx.Context
	writeStmtBufs variable.WriteStmtBufs

	tbl              table.PhysicalTable
	indexes          []table.Index
	writers          []ingest.Writer
	copReqSenderPool *copReqSenderPool
	checkpointMgr    *ingest.CheckpointManager

	resultCh chan *backfillResult
	jobID    int64
}

func newAddIndexIngestWorker(
	ctx context.Context,
	t table.PhysicalTable,
	info *reorgInfo,
	engines []ingest.Engine,
	resultCh chan *backfillResult,
	jobID int64,
	indexIDs []int64,
	writerID int,
	copReqSenderPool *copReqSenderPool,
	checkpointMgr *ingest.CheckpointManager,
) (*addIndexIngestWorker, error) {
	writeLoc, err := reorgTimeZoneWithTzLoc(info.ReorgMeta.Location)
	if err != nil {
		return nil, err
	}

	indexes := make([]table.Index, 0, len(indexIDs))
	writers := make([]ingest.Writer, 0, len(indexIDs))
	for i, indexID := range indexIDs {
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, indexID)
		index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
		lw, err := engines[i].CreateWriter(writerID)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
		writers = append(writers, lw)
	}

	writeErrCtx := errctx.NewContextWithLevels(
		reorgErrLevelsWithSQLMode(info.ReorgMeta.SQLMode),
		contextutil.IgnoreWarn,
	)

	return &addIndexIngestWorker{
		ctx:         ctx,
		d:           info.d,
		writeLoc:    writeLoc,
		writeErrCtx: writeErrCtx,
		metricCounter: metrics.BackfillTotalCounter.WithLabelValues(
			metrics.GenerateReorgLabel("add_idx_rate", info.SchemaName, t.Meta().Name.O)),
		tbl:              t,
		indexes:          indexes,
		writers:          writers,
		copReqSenderPool: copReqSenderPool,
		resultCh:         resultCh,
		jobID:            jobID,
		checkpointMgr:    checkpointMgr,
	}, nil
}

// WriteLocal will write index records to lightning engine.
func (w *addIndexIngestWorker) WriteLocal(rs *IndexRecordChunk) (count int, nextKey kv.Key, err error) {
	oprStartTime := time.Now()
	copCtx := w.copReqSenderPool.copCtx
	cnt, lastHandle, err := writeChunkToLocal(
		w.ctx, w.writers, w.indexes, copCtx, w.writeLoc, w.writeErrCtx, &w.writeStmtBufs, rs.Chunk)
	if err != nil || cnt == 0 {
		return 0, nil, err
	}
	w.metricCounter.Add(float64(cnt))
	logSlowOperations(time.Since(oprStartTime), "writeChunkToLocal", 3000)
	nextKey = tablecodec.EncodeRecordKey(w.tbl.RecordPrefix(), lastHandle)
	return cnt, nextKey, nil
}

func writeChunkToLocal(
	ctx context.Context,
	writers []ingest.Writer,
	indexes []table.Index,
	copCtx copr.CopContext,
	loc *time.Location,
	errCtx errctx.Context,
	writeStmtBufs *variable.WriteStmtBufs,
	copChunk *chunk.Chunk,
) (int, kv.Handle, error) {
	iter := chunk.NewIterator4Chunk(copChunk)
	c := copCtx.GetBase()
	ectx := c.ExprCtx.GetEvalCtx()

	maxIdxColCnt := maxIndexColumnCount(indexes)
	idxDataBuf := make([]types.Datum, maxIdxColCnt)
	handleDataBuf := make([]types.Datum, len(c.HandleOutputOffsets))
	var restoreDataBuf []types.Datum
	count := 0
	var lastHandle kv.Handle

	unlockFns := make([]func(), 0, len(writers))
	for _, w := range writers {
		unlock := w.LockForWrite()
		unlockFns = append(unlockFns, unlock)
	}
	defer func() {
		for _, unlock := range unlockFns {
			unlock()
		}
	}()
	needRestoreForIndexes := make([]bool, len(indexes))
	restore, pkNeedRestore := false, false
	if c.PrimaryKeyInfo != nil && c.TableInfo.IsCommonHandle && c.TableInfo.CommonHandleVersion != 0 {
		pkNeedRestore = tables.NeedRestoredData(c.PrimaryKeyInfo.Columns, c.TableInfo.Columns)
	}
	for i, index := range indexes {
		needRestore := pkNeedRestore || tables.NeedRestoredData(index.Meta().Columns, c.TableInfo.Columns)
		needRestoreForIndexes[i] = needRestore
		restore = restore || needRestore
	}
	if restore {
		restoreDataBuf = make([]types.Datum, len(c.HandleOutputOffsets))
	}
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		handleDataBuf := extractDatumByOffsets(ectx, row, c.HandleOutputOffsets, c.ExprColumnInfos, handleDataBuf)
		if restore {
			// restoreDataBuf should not truncate index values.
			for i, datum := range handleDataBuf {
				restoreDataBuf[i] = *datum.Clone()
			}
		}
		h, err := buildHandle(handleDataBuf, c.TableInfo, c.PrimaryKeyInfo, loc, errCtx)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		for i, index := range indexes {
			idxID := index.Meta().ID
			idxDataBuf = extractDatumByOffsets(ectx,
				row, copCtx.IndexColumnOutputOffsets(idxID), c.ExprColumnInfos, idxDataBuf)
			idxData := idxDataBuf[:len(index.Meta().Columns)]
			var rsData []types.Datum
			if needRestoreForIndexes[i] {
				rsData = getRestoreData(c.TableInfo, copCtx.IndexInfo(idxID), c.PrimaryKeyInfo, restoreDataBuf)
			}
			err = writeOneKVToLocal(ctx, writers[i], index, loc, errCtx, writeStmtBufs, idxData, rsData, h)
			if err != nil {
				return 0, nil, errors.Trace(err)
			}
		}
		count++
		lastHandle = h
	}
	return count, lastHandle, nil
}

func maxIndexColumnCount(indexes []table.Index) int {
	maxCnt := 0
	for _, idx := range indexes {
		colCnt := len(idx.Meta().Columns)
		if colCnt > maxCnt {
			maxCnt = colCnt
		}
	}
	return maxCnt
}

func writeOneKVToLocal(
	ctx context.Context,
	writer ingest.Writer,
	index table.Index,
	loc *time.Location,
	errCtx errctx.Context,
	writeBufs *variable.WriteStmtBufs,
	idxDt, rsData []types.Datum,
	handle kv.Handle,
) error {
	iter := index.GenIndexKVIter(errCtx, loc, idxDt, handle, rsData)
	for iter.Valid() {
		key, idxVal, _, err := iter.Next(writeBufs.IndexKeyBuf, writeBufs.RowValBuf)
		if err != nil {
			return errors.Trace(err)
		}
		failpoint.Inject("mockLocalWriterPanic", func() {
			panic("mock panic")
		})
		if !index.Meta().Unique {
			handle = nil
		}
		err = writer.WriteRow(ctx, key, idxVal, handle)
		if err != nil {
			return errors.Trace(err)
		}
		failpoint.Inject("mockLocalWriterError", func() {
			failpoint.Return(errors.New("mock engine error"))
		})
		writeBufs.IndexKeyBuf = key
		writeBufs.RowValBuf = idxVal
	}
	return nil
}

// BackfillData will backfill table index in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed,
// Note that index columns values may change, and an index is not allowed to be added, so the txn will rollback and retry.
// BackfillData will add w.batchCnt indices once, default value of w.batchCnt is 128.
func (w *addIndexTxnWorker) BackfillData(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	jobID := handleRange.getJobID()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) (err error) {
		taskCtx.finishTS = txn.StartTS()
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(jobID); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

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

		for i, idxRecord := range idxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// We need to add this lock to make sure pessimistic transaction can realize this operation.
			// For the normal pessimistic transaction, it's ok. But if async commit is used, it may lead to inconsistent data and index.
			err := txn.LockKeys(context.Background(), new(kv.LockCtx), idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			handle, err := w.indexes[i%len(w.indexes)].Create(
				w.tblCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData, table.WithIgnoreAssertion, table.FromBackfill)
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
	logSlowOperations(time.Since(oprStartTime), "AddIndexBackfillData", 3000)
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

// MockDMLExecutionStateBeforeImport is only used for test.
var MockDMLExecutionStateBeforeImport func()

// MockDMLExecutionStateBeforeMerge is only used for test.
var MockDMLExecutionStateBeforeMerge func()

func (w *worker) addPhysicalTableIndex(t table.PhysicalTable, reorgInfo *reorgInfo) error {
	if reorgInfo.mergingTmpIdx {
		logutil.DDLLogger().Info("start to merge temp index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
		return w.writePhysicalTableRecord(w.ctx, w.sessPool, t, typeAddIndexMergeTmpWorker, reorgInfo)
	}
	logutil.DDLLogger().Info("start to add table index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	return w.writePhysicalTableRecord(w.ctx, w.sessPool, t, typeAddIndexWorker, reorgInfo)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(t table.Table, reorgInfo *reorgInfo) error {
	// TODO: Support typeAddIndexMergeTmpWorker.
	if reorgInfo.ReorgMeta.IsDistReorg && !reorgInfo.mergingTmpIdx {
		if reorgInfo.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
			err := w.executeDistTask(t, reorgInfo)
			if err != nil {
				return err
			}
			//nolint:forcetypeassert
			discovery := w.store.(tikv.Storage).GetRegionCache().PDClient().GetServiceDiscovery()
			return checkDuplicateForUniqueIndex(w.ctx, t, reorgInfo, discovery)
		}
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
			w.ddlCtx.mu.RLock()
			w.ddlCtx.mu.hook.OnUpdateReorgInfo(reorgInfo.Job, reorgInfo.PhysicalTableID)
			w.ddlCtx.mu.RUnlock()
			failpoint.InjectCall("beforeUpdateReorgInfo-addTableIndex", reorgInfo.Job)

			finish, err = updateReorgInfo(w.sessPool, tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
			// Every time we finish a partition, we update the progress of the job.
			if rc := w.getReorgCtx(reorgInfo.Job.ID); rc != nil {
				reorgInfo.Job.SetRowCount(rc.getRowCount())
			}
		}
	} else {
		//nolint:forcetypeassert
		phyTbl := t.(table.PhysicalTable)
		err = w.addPhysicalTableIndex(phyTbl, reorgInfo)
	}
	return errors.Trace(err)
}

func checkDuplicateForUniqueIndex(ctx context.Context, t table.Table, reorgInfo *reorgInfo, discovery pd.ServiceDiscovery) error {
	var bc ingest.BackendCtx
	var err error
	defer func() {
		if bc != nil {
			ingest.LitBackCtxMgr.Unregister(reorgInfo.ID)
		}
	}()
	for _, elem := range reorgInfo.elements {
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		if indexInfo == nil {
			return errors.New("unexpected error, can't find index info")
		}
		if indexInfo.Unique {
			ctx := tidblogutil.WithCategory(ctx, "ddl-ingest")
			if bc == nil {
				bc, err = ingest.LitBackCtxMgr.Register(ctx, reorgInfo.ID, indexInfo.Unique, nil, discovery, reorgInfo.ReorgMeta.ResourceGroupName)
				if err != nil {
					return err
				}
			}
			err = bc.CollectRemoteDuplicateRows(indexInfo.ID, t)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *worker) executeDistTask(t table.Table, reorgInfo *reorgInfo) error {
	if reorgInfo.mergingTmpIdx {
		return errors.New("do not support merge index")
	}

	taskType := proto.Backfill
	taskKey := fmt.Sprintf("ddl/%s/%d", taskType, reorgInfo.Job.ID)
	g, ctx := errgroup.WithContext(w.ctx)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalDistTask)

	done := make(chan struct{})

	// generate taskKey for multi schema change.
	if mInfo := reorgInfo.Job.MultiSchemaInfo; mInfo != nil {
		taskKey = fmt.Sprintf("%s/%d", taskKey, mInfo.Seq)
	}

	// For resuming add index task.
	// Need to fetch task by taskKey in tidb_global_task and tidb_global_task_history tables.
	// When pausing the related ddl job, it is possible that the task with taskKey is succeed and in tidb_global_task_history.
	// As a result, when resuming the related ddl job,
	// it is necessary to check task exits in tidb_global_task and tidb_global_task_history tables.
	taskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	task, err := taskManager.GetTaskByKeyWithHistory(w.ctx, taskKey)
	if err != nil && err != storage.ErrTaskNotFound {
		return err
	}
	if task != nil {
		// It's possible that the task state is succeed but the ddl job is paused.
		// When task in succeed state, we can skip the dist task execution/scheduing process.
		if task.State == proto.TaskStateSucceed {
			logutil.DDLLogger().Info(
				"task succeed, start to resume the ddl job",
				zap.String("task-key", taskKey))
			return nil
		}
		g.Go(func() error {
			defer close(done)
			backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
			err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logutil.DDLLogger(),
				func(context.Context) (bool, error) {
					return true, handle.ResumeTask(w.ctx, taskKey)
				},
			)
			if err != nil {
				return err
			}
			err = handle.WaitTaskDoneOrPaused(ctx, task.ID)
			if err := w.isReorgRunnable(reorgInfo.Job.ID, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	} else {
		job := reorgInfo.Job
		workerCntLimit := int(variable.GetDDLReorgWorkerCounter())
		cpuCount, err := handle.GetCPUCountOfNode(ctx)
		if err != nil {
			return err
		}
		concurrency := min(workerCntLimit, cpuCount)
		logutil.DDLLogger().Info("adjusted add-index task concurrency",
			zap.Int("worker-cnt", workerCntLimit), zap.Int("task-concurrency", concurrency),
			zap.String("task-key", taskKey))
		rowSize := estimateTableRowSize(w.ctx, w.store, w.sess.GetRestrictedSQLExecutor(), t)
		taskMeta := &BackfillTaskMeta{
			Job:             *job.Clone(),
			EleIDs:          extractElemIDs(reorgInfo),
			EleTypeKey:      reorgInfo.currElement.TypeKey,
			CloudStorageURI: w.jobContext(job.ID, job.ReorgMeta).cloudStorageURI,
			EstimateRowSize: rowSize,
		}

		metaData, err := json.Marshal(taskMeta)
		if err != nil {
			return err
		}

		g.Go(func() error {
			defer close(done)
			err := submitAndWaitTask(ctx, taskKey, taskType, concurrency, reorgInfo.ReorgMeta.TargetScope, metaData)
			failpoint.InjectCall("pauseAfterDistTaskFinished")
			if err := w.isReorgRunnable(reorgInfo.Job.ID, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	}

	g.Go(func() error {
		checkFinishTk := time.NewTicker(CheckBackfillJobFinishInterval)
		defer checkFinishTk.Stop()
		updateRowCntTk := time.NewTicker(UpdateBackfillJobRowCountInterval)
		defer updateRowCntTk.Stop()
		for {
			select {
			case <-done:
				w.updateJobRowCount(taskKey, reorgInfo.Job.ID)
				return nil
			case <-checkFinishTk.C:
				if err = w.isReorgRunnable(reorgInfo.Job.ID, true); err != nil {
					if dbterror.ErrPausedDDLJob.Equal(err) {
						if err = handle.PauseTask(w.ctx, taskKey); err != nil {
							logutil.DDLLogger().Error("pause task error", zap.String("task_key", taskKey), zap.Error(err))
							continue
						}
						failpoint.InjectCall("syncDDLTaskPause")
					}
					if !dbterror.ErrCancelledDDLJob.Equal(err) {
						return errors.Trace(err)
					}
					if err = handle.CancelTask(w.ctx, taskKey); err != nil {
						logutil.DDLLogger().Error("cancel task error", zap.String("task_key", taskKey), zap.Error(err))
						// continue to cancel task.
						continue
					}
				}
			case <-updateRowCntTk.C:
				w.updateJobRowCount(taskKey, reorgInfo.Job.ID)
			}
		}
	})
	err = g.Wait()
	return err
}

// EstimateTableRowSizeForTest is used for test.
var EstimateTableRowSizeForTest = estimateTableRowSize

// estimateTableRowSize estimates the row size in bytes of a table.
// This function tries to retrieve row size in following orders:
//  1. AVG_ROW_LENGTH column from information_schema.tables.
//  2. region info's approximate key size / key number.
func estimateTableRowSize(
	ctx context.Context,
	store kv.Storage,
	exec sqlexec.RestrictedSQLExecutor,
	tbl table.Table,
) (sizeInBytes int) {
	defer util.Recover(metrics.LabelDDL, "estimateTableRowSize", nil, false)
	var gErr error
	defer func() {
		tidblogutil.Logger(ctx).Info("estimate row size",
			zap.Int64("tableID", tbl.Meta().ID), zap.Int("size", sizeInBytes), zap.Error(gErr))
	}()
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil,
		"select AVG_ROW_LENGTH from information_schema.tables where TIDB_TABLE_ID = %?", tbl.Meta().ID)
	if err != nil {
		gErr = err
		return 0
	}
	if len(rows) == 0 {
		gErr = errors.New("no average row data")
		return 0
	}
	avgRowSize := rows[0].GetInt64(0)
	if avgRowSize != 0 {
		return int(avgRowSize)
	}
	regionRowSize, err := estimateRowSizeFromRegion(ctx, store, tbl)
	if err != nil {
		gErr = err
		return 0
	}
	return regionRowSize
}

func estimateRowSizeFromRegion(ctx context.Context, store kv.Storage, tbl table.Table) (int, error) {
	hStore, ok := store.(helper.Storage)
	if !ok {
		return 0, errors.New("not a helper.Storage")
	}
	h := &helper.Helper{
		Store:       hStore,
		RegionCache: hStore.GetRegionCache(),
	}
	pdCli, err := h.TryGetPDHTTPClient()
	if err != nil {
		return 0, err
	}
	pid := tbl.Meta().ID
	sk, ek := tablecodec.GetTableHandleKeyRange(pid)
	sRegion, err := pdCli.GetRegionByKey(ctx, codec.EncodeBytes(nil, sk))
	if err != nil {
		return 0, err
	}
	eRegion, err := pdCli.GetRegionByKey(ctx, codec.EncodeBytes(nil, ek))
	if err != nil {
		return 0, err
	}
	sk, err = hex.DecodeString(sRegion.StartKey)
	if err != nil {
		return 0, err
	}
	ek, err = hex.DecodeString(eRegion.EndKey)
	if err != nil {
		return 0, err
	}
	// We use the second region to prevent the influence of the front and back tables.
	regionLimit := 3
	regionInfos, err := pdCli.GetRegionsByKeyRange(ctx, pdHttp.NewKeyRange(sk, ek), regionLimit)
	if err != nil {
		return 0, err
	}
	if len(regionInfos.Regions) != regionLimit {
		return 0, errors.New("less than 3 regions")
	}
	sample := regionInfos.Regions[1]
	if sample.ApproximateKeys == 0 || sample.ApproximateSize == 0 {
		return 0, errors.New("zero approximate size")
	}
	return int(uint64(sample.ApproximateSize)*size.MB) / int(sample.ApproximateKeys), nil
}

func (w *worker) updateJobRowCount(taskKey string, jobID int64) {
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task manager", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	task, err := taskMgr.GetTaskByKey(w.ctx, taskKey)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	rowCount, err := taskMgr.GetSubtaskRowCount(w.ctx, task.ID, proto.BackfillStepReadIndex)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get subtask row count", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	w.getReorgCtx(jobID).setRowCount(rowCount)
}

// submitAndWaitTask submits a task and wait for it to finish.
func submitAndWaitTask(ctx context.Context, taskKey string, taskType proto.TaskType, concurrency int, targetScope string, taskMeta []byte) error {
	task, err := handle.SubmitTask(ctx, taskKey, taskType, concurrency, targetScope, taskMeta)
	if err != nil {
		return err
	}
	return handle.WaitTaskDoneOrPaused(ctx, task.ID)
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
	if len(nextPartitionDefs) == 0 {
		nextPartitionDefs = pi.Definitions
	}
	pid, err := findNextPartitionID(currPhysicalTableID, nextPartitionDefs)
	if err != nil {
		// Fatal error, should not run here.
		logutil.DDLLogger().Error("find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
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
		elements := reorg.elements
		firstElemTempID := tablecodec.TempIndexPrefix | elements[0].ID
		lastElemTempID := tablecodec.TempIndexPrefix | elements[len(elements)-1].ID
		startKey = tablecodec.EncodeIndexSeekKey(pid, firstElemTempID, nil)
		endKey = tablecodec.EncodeIndexSeekKey(pid, lastElemTempID, []byte{255})
	} else {
		currentVer, err := getValidCurrentVersion(reorg.d.store)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		startKey, endKey, err = getTableRange(reorg.NewJobContext(), reorg.d, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
	}
	return pid, startKey, endKey, nil
}

// updateReorgInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func updateReorgInfo(sessPool *sess.Pool, t table.PartitionedTable, reorg *reorgInfo) (bool, error) {
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
	logutil.DDLLogger().Info("job update reorgInfo",
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
		logutil.DDLLogger().Warn("admin repair table : index's columns length equal to 0")
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

func newCleanUpIndexWorker(id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *JobContext) (*cleanUpIndexWorker, error) {
	bCtx, err := newBackfillCtx(id, reorgInfo, reorgInfo.SchemaName, t, jc, "cleanup_idx_rate", false)
	if err != nil {
		return nil, err
	}

	indexes := make([]table.Index, 0, len(t.Indices()))
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	for _, index := range t.Indices() {
		if index.Meta().Global {
			indexes = append(indexes, index)
		}
	}
	return &cleanUpIndexWorker{
		baseIndexWorker: baseIndexWorker{
			backfillCtx: bCtx,
			indexes:     indexes,
			rowDecoder:  rowDecoder,
			defaultVals: make([]types.Datum, len(t.WritableCols())),
			rowMap:      make(map[int64]types.Datum, len(decodeColMap)),
		},
	}, nil
}

func (w *cleanUpIndexWorker) BackfillData(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) {
			panic("panic test")
		}
	})

	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceAndTaskType(context.Background(), w.jobContext.ddlJobSourceType(), kvutil.ExplicitTypeDDL)
	errInTxn = kv.RunInNewTxn(ctx, w.ddlCtx.store, true, func(_ context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		updateTxnEntrySizeLimitIfNeeded(txn)
		txn.SetOption(kv.Priority, handleRange.priority)
		if tagger := w.GetCtx().getResourceGroupTaggerForTopSQL(handleRange.getJobID()); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}
		txn.SetOption(kv.ResourceGroupName, w.jobContext.resourceGroupName)

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
			err := w.indexes[i%n].Delete(w.tblCtx, txn, idxRecord.vals, idxRecord.handle)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "cleanUpIndexBackfillDataInTxn", 3000)
	failpoint.Inject("mockDMLExecution", func(val failpoint.Value) {
		//nolint:forcetypeassert
		if val.(bool) && MockDMLExecution != nil {
			MockDMLExecution()
		}
	})

	return
}

// cleanupPhysicalTableIndex handles the drop partition reorganization state for a non-partitioned table or a partition.
func (w *worker) cleanupPhysicalTableIndex(t table.PhysicalTable, reorgInfo *reorgInfo) error {
	logutil.DDLLogger().Info("start to clean up index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	return w.writePhysicalTableRecord(w.ctx, w.sessPool, t, typeCleanUpIndexWorker, reorgInfo)
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
			pid = partitionIDs[i+1]
			break
		}
	}

	currentVer, err := getValidCurrentVersion(reorg.d.store)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getTableRange(reorg.NewJobContext(), reorg.d, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
	if err != nil {
		return false, errors.Trace(err)
	}
	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid

	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, w.sessPool)
	logutil.DDLLogger().Info("job update reorg info", zap.Int64("jobID", reorg.Job.ID),
		zap.Stringer("element", reorg.currElement),
		zap.Int64("partition table ID", pid), zap.String("start key", hex.EncodeToString(start)),
		zap.String("end key", hex.EncodeToString(end)), zap.Error(err))
	return false, errors.Trace(err)
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

func renameHiddenColumns(tblInfo *model.TableInfo, from, to model.CIStr) {
	for _, col := range tblInfo.Columns {
		if col.Hidden && getExpressionIndexOriginName(col) == from.O {
			col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
			col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
		}
	}
}

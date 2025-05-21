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
	goerrors "errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	"github.com/pingcap/tidb/pkg/ddl/ingest"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	litconfig "github.com/pingcap/tidb/pkg/lightning/config"
	lightningmetric "github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
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
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/generatedexpr"
	"github.com/pingcap/tidb/pkg/util/intest"
	tidblogutil "github.com/pingcap/tidb/pkg/util/logutil"
	decoder "github.com/pingcap/tidb/pkg/util/rowDecoder"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	kvutil "github.com/tikv/client-go/v2/util"
	pdHttp "github.com/tikv/pd/client/http"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// MaxCommentLength is exported for testing.
	MaxCommentLength = 1024
)

func buildIndexColumns(ctx *metabuild.Context, columns []*model.ColumnInfo, indexPartSpecifications []*ast.IndexPartSpecification, columnarIndexType model.ColumnarIndexType) ([]*model.IndexColumn, bool, error) {
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
		if columnarIndexType == model.ColumnarIndexTypeVector && col.FieldType.GetType() != mysql.TypeTiDBVectorFloat32 {
			return nil, false, dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(fmt.Sprintf("only support vector type, but this is type: %s", col.FieldType.String()))
		}
		if columnarIndexType == model.ColumnarIndexTypeInverted && !types.IsTypeStoredAsInteger(col.FieldType.GetType()) {
			return nil, false, dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs(fmt.Sprintf("only support integer type, but this is type: %s", col.FieldType.String()))
		}
		if columnarIndexType == model.ColumnarIndexTypeFulltext && !types.IsString(col.FieldType.GetType()) {
			return nil, false, dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs(fmt.Sprintf("only support string type, but this is type: %s", col.FieldType.String()))
		}

		// return error in strict sql mode
		if columnarIndexType == model.ColumnarIndexTypeNA {
			if err := checkIndexColumn(col, ip.Length, ctx != nil && (!ctx.GetSQLMode().HasStrictMode() || ctx.SuppressTooLongIndexErr())); err != nil {
				return nil, false, err
			}
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
		indexColumnLength, err := getIndexColumnLength(col, indexColLen, columnarIndexType)
		if err != nil {
			return nil, false, err
		}
		sumLength += indexColumnLength

		if (ctx == nil || !ctx.SuppressTooLongIndexErr()) && sumLength > maxIndexLength {
			// The sum of all lengths must be shorter than the max length for prefix.

			// The multiple column index and the unique index in which the length sum exceeds the maximum size
			// will return an error instead produce a warning.
			if ctx == nil || ctx.GetSQLMode().HasStrictMode() || mysql.HasUniKeyFlag(col.GetFlag()) || len(indexPartSpecifications) > 1 {
				return nil, false, dbterror.ErrTooLongKey.GenWithStackByArgs(sumLength, maxIndexLength)
			}
			// truncate index length and produce warning message in non-restrict sql mode.
			colLenPerUint, err := getIndexColumnLength(col, 1, columnarIndexType)
			if err != nil {
				return nil, false, err
			}
			indexColLen = maxIndexLength / colLenPerUint
			// produce warning message
			ctx.AppendWarning(dbterror.ErrTooLongKey.FastGenByArgs(sumLength, maxIndexLength))
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

func checkIndexPrefixLength(columns []*model.ColumnInfo, idxColumns []*model.IndexColumn, columnarIndexType model.ColumnarIndexType) error {
	idxLen, err := indexColumnsLen(columns, idxColumns, columnarIndexType)
	if err != nil {
		return err
	}
	if idxLen > config.GetGlobalConfig().MaxIndexLength {
		return dbterror.ErrTooLongKey.GenWithStackByArgs(idxLen, config.GetGlobalConfig().MaxIndexLength)
	}
	return nil
}

func indexColumnsLen(cols []*model.ColumnInfo, idxCols []*model.IndexColumn, columnarIndexType model.ColumnarIndexType) (colLen int, err error) {
	for _, idxCol := range idxCols {
		col := model.FindColumnInfo(cols, idxCol.Name.L)
		if col == nil {
			err = dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", idxCol.Name.L)
			return
		}
		var l int
		l, err = getIndexColumnLength(col, idxCol.Length, columnarIndexType)
		if err != nil {
			return
		}
		colLen += l
	}
	return
}

// checkIndexColumn will be run for all non-columnar indexes.
func checkIndexColumn(col *model.ColumnInfo, indexColumnLen int, suppressTooLongKeyErr bool) error {
	if col.FieldType.GetType() == mysql.TypeNull || (col.GetFlen() == 0 && (types.IsTypeChar(col.FieldType.GetType()) || types.IsTypeVarchar(col.FieldType.GetType()))) {
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

	if col.FieldType.GetType() == mysql.TypeTiDBVectorFloat32 {
		return dbterror.ErrUnsupportedAddColumnarIndex.FastGen("only VECTOR INDEX can be added to vector column")
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
		if !suppressTooLongKeyErr {
			return dbterror.ErrTooLongKey.GenWithStackByArgs(indexColumnLen, maxIndexLength)
		}
	}
	return nil
}

// getIndexColumnLength calculate the bytes number required in an index column.
func getIndexColumnLength(col *model.ColumnInfo, colLen int, columnarIndexType model.ColumnarIndexType) (int, error) {
	if columnarIndexType != model.ColumnarIndexTypeNA {
		// Columnar index does not actually create KV index, so it has length of 0.
		// however 0 may cause some issues in other calculations, so we use 1 here.
		// 1 is also minimal enough anyway.
		return 1, nil
	}

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
	ctx *metabuild.Context,
	tblInfo *model.TableInfo,
	indexName ast.CIStr,
	isPrimary, isUnique bool,
	columnarIndexType model.ColumnarIndexType,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
	state model.SchemaState,
) (*model.IndexInfo, error) {
	if err := checkTooLongIndex(indexName); err != nil {
		return nil, errors.Trace(err)
	}

	// Create index info.
	idxInfo := &model.IndexInfo{
		Name:    indexName,
		State:   state,
		Primary: isPrimary,
		Unique:  isUnique,
	}

	switch columnarIndexType {
	case model.ColumnarIndexTypeVector:
		vectorInfo, _, err := buildVectorInfoWithCheck(indexPartSpecifications, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.VectorInfo = vectorInfo
	case model.ColumnarIndexTypeInverted:
		invertedInfo, err := buildInvertedInfoWithCheck(indexPartSpecifications, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.InvertedInfo = invertedInfo
	case model.ColumnarIndexTypeFulltext:
		ftsInfo, err := buildFullTextInfoWithCheck(indexPartSpecifications, indexOption, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.FullTextInfo = ftsInfo
	}

	var err error
	allTableColumns := tblInfo.Columns
	idxInfo.Columns, idxInfo.MVIndex, err = buildIndexColumns(ctx, allTableColumns, indexPartSpecifications, columnarIndexType)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if indexOption != nil {
		idxInfo.Comment = indexOption.Comment
		if indexOption.Visibility == ast.IndexVisibilityInvisible {
			idxInfo.Invisible = true
		}
		if indexOption.Tp == ast.IndexTypeInvalid {
			// Use btree as default index type.
			idxInfo.Tp = ast.IndexTypeBtree
		} else {
			idxInfo.Tp = indexOption.Tp
		}
		idxInfo.Global = indexOption.Global
	} else {
		// Use btree as default index type.
		idxInfo.Tp = ast.IndexTypeBtree
	}

	return idxInfo, nil
}

func buildVectorInfoWithCheck(indexPartSpecifications []*ast.IndexPartSpecification,
	tblInfo *model.TableInfo) (*model.VectorIndexInfo, string, error) {
	if len(indexPartSpecifications) != 1 {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("unsupported no function")
	}

	idxPart := indexPartSpecifications[0]
	f, ok := idxPart.Expr.(*ast.FuncCallExpr)
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(fmt.Sprintf("unsupported function: %v", idxPart.Expr))
	}
	distanceMetric, ok := model.IndexableFnNameToDistanceMetric[f.FnName.L]
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs("currently only L2 and Cosine distance is indexable")
	}
	colExpr, ok := f.Args[0].(*ast.ColumnNameExpr)
	if !ok {
		return nil, "", dbterror.ErrUnsupportedAddVectorIndex.FastGenByArgs(fmt.Sprintf("unsupported function args: %v", f.Args[0]))
	}
	colInfo := findColumnByName(colExpr.Name.Name.L, tblInfo)
	if colInfo == nil {
		return nil, "", infoschema.ErrColumnNotExists.GenWithStackByArgs(colExpr.Name.Name, tblInfo.Name)
	}

	// check duplicated function on the same column
	for _, idx := range tblInfo.Indices {
		if idx.VectorInfo == nil {
			continue
		}
		if idxCol := idx.FindColumnByName(colInfo.Name.L); idxCol == nil {
			continue
		}
		if idx.VectorInfo.DistanceMetric == distanceMetric {
			return nil, "", dbterror.ErrDupKeyName.GenWithStack(
				fmt.Sprintf("vector index %s function %s already exist on column %s",
					idx.Name, f.FnName, colInfo.Name))
		}
	}
	if colInfo.FieldType.GetFlen() <= 0 {
		return nil, "", errors.Errorf("add vector index can only be defined on fixed-dimension vector columns")
	}

	exprStr, err := restoreFuncCall(f)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	// It's used for build buildIndexColumns.
	idxPart.Column = &ast.ColumnName{Name: colInfo.Name}
	idxPart.Length = types.UnspecifiedLength

	return &model.VectorIndexInfo{
		Dimension:      uint64(colInfo.FieldType.GetFlen()),
		DistanceMetric: distanceMetric,
	}, exprStr, nil
}

func buildInvertedInfoWithCheck(indexPartSpecifications []*ast.IndexPartSpecification,
	tblInfo *model.TableInfo) (*model.InvertedIndexInfo, error) {
	if len(indexPartSpecifications) != 1 {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs("only support one column")
	}

	idxPart := indexPartSpecifications[0]
	if idxPart.Column == nil {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGenByArgs("unsupported no column")
	}
	colInfo := findColumnByName(idxPart.Column.Name.L, tblInfo)
	if colInfo == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(idxPart.Column.Name, tblInfo.Name)
	}

	// check duplicated columnar index on the same column
	for _, idx := range tblInfo.Indices {
		if idx.InvertedInfo == nil {
			continue
		}
		if idxCol := idx.FindColumnByName(colInfo.Name.L); idxCol == nil {
			continue
		}
		if idx.Tp == ast.IndexTypeInverted {
			return nil, dbterror.ErrDupKeyName.GenWithStack(fmt.Sprintf("inverted columnar index %s already exist on column %s", idx.Name, colInfo.Name))
		}
	}

	// It's used for build buildIndexColumns.
	idxPart.Column = &ast.ColumnName{Name: colInfo.Name}
	idxPart.Length = types.UnspecifiedLength

	return model.FieldTypeToInvertedIndexInfo(colInfo.FieldType, colInfo.ID), nil
}

func buildFullTextInfoWithCheck(indexPartSpecifications []*ast.IndexPartSpecification, indexOption *ast.IndexOption,
	tblInfo *model.TableInfo) (*model.FullTextIndexInfo, error) {
	if len(indexPartSpecifications) != 1 {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index only support one column")
	}
	idxPart := indexPartSpecifications[0]
	if idxPart.Column == nil {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index only support one column")
	}
	if idxPart.Length != types.UnspecifiedLength {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index does not support prefix length")
	}
	if idxPart.Desc {
		return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("FULLTEXT index does not support DESC order")
	}
	// The Default parser is STANDARD
	parser := model.FullTextParserTypeStandardV1
	if indexOption != nil && indexOption.ParserName.L != "" {
		parser = model.GetFullTextParserTypeBySQLName(indexOption.ParserName.L)
		if parser == model.FullTextParserTypeInvalid {
			// Actually indexOption must be valid. It is already checked in preprocessor.
			return nil, dbterror.ErrUnsupportedAddColumnarIndex.FastGen("fulltext index must specify a valid parser")
		}
	}
	colInfo := findColumnByName(idxPart.Column.Name.L, tblInfo)
	if colInfo == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(idxPart.Column.Name.L, tblInfo.Name)
	}
	for _, idx := range tblInfo.Indices {
		if idx.FullTextInfo == nil {
			continue
		}
		if idxCol := idx.FindColumnByName(colInfo.Name.L); idxCol == nil {
			continue
		}
		return nil, dbterror.ErrDupKeyName.GenWithStack(
			fmt.Sprintf("fulltext index '%s' already exist on column %s",
				idx.Name, colInfo.Name))
	}
	return &model.FullTextIndexInfo{
		ParserType: parser,
	}, nil
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
func ValidateRenameIndex(from, to ast.CIStr, tbl *model.TableInfo) (ignore bool, err error) {
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

func onRenameIndex(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, from, to, err := checkRenameIndex(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Rename Index"))
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		// Store the mark and enter the next DDL handling loop.
		return updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, false)
	}

	renameIndexes(tblInfo, from, to)
	renameHiddenColumns(tblInfo, from, to)

	if ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func validateAlterIndexVisibility(ctx sessionctx.Context, indexName ast.CIStr, invisible bool, tbl *model.TableInfo) (bool, error) {
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
	if invisible && idx.IsColumnarIndex() {
		return false, dbterror.ErrUnsupportedIndexType.FastGen("INVISIBLE can not be used in %s INDEX", idx.Tp)
	}
	return false, nil
}

func onAlterIndexVisibility(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, from, invisible, err := checkAlterIndexVisibility(jobCtx.metaMut, job)
	if err != nil || tblInfo == nil {
		return ver, errors.Trace(err)
	}

	if job.MultiSchemaInfo != nil && job.MultiSchemaInfo.Revertible {
		job.MarkNonRevertible()
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}

	setIndexVisibility(tblInfo, from, invisible)
	if ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, true); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	return ver, nil
}

func setIndexVisibility(tblInfo *model.TableInfo, name ast.CIStr, invisible bool) {
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

func checkPrimaryKeyNotNull(jobCtx *jobContext, w *worker, job *model.Job,
	tblInfo *model.TableInfo, indexInfo *model.IndexInfo) (warnings []string, err error) {
	if !indexInfo.Primary {
		return nil, nil
	}

	dbInfo, err := checkSchemaExistAndCancelNotExistJob(jobCtx.metaMut, job)
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

	err = modifyColsFromNull2NotNull(
		jobCtx.stepCtx,
		w,
		dbInfo,
		tblInfo,
		nullCols,
		&model.ColumnInfo{Name: ast.NewCIStr("")},
		false,
	)
	if err == nil {
		return nil, nil
	}
	_, err = convertAddIdxJob2RollbackJob(jobCtx, job, tblInfo, []*model.IndexInfo{indexInfo}, err)
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

func checkAndBuildIndexInfo(
	job *model.Job, tblInfo *model.TableInfo,
	columnarIndexType model.ColumnarIndexType, isPK bool, args *model.IndexArg,
) (*model.IndexInfo, error) {
	var err error
	indexInfo := tblInfo.FindIndexByName(args.IndexName.L)
	if indexInfo != nil {
		if indexInfo.State == model.StatePublic {
			err = dbterror.ErrDupKeyName.GenWithStack("index already exist %s", args.IndexName)
			if isPK {
				err = infoschema.ErrMultiplePriKey
			}
			return nil, err
		}
		return indexInfo, nil
	}

	for _, hiddenCol := range args.HiddenCols {
		columnInfo := model.FindColumnInfo(tblInfo.Columns, hiddenCol.Name.L)
		if columnInfo != nil && columnInfo.State == model.StatePublic {
			// We already have a column with the same column name.
			// TODO: refine the error message
			return nil, infoschema.ErrColumnExists.GenWithStackByArgs(hiddenCol.Name)
		}
	}

	if len(args.HiddenCols) > 0 {
		for _, hiddenCol := range args.HiddenCols {
			InitAndAddColumnToTable(tblInfo, hiddenCol)
		}
	}
	if err = checkAddColumnTooManyColumns(len(tblInfo.Columns)); err != nil {
		return nil, errors.Trace(err)
	}
	indexInfo, err = BuildIndexInfo(
		nil,
		tblInfo,
		args.IndexName,
		isPK,
		args.Unique,
		columnarIndexType,
		args.IndexPartSpecifications,
		args.IndexOption,
		model.StateNone,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if isPK {
		if _, err = CheckPKOnGeneratedColumn(tblInfo, args.IndexPartSpecifications); err != nil {
			return nil, err
		}
	}
	indexInfo.ID = AllocateIndexID(tblInfo)
	tblInfo.Indices = append(tblInfo.Indices, indexInfo)
	if err = checkTooManyIndexes(tblInfo.Indices); err != nil {
		return nil, errors.Trace(err)
	}
	// Here we need do this check before set state to `DeleteOnly`,
	// because if hidden columns has been set to `DeleteOnly`,
	// the `DeleteOnly` columns are missing when we do this check.
	if err := checkInvisibleIndexOnPK(tblInfo); err != nil {
		return nil, err
	}
	logutil.DDLLogger().Info("[ddl] run add index job", zap.String("job", job.String()), zap.Reflect("indexInfo", indexInfo))
	return indexInfo, nil
}

func (w *worker) onCreateColumnarIndex(jobCtx *jobContext, job *model.Job) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if err := checkTableTypeForColumnarIndex(tblInfo); err != nil {
		return ver, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	a := args.IndexArgs[0]
	columnarIndexType := a.GetColumnarIndexType()
	if columnarIndexType == model.ColumnarIndexTypeVector {
		a.IndexPartSpecifications[0].Expr, err = generatedexpr.ParseExpression(a.FuncExpr)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		defer func() {
			a.IndexPartSpecifications[0].Expr = nil
		}()
	}

	indexInfo, err := checkAndBuildIndexInfo(job, tblInfo, columnarIndexType, false, a)
	if err != nil {
		return ver, errors.Trace(err)
	}
	originalState := indexInfo.State
	switch indexInfo.State {
	case model.StateNone:
		// none -> delete only
		indexInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		indexInfo.State = model.StateWriteOnly
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		indexInfo.State = model.StateWriteReorganization
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		if job.IsCancelling() {
			return convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), []*model.IndexInfo{indexInfo}, dbterror.ErrCancelledDDLJob)
		}

		// Send sync schema notification to TiFlash.
		if job.SnapshotVer == 0 {
			currVer, err := getValidCurrentVersion(jobCtx.store)
			if err != nil {
				return ver, errors.Trace(err)
			}
			err = infosync.SyncTiFlashTableSchema(jobCtx.stepCtx, tbl.Meta().ID)
			if err != nil {
				return ver, errors.Trace(err)
			}
			job.SnapshotVer = currVer.Ver
			return ver, nil
		}

		// Check the progress of the TiFlash backfill index.
		var done bool
		done, ver, err = w.checkColumnarIndexProcessOnTiFlash(jobCtx, job, tbl, indexInfo)
		if err != nil || !done {
			return ver, err
		}

		indexInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != indexInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}

		finishedArgs := &model.ModifyIndexArgs{
			IndexArgs:    []*model.IndexArg{{IndexID: indexInfo.ID}},
			PartitionIDs: getPartitionIDs(tblInfo),
			OpType:       model.OpAddIndex,
		}
		job.FillFinishedArgs(finishedArgs)

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		logutil.DDLLogger().Info("[ddl] run add vector index job done",
			zap.Int64("ver", ver),
			zap.String("charset", job.Charset),
			zap.String("collation", job.Collate))
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", indexInfo.State)
	}

	return ver, errors.Trace(err)
}

func (w *worker) checkColumnarIndexProcessOnTiFlash(jobCtx *jobContext, job *model.Job, tbl table.Table, indexInfo *model.IndexInfo,
) (done bool, ver int64, err error) {
	err = w.checkColumnarIndexProcess(jobCtx, tbl, job, indexInfo)
	if err != nil {
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			return false, ver, nil
		}
		if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run add columnar index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), []*model.IndexInfo{indexInfo}, err)
		}
		return false, ver, errors.Trace(err)
	}

	return true, ver, nil
}

func (w *worker) checkColumnarIndexProcess(jobCtx *jobContext, tbl table.Table, job *model.Job, index *model.IndexInfo) error {
	waitTimeout := 500 * time.Millisecond
	ticker := time.NewTicker(waitTimeout)
	defer ticker.Stop()
	notAddedRowCnt := int64(-1)
	for {
		select {
		case <-w.ddlCtx.ctx.Done():
			return dbterror.ErrInvalidWorker.GenWithStack("worker is closed")
		case <-ticker.C:
			logutil.DDLLogger().Info(
				"index backfill state running, check columnar index process",
				zap.Stringer("job", job),
				zap.Stringer("index name", index.Name),
				zap.Int64("index ID", index.ID),
				zap.Duration("wait time", waitTimeout),
				zap.Int64("total added row count", job.RowCount),
				zap.Int64("not added row count", notAddedRowCnt))
			return dbterror.ErrWaitReorgTimeout
		default:
		}

		if !w.ddlCtx.isOwner() {
			// If it's not the owner, we will try later, so here just returns an error.
			logutil.DDLLogger().Info("DDL is not the DDL owner", zap.String("ID", w.ddlCtx.uuid))
			return errors.Trace(dbterror.ErrNotOwner)
		}

		isDone, notAddedIndexCnt, addedIndexCnt, err := w.checkColumnarIndexProcessOnce(jobCtx, tbl, index.ID)
		if err != nil {
			return errors.Trace(err)
		}
		notAddedRowCnt = notAddedIndexCnt
		job.RowCount = addedIndexCnt

		if isDone {
			break
		}
	}
	return nil
}

// checkColumnarIndexProcessOnce checks the backfill process of a columnar index from TiFlash once.
func (w *worker) checkColumnarIndexProcessOnce(jobCtx *jobContext, tbl table.Table, indexID int64) (
	isDone bool, notAddedIndexCnt, addedIndexCnt int64, err error) {
	failpoint.Inject("MockCheckColumnarIndexProcess", func(val failpoint.Value) {
		if valInt, ok := val.(int); ok {
			logutil.DDLLogger().Info("MockCheckColumnarIndexProcess", zap.Int("val", valInt))
			if valInt < 0 {
				failpoint.Return(false, 0, 0, dbterror.ErrTiFlashBackfillIndex.FastGenByArgs("mock a check error"))
			} else if valInt == 0 {
				failpoint.Return(false, 0, 0, nil)
			} else {
				failpoint.Return(true, 0, int64(valInt), nil)
			}
		}
	})

	sql := fmt.Sprintf("select rows_stable_not_indexed, rows_stable_indexed, error_message from information_schema.tiflash_indexes where table_id = %d and index_id = %d;",
		tbl.Meta().ID, indexID)
	rows, err := w.sess.Execute(jobCtx.stepCtx, sql, "add_vector_index_check_result")
	if err != nil || len(rows) == 0 {
		return false, 0, 0, errors.Trace(err)
	}

	// Get and process info from multiple TiFlash nodes.
	errMsg := ""
	for _, row := range rows {
		notAddedIndexCnt += row.GetInt64(0)
		addedIndexCnt += row.GetInt64(1)
		errMsg = row.GetString(2)
		if len(errMsg) != 0 {
			err = dbterror.ErrTiFlashBackfillIndex.FastGenByArgs(errMsg)
			break
		}
	}
	if err != nil {
		return false, 0, 0, errors.Trace(err)
	}
	if notAddedIndexCnt != 0 {
		return false, 0, 0, nil
	}

	return true, notAddedIndexCnt, addedIndexCnt, nil
}

func (w *worker) onCreateIndex(jobCtx *jobContext, job *model.Job, isPK bool) (ver int64, err error) {
	// Handle the rolling back job.
	if job.IsRollingback() {
		ver, err = onDropIndex(jobCtx, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return ver, nil
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(jobCtx.metaMut, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	if tblInfo.TableCacheStatusType != model.TableCacheStatusDisable {
		return ver, errors.Trace(dbterror.ErrOptOnCacheTable.GenWithStackByArgs("Create Index"))
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	allIndexInfos := make([]*model.IndexInfo, 0, len(args.IndexArgs))
	for _, arg := range args.IndexArgs {
		indexInfo, err := checkAndBuildIndexInfo(job, tblInfo, model.ColumnarIndexTypeNA, job.Type == model.ActionAddPrimaryKey, arg)
		if err != nil {
			job.State = model.JobStateCancelled
			return ver, errors.Trace(err)
		}
		allIndexInfos = append(allIndexInfos, indexInfo)
	}

	originalState := allIndexInfos[0].State

SwitchIndexState:
	switch allIndexInfos[0].State {
	case model.StateNone:
		// none -> delete only
		var reorgTp model.ReorgType
		reorgTp, err = pickBackfillType(job)
		if err != nil {
			if !isRetryableJobError(err, job.ErrorCount) {
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
		err = preSplitIndexRegions(jobCtx.stepCtx, w.sess.Context, jobCtx.store, tblInfo, allIndexInfos, job.ReorgMeta, args)
		if err != nil {
			if !isRetryableJobError(err, job.ErrorCount) {
				job.State = model.JobStateCancelled
			}
			return ver, err
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
			moveAndUpdateHiddenColumnsToPublic(tblInfo, indexInfo)
		}
		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateDeleteOnly
	case model.StateDeleteOnly:
		// delete only -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
			_, err = checkPrimaryKeyNotNull(jobCtx, w, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, err
		}
		job.SchemaState = model.StateWriteOnly
	case model.StateWriteOnly:
		// write only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteReorganization
			_, err = checkPrimaryKeyNotNull(jobCtx, w, job, tblInfo, indexInfo)
			if err != nil {
				break SwitchIndexState
			}
		}

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteReorganization)
		if err != nil {
			return ver, err
		}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		job.SchemaState = model.StateWriteReorganization
	case model.StateWriteReorganization:
		// reorganization -> public
		tbl, err := getTable(jobCtx.getAutoIDRequirement(), schemaID, tblInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}

		var done bool
		if job.MultiSchemaInfo != nil {
			done, ver, err = doReorgWorkForCreateIndexMultiSchema(w, jobCtx, job, tbl, allIndexInfos)
		} else {
			done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, allIndexInfos)
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

		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StatePublic)
		if err != nil {
			return ver, errors.Trace(err)
		}

		a := &model.ModifyIndexArgs{
			PartitionIDs: getPartitionIDs(tbl.Meta()),
			OpType:       model.OpAddIndex,
		}
		for _, indexInfo := range allIndexInfos {
			a.IndexArgs = append(a.IndexArgs, &model.IndexArg{
				IndexID:  indexInfo.ID,
				IfExist:  false,
				IsGlobal: indexInfo.Global,
			})
		}
		job.FillFinishedArgs(a)

		addIndexEvent := notifier.NewAddIndexEvent(tblInfo, allIndexInfos)
		err2 := asyncNotifyEvent(jobCtx, addIndexEvent, job, noSubJob, w.sess)
		if err2 != nil {
			return ver, errors.Trace(err2)
		}

		// Finish this job.
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
		logutil.DDLLogger().Info("run add index job done",
			zap.String("charset", job.Charset),
			zap.String("collation", job.Collate))
	default:
		err = dbterror.ErrInvalidDDLState.GenWithStackByArgs("index", allIndexInfos[0].State)
	}

	return ver, errors.Trace(err)
}

func checkIfTableReorgWorkCanSkip(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
	job *model.Job,
) bool {
	if job.SnapshotVer != 0 {
		// Reorg work has begun.
		return false
	}
	txn, err := sessCtx.Txn(false)
	validTxn := err == nil && txn != nil && txn.Valid()
	intest.Assert(validTxn)
	if !validTxn {
		logutil.DDLLogger().Warn("check if table is empty failed", zap.Error(err))
		return false
	}
	startTS := txn.StartTS()
	ctx := NewReorgContext()
	ctx.resourceGroupName = job.ReorgMeta.ResourceGroupName
	ctx.setDDLLabelForTopSQL(job.Query)
	return checkIfTableIsEmpty(ctx, store, tbl, startTS)
}

func checkIfTableIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.Table,
	startTS uint64,
) bool {
	if pTbl, ok := tbl.(table.PartitionedTable); ok {
		for _, pid := range pTbl.GetAllPartitionIDs() {
			pTbl := pTbl.GetPartition(pid)
			if !checkIfPhysicalTableIsEmpty(ctx, store, pTbl, startTS) {
				return false
			}
		}
		return true
	}
	//nolint:forcetypeassert
	plainTbl := tbl.(table.PhysicalTable)
	return checkIfPhysicalTableIsEmpty(ctx, store, plainTbl, startTS)
}

func checkIfPhysicalTableIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.PhysicalTable,
	startTS uint64,
) bool {
	hasRecord, err := existsTableRow(ctx, store, tbl, startTS)
	intest.Assert(err == nil)
	if err != nil {
		logutil.DDLLogger().Info("check if table is empty failed", zap.Error(err))
		return false
	}
	return !hasRecord
}

func checkIfTempIndexReorgWorkCanSkip(
	store kv.Storage,
	sessCtx sessionctx.Context,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
	job *model.Job,
) bool {
	failpoint.Inject("skipReorgWorkForTempIndex", func(val failpoint.Value) {
		if v, ok := val.(bool); ok {
			failpoint.Return(v)
		}
	})
	if job.SnapshotVer != 0 {
		// Reorg work has begun.
		return false
	}
	txn, err := sessCtx.Txn(false)
	validTxn := err == nil && txn != nil && txn.Valid()
	intest.Assert(validTxn)
	if !validTxn {
		logutil.DDLLogger().Warn("check if temp index is empty failed", zap.Error(err))
		return false
	}
	startTS := txn.StartTS()
	ctx := NewReorgContext()
	ctx.resourceGroupName = job.ReorgMeta.ResourceGroupName
	ctx.setDDLLabelForTopSQL(job.Query)
	firstIdxID := allIndexInfos[0].ID
	lastIdxID := allIndexInfos[len(allIndexInfos)-1].ID
	var globalIdxIDs []int64
	for _, idxInfo := range allIndexInfos {
		if idxInfo.Global {
			globalIdxIDs = append(globalIdxIDs, idxInfo.ID)
		}
	}
	return checkIfTempIndexIsEmpty(ctx, store, tbl, firstIdxID, lastIdxID, globalIdxIDs, startTS)
}

func checkIfTempIndexIsEmpty(
	ctx *ReorgContext,
	store kv.Storage,
	tbl table.Table,
	firstIdxID, lastIdxID int64,
	globalIdxIDs []int64,
	startTS uint64,
) bool {
	tblMetaID := tbl.Meta().ID
	if pTbl, ok := tbl.(table.PartitionedTable); ok {
		for _, pid := range pTbl.GetAllPartitionIDs() {
			if !checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, pid, firstIdxID, lastIdxID, startTS) {
				return false
			}
		}
		for _, globalIdxID := range globalIdxIDs {
			if !checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, tblMetaID, globalIdxID, globalIdxID, startTS) {
				return false
			}
		}
		return true
	}
	return checkIfTempIndexIsEmptyForPhysicalTable(ctx, store, tblMetaID, firstIdxID, lastIdxID, startTS)
}

func checkIfTempIndexIsEmptyForPhysicalTable(
	ctx *ReorgContext,
	store kv.Storage,
	pid int64,
	firstIdxID, lastIdxID int64,
	startTS uint64,
) bool {
	start, end := encodeTempIndexRange(pid, firstIdxID, lastIdxID)
	foundKey := false
	idxPrefix := tablecodec.GenTableIndexPrefix(pid)
	err := iterateSnapshotKeys(ctx, store, kv.PriorityLow, idxPrefix, startTS, start, end,
		func(_ kv.Handle, _ kv.Key, _ []byte) (more bool, err error) {
			foundKey = true
			return false, nil
		})
	intest.Assert(err == nil)
	if err != nil {
		logutil.DDLLogger().Info("check if temp index is empty failed", zap.Error(err))
		return false
	}
	return !foundKey
}

// pickBackfillType determines which backfill process will be used. The result is
// both stored in job.ReorgMeta.ReorgTp and returned.
func pickBackfillType(job *model.Job) (model.ReorgType, error) {
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
		if err := ingest.LitDiskRoot.PreCheckUsage(); err != nil {
			logutil.DDLIngestLogger().Info("ingest backfill is not available", zap.Error(err))
			return model.ReorgTypeNone, err
		}
		job.ReorgMeta.ReorgTp = model.ReorgTypeLitMerge
		return model.ReorgTypeLitMerge, nil
	}
	// The lightning environment is unavailable, but we can still use the txn-merge backfill.
	logutil.DDLLogger().Info("fallback to txn-merge backfill process",
		zap.Bool("lightning env initialized", ingest.LitInitialized))
	job.ReorgMeta.ReorgTp = model.ReorgTypeTxnMerge
	return model.ReorgTypeTxnMerge, nil
}

func loadCloudStorageURI(w *worker, job *model.Job) {
	jc := w.jobContext(job.ID, job.ReorgMeta)
	jc.cloudStorageURI = vardef.CloudStorageURI.Load()
	job.ReorgMeta.UseCloudStorage = len(jc.cloudStorageURI) > 0 && job.ReorgMeta.IsDistReorg
}

func doReorgWorkForCreateIndexMultiSchema(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	if job.MultiSchemaInfo.Revertible {
		done, ver, err = doReorgWorkForCreateIndex(w, jobCtx, job, tbl, allIndexInfos)
		if done {
			job.MarkNonRevertible()
			if err == nil {
				ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
			}
		}
		// We need another round to wait for all the others sub-jobs to finish.
		return false, ver, err
	}
	return true, ver, err
}

func doReorgWorkForCreateIndex(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
) (done bool, ver int64, err error) {
	var reorgTp model.ReorgType
	reorgTp, err = pickBackfillType(job)
	if err != nil {
		return false, ver, err
	}
	if !reorgTp.NeedMergeProcess() {
		skipReorg := checkIfTableReorgWorkCanSkip(w.store, w.sess.Session(), tbl, job)
		if skipReorg {
			logutil.DDLLogger().Info("table is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
			return true, ver, nil
		}
		return runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	}
	switch allIndexInfos[0].BackfillState {
	case model.BackfillStateRunning:
		skipReorg := checkIfTableReorgWorkCanSkip(w.store, w.sess.Session(), tbl, job)
		if !skipReorg {
			logutil.DDLLogger().Info("index backfill state running",
				zap.Int64("job ID", job.ID), zap.String("table", tbl.Meta().Name.O),
				zap.Bool("ingest mode", reorgTp == model.ReorgTypeLitMerge),
				zap.String("index", allIndexInfos[0].Name.O))
			switch reorgTp {
			case model.ReorgTypeLitMerge:
				if job.ReorgMeta.IsDistReorg {
					done, ver, err = runIngestReorgJobDist(w, jobCtx, job, tbl, allIndexInfos)
				} else {
					done, ver, err = runIngestReorgJob(w, jobCtx, job, tbl, allIndexInfos)
				}
			case model.ReorgTypeTxnMerge:
				done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
			}
			if err != nil || !done {
				return false, ver, errors.Trace(err)
			}
		} else {
			failpoint.InjectCall("afterCheckTableReorgCanSkip")
			logutil.DDLLogger().Info("table is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateReadyToMerge
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		failpoint.InjectCall("afterBackfillStateRunningDone", job)
		return false, ver, errors.Trace(err)
	case model.BackfillStateReadyToMerge:
		failpoint.Inject("mockDMLExecutionStateBeforeMerge", func(_ failpoint.Value) {
			if MockDMLExecutionStateBeforeMerge != nil {
				MockDMLExecutionStateBeforeMerge()
			}
		})
		failpoint.InjectCall("BeforeBackfillMerge")
		logutil.DDLLogger().Info("index backfill state ready to merge",
			zap.Int64("job ID", job.ID),
			zap.String("table", tbl.Meta().Name.O),
			zap.String("index", allIndexInfos[0].Name.O))
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateMerging
		}
		job.SnapshotVer = 0 // Reset the snapshot version for merge index reorg.
		ver, err = updateVersionAndTableInfo(jobCtx, job, tbl.Meta(), true)
		return false, ver, errors.Trace(err)
	case model.BackfillStateMerging:
		skipReorg := checkIfTempIndexReorgWorkCanSkip(w.store, w.sess.Session(), tbl, allIndexInfos, job)
		if !skipReorg {
			done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, true)
			if !done {
				return false, ver, err
			}
		} else {
			failpoint.InjectCall("afterCheckTempIndexReorgCanSkip")
			logutil.DDLLogger().Info("temp index is empty, skipping reorg work",
				zap.Int64("jobID", job.ID),
				zap.String("table", tbl.Meta().Name.O))
		}
		for _, indexInfo := range allIndexInfos {
			indexInfo.BackfillState = model.BackfillStateInapplicable // Prevent double-write on this index.
		}
		return true, ver, err
	default:
		return false, 0, dbterror.ErrInvalidDDLState.GenWithStackByArgs("backfill", allIndexInfos[0].BackfillState)
	}
}

func runIngestReorgJobDist(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	if err != nil {
		return false, ver, errors.Trace(err)
	}

	if !done {
		return false, ver, nil
	}

	return true, ver, nil
}

func runIngestReorgJob(w *worker, jobCtx *jobContext, job *model.Job,
	tbl table.Table, allIndexInfos []*model.IndexInfo) (done bool, ver int64, err error) {
	done, ver, err = runReorgJobAndHandleErr(w, jobCtx, job, tbl, allIndexInfos, false)
	if err != nil {
		if kv.ErrKeyExists.Equal(err) {
			logutil.DDLLogger().Warn("import index duplicate key, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
		} else if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run reorg job failed, convert job to rollback",
				zap.String("job", job.String()), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
		} else {
			logutil.DDLLogger().Warn("run add index ingest job error", zap.Error(err))
		}
		return false, ver, errors.Trace(err)
	}
	failpoint.InjectCall("afterRunIngestReorgJob", job, done)
	return done, ver, nil
}

func isRetryableJobError(err error, jobErrCnt int64) bool {
	if jobErrCnt+1 >= vardef.GetDDLErrorCountLimit() {
		return false
	}
	return isRetryableError(err)
}

func isRetryableError(err error) bool {
	errMsg := err.Error()
	for _, m := range dbterror.ReorgRetryableErrMsgs {
		if strings.Contains(errMsg, m) {
			return true
		}
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

func runReorgJobAndHandleErr(
	w *worker,
	jobCtx *jobContext,
	job *model.Job,
	tbl table.Table,
	allIndexInfos []*model.IndexInfo,
	mergingTmpIdx bool,
) (done bool, ver int64, err error) {
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
	dbInfo, err := jobCtx.metaMut.GetDatabase(job.SchemaID)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	reorgInfo, err := getReorgInfo(jobCtx.oldDDLCtx.jobContext(job.ID, job.ReorgMeta), jobCtx, rh, job, dbInfo, tbl, elements, mergingTmpIdx)
	if err != nil || reorgInfo == nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		return false, ver, errors.Trace(err)
	}
	err = overwriteReorgInfoFromGlobalCheckpoint(w, rh.s, job, reorgInfo)
	if err != nil {
		return false, ver, errors.Trace(err)
	}
	err = w.runReorgJob(jobCtx, reorgInfo, tbl.Meta(), func() (addIndexErr error) {
		defer util.Recover(metrics.LabelDDL, "onCreateIndex",
			func() {
				addIndexErr = dbterror.ErrCancelledDDLJob.GenWithStack("add table `%v` index `%v` panic", tbl.Meta().Name, allIndexInfos[0].Name)
			}, false)
		return w.addTableIndex(jobCtx, tbl, reorgInfo)
	})
	if err != nil {
		if dbterror.ErrPausedDDLJob.Equal(err) {
			return false, ver, nil
		}
		if dbterror.ErrWaitReorgTimeout.Equal(err) {
			// if timeout, we should return, check for the owner and re-wait job done.
			return false, ver, nil
		}
		// TODO(tangenta): get duplicate column and match index.
		err = ingest.TryConvertToKeyExistsErr(err, allIndexInfos[0], tbl.Meta())
		if !isRetryableJobError(err, job.ErrorCount) {
			logutil.DDLLogger().Warn("run add index job failed, convert job to rollback", zap.Stringer("job", job), zap.Error(err))
			ver, err = convertAddIdxJob2RollbackJob(jobCtx, job, tbl.Meta(), allIndexInfos, err)
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

func onDropIndex(jobCtx *jobContext, job *model.Job) (ver int64, _ error) {
	tblInfo, allIndexInfos, ifExists, err := checkDropIndex(jobCtx.infoCache, jobCtx.metaMut, job)
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
		return updateVersionAndTableInfo(jobCtx, job, tblInfo, false)
	}

	originalState := allIndexInfos[0].State
	switch allIndexInfos[0].State {
	case model.StatePublic:
		// public -> write only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateWriteOnly
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateWriteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateWriteOnly:
		// write only -> delete only
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteOnly
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteOnly)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteOnly:
		// delete only -> reorganization
		for _, indexInfo := range allIndexInfos {
			indexInfo.State = model.StateDeleteReorganization
		}
		ver, err = updateVersionAndTableInfo(jobCtx, job, tblInfo, originalState != model.StateDeleteReorganization)
		if err != nil {
			return ver, errors.Trace(err)
		}
	case model.StateDeleteReorganization:
		// reorganization -> absent
		isColumnarIndex := false
		indexIDs := make([]int64, 0, len(allIndexInfos))
		for _, indexInfo := range allIndexInfos {
			if indexInfo.IsColumnarIndex() {
				isColumnarIndex = true
			}
			indexInfo.State = model.StateNone
			// Set column index flag.
			DropIndexColumnFlag(tblInfo, indexInfo)
			RemoveDependentHiddenColumns(tblInfo, indexInfo)
			removeIndexInfo(tblInfo, indexInfo)
			indexIDs = append(indexIDs, indexInfo.ID)
		}

		failpoint.Inject("mockExceedErrorLimit", func(val failpoint.Value) {
			//nolint:forcetypeassert
			if val.(bool) {
				panic("panic test in cancelling add index")
			}
		})

		ver, err = updateVersionAndTableInfoWithCheck(jobCtx, job, tblInfo, originalState != model.StateNone)
		if err != nil {
			return ver, errors.Trace(err)
		}

		if isColumnarIndex {
			// Send sync schema notification to TiFlash.
			if err := infosync.SyncTiFlashTableSchema(jobCtx.stepCtx, tblInfo.ID); err != nil {
				logutil.DDLLogger().Warn("run drop column index but syncing TiFlash schema failed", zap.Error(err))
			}
		}

		// Finish this job.
		if job.IsRollingback() {
			dropArgs, err := model.GetFinishedModifyIndexArgs(job)
			job.FinishTableJob(model.JobStateRollbackDone, model.StateNone, ver, tblInfo)
			if err != nil {
				return ver, errors.Trace(err)
			}

			// Convert drop index args to finished add index args again to finish add index job.
			// Only rolled back add index jobs will get here, since drop index jobs can only be cancelled, not rolled back.
			addIndexArgs := &model.ModifyIndexArgs{
				PartitionIDs: dropArgs.PartitionIDs,
				OpType:       model.OpAddIndex,
			}
			for i, indexID := range indexIDs {
				addIndexArgs.IndexArgs = append(addIndexArgs.IndexArgs,
					&model.IndexArg{
						IndexID: indexID,
						IfExist: dropArgs.IndexArgs[i].IfExist,
					})
			}
			job.FillFinishedArgs(addIndexArgs)
		} else {
			// the partition ids were append by convertAddIdxJob2RollbackJob, it is weird, but for the compatibility,
			// we should keep appending the partitions in the convertAddIdxJob2RollbackJob.
			job.FinishTableJob(model.JobStateDone, model.StateNone, ver, tblInfo)
			// Global index key has t{tableID}_ prefix.
			// Assign partitionIDs empty to guarantee correct prefix in insertJobIntoDeleteRangeTable.
			dropArgs, err := model.GetDropIndexArgs(job)
			dropArgs.OpType = model.OpDropIndex
			if err != nil {
				return ver, errors.Trace(err)
			}
			dropArgs.IndexArgs[0].IndexID = indexIDs[0]
			dropArgs.IndexArgs[0].IsColumnar = allIndexInfos[0].IsColumnarIndex()
			dropArgs.IndexArgs[0].ColumnarIndexType = allIndexInfos[0].GetColumnarIndexType()
			if !allIndexInfos[0].Global {
				dropArgs.PartitionIDs = getPartitionIDs(tblInfo)
			}
			job.FillFinishedArgs(dropArgs)
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
	tblInfo.Indices = slices.Delete(tblInfo.Indices, offset, offset+1)
}

func checkDropIndex(infoCache *infoschema.InfoCache, t *meta.Mutator, job *model.Job) (*model.TableInfo, []*model.IndexInfo, bool /* ifExists */, error) {
	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}

	args, err := model.GetDropIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, nil, false, errors.Trace(err)
	}

	indexInfos := make([]*model.IndexInfo, 0, len(args.IndexArgs))
	for _, idxArg := range args.IndexArgs {
		indexInfo := tblInfo.FindIndexByName(idxArg.IndexName.L)
		if indexInfo == nil {
			job.State = model.JobStateCancelled
			return nil, nil, idxArg.IfExist, dbterror.ErrCantDropFieldOrKey.GenWithStack("index %s doesn't exist", idxArg.IndexName)
		}

		// Check that drop primary index will not cause invisible implicit primary index.
		if err := checkInvisibleIndexesOnPK(tblInfo, []*model.IndexInfo{indexInfo}, job); err != nil {
			job.State = model.JobStateCancelled
			return nil, nil, false, errors.Trace(err)
		}

		// Double check for drop index needed in foreign key.
		if err := checkIndexNeededInForeignKeyInOwner(infoCache, job, job.SchemaName, tblInfo, indexInfo); err != nil {
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

func checkRenameIndex(t *meta.Mutator, job *model.Job) (tblInfo *model.TableInfo, from, to ast.CIStr, err error) {
	schemaID := job.SchemaID
	tblInfo, err = GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, from, to, errors.Trace(err)
	}

	args, err := model.GetModifyIndexArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, from, to, errors.Trace(err)
	}
	from, to = args.GetRenameIndexes()

	// Double check. See function `RenameIndex` in executor.go
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

func checkAlterIndexVisibility(t *meta.Mutator, job *model.Job) (*model.TableInfo, ast.CIStr, bool, error) {
	var (
		indexName ast.CIStr
		invisible bool
	)

	schemaID := job.SchemaID
	tblInfo, err := GetTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return nil, indexName, invisible, errors.Trace(err)
	}

	args, err := model.GetAlterIndexVisibilityArgs(job)
	if err != nil {
		job.State = model.JobStateCancelled
		return nil, indexName, invisible, errors.Trace(err)
	}
	indexName, invisible = args.IndexName, args.Invisible

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
	return ddlutil.GenKeyExistsErr(key, value, idxInfo, tblInfo)
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
	return nil
}

func getLocalWriterConfig(indexCnt, writerCnt int) *backend.LocalWriterConfig {
	writerCfg := &backend.LocalWriterConfig{}
	// avoid unit test panic
	memRoot := ingest.LitMemRoot
	if memRoot == nil {
		return writerCfg
	}

	// leave some room for objects overhead
	availMem := memRoot.MaxMemoryQuota() - memRoot.CurrentUsage() - int64(10*size.MB)
	memLimitPerWriter := availMem / int64(indexCnt) / int64(writerCnt)
	memLimitPerWriter = min(memLimitPerWriter, litconfig.DefaultLocalWriterMemCacheSize)
	writerCfg.Local.MemCacheSize = memLimitPerWriter
	return writerCfg
}

func writeChunk(
	ctx context.Context,
	writers []ingest.Writer,
	indexes []table.Index,
	copCtx copr.CopContext,
	loc *time.Location,
	errCtx errctx.Context,
	writeStmtBufs *variable.WriteStmtBufs,
	copChunk *chunk.Chunk,
	tblInfo *model.TableInfo,
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
		handleDataBuf := ExtractDatumByOffsets(ectx, row, c.HandleOutputOffsets, c.ExprColumnInfos, handleDataBuf)
		if restore {
			// restoreDataBuf should not truncate index values.
			for i, datum := range handleDataBuf {
				restoreDataBuf[i] = *datum.Clone()
			}
		}
		h, err := BuildHandle(handleDataBuf, c.TableInfo, c.PrimaryKeyInfo, loc, errCtx)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		for i, index := range indexes {
			idxID := index.Meta().ID
			idxDataBuf = ExtractDatumByOffsets(ectx,
				row, copCtx.IndexColumnOutputOffsets(idxID), c.ExprColumnInfos, idxDataBuf)
			idxData := idxDataBuf[:len(index.Meta().Columns)]
			var rsData []types.Datum
			if needRestoreForIndexes[i] {
				rsData = getRestoreData(c.TableInfo, copCtx.IndexInfo(idxID), c.PrimaryKeyInfo, restoreDataBuf)
			}
			err = writeOneKV(ctx, writers[i], index, loc, errCtx, writeStmtBufs, idxData, rsData, h)
			if err != nil {
				err = ingest.TryConvertToKeyExistsErr(err, index.Meta(), tblInfo)
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

func writeOneKV(
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
			// TODO: For global index, lock the correct key?! Currently it locks the partition (phyTblID) and the handle or actual key?
			// but should really lock the table's ID + key col(s)
			err := txn.LockKeys(context.Background(), new(kv.LockCtx), idxRecord.key)
			if err != nil {
				return errors.Trace(err)
			}

			handle, err := w.indexes[i%len(w.indexes)].Create(
				w.tblCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData,
				table.WithIgnoreAssertion,
				table.FromBackfill,
				// Constrains is already checked in batchCheckUniqueKey
				table.DupKeyCheckSkip,
			)
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

func (w *worker) addPhysicalTableIndex(
	ctx context.Context,
	t table.PhysicalTable,
	reorgInfo *reorgInfo,
) error {
	if reorgInfo.mergingTmpIdx {
		logutil.DDLLogger().Info("start to merge temp index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
		return w.writePhysicalTableRecord(ctx, w.sessPool, t, typeAddIndexMergeTmpWorker, reorgInfo)
	}
	logutil.DDLLogger().Info("start to add table index", zap.Stringer("job", reorgInfo.Job), zap.Stringer("reorgInfo", reorgInfo))
	m := metrics.RegisterLightningCommonMetricsForDDL(reorgInfo.ID)
	ctx = lightningmetric.WithCommonMetric(ctx, m)
	defer func() {
		metrics.UnregisterLightningCommonMetricsForDDL(reorgInfo.ID, m)
	}()
	return w.writePhysicalTableRecord(ctx, w.sessPool, t, typeAddIndexWorker, reorgInfo)
}

// addTableIndex handles the add index reorganization state for a table.
func (w *worker) addTableIndex(
	jobCtx *jobContext,
	t table.Table,
	reorgInfo *reorgInfo,
) error {
	ctx := jobCtx.stepCtx
	// TODO: Support typeAddIndexMergeTmpWorker.
	if reorgInfo.ReorgMeta.IsDistReorg && !reorgInfo.mergingTmpIdx {
		if reorgInfo.ReorgMeta.ReorgTp == model.ReorgTypeLitMerge {
			err := w.executeDistTask(jobCtx, t, reorgInfo)
			if err != nil {
				return err
			}
			if reorgInfo.ReorgMeta.UseCloudStorage {
				// When adding unique index by global sort, it detects duplicate keys in each step.
				// A duplicate key must be detected before, so we can skip the check bellow.
				return nil
			}
			return checkDuplicateForUniqueIndex(ctx, t, reorgInfo, w.store)
		}
	}

	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish, ok bool
		for !finish {
			var p table.PhysicalTable
			if tbl.Meta().ID == reorgInfo.PhysicalTableID {
				p, ok = t.(table.PhysicalTable) // global index
				if !ok {
					return fmt.Errorf("unexpected error, can't cast %T to table.PhysicalTable", t)
				}
			} else {
				p = tbl.GetPartition(reorgInfo.PhysicalTableID)
				if p == nil {
					return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
				}
			}
			err = w.addPhysicalTableIndex(ctx, p, reorgInfo)
			if err != nil {
				break
			}

			finish, err = updateReorgInfo(w.sessPool, tbl, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
			failpoint.InjectCall("afterUpdatePartitionReorgInfo", reorgInfo.Job)
			// Every time we finish a partition, we update the progress of the job.
			if rc := w.getReorgCtx(reorgInfo.Job.ID); rc != nil {
				reorgInfo.Job.SetRowCount(rc.getRowCount())
			}
		}
	} else {
		//nolint:forcetypeassert
		phyTbl := t.(table.PhysicalTable)
		err = w.addPhysicalTableIndex(ctx, phyTbl, reorgInfo)
	}
	return errors.Trace(err)
}

func checkDuplicateForUniqueIndex(ctx context.Context, t table.Table, reorgInfo *reorgInfo, store kv.Storage) (err error) {
	var (
		backendCtx ingest.BackendCtx
		cfg        *local.BackendConfig
		backend    *local.Backend
	)
	defer func() {
		if backendCtx != nil {
			backendCtx.Close()
		}
		if backend != nil {
			backend.Close()
		}
	}()
	for _, elem := range reorgInfo.elements {
		indexInfo := model.FindIndexInfoByID(t.Meta().Indices, elem.ID)
		if indexInfo == nil {
			return errors.New("unexpected error, can't find index info")
		}
		if indexInfo.Unique {
			ctx := tidblogutil.WithCategory(ctx, "ddl-ingest")
			if backendCtx == nil {
				if config.GetGlobalConfig().Store == config.StoreTypeTiKV {
					cfg, backend, err = ingest.CreateLocalBackend(ctx, store, reorgInfo.Job, true, 0)
					if err != nil {
						return errors.Trace(err)
					}
				}
				backendCtx, err = ingest.NewBackendCtxBuilder(ctx, store, reorgInfo.Job).
					ForDuplicateCheck().
					Build(cfg, backend)
				if err != nil {
					return err
				}
			}
			err = backendCtx.CollectRemoteDuplicateRows(indexInfo.ID, t)
			failpoint.Inject("mockCheckDuplicateForUniqueIndexError", func(_ failpoint.Value) {
				err = context.DeadlineExceeded
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *worker) executeDistTask(jobCtx *jobContext, t table.Table, reorgInfo *reorgInfo) error {
	if reorgInfo.mergingTmpIdx {
		return errors.New("do not support merge index")
	}

	stepCtx := jobCtx.stepCtx
	taskType := proto.Backfill
	taskKey := fmt.Sprintf("ddl/%s/%d", taskType, reorgInfo.Job.ID)
	g, ctx := errgroup.WithContext(w.workCtx)
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
	task, err := taskManager.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil && !goerrors.Is(err, storage.ErrTaskNotFound) {
		return err
	}

	var (
		taskID                                            int64
		lastConcurrency, lastBatchSize, lastMaxWriteSpeed int
	)
	if task != nil {
		// It's possible that the task state is succeed but the ddl job is paused.
		// When task in succeed state, we can skip the dist task execution/scheduling process.
		if task.State == proto.TaskStateSucceed {
			w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
			logutil.DDLLogger().Info(
				"task succeed, start to resume the ddl job",
				zap.String("task-key", taskKey))
			return nil
		}
		taskMeta := &BackfillTaskMeta{}
		if err := json.Unmarshal(task.Meta, taskMeta); err != nil {
			return errors.Trace(err)
		}
		taskID = task.ID
		lastConcurrency = task.Concurrency
		lastBatchSize = taskMeta.Job.ReorgMeta.GetBatchSize()
		lastMaxWriteSpeed = taskMeta.Job.ReorgMeta.GetMaxWriteSpeed()
		g.Go(func() error {
			defer close(done)
			backoffer := backoff.NewExponential(scheduler.RetrySQLInterval, 2, scheduler.RetrySQLMaxInterval)
			err := handle.RunWithRetry(ctx, scheduler.RetrySQLTimes, backoffer, logutil.DDLLogger(),
				func(context.Context) (bool, error) {
					return true, handle.ResumeTask(w.workCtx, taskKey)
				},
			)
			if err != nil {
				return err
			}
			err = handle.WaitTaskDoneOrPaused(ctx, task.ID)
			if err := w.isReorgRunnable(stepCtx, true); err != nil {
				if dbterror.ErrPausedDDLJob.Equal(err) {
					logutil.DDLLogger().Warn("job paused by user", zap.Error(err))
					return dbterror.ErrPausedDDLJob.GenWithStackByArgs(reorgInfo.Job.ID)
				}
			}
			return err
		})
	} else {
		job := reorgInfo.Job
		workerCntLimit := job.ReorgMeta.GetConcurrency()
		concurrency, err := adjustConcurrency(ctx, taskManager, workerCntLimit)
		if err != nil {
			return err
		}
		logutil.DDLLogger().Info("adjusted add-index task concurrency",
			zap.Int("worker-cnt", workerCntLimit), zap.Int("task-concurrency", concurrency),
			zap.String("task-key", taskKey))
		rowSize := estimateTableRowSize(w.workCtx, w.store, w.sess.GetRestrictedSQLExecutor(), t)
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

		targetScope := reorgInfo.ReorgMeta.TargetScope
		maxNodeCnt := reorgInfo.ReorgMeta.MaxNodeCount
		task, err := handle.SubmitTask(ctx, taskKey, taskType, concurrency, targetScope, maxNodeCnt, metaData)
		if err != nil {
			return err
		}

		taskID = task.ID
		lastConcurrency = concurrency
		lastBatchSize = taskMeta.Job.ReorgMeta.GetBatchSize()
		lastMaxWriteSpeed = taskMeta.Job.ReorgMeta.GetMaxWriteSpeed()

		g.Go(func() error {
			defer close(done)
			err := handle.WaitTaskDoneOrPaused(ctx, task.ID)
			failpoint.InjectCall("pauseAfterDistTaskFinished")
			if err := w.isReorgRunnable(stepCtx, true); err != nil {
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
				w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
				return nil
			case <-checkFinishTk.C:
				if err = w.isReorgRunnable(stepCtx, true); err != nil {
					if dbterror.ErrPausedDDLJob.Equal(err) {
						if err = handle.PauseTask(w.workCtx, taskKey); err != nil {
							logutil.DDLLogger().Error("pause task error", zap.String("task_key", taskKey), zap.Error(err))
							continue
						}
						failpoint.InjectCall("syncDDLTaskPause")
					}
					if !dbterror.ErrCancelledDDLJob.Equal(err) {
						return errors.Trace(err)
					}
					if err = handle.CancelTask(w.workCtx, taskKey); err != nil {
						logutil.DDLLogger().Error("cancel task error", zap.String("task_key", taskKey), zap.Error(err))
						// continue to cancel task.
						continue
					}
				}
			case <-updateRowCntTk.C:
				w.updateDistTaskRowCount(taskKey, reorgInfo.Job.ID)
			}
		}
	})

	g.Go(func() error {
		modifyTaskParamLoop(ctx, jobCtx.sysTblMgr, taskManager, done,
			reorgInfo.Job.ID, taskID, lastConcurrency, lastBatchSize, lastMaxWriteSpeed)
		return nil
	})

	err = g.Wait()
	return err
}

// Note: we can achieve the same effect by calling ModifyTaskByID directly inside
// the process of 'ADMIN ALTER DDL JOB xxx', so we can eliminate the goroutine,
// but if the task hasn't been created we need to make sure the task is created
// with config after ALTER DDL JOB is executed. A possible solution is to make
// the DXF task submission and 'ADMIN ALTER DDL JOB xxx' txn conflict with each
// other when they overlap in time, by modify the job at the same time when submit
// task, as we are using optimistic txn. But this will cause WRITE CONFLICT with
// outer txn in transitOneJobStep.
func modifyTaskParamLoop(
	ctx context.Context,
	sysTblMgr systable.Manager,
	taskManager storage.Manager,
	done chan struct{},
	jobID, taskID int64,
	lastConcurrency, lastBatchSize, lastMaxWriteSpeed int,
) {
	logger := logutil.DDLLogger().With(zap.Int64("jobID", jobID), zap.Int64("taskID", taskID))
	ticker := time.NewTicker(UpdateDDLJobReorgCfgInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}

		latestJob, err := sysTblMgr.GetJobByID(ctx, jobID)
		if err != nil {
			if goerrors.Is(err, systable.ErrNotFound) {
				logger.Info("job not found, might already finished")
				return
			}
			logger.Error("get job failed, will retry later", zap.Error(err))
			continue
		}

		modifies := make([]proto.Modification, 0, 3)
		workerCntLimit := latestJob.ReorgMeta.GetConcurrency()
		concurrency, err := adjustConcurrency(ctx, taskManager, workerCntLimit)
		if err != nil {
			logger.Error("adjust concurrency failed", zap.Error(err))
			continue
		}
		if concurrency != lastConcurrency {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyConcurrency,
				To:   int64(concurrency),
			})
		}
		batchSize := latestJob.ReorgMeta.GetBatchSize()
		if batchSize != lastBatchSize {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyBatchSize,
				To:   int64(batchSize),
			})
		}
		maxWriteSpeed := latestJob.ReorgMeta.GetMaxWriteSpeed()
		if maxWriteSpeed != lastMaxWriteSpeed {
			modifies = append(modifies, proto.Modification{
				Type: proto.ModifyMaxWriteSpeed,
				To:   int64(maxWriteSpeed),
			})
		}
		if len(modifies) == 0 {
			continue
		}
		currTask, err := taskManager.GetTaskByID(ctx, taskID)
		if err != nil {
			if goerrors.Is(err, storage.ErrTaskNotFound) {
				logger.Info("task not found, might already finished")
				return
			}
			logger.Error("get task failed, will retry later", zap.Error(err))
			continue
		}
		if !currTask.State.CanMoveToModifying() {
			// user might modify param again while another modify is ongoing.
			logger.Info("task state is not suitable for modifying, will retry later",
				zap.String("state", currTask.State.String()))
			continue
		}
		if err = taskManager.ModifyTaskByID(ctx, taskID, &proto.ModifyParam{
			PrevState:     currTask.State,
			Modifications: modifies,
		}); err != nil {
			logger.Error("modify task failed", zap.Error(err))
			continue
		}
		logger.Info("modify task success",
			zap.Int("oldConcurrency", lastConcurrency), zap.Int("newConcurrency", concurrency),
			zap.Int("oldBatchSize", lastBatchSize), zap.Int("newBatchSize", batchSize),
			zap.String("oldMaxWriteSpeed", units.HumanSize(float64(lastMaxWriteSpeed))),
			zap.String("newMaxWriteSpeed", units.HumanSize(float64(maxWriteSpeed))),
		)
		lastConcurrency = concurrency
		lastBatchSize = batchSize
		lastMaxWriteSpeed = maxWriteSpeed
	}
}

func adjustConcurrency(ctx context.Context, taskMgr storage.Manager, workerCnt int) (int, error) {
	cpuCount, err := taskMgr.GetCPUCountOfNode(ctx)
	if err != nil {
		return 0, err
	}
	return min(workerCnt, cpuCount), nil
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
		return 0, fmt.Errorf("not a helper.Storage")
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
		return 0, fmt.Errorf("less than 3 regions")
	}
	sample := regionInfos.Regions[1]
	if sample.ApproximateKeys == 0 || sample.ApproximateSize == 0 {
		return 0, fmt.Errorf("zero approximate size")
	}
	return int(uint64(sample.ApproximateSize)*size.MB) / int(sample.ApproximateKeys), nil
}

func (w *worker) updateDistTaskRowCount(taskKey string, jobID int64) {
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task manager", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	task, err := taskMgr.GetTaskByKeyWithHistory(w.workCtx, taskKey)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get task", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	rowCount, err := taskMgr.GetSubtaskRowCount(w.workCtx, task.ID, proto.BackfillStepReadIndex)
	if err != nil {
		logutil.DDLLogger().Warn("cannot get subtask row count", zap.String("task_key", taskKey), zap.Error(err))
		return
	}
	w.getReorgCtx(jobID).setRowCount(rowCount)
}

func getNextPartitionInfo(reorg *reorgInfo, t table.PartitionedTable, currPhysicalTableID int64) (pid int64, startKey, endKey kv.Key, err error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return 0, nil, nil, nil
	}

	// This will be used in multiple different scenarios/ALTER TABLE:
	// ADD INDEX - no change in partitions, just use pi.Definitions (1)
	// REORGANIZE PARTITION - copy data from partitions to be dropped (2)
	// REORGANIZE PARTITION - (re)create indexes on partitions to be added (3)
	// REORGANIZE PARTITION - Update new Global indexes with data from non-touched partitions (4)
	// (i.e. pi.Definitions - pi.DroppingDefinitions)
	if bytes.Equal(reorg.currElement.TypeKey, meta.IndexElementKey) {
		// case 1, 3 or 4
		if len(pi.AddingDefinitions) == 0 {
			// case 1
			// Simply AddIndex, without any partitions added or dropped!
			if reorg.mergingTmpIdx && currPhysicalTableID == t.Meta().ID {
				// If the current Physical id is the table id,
				// 1. All indexes are global index, the next Physical id should be the first partition id.
				// 2. Not all indexes are global index, return 0.
				allGlobal := true
				for _, element := range reorg.elements {
					if !bytes.Equal(element.TypeKey, meta.IndexElementKey) {
						allGlobal = false
						break
					}
					idxInfo := model.FindIndexInfoByID(t.Meta().Indices, element.ID)
					if !idxInfo.Global {
						allGlobal = false
						break
					}
				}
				if allGlobal {
					pid = 0
				} else {
					pid = pi.Definitions[0].ID
				}
			} else {
				pid, err = findNextPartitionID(currPhysicalTableID, pi.Definitions)
			}
		} else {
			// case 3 (or if not found AddingDefinitions; 4)
			// check if recreating Global Index (during Reorg Partition)
			pid, err = findNextPartitionID(currPhysicalTableID, pi.AddingDefinitions)
			if err != nil {
				// case 4
				// Not a partition in the AddingDefinitions, so it must be an existing
				// non-touched partition, i.e. recreating Global Index for the non-touched partitions
				pid, err = findNextNonTouchedPartitionID(currPhysicalTableID, pi)
			}
		}
	} else {
		// case 2
		pid, err = findNextPartitionID(currPhysicalTableID, pi.DroppingDefinitions)
	}
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
			s := reorg.jobCtx.store.(tikv.Storage)
			s.UpdateSPCache(ts, time.Now())
			time.Sleep(time.Second * 3)
		}
	})

	if reorg.mergingTmpIdx {
		elements := reorg.elements
		firstElemTempID := tablecodec.TempIndexPrefix | elements[0].ID
		lastElemTempID := tablecodec.TempIndexPrefix | elements[len(elements)-1].ID
		startKey = tablecodec.EncodeIndexSeekKey(pid, firstElemTempID, nil)
		endKey = tablecodec.EncodeIndexSeekKey(pid, lastElemTempID, []byte{255})
	} else {
		currentVer, err := getValidCurrentVersion(reorg.jobCtx.store)
		if err != nil {
			return 0, nil, nil, errors.Trace(err)
		}
		startKey, endKey, err = getTableRange(reorg.NewJobContext(), reorg.jobCtx.store, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
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

func findNextNonTouchedPartitionID(currPartitionID int64, pi *model.PartitionInfo) (int64, error) {
	pid, err := findNextPartitionID(currPartitionID, pi.Definitions)
	if err != nil {
		return 0, err
	}
	if pid == 0 {
		return 0, nil
	}
	for _, notFoundErr := findNextPartitionID(pid, pi.DroppingDefinitions); notFoundErr == nil; {
		// This can be optimized, but it is not frequently called, so keeping as-is
		pid, err = findNextPartitionID(pid, pi.Definitions)
		if pid == 0 {
			break
		}
	}
	return pid, err
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

func newCleanUpIndexWorker(id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *ReorgContext) (*cleanUpIndexWorker, error) {
	bCtx, err := newBackfillCtx(id, reorgInfo, reorgInfo.SchemaName, t, jc, metrics.LblCleanupIdxRate, false, false)
	if err != nil {
		return nil, err
	}

	indexes := make([]table.Index, 0, len(t.Indices()))
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	for _, index := range t.Indices() {
		if index.Meta().IsColumnarIndex() {
			continue
		}
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
	return w.writePhysicalTableRecord(w.workCtx, w.sessPool, t, typeCleanUpIndexWorker, reorgInfo)
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

	currentVer, err := getValidCurrentVersion(reorg.jobCtx.store)
	if err != nil {
		return false, errors.Trace(err)
	}
	start, end, err := getTableRange(reorg.NewJobContext(), reorg.jobCtx.store, t.GetPartition(pid), currentVer.Ver, reorg.Job.Priority)
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
// The normal one will be overwritten by the temp one.
func FindRelatedIndexesToChange(tblInfo *model.TableInfo, colName ast.CIStr) []changingIndex {
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

func findIdxCol(idxInfo *model.IndexInfo, colName ast.CIStr) int {
	for offset, idxCol := range idxInfo.Columns {
		if idxCol.Name.L == colName.L {
			return offset
		}
	}
	return -1
}

func renameIndexes(tblInfo *model.TableInfo, from, to ast.CIStr) {
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == from.L {
			idx.Name = to
		} else if isTempIdxInfo(idx, tblInfo) && getChangingIndexOriginName(idx) == from.O {
			idx.Name.L = strings.Replace(idx.Name.L, from.L, to.L, 1)
			idx.Name.O = strings.Replace(idx.Name.O, from.O, to.O, 1)
		}
		for _, col := range idx.Columns {
			originalCol := tblInfo.Columns[col.Offset]
			if originalCol.Hidden && getExpressionIndexOriginName(col.Name) == from.O {
				col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
				col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
			}
		}
	}
}

func renameHiddenColumns(tblInfo *model.TableInfo, from, to ast.CIStr) {
	for _, col := range tblInfo.Columns {
		if col.Hidden && getExpressionIndexOriginName(col.Name) == from.O {
			col.Name.L = strings.Replace(col.Name.L, from.L, to.L, 1)
			col.Name.O = strings.Replace(col.Name.O, from.O, to.O, 1)
		}
	}
}

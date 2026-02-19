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
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
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
		if lastCol.IsVirtualGenerated() {
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

// Set global index version for new global indexes.
// Version 1 is needed for non-clustered tables to prevent collisions after
// EXCHANGE PARTITION due to duplicate _tidb_rowid values.
// For non-unique indexes, the handle is always encoded in the key.
// For unique indexes with NULL values, the handle is also encoded in the key
// (since NULL != NULL, multiple NULLs are allowed).
// In both cases, we need the partition ID in the key to distinguish rows
// from different partitions that may have the same _tidb_rowid.
// Clustered tables don't have this issue and use version 0.
func setGlobalIndexVersion(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) {
	idxInfo.GlobalIndexVersion = 0
	if idxInfo.Global && !tblInfo.HasClusteredIndex() {
		needPartitionInKey := !idxInfo.Unique
		if !needPartitionInKey {
			nullCols := getNullColInfos(tblInfo, idxInfo.Columns)
			if len(nullCols) > 0 {
				needPartitionInKey = true
			}
		}
		if needPartitionInKey {
			idxInfo.GlobalIndexVersion = model.GlobalIndexVersionV1
			failpoint.Inject("SetGlobalIndexVersion", func(val failpoint.Value) {
				if valInt, ok := val.(int); ok {
					idxInfo.GlobalIndexVersion = uint8(valInt)
				}
			})
		}
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
		setGlobalIndexVersion(tblInfo, idxInfo)

		conditionString, err := CheckAndBuildIndexConditionString(tblInfo, indexOption.Condition)
		if err != nil {
			return nil, errors.Trace(err)
		}
		idxInfo.ConditionExprString = conditionString
		idxInfo.AffectColumn, err = buildAffectColumn(idxInfo, tblInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
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


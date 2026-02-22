// Copyright 2024 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

func noReorgDataStrict(tblInfo *model.TableInfo, oldCol, newCol *model.ColumnInfo) bool {
	toUnsigned := mysql.HasUnsignedFlag(newCol.GetFlag())
	originUnsigned := mysql.HasUnsignedFlag(oldCol.GetFlag())
	needTruncationOrToggleSign := func() bool {
		return (newCol.GetFlen() > 0 && (newCol.GetFlen() < oldCol.GetFlen() || newCol.GetDecimal() < oldCol.GetDecimal())) ||
			(toUnsigned != originUnsigned)
	}
	// Ignore the potential max display length represented by integer's flen, use default flen instead.
	defaultOldColFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(oldCol.GetType())
	defaultNewColFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(newCol.GetType())
	needTruncationOrToggleSignForInteger := func() bool {
		return (defaultNewColFlen > 0 && defaultNewColFlen < defaultOldColFlen) || (toUnsigned != originUnsigned)
	}

	// Deal with the same type.
	if oldCol.GetType() == newCol.GetType() {
		switch oldCol.GetType() {
		case mysql.TypeNewDecimal:
			// Since type decimal will encode the precision, frac, negative(signed) and wordBuf into storage together, there is no short
			// cut to eliminate data reorg change for column type change between decimal.
			return oldCol.GetFlen() == newCol.GetFlen() && oldCol.GetDecimal() == newCol.GetDecimal() && toUnsigned == originUnsigned
		case mysql.TypeEnum, mysql.TypeSet:
			return !IsElemsChangedToModifyColumn(oldCol.GetElems(), newCol.GetElems())
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return toUnsigned == originUnsigned
		case mysql.TypeString:
			// Due to the behavior of padding \x00 at binary type, always change column data when binary length changed
			if types.IsBinaryStr(&oldCol.FieldType) {
				return newCol.GetFlen() == oldCol.GetFlen()
			}
		case mysql.TypeTiDBVectorFloat32:
			return !(newCol.GetFlen() != types.UnspecifiedLength && oldCol.GetFlen() != newCol.GetFlen())
		}

		return !needTruncationOrToggleSign()
	}

	oldTp := oldCol.GetType()
	newTp := newCol.GetType()
	// VARCHAR->CHAR, may need reorg.
	if types.IsTypeVarchar(oldTp) && newTp == mysql.TypeString {
		return false
	}
	// CHAR->VARCHAR
	if oldTp == mysql.TypeString && types.IsTypeVarchar(newTp) {
		// If there are related index, the index may need reorg.
		relatedIndexes := getRelatedIndexIDs(tblInfo, oldCol.ID, false)
		if len(relatedIndexes) > 0 {
			return false
		}
	}

	// Deal with the different type.
	switch oldCol.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch newCol.GetType() {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			return !needTruncationOrToggleSign()
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch newCol.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			return !needTruncationOrToggleSignForInteger()
		}
		// conversion between float and double needs reorganization, see issue #31372
	}

	return false
}

// ConvertBetweenCharAndVarchar check whether column converted between char and varchar
// TODO: it is used for plugins. so change plugin's using and remove it.
func ConvertBetweenCharAndVarchar(oldCol, newCol byte) bool {
	return types.ConvertBetweenCharAndVarchar(oldCol, newCol)
}

// IsElemsChangedToModifyColumn check elems changed
func IsElemsChangedToModifyColumn(oldElems, newElems []string) bool {
	if len(newElems) < len(oldElems) {
		return true
	}
	for index, oldElem := range oldElems {
		newElem := newElems[index]
		if oldElem != newElem {
			return true
		}
	}
	return false
}

// ProcessColumnCharsetAndCollation process column charset and collation
func ProcessColumnCharsetAndCollation(ctx *metabuild.Context, col *table.Column, newCol *table.Column, meta *model.TableInfo, specNewColumn *ast.ColumnDef, schema *model.DBInfo) error {
	var chs, coll string
	var err error
	// TODO: Remove it when all table versions are greater than or equal to TableInfoVersion1.
	// If newCol's charset is empty and the table's version less than TableInfoVersion1,
	// we will not modify the charset of the column. This behavior is not compatible with MySQL.
	if len(newCol.FieldType.GetCharset()) == 0 && meta.Version < model.TableInfoVersion1 {
		chs = col.FieldType.GetCharset()
		coll = col.FieldType.GetCollate()
	} else {
		chs, coll, err = getCharsetAndCollateInColumnDef(specNewColumn, ctx.GetDefaultCollationForUTF8MB4())
		if err != nil {
			return errors.Trace(err)
		}
		chs, coll, err = ResolveCharsetCollation([]ast.CharsetOpt{
			{Chs: chs, Col: coll},
			{Chs: meta.Charset, Col: meta.Collate},
			{Chs: schema.Charset, Col: schema.Collate},
		}, ctx.GetDefaultCollationForUTF8MB4())
		chs, coll = OverwriteCollationWithBinaryFlag(specNewColumn, chs, coll, ctx.GetDefaultCollationForUTF8MB4())
		if err != nil {
			return errors.Trace(err)
		}
	}

	if err = setCharsetCollationFlenDecimal(ctx, &newCol.FieldType, newCol.Name.O, chs, coll); err != nil {
		return errors.Trace(err)
	}
	decodeEnumSetBinaryLiteralToUTF8(&newCol.FieldType, chs)
	return nil
}

func getReplacedColumns(tbInfo *model.TableInfo, oldCol, newCol *model.ColumnInfo) []*model.ColumnInfo {
	columns := make([]*model.ColumnInfo, 0, len(tbInfo.Columns))
	columns = append(columns, tbInfo.Columns...)
	// Replace old column with new column.
	for i, col := range columns {
		if col.Name.L != oldCol.Name.L {
			continue
		}
		columns[i] = newCol.Clone()
		columns[i].Name = oldCol.Name
		return columns
	}

	return columns
}

// checkColumnWithIndexConstraint is used to check the related index constraint of the modified column.
// Index has a max-prefix-length constraint. eg: a varchar(100), index idx(a), modifying column a to a varchar(4000)
// will cause index idx to break the max-prefix-length constraint.
func checkColumnWithIndexConstraint(tbInfo *model.TableInfo, originalCol, newCol *model.ColumnInfo) error {
	columns := getReplacedColumns(tbInfo, originalCol, newCol)

	pkIndex := tables.FindPrimaryIndex(tbInfo)

	checkOneIndex := func(indexInfo *model.IndexInfo) (err error) {
		var modified bool
		for _, col := range indexInfo.Columns {
			if col.Name.L == originalCol.Name.L {
				modified = true
				break
			}
		}
		if !modified {
			return
		}
		err = checkIndexInModifiableColumns(columns, indexInfo)
		if err != nil {
			return
		}
		err = checkIndexPrefixLength(columns, indexInfo.Columns, indexInfo.GetColumnarIndexType())
		return
	}

	// Check primary key first.
	var err error

	if pkIndex != nil {
		err = checkOneIndex(pkIndex)
		if err != nil {
			return err
		}
	}

	// Check secondary indexes.
	for _, indexInfo := range tbInfo.Indices {
		if indexInfo.Primary {
			continue
		}
		// the second param should always be set to true, check index length only if it was modified
		// checkOneIndex needs one param only.
		err = checkOneIndex(indexInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkIndexInModifiableColumns(columns []*model.ColumnInfo, idxInfo *model.IndexInfo) error {
	indexType := idxInfo.GetColumnarIndexType()
	for _, ic := range idxInfo.Columns {
		col := model.FindColumnInfo(columns, ic.Name.L)
		if col == nil {
			return dbterror.ErrKeyColumnDoesNotExits.GenWithStack("column does not exist: %s", ic.Name)
		}

		if indexType != model.ColumnarIndexTypeNA {
			continue
		}

		prefixLength := types.UnspecifiedLength
		if types.IsTypePrefixable(col.FieldType.GetType()) && col.FieldType.GetFlen() > ic.Length {
			// When the index column is changed, prefix length is only valid
			// if the type is still prefixable and larger than old prefix length.
			prefixLength = ic.Length
		}
		if err := checkIndexColumn(col, prefixLength, false); err != nil {
			return err
		}
	}
	return nil
}

// checkModifyTypes checks if the 'origin' type can be modified to 'to' type no matter directly change
// or change by reorg. It returns error if the two types are incompatible and correlated change are not
// supported. However, even the two types can be change, if the "origin" type contains primary key, error will be returned.
func checkModifyTypes(from, to *model.ColumnInfo, needRewriteCollationData bool) error {
	fromFt := &from.FieldType
	toFt := &to.FieldType
	canReorg, err := types.CheckModifyTypeCompatible(fromFt, toFt)
	if err != nil {
		if !canReorg {
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(err.Error()))
		}
		if mysql.HasPriKeyFlag(fromFt.GetFlag()) {
			msg := "this column has primary key flag"
			return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
		}
	}

	err = checkModifyCharsetAndCollation(toFt.GetCharset(), toFt.GetCollate(), fromFt.GetCharset(), fromFt.GetCollate(), needRewriteCollationData)

	if err != nil {
		if toFt.GetCharset() == charset.CharsetGBK || fromFt.GetCharset() == charset.CharsetGBK {
			return errors.Trace(err)
		}
		if strings.Contains(err.Error(), "Unsupported modifying collation") {
			colErrMsg := "Unsupported modifying collation of column '%s' from '%s' to '%s' when index is defined on it."
			err = dbterror.ErrUnsupportedModifyCollation.GenWithStack(colErrMsg, from.Name.L, from.GetCollate(), to.GetCollate())
		}

		// column type change can handle the charset change between these two types in the process of the reorg.
		if dbterror.ErrUnsupportedModifyCharset.Equal(err) && canReorg {
			return nil
		}
	}
	return errors.Trace(err)
}

// ProcessModifyColumnOptions process column options.
// Export for tiflow.
func ProcessModifyColumnOptions(ctx sessionctx.Context, col *table.Column, options []*ast.ColumnOption) error {
	var sb strings.Builder
	restoreFlags := format.RestoreStringSingleQuotes | format.RestoreKeyWordLowercase | format.RestoreNameBackQuotes |
		format.RestoreSpacesAroundBinaryOperation | format.RestoreWithoutSchemaName | format.RestoreWithoutSchemaName
	restoreCtx := format.NewRestoreCtx(restoreFlags, &sb)

	var hasDefaultValue, setOnUpdateNow bool
	var err error
	var hasNullFlag bool
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionDefaultValue:
			hasDefaultValue, err = SetDefaultValue(ctx.GetExprCtx(), col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionComment:
			err := setColumnComment(ctx.GetExprCtx(), col, opt)
			if err != nil {
				return errors.Trace(err)
			}
		case ast.ColumnOptionNotNull:
			col.AddFlag(mysql.NotNullFlag)
		case ast.ColumnOptionNull:
			hasNullFlag = true
			col.DelFlag(mysql.NotNullFlag)
		case ast.ColumnOptionAutoIncrement:
			col.AddFlag(mysql.AutoIncrementFlag)
		case ast.ColumnOptionPrimaryKey:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStack("can't change column constraint (PRIMARY KEY)"))
		case ast.ColumnOptionUniqKey:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStack("can't change column constraint (UNIQUE KEY)"))
		case ast.ColumnOptionOnUpdate:
			// TODO: Support other time functions.
			if !(col.GetType() == mysql.TypeTimestamp || col.GetType() == mysql.TypeDatetime) {
				return dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			if !expression.IsValidCurrentTimestampExpr(opt.Expr, &col.FieldType) {
				return dbterror.ErrInvalidOnUpdate.GenWithStackByArgs(col.Name)
			}
			col.AddFlag(mysql.OnUpdateNowFlag)
			setOnUpdateNow = true
		case ast.ColumnOptionGenerated:
			sb.Reset()
			err = opt.Expr.Restore(restoreCtx)
			if err != nil {
				return errors.Trace(err)
			}
			col.GeneratedExprString = sb.String()
			col.GeneratedStored = opt.Stored
			col.Dependences = make(map[string]struct{})
			// Only used by checkModifyGeneratedColumn, there is no need to set a ctor for it.
			col.GeneratedExpr = table.NewClonableExprNode(nil, opt.Expr)
			for _, colName := range FindColumnNamesInExpr(opt.Expr) {
				col.Dependences[colName.Name.L] = struct{}{}
			}
		case ast.ColumnOptionCollate:
			col.SetCollate(opt.StrValue)
		case ast.ColumnOptionReference:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with references"))
		case ast.ColumnOptionFulltext:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with full text"))
		case ast.ColumnOptionCheck:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("can't modify with check"))
		// Ignore ColumnOptionAutoRandom. It will be handled later.
		case ast.ColumnOptionAutoRandom:
		default:
			return errors.Trace(dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(fmt.Sprintf("unknown column option type: %d", opt.Tp)))
		}
	}

	if err = processAndCheckDefaultValueAndColumn(ctx.GetExprCtx(), col, nil, hasDefaultValue, setOnUpdateNow, hasNullFlag); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func checkAutoRandom(tableInfo *model.TableInfo, originCol *table.Column, specNewColumn *ast.ColumnDef) (uint64, error) {
	var oldShardBits, oldRangeBits uint64
	if isClusteredPKColumn(originCol, tableInfo) {
		oldShardBits = tableInfo.AutoRandomBits
		oldRangeBits = tableInfo.AutoRandomRangeBits
	}
	newShardBits, newRangeBits, err := extractAutoRandomBitsFromColDef(specNewColumn)
	if err != nil {
		return 0, errors.Trace(err)
	}
	switch {
	case oldShardBits == newShardBits:
	case oldShardBits < newShardBits:
		addingAutoRandom := oldShardBits == 0
		if addingAutoRandom {
			convFromAutoInc := mysql.HasAutoIncrementFlag(originCol.GetFlag()) && originCol.IsPKHandleColumn(tableInfo)
			if !convFromAutoInc {
				return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterChangeFromAutoInc)
			}
		}
		if autoid.AutoRandomShardBitsMax < newShardBits {
			errMsg := fmt.Sprintf(autoid.AutoRandomOverflowErrMsg,
				autoid.AutoRandomShardBitsMax, newShardBits, specNewColumn.Name.Name.O)
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(errMsg)
		}
		// increasing auto_random shard bits is allowed.
	case oldShardBits > newShardBits:
		if newShardBits == 0 {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomAlterErrMsg)
		}
		return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomDecreaseBitErrMsg)
	}

	modifyingAutoRandCol := oldShardBits > 0 || newShardBits > 0
	if modifyingAutoRandCol {
		// Disallow changing the column field type.
		if originCol.GetType() != specNewColumn.Tp.GetType() {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomModifyColTypeErrMsg)
		}
		if originCol.GetType() != mysql.TypeLonglong {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(fmt.Sprintf(autoid.AutoRandomOnNonBigIntColumn, types.TypeStr(originCol.GetType())))
		}
		// Disallow changing from auto_random to auto_increment column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionAutoIncrement) {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithAutoIncErrMsg)
		}
		// Disallow specifying a default value on auto_random column.
		if containsColumnOption(specNewColumn, ast.ColumnOptionDefaultValue) {
			return 0, dbterror.ErrInvalidAutoRandom.GenWithStackByArgs(autoid.AutoRandomIncompatibleWithDefaultValueErrMsg)
		}
	}
	if rangeBitsIsChanged(oldRangeBits, newRangeBits) {
		return 0, dbterror.ErrInvalidAutoRandom.FastGenByArgs(autoid.AutoRandomUnsupportedAlterRangeBits)
	}
	return newShardBits, nil
}

func isClusteredPKColumn(col *table.Column, tblInfo *model.TableInfo) bool {
	switch {
	case tblInfo.PKIsHandle:
		return mysql.HasPriKeyFlag(col.GetFlag())
	case tblInfo.IsCommonHandle:
		pk := tables.FindPrimaryIndex(tblInfo)
		for _, c := range pk.Columns {
			if c.Name.L == col.Name.L {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func rangeBitsIsChanged(oldBits, newBits uint64) bool {
	if oldBits == 0 {
		oldBits = autoid.AutoRandomRangeBitsDefault
	}
	if newBits == 0 {
		newBits = autoid.AutoRandomRangeBitsDefault
	}
	return oldBits != newBits
}

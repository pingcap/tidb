// Copyright 2026 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// analyzeModifyColumn is shared by submission and execution. Execution must use
// current metadata, since another DDL may have changed the column since submission.
func analyzeModifyColumn(tbl *model.TableInfo, oldCol, newCol *model.ColumnInfo) (byte, error) {
	if err := checkModifyTypes(oldCol, newCol, isColumnWithIndex(oldCol.Name.L, tbl.Indices)); err != nil {
		return model.ModifyTypeReorg, err
	}
	return classifyColumnChange(tbl, oldCol, newCol), nil
}

// classifyColumnChange returns the base ModifyType for the conversion. Execution
// may add nullability checks or fall back to a more conservative ModifyType.
func classifyColumnChange(tbl *model.TableInfo, oldCol, newCol *model.ColumnInfo) byte {
	change := classifyColumnValues(oldCol, newCol)
	if change == model.ModifyTypeReorg {
		return change
	}
	hasIndex := len(getRelatedIndexIDs(tbl, oldCol.ID, false)) != 0
	// Indexed CHAR -> VARCHAR retains the existing validation path because
	// restored index values can change even when row values fit unchanged.
	if hasIndex && oldCol.GetType() == mysql.TypeString && types.IsTypeVarchar(newCol.GetType()) {
		if !isCharChange(oldCol, newCol) || types.IsBinaryStr(&oldCol.FieldType) || types.IsBinaryStr(&newCol.FieldType) {
			return model.ModifyTypeReorg
		}
		change = model.ModifyTypeNoReorgWithCheck
	}
	if change == model.ModifyTypeNoReorgWithCheck && hasIndex && needIndexReorg(oldCol, newCol) {
		return model.ModifyTypeIndexReorg
	}
	return change
}

// noReorgDataStrict is the data-independent predicate used by dependent objects
// and execution flags. Nullability is checked separately by the caller.
func noReorgDataStrict(tbl *model.TableInfo, oldCol, newCol *model.ColumnInfo) bool {
	return classifyColumnChange(tbl, oldCol, newCol) == model.ModifyTypeNoReorg
}

// classifyColumnValues decides whether row values can be reused, need validation,
// or must be rewritten. Index requirements and job restrictions are applied later.
func classifyColumnValues(oldCol, newCol *model.ColumnInfo) byte {
	// ENUM/SET store ordinals rather than encoded strings. Safe UTF8/Latin1
	// widening also preserves the existing representation.
	if oldCol.GetCharset() != newCol.GetCharset() &&
		oldCol.GetType() != mysql.TypeEnum && oldCol.GetType() != mysql.TypeSet &&
		!(newCol.GetCharset() == charset.CharsetUTF8MB4 &&
			(oldCol.GetCharset() == charset.CharsetUTF8 || oldCol.GetCharset() == charset.CharsetLatin1)) {
		return model.ModifyTypeReorg
	}
	sameType := oldCol.GetType() == newCol.GetType()
	sameSign := mysql.HasUnsignedFlag(oldCol.GetFlag()) == mysql.HasUnsignedFlag(newCol.GetFlag())
	switch {
	case isIntegerChange(oldCol, newCol):
		// Display widths do not describe the stored integer range.
		oldWidth, _ := mysql.GetDefaultFieldLengthAndDecimal(oldCol.GetType())
		newWidth, _ := mysql.GetDefaultFieldLengthAndDecimal(newCol.GetType())
		if sameSign && newWidth >= oldWidth {
			return model.ModifyTypeNoReorg
		}
		return model.ModifyTypeNoReorgWithCheck
	case sameType && oldCol.GetType() == mysql.TypeNewDecimal:
		// DECIMAL stores precision and scale alongside its value, even on widening.
		if sameSign && oldCol.GetFlen() == newCol.GetFlen() && oldCol.GetDecimal() == newCol.GetDecimal() {
			return model.ModifyTypeNoReorg
		}
	case sameType && (oldCol.GetType() == mysql.TypeEnum || oldCol.GetType() == mysql.TypeSet):
		if !IsElemsChangedToModifyColumn(oldCol.GetElems(), newCol.GetElems()) {
			return model.ModifyTypeNoReorg
		}
	case sameType && oldCol.GetType() == mysql.TypeTiDBVectorFloat32:
		if newCol.GetFlen() == types.UnspecifiedLength || oldCol.GetFlen() == newCol.GetFlen() {
			return model.ModifyTypeNoReorg
		}
	case isStringStorageType(oldCol.GetType()) && isStringStorageType(newCol.GetType()):
		return classifyStringValues(oldCol, newCol)
	case sameType:
		if preservesColumnRange(oldCol, newCol) {
			return model.ModifyTypeNoReorg
		}
	}
	// Cross-family conversions and unsupported validation paths rewrite rows.
	return model.ModifyTypeReorg
}

func isStringStorageType(tp byte) bool {
	switch tp {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString,
		mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return true
	default:
		return false
	}
}

func classifyStringValues(oldCol, newCol *model.ColumnInfo) byte {
	if oldCol.GetType() == mysql.TypeString && newCol.GetType() == mysql.TypeString &&
		types.IsBinaryStr(&oldCol.FieldType) {
		// BINARY pads with zero bytes; widening changes existing values too.
		if oldCol.GetFlen() == newCol.GetFlen() {
			return model.ModifyTypeNoReorg
		}
		return model.ModifyTypeReorg
	}
	varcharToChar := types.IsTypeVarchar(oldCol.GetType()) && newCol.GetType() == mysql.TypeString
	if !varcharToChar && preservesColumnRange(oldCol, newCol) {
		return model.ModifyTypeNoReorg
	}
	// Only nonbinary CHAR/VARCHAR conversions have a length/trailing-space
	// validation path. Binary padding and other string conversions require rewriting.
	if isCharChange(oldCol, newCol) && !types.IsBinaryStr(&oldCol.FieldType) && !types.IsBinaryStr(&newCol.FieldType) {
		return model.ModifyTypeNoReorgWithCheck
	}
	return model.ModifyTypeReorg
}

// preservesColumnRange is the default rule for unchanged storage formats:
// do not narrow length/scale or change signedness. An unspecified target length
// retains the historical unrestricted-length behavior.
func preservesColumnRange(oldCol, newCol *model.ColumnInfo) bool {
	return mysql.HasUnsignedFlag(oldCol.GetFlag()) == mysql.HasUnsignedFlag(newCol.GetFlag()) &&
		(newCol.GetFlen() <= 0 ||
			(newCol.GetFlen() >= oldCol.GetFlen() && newCol.GetDecimal() >= oldCol.GetDecimal()))
}

func needIndexReorg(oldCol, changingCol *model.ColumnInfo) bool {
	if isIntegerChange(oldCol, changingCol) {
		return mysql.HasUnsignedFlag(oldCol.GetFlag()) != mysql.HasUnsignedFlag(changingCol.GetFlag())
	}

	intest.Assert(isCharChange(oldCol, changingCol))

	// Check index key part, ref tablecodec.GenIndexKey
	if !collate.CompatibleCollate(oldCol.GetCollate(), changingCol.GetCollate()) {
		return true
	}

	// Check index value part, ref tablecodec.GenIndexValuePortal
	// TODO(joechenrh): It's better to check each index here, because not all indexes need
	// reorg even if the below condition is true.
	return types.NeedRestoredData(&oldCol.FieldType) != types.NeedRestoredData(&changingCol.FieldType)
}

// checkModifyTypes validates whether the conversion is supported, including
// conversions that require reorganization. It does not imply metadata safety.
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

func isIntegerChange(from, to *model.ColumnInfo) bool {
	return mysql.IsIntegerType(from.GetType()) && mysql.IsIntegerType(to.GetType())
}

func isCharChange(from, to *model.ColumnInfo) bool {
	return types.IsTypeChar(from.GetType()) && types.IsTypeChar(to.GetType())
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

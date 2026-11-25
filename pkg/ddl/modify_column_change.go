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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// =============================================================================
// 1. Entry points
// =============================================================================

// analyzeModifyColumn validates conversion support and returns the base
// ModifyColumnType (metadata-only, validation-only, index rewrite, or row
// rewrite) without execution-time restrictions.
//
// Shared by submission and execution. Execution must evaluate against current
// metadata, since another DDL may have changed the column since submission.
func analyzeModifyColumn(tbl *model.TableInfo, oldCol, newCol *model.ColumnInfo) (model.ModifyColumnType, error) {
	if err := checkModifyTypes(oldCol, newCol, isColumnWithIndex(oldCol.Name.L, tbl.Indices)); err != nil {
		return model.ModifyTypeReorg, err
	}
	return classifyColumnChange(tbl, oldCol, newCol), nil
}

// resolveModifyColumnType validates and determines the initial execution
// ModifyColumnType for a MODIFY/CHANGE COLUMN job against current metadata.
func resolveModifyColumnType(
	tbl *model.TableInfo,
	oldCol, newCol *model.ColumnInfo,
	storedType model.ModifyColumnType,
	sqlMode mysql.SQLMode,
) (model.ModifyColumnType, error) {
	baseType, err := analyzeModifyColumn(tbl, oldCol, newCol)
	if err != nil {
		return model.ModifyTypeReorg, err
	}
	modifyType := applyExecutionRestrictions(tbl, oldCol, newCol, storedType, sqlMode, baseType)
	if needsVarcharToCharPrecheck(oldCol, newCol, modifyType) {
		return model.ModifyTypePrecheck, nil
	}
	return modifyType, nil
}

// resolveModifyColumnTypeAfterPrecheck determines the execution
// ModifyColumnType after VARCHAR -> CHAR data precheck succeeds.
func resolveModifyColumnTypeAfterPrecheck(
	tbl *model.TableInfo,
	oldCol, newCol *model.ColumnInfo,
	storedType model.ModifyColumnType,
	sqlMode mysql.SQLMode,
) model.ModifyColumnType {
	baseType := classifyColumnChange(tbl, oldCol, newCol)
	return applyExecutionRestrictions(tbl, oldCol, newCol, storedType, sqlMode, baseType)
}

// noReorgDataStrict reports whether the column conversion is strictly
// metadata-only regardless of existing row data. Nullability is checked
// separately by callers.
func noReorgDataStrict(tbl *model.TableInfo, oldCol, newCol *model.ColumnInfo) bool {
	return classifyColumnChange(tbl, oldCol, newCol) == model.ModifyTypeNoReorg
}

func isNullToNotNullChange(oldCol, newCol *model.ColumnInfo) bool {
	return !mysql.HasNotNullFlag(oldCol.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag())
}

// =============================================================================
// 2. Conversion support validation
// =============================================================================

// checkModifyTypes validates whether modifying from -> to is supported at all
// (either metadata-only or via reorganization). It checks cross-type support,
// primary key restrictions, and charset/collation compatibility.
func checkModifyTypes(from, to *model.ColumnInfo, hasIndex bool) error {
	fromFt := &from.FieldType
	toFt := &to.FieldType

	if from.GetType() != to.GetType() && !isTypeChangeSupported(fromFt.GetType(), toFt.GetType()) {
		msg := fmt.Sprintf("change from original type %v to %v is currently unsupported yet", fromFt.CompactStr(), toFt.CompactStr())
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}

	// Check whether the type/length/scale/signedness/member change itself
	// (ignoring charset) requires data validation or reorganization rather than
	// a metadata-only update. CHAR -> VARCHAR under new collation is included
	// so primary-key rejection and reorg-assisted charset conversion stay consistent.
	typeNeedsReorg := classifyColumnValueStorage(from, to) != model.ModifyTypeNoReorg ||
		convertBetweenCharAndVarchar(from.GetType(), to.GetType())
	if typeNeedsReorg && mysql.HasPriKeyFlag(fromFt.GetFlag()) {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("this column has primary key flag")
	}

	err := checkModifyCharsetAndCollation(toFt.GetCharset(), toFt.GetCollate(), fromFt.GetCharset(), fromFt.GetCollate(), hasIndex)
	if err == nil {
		return nil
	}
	if toFt.GetCharset() == charset.CharsetGBK || fromFt.GetCharset() == charset.CharsetGBK {
		return errors.Trace(err)
	}
	if strings.Contains(err.Error(), "Unsupported modifying collation") {
		colErrMsg := "Unsupported modifying collation of column '%s' from '%s' to '%s' when index is defined on it."
		err = dbterror.ErrUnsupportedModifyCollation.GenWithStack(colErrMsg, from.Name.L, from.GetCollate(), to.GetCollate())
	}

	// When the type change already triggers reorganization, non-GBK charset and
	// collation changes are handled during reorg rather than rejected up front.
	// Note: ErrUnsupportedModifyCharset and ErrUnsupportedModifyCollation share
	// ErrUnsupportedDDLOperation (code 8200).
	if typeNeedsReorg && (dbterror.ErrUnsupportedModifyCharset.Equal(err) || dbterror.ErrUnsupportedModifyCollation.Equal(err)) {
		return nil
	}
	return errors.Trace(err)
}

// isTypeChangeSupported reports whether cross-type reorganization from
// originTp to toTp is implemented. Callers handle originTp == toTp separately.
func isTypeChangeSupported(originTp, toTp byte) bool {
	if originTp == mysql.TypeTiDBVectorFloat32 || toTp == mysql.TypeTiDBVectorFloat32 {
		// TODO: Cross-type conversion involving vector is not supported.
		return false
	}

	switch toTp {
	case mysql.TypeBit:
		// TODO: date/datetime/timestamp/time/year/string/json -> bit are not compatible with MySQL yet.
		switch originTp {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp,
			mysql.TypeDuration, mysql.TypeYear, mysql.TypeJSON:
			return false
		default:
			return !types.IsString(originTp)
		}
	case mysql.TypeEnum, mysql.TypeSet:
		// TODO: temporal/decimal/float/double/json/bit -> enum/set are not supported yet.
		switch originTp {
		case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp,
			mysql.TypeDuration, mysql.TypeYear,
			mysql.TypeNewDecimal, mysql.TypeFloat, mysql.TypeDouble,
			mysql.TypeJSON, mysql.TypeBit:
			return false
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		// TODO: enum/set/bit/decimal/float/double -> date/datetime/timestamp are not supported yet.
		switch originTp {
		case mysql.TypeEnum, mysql.TypeSet, mysql.TypeBit,
			mysql.TypeNewDecimal, mysql.TypeFloat, mysql.TypeDouble:
			return false
		}
	case mysql.TypeDuration:
		// TODO: enum/set/bit -> time are not supported yet.
		switch originTp {
		case mysql.TypeEnum, mysql.TypeSet, mysql.TypeBit:
			return false
		}
	}
	return true
}

// =============================================================================
// 3. Storage & index conversion classification
// =============================================================================

// classifyColumnChange returns the base ModifyColumnType from row storage and
// index encoding requirements. Nullability and execution-time restrictions are
// applied later in applyExecutionRestrictions.
func classifyColumnChange(tbl *model.TableInfo, oldCol, newCol *model.ColumnInfo) model.ModifyColumnType {
	change := classifyColumnValues(oldCol, newCol)
	if change == model.ModifyTypeReorg {
		return change
	}
	if !hasRelatedIndex(tbl, oldCol.ID) {
		return change
	}
	// Indexed CHAR -> VARCHAR cannot stay metadata-only because index restored
	// data encoding can change even when row values fit unchanged.
	if isCharToVarchar(oldCol, newCol) {
		if !isNonBinaryCharChange(oldCol, newCol) {
			return model.ModifyTypeReorg
		}
		change = model.ModifyTypeNoReorgWithCheck
	}
	if change == model.ModifyTypeNoReorgWithCheck && needIndexReorg(oldCol, newCol) {
		return model.ModifyTypeIndexReorg
	}
	return change
}

// classifyColumnValues decides whether existing row values can be reused as-is,
// need validation, or must be rewritten.
func classifyColumnValues(oldCol, newCol *model.ColumnInfo) model.ModifyColumnType {
	if !isCharsetMetadataCompatible(oldCol, newCol) {
		return model.ModifyTypeReorg
	}
	return classifyColumnValueStorage(oldCol, newCol)
}

// isCharsetMetadataCompatible reports whether stored values remain valid
// without row conversion when the column charset changes.
// ENUM/SET store ordinals rather than encoded strings; UTF8/Latin1 -> UTF8MB4
// widening also preserves existing byte representations.
func isCharsetMetadataCompatible(oldCol, newCol *model.ColumnInfo) bool {
	if oldCol.GetCharset() == newCol.GetCharset() {
		return true
	}
	if oldCol.GetType() == mysql.TypeEnum || oldCol.GetType() == mysql.TypeSet {
		return true
	}
	return newCol.GetCharset() == charset.CharsetUTF8MB4 &&
		(oldCol.GetCharset() == charset.CharsetUTF8 || oldCol.GetCharset() == charset.CharsetLatin1)
}

// classifyColumnValueStorage classifies a column conversion by type family,
// length, scale, signedness, and member list, independent of charset changes.
func classifyColumnValueStorage(oldCol, newCol *model.ColumnInfo) model.ModifyColumnType {
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

func classifyStringValues(oldCol, newCol *model.ColumnInfo) model.ModifyColumnType {
	if oldCol.GetType() == mysql.TypeString && newCol.GetType() == mysql.TypeString &&
		types.IsBinaryStr(&oldCol.FieldType) && types.IsBinaryStr(&newCol.FieldType) {
		// BINARY pads with zero bytes; widening changes existing values too.
		if oldCol.GetFlen() == newCol.GetFlen() {
			return model.ModifyTypeNoReorg
		}
		return model.ModifyTypeReorg
	}
	if !isVarcharToChar(oldCol, newCol) && preservesColumnRange(oldCol, newCol) {
		return model.ModifyTypeNoReorg
	}
	// Only non-binary CHAR/VARCHAR conversions have a length/trailing-space
	// validation path. Binary padding and other string conversions require rewriting.
	if isNonBinaryCharChange(oldCol, newCol) {
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

	// Check index key part, ref tablecodec.GenIndexKey.
	if !collate.CompatibleCollate(oldCol.GetCollate(), changingCol.GetCollate()) {
		return true
	}

	// Check index value part, ref tablecodec.GenIndexValuePortal.
	// TODO(joechenrh): It's better to check each index here, because not all indexes need
	// reorg even if the below condition is true.
	return types.NeedRestoredData(&oldCol.FieldType) != types.NeedRestoredData(&changingCol.FieldType)
}

func hasRelatedIndex(tbl *model.TableInfo, colID int64) bool {
	for _, idx := range tbl.Indices {
		if idx.HasColumnInIndexColumns(tbl, colID) {
			return true
		}
	}
	return false
}

// =============================================================================
// 4. Execution-time restrictions & precheck
// =============================================================================

// applyExecutionRestrictions adjusts a base conversion classification for
// nullability checks, legacy job compatibility, and runtime restrictions
// (partitions, TiFlash, stats encoding, and non-strict SQL mode).
// Restrictions can only move a change to a more conservative execution path.
func applyExecutionRestrictions(
	tbl *model.TableInfo,
	oldCol, newCol *model.ColumnInfo,
	storedType model.ModifyColumnType,
	sqlMode mysql.SQLMode,
	baseType model.ModifyColumnType,
) model.ModifyColumnType {
	if baseType == model.ModifyTypeNoReorg {
		if isNullToNotNullChange(oldCol, newCol) {
			return model.ModifyTypeNoReorgWithCheck
		}
		return model.ModifyTypeNoReorg
	}

	// Backward compatibility: older TiDB versions stored mysql.TypeNull to mark
	// jobs that could need reorg; keep them on the conservative full-reorg path.
	if storedType == model.ModifyColumnType(mysql.TypeNull) {
		return model.ModifyTypeReorg
	}

	// FIXME(joechenrh): handle partition and TiFlash replica case.
	if tbl.Partition != nil || (tbl.TiFlashReplica != nil && tbl.TiFlashReplica.Count > 0) {
		return model.ModifyTypeReorg
	}

	failpoint.Inject("disableLossyDDLOptimization", func(val failpoint.Value) {
		if v, ok := val.(bool); ok && v {
			failpoint.Return(model.ModifyTypeReorg)
		}
	})

	// FIXME(joechenrh): remove this when stats correctness is resolved.
	// Since stats may store bytes encoded by codec.EncodeKey, disable the
	// optimization if the same data produces different encoded bytes for the
	// old and new types.
	if hasIncompatibleStatsEncoding(oldCol, newCol) {
		return model.ModifyTypeReorg
	}

	if !sqlMode.HasStrictMode() {
		return model.ModifyTypeReorg
	}

	return baseType
}

func hasIncompatibleStatsEncoding(oldCol, newCol *model.ColumnInfo) bool {
	if isIntegerChange(oldCol, newCol) {
		return mysql.HasUnsignedFlag(oldCol.GetFlag()) != mysql.HasUnsignedFlag(newCol.GetFlag())
	}
	if isCharChange(oldCol, newCol) {
		return !collate.CompatibleCollate(oldCol.GetCollate(), newCol.GetCollate())
	}
	return false
}

// needsVarcharToCharPrecheck reports whether a VARCHAR -> CHAR conversion
// should start in ModifyTypePrecheck to verify no existing values have trailing
// spaces before deciding between a no-row-reorg path and full reorg.
func needsVarcharToCharPrecheck(oldCol, newCol *model.ColumnInfo, modifyType model.ModifyColumnType) bool {
	return oldCol.GetType() == mysql.TypeVarchar &&
		newCol.GetType() == mysql.TypeString &&
		(modifyType == model.ModifyTypeNoReorgWithCheck || modifyType == model.ModifyTypeIndexReorg)
}

// =============================================================================
// 5. Type predicates
// =============================================================================

func isIntegerChange(from, to *model.ColumnInfo) bool {
	return mysql.IsIntegerType(from.GetType()) && mysql.IsIntegerType(to.GetType())
}

func isCharChange(from, to *model.ColumnInfo) bool {
	return types.IsTypeChar(from.GetType()) && types.IsTypeChar(to.GetType())
}

func isCharToVarchar(oldCol, newCol *model.ColumnInfo) bool {
	return oldCol.GetType() == mysql.TypeString && types.IsTypeVarchar(newCol.GetType())
}

func isVarcharToChar(oldCol, newCol *model.ColumnInfo) bool {
	return types.IsTypeVarchar(oldCol.GetType()) && newCol.GetType() == mysql.TypeString
}

func isNonBinaryCharChange(oldCol, newCol *model.ColumnInfo) bool {
	return isCharChange(oldCol, newCol) &&
		!types.IsBinaryStr(&oldCol.FieldType) &&
		!types.IsBinaryStr(&newCol.FieldType)
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

// convertBetweenCharAndVarchar reports whether converting between VARCHAR and
// CHAR requires reorganization for compatibility checks:
//  1. VARCHAR -> CHAR strips trailing spaces.
//  2. CHAR -> VARCHAR under new collation can change secondary index restored data.
func convertBetweenCharAndVarchar(oldCol, newCol byte) bool {
	return (types.IsTypeVarchar(oldCol) && newCol == mysql.TypeString) ||
		(oldCol == mysql.TypeString && types.IsTypeVarchar(newCol) && collate.NewCollationEnabled())
}

// IsElemsChangedToModifyColumn reports whether modifying ENUM/SET elements
// removes or changes any existing element ordinal.
func IsElemsChangedToModifyColumn(oldElems, newElems []string) bool {
	if len(newElems) < len(oldElems) {
		return true
	}
	for index, oldElem := range oldElems {
		if oldElem != newElems[index] {
			return true
		}
	}
	return false
}

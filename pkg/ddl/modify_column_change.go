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
	"unicode/utf8"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// =============================================================================
// 1. Entry points
// =============================================================================

// analyzeModifyColumn classifies the conversion and validates that it is
// supported. Returns the base ModifyColumnType (metadata-only, validation-only,
// index rewrite, or row rewrite) without execution-time restrictions.
//
// Shared by submission and execution. Execution must evaluate against current
// metadata, since another DDL may have changed the column since submission.
func analyzeModifyColumn(tbl *model.TableInfo, oldCol, newCol *model.ColumnInfo) (model.ModifyColumnType, error) {
	change := classifyColumnChange(tbl, oldCol, newCol)
	if err := validateModifyColumn(oldCol, newCol, change); err != nil {
		return model.ModifyTypeReorg, err
	}
	return change, nil
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

// isMetaOnlyChange reports whether the column conversion is strictly
// metadata-only regardless of existing row data. Nullability is checked
// separately by callers.
func isMetaOnlyChange(tbl *model.TableInfo, oldCol, newCol *model.ColumnInfo) bool {
	return classifyColumnChange(tbl, oldCol, newCol) == model.ModifyTypeNoReorg
}

func isNullToNotNullChange(oldCol, newCol *model.ColumnInfo) bool {
	return !mysql.HasNotNullFlag(oldCol.GetFlag()) && mysql.HasNotNullFlag(newCol.GetFlag())
}

// =============================================================================
// 2. Conversion support validation
// =============================================================================

// validateModifyColumn validates whether modifying from -> to is supported at all
// (either metadata-only or via reorganization). Non-GBK charset and collation
// changes are allowed and routed to the appropriate execution path by
// classifyColumnChange.
func validateModifyColumn(from, to *model.ColumnInfo, change model.ModifyColumnType) error {
	fromFt, toFt := &from.FieldType, &to.FieldType
	if !charset.ValidCharsetAndCollation(toFt.GetCharset(), toFt.GetCollate()) {
		return dbterror.ErrUnknownCharacterSet.GenWithStack("Unknown character set: '%s', collation: '%s'", toFt.GetCharset(), toFt.GetCollate())
	}

	if from.GetType() != to.GetType() && !isCrossTypeChangeSupported(fromFt.GetType(), toFt.GetType()) {
		msg := fmt.Sprintf("change from original type %v to %v is currently unsupported yet", fromFt.CompactStr(), toFt.CompactStr())
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs(msg)
	}

	// Modifying charset or collation to/from GBK is not supported yet.
	fromChs, toChs := fromFt.GetCharset(), toFt.GetCharset()
	if fromChs == charset.CharsetGBK || toChs == charset.CharsetGBK {
		if fromChs != toChs {
			return dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs(fmt.Sprintf("charset from %s to %s", fromChs, toChs))
		}
		if fromFt.GetCollate() != toFt.GetCollate() {
			return dbterror.ErrUnsupportedModifyCharset.GenWithStackByArgs(fmt.Sprintf("change collate from %s to %s", fromFt.GetCollate(), toFt.GetCollate()))
		}
	}

	if change != model.ModifyTypeNoReorg && mysql.HasPriKeyFlag(fromFt.GetFlag()) {
		return dbterror.ErrUnsupportedModifyColumn.GenWithStackByArgs("this column has primary key flag")
	}
	return nil
}

// isCrossTypeChangeSupported reports whether cross-type reorganization from
// originTp to toTp is implemented. Callers handle originTp == toTp separately.
func isCrossTypeChangeSupported(originTp, toTp byte) bool {
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
	change := classifyByRowValue(oldCol, newCol)
	if change == model.ModifyTypeReorg {
		return change
	}
	if !hasRelatedIndex(tbl, oldCol.ID) {
		return change
	}
	if needsIndexReorg(oldCol, newCol) {
		// Index-only reorg only runs validation SQL for integers and non-binary
		// CHAR/VARCHAR; other indexed string types (e.g. TEXT with a prefix
		// index) fall back to full row+index reorg.
		if isIntegerChange(oldCol, newCol) || isNonBinaryCharChange(oldCol, newCol) {
			return model.ModifyTypeIndexReorg
		}
		return model.ModifyTypeReorg
	}
	return change
}

// classifyByRowValue decides whether existing row values can be reused as-is,
// need validation, or must be rewritten.
func classifyByRowValue(oldCol, newCol *model.ColumnInfo) model.ModifyColumnType {
	if !isCharsetMetadataCompatible(oldCol, newCol) {
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
		return classifyByStringRowValue(oldCol, newCol)
	case sameType:
		if preservesColumnRange(oldCol, newCol) {
			return model.ModifyTypeNoReorg
		}
	}
	// Cross-family conversions and unsupported validation paths rewrite rows.
	return model.ModifyTypeReorg
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

func classifyByStringRowValue(oldCol, newCol *model.ColumnInfo) model.ModifyColumnType {
	if oldCol.GetType() == mysql.TypeString && newCol.GetType() == mysql.TypeString &&
		types.IsBinaryStr(&oldCol.FieldType) && types.IsBinaryStr(&newCol.FieldType) {
		// BINARY pads with zero bytes; widening changes existing values too.
		if oldCol.GetFlen() == newCol.GetFlen() {
			return model.ModifyTypeNoReorg
		}
		return model.ModifyTypeReorg
	}
	if needsStringByteLengthCheck(oldCol, newCol) {
		if oldCol.GetCharset() == newCol.GetCharset() {
			return model.ModifyTypeNoReorgWithCheck
		}
		return model.ModifyTypeReorg
	}
	if !isVarcharToChar(oldCol, newCol) && preservesColumnRange(oldCol, newCol) {
		return model.ModifyTypeNoReorg
	}
	if isNonBinaryCharChange(oldCol, newCol) {
		return model.ModifyTypeNoReorgWithCheck
	}
	return model.ModifyTypeReorg
}

// needsStringByteLengthCheck reports whether a character limit fails to
// guarantee that every source value fits a target with a byte limit.
func needsStringByteLengthCheck(oldCol, newCol *model.ColumnInfo) bool {
	if stringLengthInBytes(oldCol) || !stringLengthInBytes(newCol) {
		return false
	}

	sourceLimit, targetLimit := oldCol.GetFlen(), newCol.GetFlen()
	// Dividing converts the target byte limit to a guaranteed UTF-8
	// character capacity without overflowing the source limit.
	return targetLimit >= 0 && (sourceLimit < 0 || sourceLimit > targetLimit/utf8.UTFMax)
}

func stringLengthInBytes(col *model.ColumnInfo) bool {
	return col.GetCharset() == charset.CharsetBin || types.IsTypeBlob(col.GetType())
}

// preservesColumnRange is the default rule for unchanged storage formats:
// do not narrow length/scale or change signedness. An unspecified target length
// retains the historical unrestricted-length behavior.
func preservesColumnRange(oldCol, newCol *model.ColumnInfo) bool {
	return mysql.HasUnsignedFlag(oldCol.GetFlag()) == mysql.HasUnsignedFlag(newCol.GetFlag()) &&
		(newCol.GetFlen() <= 0 ||
			(newCol.GetFlen() >= oldCol.GetFlen() && newCol.GetDecimal() >= oldCol.GetDecimal()))
}

func needsIndexReorg(oldCol, newCol *model.ColumnInfo) bool {
	if isIntegerChange(oldCol, newCol) {
		return mysql.HasUnsignedFlag(oldCol.GetFlag()) != mysql.HasUnsignedFlag(newCol.GetFlag())
	}
	if !isStringStorageType(oldCol.GetType()) || !isStringStorageType(newCol.GetType()) {
		return false
	}

	// Check index key part, ref tablecodec.GenIndexKey.
	if collate.NewCollationEnabled() && !collate.CompatibleCollate(oldCol.GetCollate(), newCol.GetCollate()) {
		return true
	}

	// Check index value part, ref tablecodec.GenIndexValuePortal.
	// TODO(joechenrh): It's better to check each index here, because not all indexes need
	// reorg even if the below condition is true.
	return types.NeedRestoredData(&oldCol.FieldType) != types.NeedRestoredData(&newCol.FieldType)
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
	if isCharOrVarcharChange(oldCol, newCol) {
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

func isCharOrVarcharChange(from, to *model.ColumnInfo) bool {
	return types.IsTypeChar(from.GetType()) && types.IsTypeChar(to.GetType())
}

func isVarcharToChar(oldCol, newCol *model.ColumnInfo) bool {
	return types.IsTypeVarchar(oldCol.GetType()) && newCol.GetType() == mysql.TypeString
}

func isNonBinaryCharChange(oldCol, newCol *model.ColumnInfo) bool {
	return isCharOrVarcharChange(oldCol, newCol) &&
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

// ConvertBetweenCharAndVarchar reports whether a CHAR/VARCHAR type change
// requires reorganization under the legacy rules. The TiFlow DM syncer uses
// this predicate to classify DDL events.
func ConvertBetweenCharAndVarchar(oldType, newType byte) bool {
	return (types.IsTypeVarchar(oldType) && newType == mysql.TypeString) ||
		(oldType == mysql.TypeString && types.IsTypeVarchar(newType) && collate.NewCollationEnabled())
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

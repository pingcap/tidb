// Copyright 2025 PingCAP, Inc.
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

package mydump

import (
	"encoding/binary"
	"math"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

type setter[T parquet.ColumnTypes] func(T, *types.Datum) error

// CastingCheckType indicates the cast-skip decision and value-level
// check to run in the encoder.
type CastingCheckType uint8

const (
	// CastingCheckNoSkip means this column/value must go through cast.
	CastingCheckNoSkip CastingCheckType = iota
	// CastingCheckSkip means this column/value can skip cast directly.
	CastingCheckSkip
	// CastingCheckStringLength verifies string length before skipping cast.
	CastingCheckStringLength
	// CastingCheckDecimal validates decimal shape before skipping cast.
	CastingCheckDecimal
)

// ColumnSkipCastInfo describes whether a parquet column can skip cast
// for the mapped target column under strict-mode semantics.
type ColumnSkipCastInfo struct {
	CastingCheck CastingCheckType

	TargetType    byte
	TargetFlen    int
	TargetDecimal int
	Unsigned      bool
}

var (
	zeroMyDecimal = types.MyDecimal{}

	unsupportedParquetTypes = map[schema.ConvertedType]struct{}{
		// TODO(joechenrh): support read list type as vector
		schema.ConvertedTypes.List: {},
		// The below types are not used for data exported from aurora/snowflake,
		// so we don't support them for now.
		schema.ConvertedTypes.Map:         {},
		schema.ConvertedTypes.MapKeyValue: {},
		schema.ConvertedTypes.Interval:    {},
		schema.ConvertedTypes.NA:          {},
	}
)

const (
	// maximumDecimalBytes is the maximum byte length allowed to be parsed directly.
	// It guarantees the value can be stored in MyDecimal wordbuf without overflow.
	// That is: floor(log256(10^81-1))
	maximumDecimalBytes = 33
)

func extractColumnTypes(
	fileMeta *metadata.FileMetaData,
) ([]convertedType, []string, error) {
	colTypes := make([]convertedType, fileMeta.NumColumns())
	colNames := make([]string, 0, fileMeta.NumColumns())

	for i := range colTypes {
		desc := fileMeta.Schema.Column(i)
		colNames = append(colNames, strings.ToLower(desc.Name()))

		logicalType := desc.LogicalType()
		if logicalType.IsValid() {
			colTypes[i].converted, colTypes[i].decimalMeta = logicalType.ToConvertedType()
			if t, ok := logicalType.(*schema.TimeLogicalType); ok {
				colTypes[i].IsAdjustedToUTC = t.IsAdjustedToUTC()
			} else {
				colTypes[i].IsAdjustedToUTC = true
			}
		} else {
			colTypes[i].converted = desc.ConvertedType()
			colTypes[i].IsAdjustedToUTC = true
			pnode, _ := desc.SchemaNode().(*schema.PrimitiveNode)
			colTypes[i].decimalMeta = pnode.DecimalMetadata()
		}

		if _, ok := unsupportedParquetTypes[colTypes[i].converted]; ok {
			return nil, nil,
				errors.Errorf("unsupported parquet logical type %s",
					colTypes[i].converted.String())
		}
	}
	return colTypes, colNames, nil
}

func buildParquetSkipCastInfos(
	colTypes []convertedType,
	physicalTypes []parquet.Type,
	targetCols []*model.ColumnInfo,
) []ColumnSkipCastInfo {
	infos := make([]ColumnSkipCastInfo, len(colTypes))
	for i := range colTypes {
		if i >= len(physicalTypes) || i >= len(targetCols) || targetCols[i] == nil {
			continue
		}
		infos[i] = parquetColumnSkipCastInfo(colTypes[i], physicalTypes[i], targetCols[i])
	}
	return infos
}

func parquetColumnSkipCastInfo(
	converted convertedType,
	physicalType parquet.Type,
	target *model.ColumnInfo,
) ColumnSkipCastInfo {
	info := ColumnSkipCastInfo{
		TargetType:    target.GetType(),
		TargetFlen:    target.GetFlen(),
		TargetDecimal: target.GetDecimal(),
		Unsigned:      mysql.HasUnsignedFlag(target.GetFlag()),
	}

	if target.GetType() == mysql.TypeTimestamp {
		// TIMESTAMP always requires cast/conversion because of timezone and
		// DST semantics.
		return info
	}

	switch physicalType {
	case parquet.Types.Boolean:
		if canSkipBoolToInteger(target) {
			info.CastingCheck = CastingCheckSkip
		}
		return info
	case parquet.Types.Float:
		if target.GetType() == mysql.TypeFloat {
			info.CastingCheck = CastingCheckSkip
		}
		return info
	case parquet.Types.Double:
		if target.GetType() == mysql.TypeDouble {
			info.CastingCheck = CastingCheckSkip
		}
		return info
	case parquet.Types.Int32:
		info = parquetInt32SkipCastInfo(converted, target, info)
		return info
	case parquet.Types.Int64:
		info = parquetInt64SkipCastInfo(converted, target, info)
		return info
	case parquet.Types.Int96:
		if target.GetType() == mysql.TypeDate || target.GetType() == mysql.TypeDatetime {
			info.CastingCheck = CastingCheckSkip
		}
		return info
	case parquet.Types.ByteArray, parquet.Types.FixedLenByteArray:
		if converted.converted == schema.ConvertedTypes.Decimal && target.GetType() == mysql.TypeNewDecimal {
			if canSkipDecimalByMeta(converted.decimalMeta, target) {
				info.CastingCheck = CastingCheckDecimal
			}
			return info
		}
		if converted.converted == schema.ConvertedTypes.UTF8 && canSkipUTF8StringTarget(target) {
			info.CastingCheck = CastingCheckStringLength
		}
		return info
	default:
		return info
	}
}

func parquetInt32SkipCastInfo(
	converted convertedType,
	target *model.ColumnInfo,
	info ColumnSkipCastInfo,
) ColumnSkipCastInfo {
	switch converted.converted {
	case schema.ConvertedTypes.Date:
		if target.GetType() == mysql.TypeDate {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.TimeMillis:
		if target.GetType() == mysql.TypeDate || target.GetType() == mysql.TypeDatetime {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Decimal:
		if target.GetType() == mysql.TypeNewDecimal && canSkipDecimalByMeta(converted.decimalMeta, target) {
			info.CastingCheck = CastingCheckDecimal
		}
	case schema.ConvertedTypes.Int8:
		if signedRangeFitsTarget(-1<<7, 1<<7-1, target) {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Int16:
		if signedRangeFitsTarget(-1<<15, 1<<15-1, target) {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Int32, schema.ConvertedTypes.None:
		if signedRangeFitsTarget(math.MinInt32, math.MaxInt32, target) {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Uint8:
		if unsignedRangeFitsTarget(math.MaxUint8, target) {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Uint16:
		if unsignedRangeFitsTarget(math.MaxUint16, target) {
			info.CastingCheck = CastingCheckSkip
		}
	}
	return info
}

func parquetInt64SkipCastInfo(
	converted convertedType,
	target *model.ColumnInfo,
	info ColumnSkipCastInfo,
) ColumnSkipCastInfo {
	switch converted.converted {
	case schema.ConvertedTypes.TimeMicros, schema.ConvertedTypes.TimestampMillis, schema.ConvertedTypes.TimestampMicros:
		if target.GetType() == mysql.TypeDate || target.GetType() == mysql.TypeDatetime {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Decimal:
		if target.GetType() == mysql.TypeNewDecimal && canSkipDecimalByMeta(converted.decimalMeta, target) {
			info.CastingCheck = CastingCheckDecimal
		}
	case schema.ConvertedTypes.Int64, schema.ConvertedTypes.None:
		if signedRangeFitsTarget(math.MinInt64, math.MaxInt64, target) {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Uint8:
		if unsignedRangeFitsTarget(math.MaxUint8, target) {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Uint16:
		if unsignedRangeFitsTarget(math.MaxUint16, target) {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Uint32:
		if unsignedRangeFitsTarget(math.MaxUint32, target) {
			info.CastingCheck = CastingCheckSkip
		}
	case schema.ConvertedTypes.Uint64:
		if unsignedRangeFitsTarget(math.MaxUint64, target) {
			info.CastingCheck = CastingCheckSkip
		}
	}
	return info
}

func canSkipBoolToInteger(target *model.ColumnInfo) bool {
	if !isIntegerType(target.GetType()) {
		return false
	}
	if mysql.HasUnsignedFlag(target.GetFlag()) {
		_, maxValue, ok := unsignedIntTypeRange(target.GetType())
		return ok && maxValue >= 1
	}
	minValue, maxValue, ok := signedIntTypeRange(target.GetType())
	return ok && minValue <= 0 && maxValue >= 1
}

func signedRangeFitsTarget(sourceMin int64, sourceMax int64, target *model.ColumnInfo) bool {
	if !isIntegerType(target.GetType()) {
		return false
	}
	if mysql.HasUnsignedFlag(target.GetFlag()) {
		if sourceMin < 0 {
			return false
		}
		_, maxValue, ok := unsignedIntTypeRange(target.GetType())
		return ok && uint64(sourceMax) <= maxValue
	}
	minValue, maxValue, ok := signedIntTypeRange(target.GetType())
	return ok && sourceMin >= minValue && sourceMax <= maxValue
}

func unsignedRangeFitsTarget(sourceMax uint64, target *model.ColumnInfo) bool {
	if !isIntegerType(target.GetType()) {
		return false
	}
	if mysql.HasUnsignedFlag(target.GetFlag()) {
		_, maxValue, ok := unsignedIntTypeRange(target.GetType())
		return ok && sourceMax <= maxValue
	}
	_, maxValue, ok := signedIntTypeRange(target.GetType())
	return ok && sourceMax <= uint64(maxValue)
}

func isIntegerType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		return true
	default:
		return false
	}
}

func signedIntTypeRange(tp byte) (minValue int64, maxValue int64, ok bool) {
	switch tp {
	case mysql.TypeTiny:
		return math.MinInt8, math.MaxInt8, true
	case mysql.TypeShort:
		return math.MinInt16, math.MaxInt16, true
	case mysql.TypeInt24:
		return -(1 << 23), (1 << 23) - 1, true
	case mysql.TypeLong:
		return math.MinInt32, math.MaxInt32, true
	case mysql.TypeLonglong:
		return math.MinInt64, math.MaxInt64, true
	default:
		return 0, 0, false
	}
}

func unsignedIntTypeRange(tp byte) (minValue uint64, maxValue uint64, ok bool) {
	switch tp {
	case mysql.TypeTiny:
		return 0, math.MaxUint8, true
	case mysql.TypeShort:
		return 0, math.MaxUint16, true
	case mysql.TypeInt24:
		return 0, (1 << 24) - 1, true
	case mysql.TypeLong:
		return 0, math.MaxUint32, true
	case mysql.TypeLonglong:
		return 0, math.MaxUint64, true
	default:
		return 0, 0, false
	}
}

func canSkipDecimalByMeta(decimalMeta schema.DecimalMetadata, target *model.ColumnInfo) bool {
	if target.GetType() != mysql.TypeNewDecimal || target.GetFlen() <= 0 || target.GetDecimal() < 0 {
		return false
	}
	if !decimalMeta.IsSet {
		return false
	}
	if int(decimalMeta.Scale) != target.GetDecimal() {
		return false
	}
	if int(decimalMeta.Precision) > target.GetFlen() {
		return false
	}
	return true
}

func canSkipUTF8StringTarget(target *model.ColumnInfo) bool {
	if target.GetType() != mysql.TypeVarchar && target.GetType() != mysql.TypeVarString {
		return false
	}
	if mysql.HasBinaryFlag(target.GetFlag()) || strings.EqualFold(target.GetCharset(), "binary") {
		return false
	}
	return strings.EqualFold(target.GetCharset(), "utf8mb4")
}

func temporalTargetType(target *model.ColumnInfo, defaultType byte) byte {
	if target == nil {
		return defaultType
	}
	switch target.GetType() {
	case mysql.TypeDate:
		return mysql.TypeDate
	case mysql.TypeDatetime:
		return mysql.TypeDatetime
	default:
		return defaultType
	}
}

func temporalTargetFSP(target *model.ColumnInfo, defaultFSP int) int {
	if target == nil {
		return defaultFSP
	}
	switch target.GetType() {
	case mysql.TypeDate:
		return 0
	case mysql.TypeDatetime:
		if target.GetDecimal() < 0 {
			return defaultFSP
		}
		return min(target.GetDecimal(), types.MaxFsp)
	default:
		return defaultFSP
	}
}

func setTemporalDatum(t time.Time, d *types.Datum, tp byte, fsp int) {
	d.SetMysqlTime(types.NewTime(types.FromGoTime(t), tp, fsp))
}

func initializeMyDecimal(d *types.Datum) *types.MyDecimal {
	// reuse existing decimal
	if d.Kind() == types.KindMysqlDecimal {
		dec := d.GetMysqlDecimal()
		*dec = zeroMyDecimal
		return dec
	}

	dec := new(types.MyDecimal)
	d.SetMysqlDecimal(dec)
	return dec
}

func setDatumFromDecimalByte(d *types.Datum, val []byte, scale int) error {
	// Typically it shouldn't happen.
	if len(val) == 0 {
		return errors.New("invalid parquet decimal byte array")
	}

	// Truncate leading zeros in two's complement representation.
	negative := (val[0] & 0x80) != 0
	start := 0
	for ; start < len(val); start++ {
		if negative && val[start] != 0xff || !negative && val[start] != 0x00 {
			break
		}
	}
	// Keep at least one byte.
	start = max(start-1, 0)
	val = val[start:]

	// If the length or scale is too large, fallback to string parsing.
	if len(val) >= maximumDecimalBytes || scale > 81 {
		s := getStringFromParquetByte(val, scale)
		d.SetBytesAsString(s, "utf8mb4_bin", 0)
		return nil
	}

	dec := initializeMyDecimal(d)
	return dec.FromParquetArray(val, scale)
}

func getStringFromParquetByte(rawBytes []byte, scale int) []byte {
	base := uint64(1_000_000_000)
	baseDigits := 9

	negative := (rawBytes[0] & 0x80) != 0
	if negative {
		for i := range rawBytes {
			rawBytes[i] = ^rawBytes[i]
		}
		for i := len(rawBytes) - 1; i >= 0; i-- {
			rawBytes[i]++
			if rawBytes[i] != 0 {
				break
			}
		}
	}

	var (
		s          = make([]byte, 0, 64)
		n          int
		nDigits    int
		startIndex = 0
		endIndex   = len(rawBytes)
	)

	for startIndex < endIndex && rawBytes[startIndex] == 0 {
		startIndex++
	}

	// Convert base-256 bytes to base-10 string representation.
	for startIndex < endIndex {
		var rem uint64
		for i := startIndex; i < endIndex; i++ {
			v := (rem << 8) | uint64(rawBytes[i])
			q := v / base
			rem = v % base
			rawBytes[i] = byte(q)
			if q == 0 && i == startIndex {
				startIndex++
			}
		}

		for range baseDigits {
			s = append(s, byte(48+rem%10))
			n++
			nDigits++
			rem /= 10
			if nDigits == scale {
				s = append(s, '.')
				n++
			}
			if startIndex == endIndex && rem == 0 {
				break
			}
		}
	}

	for nDigits < scale+1 {
		s = append(s, '0')
		n++
		nDigits++
		if nDigits == scale {
			s = append(s, '.')
			n++
		}
	}

	if negative {
		s = append(s, '-')
	}

	// Reverse the string.
	for i := range len(s) / 2 {
		j := len(s) - 1 - i
		s[i], s[j] = s[j], s[i]
	}

	return s
}

func setParquetDecimalFromInt64(
	unscaled int64,
	dec *types.MyDecimal,
	decimalMeta schema.DecimalMetadata,
) error {
	dec.FromInt(unscaled)

	scale := int(decimalMeta.Scale)
	if err := dec.Shift(-scale); err != nil {
		return err
	}

	return dec.Round(dec, scale, types.ModeTruncate)
}

//nolint:all_revive
func getBoolDataSetter(val bool, d *types.Datum) error {
	if val {
		d.SetUint64(1)
	} else {
		d.SetUint64(0)
	}
	return nil
}

func getInt32Setter(converted *convertedType, loc *time.Location, target *model.ColumnInfo) setter[int32] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		return func(val int32, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(int64(val), dec, converted.decimalMeta)
		}
	case schema.ConvertedTypes.Date:
		return func(val int32, d *types.Datum) error {
			// Convert days since Unix epoch to time.Time
			t := time.Unix(int64(val)*86400, 0).In(time.UTC)
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeDate, 0)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.TimeMillis:
		return func(val int32, d *types.Datum) error {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(int64(val)).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return nil
		}
	case schema.ConvertedTypes.Int32, schema.ConvertedTypes.Uint32,
		schema.ConvertedTypes.Int16, schema.ConvertedTypes.Uint16,
		schema.ConvertedTypes.Int8, schema.ConvertedTypes.Uint8,
		schema.ConvertedTypes.None:
		return func(val int32, d *types.Datum) error {
			d.SetInt64(int64(val))
			return nil
		}
	}

	return nil
}

func getInt64Setter(converted *convertedType, loc *time.Location, target *model.ColumnInfo) setter[int64] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	switch converted.converted {
	case schema.ConvertedTypes.Uint64,
		schema.ConvertedTypes.Uint32, schema.ConvertedTypes.Int32,
		schema.ConvertedTypes.Uint16, schema.ConvertedTypes.Int16,
		schema.ConvertedTypes.Uint8, schema.ConvertedTypes.Int8:
		return func(val int64, d *types.Datum) error {
			d.SetUint64(uint64(val))
			return nil
		}
	case schema.ConvertedTypes.None, schema.ConvertedTypes.Int64:
		return func(val int64, d *types.Datum) error {
			d.SetInt64(val)
			return nil
		}
	case schema.ConvertedTypes.TimeMicros:
		return func(val int64, d *types.Datum) error {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return nil
		}
	case schema.ConvertedTypes.TimestampMillis:
		return func(val int64, d *types.Datum) error {
			// Convert milliseconds to time.Time
			t := time.UnixMilli(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return nil
		}
	case schema.ConvertedTypes.TimestampMicros:
		return func(val int64, d *types.Datum) error {
			// Convert microseconds to time.Time
			t := time.UnixMicro(val).In(time.UTC)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			setTemporalDatum(t, d, temporalType, temporalFSP)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val int64, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(val, dec, converted.decimalMeta)
		}
	}

	return nil
}

// newInt96 is a utility function to create a parquet.Int96 for test,
// where microseconds is the number of microseconds since Unix epoch.
func newInt96(microseconds int64) parquet.Int96 {
	day := uint32(microseconds/(86400*1e6) + 2440588)
	nanoOfDay := uint64(microseconds % (86400 * 1e6) * 1e3)
	var b [12]byte
	binary.LittleEndian.PutUint64(b[:8], nanoOfDay)
	binary.LittleEndian.PutUint32(b[8:], day)
	return parquet.Int96(b)
}

func setInt96Data(
	val parquet.Int96,
	d *types.Datum,
	loc *time.Location,
	adjustToUTC bool,
	targetType byte,
	targetFSP int,
) {
	// FYI: https://github.com/apache/spark/blob/d66a4e82eceb89a274edeb22c2fb4384bed5078b/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetWriteSupport.scala#L171-L178
	// INT96 timestamp layout
	// --------------------------
	// |   64 bit   |   32 bit   |
	// ---------------------------
	// |  nano sec  |  julian day  |
	// ---------------------------
	// NOTE:
	// INT96 is a deprecated type in parquet format to store timestamp, which consists of
	// two parts: the first 8 bytes is the nanoseconds within the day, and the last 4 bytes
	// is the Julian Day (days since noon on January 1, 4713 BC). And it will be converted it to UTC by
	//   julian day - 2440588 (Julian Day of the Unix epoch 1970-01-01 00:00:00)
	// As julian day is decoded as uint32, so if user store a date before 1970-01-01, the converted time will be wrong
	// and possibly to be truncated.
	t := val.ToTime().In(time.UTC)
	if adjustToUTC {
		t = t.In(loc)
	}
	setTemporalDatum(t, d, targetType, targetFSP)
}

func getInt96Setter(converted *convertedType, loc *time.Location, target *model.ColumnInfo) setter[parquet.Int96] {
	temporalType := temporalTargetType(target, mysql.TypeTimestamp)
	temporalFSP := temporalTargetFSP(target, 6)
	if temporalType == mysql.TypeDate {
		temporalFSP = 0
	}
	return func(val parquet.Int96, d *types.Datum) error {
		setInt96Data(val, d, loc, converted.IsAdjustedToUTC, temporalType, temporalFSP)
		return nil
	}
}

func setFloat32Data(val float32, d *types.Datum) error {
	d.SetFloat32(val)
	return nil
}

func setFloat64Data(val float64, d *types.Datum) error {
	d.SetFloat64(val)
	return nil
}

func getByteArraySetter(converted *convertedType) setter[parquet.ByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.ByteArray, d *types.Datum) error {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.ByteArray, d *types.Datum) error {
			return setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
		}
	}

	return nil
}

func getFixedLenByteArraySetter(converted *convertedType) setter[parquet.FixedLenByteArray] {
	switch converted.converted {
	case schema.ConvertedTypes.None, schema.ConvertedTypes.BSON, schema.ConvertedTypes.JSON, schema.ConvertedTypes.UTF8, schema.ConvertedTypes.Enum:
		return func(val parquet.FixedLenByteArray, d *types.Datum) error {
			// length is unused here
			d.SetBytesAsString(val, "utf8mb4_bin", 0)
			return nil
		}
	case schema.ConvertedTypes.Decimal:
		return func(val parquet.FixedLenByteArray, d *types.Datum) error {
			return setDatumFromDecimalByte(d, val, int(converted.decimalMeta.Scale))
		}
	}

	return nil
}

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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

type setter[T parquet.ColumnTypes] func(T, *types.Datum) error

var (
	zeroMyDecimal = types.MyDecimal{}
	// Spark's legacy timestamp rebasing only applies to instants before
	// 1900-01-01T00:00:00Z. Keep the cutoff in Unix microseconds because this
	// helper rebases microsecond-encoded timestamps and can return newer values
	// as-is with a cheap comparison. This is a Spark compatibility threshold,
	// not a Go time package constant.
	legacyTimestampRebaseCutoffMicros = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	// The entries here are copied from Spark's RebaseDateTime.rebaseJulianToGregorianDays.
	// Each element in sparkLegacyDateRebaseSwitchDays is the first stored legacy
	// Spark DATE day count in an interval; the diff at the same index in
	// sparkLegacyDateRebaseDiffs applies until the next switch day.
	sparkLegacyDateRebaseDiffs      = [...]int{2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 0}
	sparkLegacyDateRebaseSwitchDays = [...]int{
		-719164, // 0001-01-01: first positive-year legacy DATE Spark handles with the switch table
		-682945, // 0100-03-01: switch from a +2-day to a +1-day rebase
		-646420, // 0200-03-01: start of Spark's zero-delta legacy DATE interval
		-609895, // 0300-03-02: switch from 0 days to -1 day
		-536845, // 0500-03-03: switch from -1 day to -2 days
		-500320, // 0600-03-04: switch from -2 days to -3 days
		-463795, // 0700-03-05: switch from -3 days to -4 days
		-390745, // 0900-03-06: switch from -4 days to -5 days
		-354220, // 1000-03-07: switch from -5 days to -6 days
		-317695, // 1100-03-08: switch from -6 days to -7 days
		-244645, // 1300-03-09: switch from -7 days to -8 days
		-208120, // 1400-03-10: switch from -8 days to -9 days
		-171595, // 1500-03-11: switch from -9 days to -10 days
		-141427, // 1582-10-15: Gregorian cutover day; Spark stops shifting dates from here onward
	}
)

const (
	// maximumDecimalBytes is the maximum byte length allowed to be parsed directly.
	// It guarantees the value can be stored in MyDecimal wordbuf without overflow.
	// That is: floor(log256(10^81-1))
	maximumDecimalBytes = 33
	microsPerDay        = int64(24 * time.Hour / time.Microsecond)
	// unixSecondsPerDay converts a midnight UTC Unix timestamp into a whole-day
	// count. DATE values are stored as days since the Unix epoch, so the fallback
	// path below divides Unix seconds by this constant to get back to a day count.
	unixSecondsPerDay = int64(24 * time.Hour / time.Second)
	// julianDayOfUnixEpoch is the Julian day number for 1970-01-01.
	julianDayOfUnixEpoch = int64(2440588)
	// julianDayNumberConversionOffset is the constant from the standard
	// Julian-day-number to Julian-calendar conversion. It shifts the input JDN
	// into a four-year cycle form before extracting year/month/day fields.
	julianDayNumberConversionOffset = int64(32082)
	// julianDaysPerFourYearCycle is the number of days in four Julian calendar
	// years: 365*4 plus one leap day.
	julianDaysPerFourYearCycle = int64(1461)
	// julianMonthConversionFactor is the month extractor denominator from the
	// March-based civil-date conversion formula.
	julianMonthConversionFactor = int64(153)
	// julianCalendarYearOffset is the normalization offset used by the
	// Julian-day-number conversion before mapping back to the civil year.
	julianCalendarYearOffset = int64(4800)
)

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

// julianDayNumberToDate converts a Julian day number into a Julian calendar
// date. Spark's DATE fallback for very early values needs the Julian calendar
// label first, then re-encodes that same label on the proleptic Gregorian day
// axis. The arithmetic here follows the standard Julian-day-number conversion
// formula, written with named constants so each step's meaning is visible.
func julianDayNumberToDate(jdn int64) (year int, month time.Month, day int) {
	c := jdn + julianDayNumberConversionOffset
	d := (4*c + 3) / julianDaysPerFourYearCycle
	e := c - (julianDaysPerFourYearCycle*d)/4
	m := (5*e + 2) / julianMonthConversionFactor
	day = int(e - (julianMonthConversionFactor*m+2)/5 + 1)
	month = time.Month(m + 3 - 12*(m/10))
	year = int(d - julianCalendarYearOffset + m/10)
	return
}

// rebaseJulianToGregorianDays converts a Julian-calendar day count into the
// corresponding proleptic Gregorian day count while preserving the wall-clock
// date label.
func rebaseJulianToGregorianDays(days int) int {
	if days < sparkLegacyDateRebaseSwitchDays[0] {
		year, month, day := julianDayNumberToDate(int64(days) + julianDayOfUnixEpoch)
		return int(time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Unix() / unixSecondsPerDay)
	}

	i := len(sparkLegacyDateRebaseSwitchDays)
	for i > 1 && days < sparkLegacyDateRebaseSwitchDays[i-1] {
		i--
	}
	return days + sparkLegacyDateRebaseDiffs[i-1]
}

// floorDivInt64 returns floor(x/y). Go integer division truncates toward zero,
// which is not the semantics Spark uses when splitting negative microsecond
// timestamps into days and micros-of-day.
func floorDivInt64(x, y int64) int64 {
	q := x / y
	if r := x % y; r != 0 && (r < 0) != (y < 0) {
		q--
	}
	return q
}

// floorModInt64 returns x modulo y using floor division, matching Java/Scala
// Math.floorMod for negative timestamps.
func floorModInt64(x, y int64) int64 {
	return x - floorDivInt64(x, y)*y
}

// Spark falls back to a hybrid-calendar conversion before the generated switch
// table starts. The first switch and diff encode the source Julian-side offset
// and target Gregorian-side offset at the Common Era boundary.
// Spark source:
// https://github.com/apache/spark/blob/v3.5.7/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/RebaseDateTime.scala#L438-L478
// Fallback branch:
// https://github.com/apache/spark/blob/v3.5.7/sql/api/src/main/scala/org/apache/spark/sql/catalyst/util/RebaseDateTime.scala#L502-L510
func rebaseSparkJulianToGregorianMicrosBeforeSwitch(micros, firstSwitch, firstDiff int64) int64 {
	julianCommonEraStartMicros := int64(sparkLegacyDateRebaseSwitchDays[0]) * microsPerDay
	gregorianCommonEraStartMicros := time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	sourceOffset := julianCommonEraStartMicros - firstSwitch
	targetOffset := gregorianCommonEraStartMicros - firstSwitch - firstDiff

	localMicros := micros + sourceOffset
	localDays := floorDivInt64(localMicros, microsPerDay)
	microsOfDay := floorModInt64(localMicros, microsPerDay)
	year, month, day := julianDayNumberToDate(localDays + julianDayOfUnixEpoch)
	gregorianLocalMicros := time.Date(year, month, day, 0, 0, 0, 0, time.UTC).UnixMicro() + microsOfDay
	return gregorianLocalMicros - targetOffset
}

func rebaseSparkJulianToGregorianMicros(timeZoneID string, micros int64) (int64, error) {
	if micros >= legacyTimestampRebaseCutoffMicros {
		return micros, nil
	}

	index, ok := sparkJulianGregorianRebaseMicrosIndex(timeZoneID)
	if !ok {
		// This should not happen for production callers: sparkRebaseTimeZoneID
		// only returns IDs that exist in the generated Spark rebase table, and
		// falls back to UTC, which is also generated.
		return 0, errors.Errorf("unknown Spark legacy timestamp rebase timezone %q", timeZoneID)
	}

	switches, diffs := sparkJulianGregorianRebaseMicrosSlices(index)
	if len(switches) == 0 {
		// This should not happen unless the generated Spark rebase table is
		// malformed. Every generated timezone record has at least one switch.
		return 0, errors.Errorf("empty Spark legacy timestamp rebase table for timezone %q", timeZoneID)
	}
	if micros < switches[0] {
		return rebaseSparkJulianToGregorianMicrosBeforeSwitch(micros, switches[0], diffs[0]), nil
	}

	i := len(switches)
	for i > 1 && micros < switches[i-1] {
		i--
	}
	return micros + diffs[i-1], nil
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

func getInt32Setter(converted *convertedType, loc *time.Location) setter[int32] {
	switch converted.converted {
	case schema.ConvertedTypes.Decimal:
		return func(val int32, d *types.Datum) error {
			dec := initializeMyDecimal(d)
			return setParquetDecimalFromInt64(int64(val), dec, converted.decimalMeta)
		}
	case schema.ConvertedTypes.Date:
		return func(val int32, d *types.Datum) error {
			if converted.sparkRebaseTimeZoneID != "" {
				val = int32(rebaseJulianToGregorianDays(int(val)))
			}
			t := arrow.Date32(val).ToTime()
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
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
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

func getInt64Setter(converted *convertedType, loc *time.Location) setter[int64] {
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
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.TimestampMillis:
		return func(val int64, d *types.Datum) error {
			if converted.sparkRebaseTimeZoneID != "" {
				rebased, err := rebaseSparkJulianToGregorianMicros(converted.sparkRebaseTimeZoneID, val*1000)
				if err != nil {
					return err
				}
				val = rebased / 1000
			}
			t := arrow.Timestamp(val).ToTime(arrow.Millisecond)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
			return nil
		}
	case schema.ConvertedTypes.TimestampMicros:
		return func(val int64, d *types.Datum) error {
			if converted.sparkRebaseTimeZoneID != "" {
				rebased, err := rebaseSparkJulianToGregorianMicros(converted.sparkRebaseTimeZoneID, val)
				if err != nil {
					return err
				}
				val = rebased
			}
			t := arrow.Timestamp(val).ToTime(arrow.Microsecond)
			if converted.IsAdjustedToUTC {
				t = t.In(loc)
			}
			mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
			d.SetMysqlTime(mysqlTime)
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
	// INT96 stores the time-of-day as a non-negative field, so pre-epoch
	// timestamps must be split with floor division instead of Go's truncation.
	dayOffset := floorDivInt64(microseconds, microsPerDay)
	microsOfDay := floorModInt64(microseconds, microsPerDay)
	day := uint32(dayOffset + julianDayOfUnixEpoch)
	nanoOfDay := uint64(microsOfDay * int64(time.Microsecond))
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
	rebaseTimeZoneID string,
) error {
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
	// subtracting the Julian day of the Unix epoch (1970-01-01 00:00:00).
	micros := int96ToUnixMicros(val)
	if rebaseTimeZoneID != "" {
		rebased, err := rebaseSparkJulianToGregorianMicros(rebaseTimeZoneID, micros)
		if err != nil {
			return err
		}
		micros = rebased
	}
	t := arrow.Timestamp(micros).ToTime(arrow.Microsecond)
	if adjustToUTC {
		t = t.In(loc)
	}
	mysqlTime := types.NewTime(types.FromGoTime(t), mysql.TypeTimestamp, 6)
	d.SetMysqlTime(mysqlTime)
	return nil
}

func int96ToUnixMicros(val parquet.Int96) int64 {
	nanosOfDay := int64(binary.LittleEndian.Uint64(val[:8]))
	julianDay := int64(binary.LittleEndian.Uint32(val[8:]))
	return (julianDay-julianDayOfUnixEpoch)*microsPerDay + nanosOfDay/int64(time.Microsecond)
}

func getInt96Setter(converted *convertedType, loc *time.Location) setter[parquet.Int96] {
	return func(val parquet.Int96, d *types.Datum) error {
		return setInt96Data(val, d, loc, converted.IsAdjustedToUTC, converted.sparkRebaseTimeZoneID)
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

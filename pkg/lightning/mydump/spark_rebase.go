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

package mydump

import (
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/pingcap/errors"
)

const (
	// sparkRebaseDefaultTimeZoneID must exist in the generated Spark rebase
	// table. Tests assert this invariant so the parser can return it directly.
	sparkRebaseDefaultTimeZoneID = "UTC"
	// This literal identifies footer metadata written by Spark. Keep it in one
	// place because Arrow's AppVersion comparison is app-sensitive: LessThan
	// returns false when the parsed writer app and the cutoff app differ. That
	// matters here because the cutoffs below are Spark-specific calendar
	// boundaries; non-Spark writers like parquet-mr, Hive, or Aurora snapshots
	// should not match them and therefore will not trigger Spark-version-based
	// rebasing. The same term must be used both when parsing
	// "org.apache.spark.version" and when constructing the Spark cutoffs below,
	// otherwise legacy Spark files could be skipped silently.
	sparkAppName            = "spark"
	sparkVersionMetadataKey = "org.apache.spark.version"
	// Spark writes this file-level footer key alongside its legacy rebase markers
	// and reads it back only to choose the timezone for legacy date/timestamp
	// rebasing. It is Spark-specific metadata, not Parquet's column-level
	// TIMESTAMP semantics, so its presence does not mean an INT64
	// TIMESTAMP_MICROS value is a "timestamp with timezone".
	sparkTimeZoneMetadataKey       = "org.apache.spark.timeZone"
	sparkLegacyDateTimeMetadataKey = "org.apache.spark.legacyDateTime"
	sparkLegacyINT96MetadataKey    = "org.apache.spark.legacyINT96"
)

var (
	// See https://spark.apache.org/docs/3.5.7/sql-data-sources-parquet.html for
	// the cutoff versions. Spark 3.0.0 switched to Proleptic Gregorian calendar
	// for DATE and TIMESTAMP, and Spark 3.1.0 switched to a new INT96 timestamp
	// rebasing.
	sparkDatetimeRebaseCutoff = metadata.NewAppVersionExplicit(sparkAppName, 3, 0, 0)
	sparkINT96RebaseCutoff    = metadata.NewAppVersionExplicit(sparkAppName, 3, 1, 0)
)

const (
	microsPerDay = int64(24 * time.Hour / time.Microsecond)
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

func sparkVersionFromMetadata(fileMeta *metadata.FileMetaData) *metadata.AppVersion {
	if fileMeta == nil {
		return nil
	}

	if kv := fileMeta.KeyValueMetadata(); kv != nil {
		if version := kv.FindValue(sparkVersionMetadataKey); version != nil {
			return sparkAppVersion(*version)
		}
	}

	return metadata.NewAppVersion(fileMeta.GetCreatedBy())
}

func sparkAppVersion(version string) *metadata.AppVersion {
	version = strings.TrimSpace(version)
	if version == "" {
		return nil
	}

	appVersion := metadata.NewAppVersion(sparkAppName + " version " + version)
	if appVersion.App != sparkAppName {
		return nil
	}
	return appVersion
}

func sparkRebaseTimeZoneID(
	fileMeta *metadata.FileMetaData,
	cutoff *metadata.AppVersion,
	legacyKey string,
	fallback *time.Location,
) string {
	if fileMeta == nil {
		return ""
	}

	kv := fileMeta.KeyValueMetadata()
	legacy := false
	if kv != nil {
		if kv.FindValue(legacyKey) != nil {
			legacy = true
		}
	}

	if !legacy {
		version := sparkVersionFromMetadata(fileMeta)
		if version != nil && version.App == sparkAppName && version.LessThan(cutoff) {
			legacy = true
		}
	}

	if !legacy {
		// Empty string means this file should stay on the normal Proleptic
		// Gregorian read path, without Spark's legacy hybrid-calendar correction.
		return ""
	}

	if kv != nil {
		if tzName := kv.FindValue(sparkTimeZoneMetadataKey); tzName != nil {
			timeZoneID := strings.TrimSpace(*tzName)
			if _, ok := sparkJulianGregorianRebaseMicrosIndex(timeZoneID); ok {
				return timeZoneID
			}
			// Spark falls back to Java TimeZone calendar rebasing when the
			// footer timezone is not in its generated rebase table. TiDB keeps
			// this path deterministic instead of partially emulating Java's
			// timezone aliases and historical tzdb behavior in Go; use only
			// zones that exist in the generated Spark table.
		}
	}

	if fallback != nil {
		timeZoneID := fallback.String()
		if _, ok := sparkJulianGregorianRebaseMicrosIndex(timeZoneID); ok {
			return timeZoneID
		}
	}
	// Spark's timezone footer is optional, but timestamp rebasing still needs a
	// deterministic location. UTC is the safest last resort because it preserves
	// the encoded instant without introducing an extra local-time shift, and
	// DATE rebasing ignores the location entirely.
	return sparkRebaseDefaultTimeZoneID
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

type sparkRebaseMicrosLookup struct {
	timeZoneID string
	switches   []int64
	diffs      []int64
}

var (
	// Spark's legacy timestamp rebasing only applies to instants before
	// 1900-01-01T00:00:00Z. Keep the cutoff in Unix microseconds because this
	// helper rebases microsecond-encoded timestamps and can return newer values
	// as-is with a cheap comparison. This is a Spark compatibility threshold,
	// not a Go time package constant.
	legacyTimestampRebaseCutoffMicros = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
)

func sparkRebaseMicrosFromMetadata(
	fileMeta *metadata.FileMetaData,
	cutoff *metadata.AppVersion,
	legacyKey string,
	fallback *time.Location,
) (sparkRebaseMicrosLookup, error) {
	timeZoneID := sparkRebaseTimeZoneID(fileMeta, cutoff, legacyKey, fallback)
	if timeZoneID == "" {
		return sparkRebaseMicrosLookup{}, nil
	}
	return newSparkRebaseMicrosLookup(timeZoneID)
}

func newSparkRebaseMicrosLookup(timeZoneID string) (sparkRebaseMicrosLookup, error) {
	index, ok := sparkJulianGregorianRebaseMicrosIndex(timeZoneID)
	if !ok {
		// This should not happen for production callers: sparkRebaseTimeZoneID
		// only returns IDs that exist in the generated Spark rebase table, and
		// falls back to UTC, which is also generated.
		return sparkRebaseMicrosLookup{}, errors.Errorf("unknown Spark legacy timestamp rebase timezone %q", timeZoneID)
	}

	switches, diffs := sparkJulianGregorianRebaseMicrosSlices(index)
	if len(switches) == 0 {
		// This should not happen unless the generated Spark rebase table is
		// malformed. Every generated timezone record has at least one switch.
		return sparkRebaseMicrosLookup{}, errors.Errorf("empty Spark legacy timestamp rebase table for timezone %q", timeZoneID)
	}
	return sparkRebaseMicrosLookup{
		timeZoneID: timeZoneID,
		switches:   switches,
		diffs:      diffs,
	}, nil
}

func (lookup *sparkRebaseMicrosLookup) rebase(micros int64) (int64, error) {
	if micros >= legacyTimestampRebaseCutoffMicros {
		return micros, nil
	}
	if len(lookup.switches) == 0 {
		return 0, errors.Errorf("empty Spark legacy timestamp rebase table for timezone %q", lookup.timeZoneID)
	}
	if micros < lookup.switches[0] {
		return rebaseSparkJulianToGregorianMicrosBeforeSwitch(micros, lookup.switches[0], lookup.diffs[0]), nil
	}

	i := len(lookup.switches)
	for i > 1 && micros < lookup.switches[i-1] {
		i--
	}
	return micros + lookup.diffs[i-1], nil
}

func rebaseSparkJulianToGregorianMicros(timeZoneID string, micros int64) (int64, error) {
	if micros >= legacyTimestampRebaseCutoffMicros {
		return micros, nil
	}
	lookup, err := newSparkRebaseMicrosLookup(timeZoneID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return lookup.rebase(micros)
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

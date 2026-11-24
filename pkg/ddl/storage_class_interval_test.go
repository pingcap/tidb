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
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestParseStorageClassInterval(t *testing.T) {
	valid := map[string]storageClassInterval{
		"INTERVAL 3 YEAR":         {count: 3, unit: ast.TimeUnitYear},
		"interval 1 quarter":      {count: 1, unit: ast.TimeUnitQuarter},
		"  Interval   12  Month ": {count: 12, unit: ast.TimeUnitMonth},
		"INTERVAL 2 WEEK":         {count: 2, unit: ast.TimeUnitWeek},
		"INTERVAL 90 DAY":         {count: 90, unit: ast.TimeUnitDay},
		"INTERVAL 3650000 DAY":    {count: 3650000, unit: ast.TimeUnitDay},
	}
	for value, expected := range valid {
		interval, isInterval, err := parseStorageClassInterval(value)
		require.NoError(t, err, value)
		require.True(t, isInterval, value)
		require.Equal(t, expected, interval, value)
	}

	for _, literal := range []string{"2023-01-01", "'2023-01-01'", "100", "MAXVALUE", "", "INTERVALS 3 YEAR", "TO_DAYS('2023-01-01')"} {
		_, isInterval, err := parseStorageClassInterval(literal)
		require.NoError(t, err, literal)
		require.False(t, isInterval, literal)
	}

	for _, invalid := range []string{
		"INTERVAL", "INTERVAL 3", "INTERVAL YEAR", "INTERVAL 0 DAY", "INTERVAL -1 DAY", "INTERVAL +1 DAY",
		"INTERVAL 1.5 DAY", "INTERVAL 1 HOUR", "INTERVAL 1 SECOND", "INTERVAL 1 DAYS", "INTERVAL 1 DAY + 1",
		"INTERVAL 1 DAY 1", "INTERVAL @n DAY", "INTERVAL (SELECT 1) DAY", "INTERVAL a DAY", "INTERVAL 3650001 DAY",
		"INTERVAL 99999999999999999999 DAY", "INTERVAL '3' YEAR",
	} {
		_, isInterval, err := parseStorageClassInterval(invalid)
		require.True(t, isInterval, invalid)
		require.ErrorContains(t, err, "invalid 'less_than' interval", invalid)
	}
}

func TestStorageClassIntervalCutoff(t *testing.T) {
	utc := func(s string) time.Time {
		tm, err := time.Parse(time.DateTime, s)
		require.NoError(t, err)
		return tm
	}
	cases := []struct {
		now      time.Time
		interval string
		expected time.Time
	}{
		{utc("2026-10-31 18:00:00"), "INTERVAL 3 YEAR", utc("2023-10-31 18:00:00")},
		// MySQL month-end semantics: clamp to the last day of the month.
		{utc("2024-03-31 00:00:00"), "INTERVAL 1 MONTH", utc("2024-02-29 00:00:00")},
		{utc("2023-03-31 00:00:00"), "INTERVAL 1 MONTH", utc("2023-02-28 00:00:00")},
		{utc("2024-02-29 12:00:00"), "INTERVAL 1 YEAR", utc("2023-02-28 12:00:00")},
		{utc("2026-05-31 00:00:00"), "INTERVAL 1 QUARTER", utc("2026-02-28 00:00:00")},
		{utc("2026-01-07 00:00:00"), "INTERVAL 2 WEEK", utc("2025-12-24 00:00:00")},
		{utc("2026-03-01 00:00:00"), "INTERVAL 1 DAY", utc("2026-02-28 00:00:00")},
		// The cutoff is always computed in UTC.
		{time.Date(2026, 11, 1, 2, 0, 0, 0, time.FixedZone("UTC+8", 8*3600)), "INTERVAL 3 YEAR", utc("2023-10-31 18:00:00")},
	}
	for _, c := range cases {
		interval, _, err := parseStorageClassInterval(c.interval)
		require.NoError(t, err)
		cutoff, err := interval.cutoff(c.now)
		require.NoError(t, err)
		require.Equal(t, c.expected, cutoff, "%s - %s", c.now, c.interval)
		require.Equal(t, time.UTC, cutoff.Location())
	}

	interval, _, err := parseStorageClassInterval("INTERVAL 2027 YEAR")
	require.NoError(t, err)
	_, err = interval.cutoff(utc("2026-01-01 00:00:00"))
	require.ErrorContains(t, err, "out of range")
}

func TestStorageClassIntervalPartitionKeys(t *testing.T) {
	ctx := metabuild.NewContext()
	supported := []string{
		`create table t (dt date) partition by range columns (dt) (partition p0 values less than ('2023-01-01'))`,
		`create table t (dt datetime) partition by range columns (dt) (partition p0 values less than ('2023-01-01 00:00:00'))`,
		`create table t (dt date) partition by range (to_days(dt)) (partition p0 values less than (to_days('2023-01-01')))`,
		`create table t (dt datetime) partition by range (TO_DAYS(dt)) (partition p0 values less than (738886))`,
		`create table t (ts timestamp) partition by range (unix_timestamp(ts)) (partition p0 values less than (unix_timestamp('2023-01-01 00:00:00')))`,
	}
	for _, sql := range supported {
		tbInfo := buildStorageClassTableInfo(t, ctx, sql)
		_, err := getStorageClassIntervalKeyKind(tbInfo)
		require.NoError(t, err, sql)
	}

	unsupported := []string{
		`create table t (id int)`,
		`create table t (id int) partition by range columns (id) (partition p0 values less than (10))`,
		`create table t (s varchar(10)) partition by range columns (s) (partition p0 values less than ('2023-01-01'))`,
		`create table t (dt date, id int) partition by range columns (dt, id) (partition p0 values less than ('2023-01-01', 1))`,
		`create table t (id int) partition by range (id) (partition p0 values less than (10))`,
		`create table t (dt date) partition by range (year(dt)) (partition p0 values less than (2023))`,
		`create table t (dt date) partition by range (to_seconds(dt)) (partition p0 values less than (10))`,
		`create table t (dt date) partition by list (to_days(dt)) (partition p0 values in (10))`,
		`create table t (dt date) partition by hash (to_days(dt)) partitions 2`,
	}
	for _, sql := range unsupported {
		tbInfo := buildStorageClassTableInfo(t, ctx, sql)
		_, err := getStorageClassIntervalKeyKind(tbInfo)
		require.ErrorContains(t, err, "'less_than' interval only supports", sql)
	}
}

func evalStorageClassTiers(t *testing.T, tbInfo *model.TableInfo, storageClass string, now time.Time) map[string]string {
	settings, err := BuildStorageClassSettingsFromJSON(json.RawMessage(storageClass))
	require.NoError(t, err)
	evaluator, err := newStorageClassPartitionEvaluator(tbInfo, tbInfo.Partition.Definitions, settings, now)
	require.NoError(t, err)
	tiers := make(map[string]string)
	for i := range tbInfo.Partition.Definitions {
		part := &tbInfo.Partition.Definitions[i]
		def, err := evaluator.matchingDef(part)
		require.NoError(t, err)
		tier := model.StorageClassTierDefault
		if def != nil {
			tier = def.Tier
		}
		tiers[part.Name.L] = tier
	}
	return tiers
}

func TestStorageClassIntervalMatchesPartitions(t *testing.T) {
	ctx := metabuild.NewContext()
	// B4: evaluated at UTC 2026-10-31 18:00 (2026-11-01 02:00 at +08:00), so the
	// cutoff is 2023-10-31 18:00 and p202311 does not match.
	now := time.Date(2026, 10, 31, 18, 0, 0, 0, time.UTC)
	const rule = `{"tier":"IA","less_than":"INTERVAL 3 YEAR"}`
	ia, std := model.StorageClassTierIA, model.StorageClassTierStandard

	cases := []struct {
		create   string
		expected map[string]string
	}{
		{
			create: `create table t (dt date) partition by range columns (dt) (
partition p202310 values less than ('2023-10-01'), partition p20231031 values less than ('2023-10-31'),
partition p202311 values less than ('2023-11-01'), partition pmax values less than (maxvalue))`,
			expected: map[string]string{"p202310": ia, "p20231031": ia, "p202311": std, "pmax": std},
		},
		{
			create: `create table t (dt datetime) partition by range columns (dt) (
partition p0 values less than ('2023-10-31 18:00:00'), partition p1 values less than ('2023-10-31 18:00:01'),
partition pmax values less than (maxvalue))`,
			expected: map[string]string{"p0": ia, "p1": std, "pmax": std},
		},
		{
			create: `create table t (dt date) partition by range (to_days(dt)) (
partition p202310 values less than (to_days('2023-10-31')), partition p202311 values less than (to_days('2023-11-01')),
partition pmax values less than maxvalue)`,
			expected: map[string]string{"p202310": ia, "p202311": std, "pmax": std},
		},
		{
			create: `create table t (ts timestamp) partition by range (unix_timestamp(ts)) (
partition p0 values less than (unix_timestamp('2023-10-31 18:00:00')), partition p1 values less than (unix_timestamp('2023-10-31 18:00:01')),
partition pmax values less than maxvalue)`,
			expected: map[string]string{"p0": ia, "p1": std, "pmax": std},
		},
	}
	for _, c := range cases {
		tbInfo := buildStorageClassTableInfo(t, ctx, c.create)
		require.Equal(t, c.expected, evalStorageClassTiers(t, tbInfo, rule, now), c.create)
	}

	// names_in and a literal less_than keep their position in the rule order.
	tbInfo := buildStorageClassTableInfo(t, ctx, cases[0].create)
	tiers := evalStorageClassTiers(t, tbInfo,
		`[{"tier":"STANDARD","names_in":["p202310"]},{"tier":"IA","less_than":"INTERVAL 3 YEAR"},{"tier":"IA","less_than":"maxvalue"}]`, now)
	require.Equal(t, map[string]string{"p202310": std, "p20231031": ia, "p202311": ia, "pmax": ia}, tiers)
}

func TestCheckStorageClassIntervalDef(t *testing.T) {
	for _, storageClass := range []string{
		`{"tier":"STANDARD","less_than":"INTERVAL 1 YEAR"}`,
		`[{"tier":"IA"},{"tier":"STANDARD","less_than":"INTERVAL 1 YEAR"}]`,
	} {
		_, err := BuildStorageClassSettingsFromJSON(json.RawMessage(storageClass))
		require.ErrorContains(t, err, "only supports tier 'IA'", storageClass)
	}
	_, err := BuildStorageClassSettingsFromJSON(json.RawMessage(
		`{"tier":"IA","less_than":"INTERVAL 1 YEAR","transitions":[{"tier":"IA","after_days":1}]}`))
	require.Error(t, err)
	err = checkStorageClassIntervalDef(&model.StorageClassDef{
		Tier: model.StorageClassTierIA, LessThan: new(string), Transitions: []model.StorageClassTransitRule{{Tier: "IA", AfterDays: 1}},
	})
	require.NoError(t, err)
	lessThan := "INTERVAL 1 YEAR"
	err = checkStorageClassIntervalDef(&model.StorageClassDef{
		Tier: model.StorageClassTierIA, LessThan: &lessThan, Transitions: []model.StorageClassTransitRule{{Tier: "IA", AfterDays: 1}},
	})
	require.ErrorContains(t, err, "'transitions' together")
	_, err = BuildStorageClassSettingsFromJSON(json.RawMessage(`{"tier":"ia","less_than":"interval 1 year"}`))
	require.NoError(t, err)
	_, err = BuildStorageClassSettingsFromJSON(json.RawMessage(`{"tier":"IA","less_than":"INTERVAL 1 HOUR"}`))
	require.ErrorContains(t, err, "invalid 'less_than' interval")
}

func TestCheckStorageClassIntervalPartitionKey(t *testing.T) {
	ctx := metabuild.NewContext()
	toDays := buildStorageClassTableInfo(t, ctx, `create table t (dt date) partition by range (to_days(dt)) (
partition p0 values less than (to_days('2023-02-01')))`)
	rangeColumns := buildStorageClassTableInfo(t, ctx, `create table t (dt date) partition by range columns (dt) (
partition p0 values less than ('2023-02-01'))`)
	nonPartitioned := buildStorageClassTableInfo(t, ctx, `create table t (dt date)`)
	attr := func(lessThan string) string {
		return fmt.Sprintf(`{"storage_class":{"tier":"IA","less_than":%q}}`, lessThan)
	}

	for _, lessThan := range []string{"744913", "'744913'", "TO_DAYS('2023-02-01')", "MAXVALUE", "INTERVAL 3 YEAR"} {
		require.NoError(t, checkStorageClassIntervalPartitionKey(attr(lessThan), toDays), lessThan)
	}
	require.NoError(t, checkStorageClassIntervalPartitionKey(attr("2023-02-01"), rangeColumns))
	require.ErrorContains(t, checkStorageClassIntervalPartitionKey(attr("INTERVAL 3 YEAR"), nonPartitioned), "only supports")
	require.NoError(t, checkStorageClassIntervalPartitionKey(`{"storage_class":"IA"}`, nonPartitioned))
	require.NoError(t, checkStorageClassIntervalPartitionKey("", toDays))
}

func TestEvalStorageClassIntervalChanges(t *testing.T) {
	ctx := metabuild.NewContext()
	tbInfo := buildStorageClassTableInfo(t, ctx, `create table t (dt date) ENGINE_ATTRIBUTE = '{"storage_class":{"tier":"IA","less_than":"INTERVAL 1 MONTH"}}'
partition by range columns (dt) (partition p1 values less than ('2026-01-01'), partition p2 values less than ('2026-02-01'),
partition p3 values less than ('2026-03-01'))`)
	require.NoError(t, rebuildStorageClassForPartitions(tbInfo, tbInfo.Partition.Definitions))
	require.NoError(t, BuildStorageClassForPartitions(tbInfo.Partition.Definitions, tbInfo,
		mustStorageClassSettings(t, `{"tier":"IA","less_than":"2026-01-01"}`)))

	changes, err := evalStorageClassIntervalChanges(tbInfo, time.Date(2026, 2, 15, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.True(t, changes.hasInterval)
	require.Empty(t, changes.toIA)
	require.Empty(t, changes.notToIA)

	changes, err = evalStorageClassIntervalChanges(tbInfo, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, []string{"p2"}, changes.toIA)
	require.Empty(t, changes.notToIA)

	// The clock moved backwards: p1 would leave IA.
	changes, err = evalStorageClassIntervalChanges(tbInfo, time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Empty(t, changes.toIA)
	require.Equal(t, []string{"p1"}, changes.notToIA)

	tbInfo.ID = 100
	for i := range tbInfo.Partition.Definitions {
		tbInfo.Partition.Definitions[i].ID = int64(101 + i)
	}
	old := snapshotPhysicalStorageClasses(tbInfo)
	toIA := tbInfo.Clone()
	toIA.Partition.Definitions[1].StorageClassTier = model.StorageClassTierIA
	require.True(t, shouldApplyStorageClassIntervalRecalculation(tbInfo, old, snapshotPhysicalStorageClasses(toIA)))
	require.False(t, shouldApplyStorageClassIntervalRecalculation(tbInfo, old, old))
	toStandard := toIA.Clone()
	toStandard.Partition.Definitions[0].StorageClassTier = model.StorageClassTierStandard
	require.False(t, shouldApplyStorageClassIntervalRecalculation(tbInfo, old, snapshotPhysicalStorageClasses(toStandard)))

	tbInfo.EngineAttribute = `{"storage_class":{"tier":"IA","names_in":["p1"]}}`
	changes, err = evalStorageClassIntervalChanges(tbInfo, time.Now())
	require.NoError(t, err)
	require.False(t, changes.hasInterval)
}

func mustStorageClassSettings(t *testing.T, storageClass string) *model.StorageClassSettings {
	settings, err := BuildStorageClassSettingsFromJSON(json.RawMessage(storageClass))
	require.NoError(t, err)
	return settings
}

// FuzzStorageClassIntervalCutoff checks that evaluation never panics and that
// RANGE COLUMNS(DATE) and RANGE(TO_DAYS) agree on the same boundary dates.
func FuzzStorageClassIntervalCutoff(f *testing.F) {
	f.Add(int64(3), uint8(0), int64(1793469600), int64(19661))
	f.Add(int64(1), uint8(2), int64(1711843200), int64(19782))
	f.Add(int64(3650000), uint8(4), int64(253402300799), int64(0))
	ctx := metabuild.NewContext()
	columns := buildStorageClassTableInfo(f, ctx, `create table t (dt date) partition by range columns (dt) (partition p0 values less than ('2023-01-01'))`)
	toDays := buildStorageClassTableInfo(f, ctx, `create table t (dt date) partition by range (to_days(dt)) (partition p0 values less than (738886))`)
	units := []string{"YEAR", "QUARTER", "MONTH", "WEEK", "DAY"}

	f.Fuzz(func(t *testing.T, count int64, unitIndex uint8, nowUnix int64, boundaryDaysSinceEpoch int64) {
		now := time.Unix(nowUnix%253402300800, 0).UTC()
		interval, _, err := parseStorageClassInterval(fmt.Sprintf("INTERVAL %d %s", count, units[int(unitIndex)%len(units)]))
		if err != nil {
			return
		}
		boundary := time.Unix(0, 0).UTC().AddDate(0, 0, int(boundaryDaysSinceEpoch%3000000))
		if boundary.Year() < 1 || boundary.Year() > 9999 {
			return
		}
		columnsLessThan, err := convertStorageClassIntervalToLessThan(columns, interval, now)
		if err != nil {
			return
		}
		toDaysLessThan, err := convertStorageClassIntervalToLessThan(toDays, interval, now)
		require.NoError(t, err)

		boundaryTime := types.NewTime(types.FromGoTime(boundary), mysql.TypeDate, 0)
		columnsMatched, err := partitionValueLessThanOrEqual(columns, "'"+boundary.Format(time.DateOnly)+"'", columnsLessThan)
		require.NoError(t, err)
		toDaysMatched, err := partitionValueLessThanOrEqual(toDays, fmt.Sprint(types.TimestampDiff("DAY", types.ZeroDate, boundaryTime)), toDaysLessThan)
		require.NoError(t, err)
		require.Equal(t, columnsMatched, toDaysMatched, "now %s, interval %+v, boundary %s", now, interval, boundary)
	})
}

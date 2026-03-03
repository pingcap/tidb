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

package tables_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// waitForCache repeatedly queries the table until the cache is populated.
// Returns true if cache was used within maxAttempts.
func waitForCache(tk *testkit.TestKit, query string, maxAttempts int) bool {
	for i := 0; i < maxAttempts; i++ {
		tk.MustQuery(query)
		if lastReadFromCache(tk) {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// TestCachedTableTimestampTZConvert verifies that TIMESTAMP values are correctly
// converted to the session timezone when reading from the datum cache.
func TestCachedTableTimestampTZConvert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = '+00:00'")
	tk.MustExec("create table t_ts (id int primary key, ts timestamp)")
	tk.MustExec("insert into t_ts values (1, '2024-01-01 12:00:00')")
	tk.MustExec("insert into t_ts values (2, '2024-06-15 00:00:00')")
	tk.MustExec("alter table t_ts cache")

	// Wait for cache to be populated.
	require.True(t, waitForCache(tk, "select * from t_ts", 100))

	// Test different timezones on the same cached data.
	tests := []struct {
		tz       string
		id       int
		expected string
	}{
		{"+00:00", 1, "2024-01-01 12:00:00"},
		{"+08:00", 1, "2024-01-01 20:00:00"},
		{"-05:00", 1, "2024-01-01 07:00:00"},
		{"+05:30", 1, "2024-01-01 17:30:00"},
		{"+00:00", 2, "2024-06-15 00:00:00"},
		{"+08:00", 2, "2024-06-15 08:00:00"},
		{"-05:00", 2, "2024-06-14 19:00:00"},
	}

	for _, tt := range tests {
		tk.MustExec(fmt.Sprintf("set @@time_zone = '%s'", tt.tz))
		result := tk.MustQuery(fmt.Sprintf("select ts from t_ts where id = %d", tt.id))
		result.Check(testkit.Rows(tt.expected))
		require.True(t, lastReadFromCache(tk),
			"expected to read from cache for tz=%s id=%d", tt.tz, tt.id)
	}
}

// TestCachedTableTimestampNULL verifies that NULL TIMESTAMP values are not
// affected by timezone conversion.
func TestCachedTableTimestampNULL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = '+00:00'")
	tk.MustExec("create table t_ts_null (id int primary key, ts timestamp null)")
	tk.MustExec("insert into t_ts_null values (1, '2024-01-01 12:00:00')")
	tk.MustExec("insert into t_ts_null values (2, null)")
	tk.MustExec("alter table t_ts_null cache")

	require.True(t, waitForCache(tk, "select * from t_ts_null", 100))

	// NULL should remain NULL regardless of timezone.
	for _, tz := range []string{"+00:00", "+08:00", "-05:00"} {
		tk.MustExec(fmt.Sprintf("set @@time_zone = '%s'", tz))
		tk.MustQuery("select ts from t_ts_null where id = 2").Check(testkit.Rows("<nil>"))
		require.True(t, lastReadFromCache(tk), "expected cache read for tz=%s", tz)
	}

	// Non-NULL row should still convert correctly.
	tk.MustExec("set @@time_zone = '+08:00'")
	tk.MustQuery("select ts from t_ts_null where id = 1").Check(testkit.Rows("2024-01-01 20:00:00"))
	require.True(t, lastReadFromCache(tk))
}

// TestCachedTableTimestampZero verifies that zero timestamps (0000-00-00 00:00:00)
// are not converted across timezones, consistent with the KV decode path.
func TestCachedTableTimestampZero(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = '+00:00'")
	tk.MustExec("set @@sql_mode = 'ALLOW_INVALID_DATES'")
	tk.MustExec("create table t_ts_zero (id int primary key, ts timestamp null)")
	tk.MustExec("insert into t_ts_zero values (1, '2024-01-01 12:00:00')")
	tk.MustExec("insert into t_ts_zero values (2, '0000-00-00 00:00:00')")
	tk.MustExec("alter table t_ts_zero cache")

	require.True(t, waitForCache(tk, "select * from t_ts_zero", 100))

	// Zero timestamp should remain zero regardless of timezone.
	for _, tz := range []string{"+00:00", "+08:00", "-05:00"} {
		tk.MustExec(fmt.Sprintf("set @@time_zone = '%s'", tz))
		tk.MustQuery("select ts from t_ts_zero where id = 2").Check(testkit.Rows("0000-00-00 00:00:00"))
		require.True(t, lastReadFromCache(tk), "expected cache read for tz=%s", tz)
	}
}

// TestCachedTableTimestampFilter verifies that WHERE conditions on TIMESTAMP
// columns work correctly with timezone conversion on the datum cache path.
func TestCachedTableTimestampFilter(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = '+00:00'")
	tk.MustExec("create table t_ts_filter (id int primary key, ts timestamp)")
	tk.MustExec("insert into t_ts_filter values (1, '2024-01-01 10:00:00')")
	tk.MustExec("insert into t_ts_filter values (2, '2024-01-01 12:00:00')")
	tk.MustExec("insert into t_ts_filter values (3, '2024-01-01 14:00:00')")
	tk.MustExec("insert into t_ts_filter values (4, '2024-01-01 16:00:00')")
	tk.MustExec("alter table t_ts_filter cache")

	require.True(t, waitForCache(tk, "select * from t_ts_filter", 100))

	// In +08:00, the values become 18:00, 20:00, 22:00, 00:00(+1day).
	// Filter ts > '2024-01-01 21:00:00' should match id=3 (22:00) and id=4 (00:00 next day).
	tk.MustExec("set @@time_zone = '+08:00'")
	tk.MustQuery("select id, ts from t_ts_filter where ts > '2024-01-01 21:00:00' order by id").Check(testkit.Rows(
		"3 2024-01-01 22:00:00",
		"4 2024-01-02 00:00:00",
	))
	require.True(t, lastReadFromCache(tk))

	// In -05:00, the values become 05:00, 07:00, 09:00, 11:00.
	// Filter ts < '2024-01-01 08:00:00' should match id=1 (05:00) and id=2 (07:00).
	tk.MustExec("set @@time_zone = '-05:00'")
	tk.MustQuery("select id, ts from t_ts_filter where ts < '2024-01-01 08:00:00' order by id").Check(testkit.Rows(
		"1 2024-01-01 05:00:00",
		"2 2024-01-01 07:00:00",
	))
	require.True(t, lastReadFromCache(tk))
}

// TestCachedTableMultiTimestampCols verifies timezone conversion works correctly
// when a table has multiple TIMESTAMP columns.
func TestCachedTableMultiTimestampCols(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = '+00:00'")
	tk.MustExec("create table t_multi_ts (id int primary key, created_at timestamp, updated_at timestamp)")
	tk.MustExec("insert into t_multi_ts values (1, '2024-01-01 12:00:00', '2024-06-15 18:00:00')")
	tk.MustExec("alter table t_multi_ts cache")

	require.True(t, waitForCache(tk, "select * from t_multi_ts", 100))

	tk.MustExec("set @@time_zone = '+08:00'")
	tk.MustQuery("select created_at, updated_at from t_multi_ts where id = 1").Check(testkit.Rows(
		"2024-01-01 20:00:00 2024-06-16 02:00:00",
	))
	require.True(t, lastReadFromCache(tk))

	tk.MustExec("set @@time_zone = '-05:00'")
	tk.MustQuery("select created_at, updated_at from t_multi_ts where id = 1").Check(testkit.Rows(
		"2024-01-01 07:00:00 2024-06-15 13:00:00",
	))
	require.True(t, lastReadFromCache(tk))
}

// TestCachedTableNoTimestamp verifies that tables without TIMESTAMP columns
// are not affected by timezone settings (no conversion overhead).
func TestCachedTableNoTimestamp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_no_ts (id int primary key, name varchar(100), dt datetime)")
	tk.MustExec("insert into t_no_ts values (1, 'alice', '2024-01-01 12:00:00')")
	tk.MustExec("alter table t_no_ts cache")

	require.True(t, waitForCache(tk, "select * from t_no_ts", 100))

	// DATETIME is not timezone-aware; result should be the same regardless of time_zone.
	for _, tz := range []string{"+00:00", "+08:00", "-05:00"} {
		tk.MustExec(fmt.Sprintf("set @@time_zone = '%s'", tz))
		tk.MustQuery("select dt from t_no_ts where id = 1").Check(testkit.Rows("2024-01-01 12:00:00"))
		require.True(t, lastReadFromCache(tk), "expected cache read for tz=%s", tz)
	}
}

// TestCachedTableTimestampConsistency verifies that full table scans through
// the datum cache produce correct results across multiple timezone switches
// and that switching timezones does not corrupt cached data.
func TestCachedTableTimestampConsistency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@time_zone = '+00:00'")
	tk.MustExec("create table t_ts_consist (id int primary key, ts timestamp, val varchar(20))")
	tk.MustExec("insert into t_ts_consist values (1, '2024-01-01 00:00:00', 'midnight')")
	tk.MustExec("insert into t_ts_consist values (2, '2024-01-01 12:00:00', 'noon')")
	tk.MustExec("insert into t_ts_consist values (3, '2024-12-31 23:59:59', 'nye')")
	tk.MustExec("alter table t_ts_consist cache")

	require.True(t, waitForCache(tk, "select * from t_ts_consist", 100))

	// Full scan at +08:00: all timestamps should shift by +8 hours.
	tk.MustExec("set @@time_zone = '+08:00'")
	tk.MustQuery("select id, ts, val from t_ts_consist order by id").Check(testkit.Rows(
		"1 2024-01-01 08:00:00 midnight",
		"2 2024-01-01 20:00:00 noon",
		"3 2025-01-01 07:59:59 nye",
	))
	require.True(t, lastReadFromCache(tk))

	// Full scan at +00:00: timestamps should be in UTC (no corruption from +08:00 read).
	tk.MustExec("set @@time_zone = '+00:00'")
	tk.MustQuery("select id, ts, val from t_ts_consist order by id").Check(testkit.Rows(
		"1 2024-01-01 00:00:00 midnight",
		"2 2024-01-01 12:00:00 noon",
		"3 2024-12-31 23:59:59 nye",
	))
	require.True(t, lastReadFromCache(tk))

	// Full scan at -05:00: timestamps should shift by -5 hours.
	tk.MustExec("set @@time_zone = '-05:00'")
	tk.MustQuery("select id, ts, val from t_ts_consist order by id").Check(testkit.Rows(
		"1 2023-12-31 19:00:00 midnight",
		"2 2024-01-01 07:00:00 noon",
		"3 2024-12-31 18:59:59 nye",
	))
	require.True(t, lastReadFromCache(tk))

	// Back to +00:00 again: verify no corruption after multiple timezone switches.
	tk.MustExec("set @@time_zone = '+00:00'")
	tk.MustQuery("select id, ts, val from t_ts_consist order by id").Check(testkit.Rows(
		"1 2024-01-01 00:00:00 midnight",
		"2 2024-01-01 12:00:00 noon",
		"3 2024-12-31 23:59:59 nye",
	))
	require.True(t, lastReadFromCache(tk))
}

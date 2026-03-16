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

package reporter

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestRUItemToProto(t *testing.T) {
	item := ruItem{
		timestamp:    1000,
		totalRU:      100.5,
		execCount:    10,
		execDuration: 5000,
	}
	proto := item.toProto()
	require.Equal(t, uint64(1000), proto.TimestampSec)
	require.Equal(t, 100.5, proto.TotalRu)
	require.Equal(t, uint64(10), proto.ExecCount)
	require.Equal(t, uint64(5000), proto.ExecDuration)
}

func TestRURecordAdd(t *testing.T) {
	rec := newRURecord(stmtstats.BinaryDigest("sql1"), stmtstats.BinaryDigest("plan1"))

	// First add
	rec.add(1000, 10.0, 1, 100)
	require.Len(t, rec.items, 1)
	require.Equal(t, 10.0, rec.totalRU)
	require.Equal(t, uint64(1000), rec.items[0].timestamp)

	// Add to same timestamp
	rec.add(1000, 5.0, 2, 50)
	require.Len(t, rec.items, 1)
	require.Equal(t, 15.0, rec.totalRU)
	require.Equal(t, 15.0, rec.items[0].totalRU)
	require.Equal(t, uint64(3), rec.items[0].execCount)

	// Add to different timestamp
	rec.add(1001, 20.0, 1, 200)
	require.Len(t, rec.items, 2)
	require.Equal(t, 35.0, rec.totalRU)
}

func TestRURecordMerge(t *testing.T) {
	rec1 := newRURecord(stmtstats.BinaryDigest("sql1"), stmtstats.BinaryDigest("plan1"))
	rec1.add(1000, 10.0, 1, 100)
	rec1.add(1001, 20.0, 2, 200)

	rec2 := newRURecord(stmtstats.BinaryDigest("sql1"), stmtstats.BinaryDigest("plan1"))
	rec2.add(1000, 5.0, 1, 50) // Same timestamp as rec1
	rec2.add(1002, 15.0, 1, 150)

	rec1.merge(rec2)
	require.Equal(t, 50.0, rec1.totalRU)
	require.Len(t, rec1.items, 3)

	// Check merged timestamp 1000
	require.Equal(t, 15.0, rec1.items[0].totalRU)
	require.Equal(t, uint64(2), rec1.items[0].execCount)
}

func TestRURecordsTopN(t *testing.T) {
	records := make(ruRecords, 5)
	for i := 0; i < 5; i++ {
		rec := newRURecord(stmtstats.BinaryDigest(fmt.Sprintf("sql%d", i)), "")
		rec.totalRU = float64((i + 1) * 10) // 10, 20, 30, 40, 50
		records[i] = rec
	}

	// Get top 3
	top, evicted := records.topN(3)
	require.Len(t, top, 3)
	require.Len(t, evicted, 2)

	// Verify top contains highest RU records
	topRU := 0.0
	for _, r := range top {
		topRU += r.totalRU
	}
	require.GreaterOrEqual(t, topRU, 120.0) // 50+40+30=120
}

func TestUserRUCollectingTopNSQLs(t *testing.T) {
	user := newUserRUCollecting("user1")

	// Add more SQLs than maxTopSQLsPerUser
	numSQLs := maxTopSQLsPerUser + 10
	for i := 0; i < numSQLs; i++ {
		sqlDigest := stmtstats.BinaryDigest(fmt.Sprintf("sql%d", i))
		ru := float64(i + 1) // 1, 2, 3, ..., numSQLs
		user.add(1000, sqlDigest, "", &stmtstats.RUIncrement{TotalRU: ru, ExecCount: 1, ExecDuration: 100})
	}

	require.Len(t, user.records, numSQLs)

	// Get report records - should apply TopN
	reportRecords := user.getReportRecordsWithLimit(maxTopSQLsPerUser)

	// Should have maxTopSQLsPerUser + 1 (for "others")
	require.Len(t, reportRecords, maxTopSQLsPerUser+1)

	// Find the "others" record (nil sqlDigest)
	var othersRec *ruRecord
	normalCount := 0
	for _, rec := range reportRecords {
		if len(rec.sqlDigest) == 0 && len(rec.planDigest) == 0 {
			othersRec = rec
		} else {
			normalCount++
		}
	}
	require.NotNil(t, othersRec, "should have 'others' record")
	require.Equal(t, maxTopSQLsPerUser, normalCount)

	// "others" should contain evicted SQLs' RU
	// Evicted: SQL 1..10 with RU 1+2+...+10 = 55
	require.Equal(t, 55.0, othersRec.totalRU)
}

func TestUserRUCollectingPreTopNSQLCap(t *testing.T) {
	user := newUserRUCollecting("user1")
	extra := 5
	for i := 0; i < maxPreTopNSQLsPerUser+extra; i++ {
		sqlDigest := stmtstats.BinaryDigest(fmt.Sprintf("sql%d", i))
		user.add(1000, sqlDigest, "", &stmtstats.RUIncrement{TotalRU: 1.0, ExecCount: 1, ExecDuration: 10})
	}

	require.Len(t, user.records, maxPreTopNSQLsPerUser)
	require.NotNil(t, user.othersRec)
	require.Equal(t, float64(extra), user.othersRec.totalRU)
}

func TestOthersKeySentinel(t *testing.T) {
	require.Equal(t, othersKey, makeKey("", ""))
	require.NotEqual(t, othersKey, makeKey("sql", ""))
	require.NotEqual(t, othersKey, makeKey("", "plan"))
	require.True(t, isOthersKey(othersKey))
	require.False(t, isOthersKey(makeKey("sql", "")))
}

func TestUserRUCollectingEmptyDigestsGoToOthersRec(t *testing.T) {
	// Both empty digests are reserved for the aggregated "others SQL" bucket.
	user := newUserRUCollectingWithCap("user1", 10)

	user.add(1000, "", "", &stmtstats.RUIncrement{TotalRU: 3, ExecCount: 1, ExecDuration: 10})
	require.Empty(t, user.records)
	require.NotNil(t, user.othersRec)
	require.Equal(t, 3.0, user.othersRec.totalRU)

	user.add(1001, "sql1", "", &stmtstats.RUIncrement{TotalRU: 2, ExecCount: 1, ExecDuration: 10})
	require.Len(t, user.records, 1)
}

func TestUserRUCollectingAddOthersFoldsLegacyOthersKey(t *testing.T) {
	// Legacy shape: "others SQL" may have been stored in records[othersKey].
	user := newUserRUCollectingWithCap("user1", 10)

	legacy := newOthersRURecord()
	legacy.add(1000, 3, 1, 10)
	user.records[othersKey] = legacy
	user.totalRU = legacy.totalRU

	user.addOthers(1001, &stmtstats.RUIncrement{TotalRU: 2, ExecCount: 1, ExecDuration: 10})
	require.NotContains(t, user.records, othersKey)
	require.NotNil(t, user.othersRec)
	require.Equal(t, 5.0, user.othersRec.totalRU)
}

func TestRUCollectingHybridTopN(t *testing.T) {
	// Build maxTopUsers+5 users so compaction must evict users into "others user".
	// Contract: compact+toProto still returns records with explicit others-user coverage.
	collecting := newRUCollecting()

	// Add more users than maxTopUsers
	numUsers := maxTopUsers + 5
	for u := 0; u < numUsers; u++ {
		user := fmt.Sprintf("user%d", u)
		// Each user has some SQLs
		for s := 0; s < 3; s++ {
			key := stmtstats.RUKey{
				User:      user,
				SQLDigest: stmtstats.BinaryDigest(fmt.Sprintf("sql%d", s)),
			}
			incr := &stmtstats.RUIncrement{
				TotalRU:      float64(u + 1), // user0=1, user1=2, ...
				ExecCount:    1,
				ExecDuration: 100,
			}
			collecting.add(1000, key, incr)
		}
	}

	require.Len(t, collecting.users, numUsers)

	// Get report records via compact + toTopRURecords (same semantics as former getReportRecords).
	compacted := collecting.compactWithLimits(maxTopUsers, maxTopSQLsPerUser)
	var records []tipb.TopRURecord
	if compacted != nil {
		records = compacted.toTopRURecords([]byte("test-keyspace"))
	}

	// Should have TopN users' SQLs + "others user" record
	// Calculate expected: maxTopUsers * 3 SQLs each + 1 "others user" record
	// But actually it's more complex due to per-user "others SQL" - let's just check basics

	// Find "others user" records (empty user string)
	var hasOthersUser bool
	for _, rec := range records {
		if rec.User == keyRUOthersUser {
			hasOthersUser = true
			break
		}
	}
	require.True(t, hasOthersUser, "should have 'others user' record for evicted users")

	// Verify keyspace name is set
	for _, rec := range records {
		require.Equal(t, []byte("test-keyspace"), rec.KeyspaceName)
	}
}

func TestRUCollectingPreTopNUserCap(t *testing.T) {
	collecting := newRUCollecting()
	extra := 5
	for u := 0; u < maxPreTopNUsers+extra; u++ {
		key := stmtstats.RUKey{
			User:      fmt.Sprintf("user%d", u),
			SQLDigest: stmtstats.BinaryDigest("sql"),
		}
		incr := &stmtstats.RUIncrement{TotalRU: 1}
		collecting.add(1000, key, incr)
	}

	require.Len(t, collecting.users, maxPreTopNUsers)
	require.NotNil(t, collecting.othersUser)
	require.Equal(t, float64(extra), collecting.othersUser.totalRU)
}

func TestRUCollectingAddBatch(t *testing.T) {
	collecting := newRUCollecting()

	batch := stmtstats.RUIncrementMap{
		stmtstats.RUKey{User: "user1", SQLDigest: stmtstats.BinaryDigest("sql1")}: &stmtstats.RUIncrement{TotalRU: 10},
		stmtstats.RUKey{User: "user1", SQLDigest: stmtstats.BinaryDigest("sql2")}: &stmtstats.RUIncrement{TotalRU: 20},
		stmtstats.RUKey{User: "user2", SQLDigest: stmtstats.BinaryDigest("sql1")}: &stmtstats.RUIncrement{TotalRU: 30},
	}

	collecting.addBatch(1000, batch)

	require.Len(t, collecting.users, 2)
	require.Equal(t, 30.0, collecting.users["user1"].totalRU)
	require.Equal(t, 30.0, collecting.users["user2"].totalRU)
}

func TestRUCollectingTake(t *testing.T) {
	collecting := newRUCollecting()

	key := stmtstats.RUKey{User: "user1", SQLDigest: stmtstats.BinaryDigest("sql1")}
	incr := &stmtstats.RUIncrement{TotalRU: 10}
	collecting.add(1000, key, incr)

	require.Len(t, collecting.users, 1)

	taken := collecting.take()
	require.Len(t, taken.users, 1)
	require.Len(t, collecting.users, 0) // Original should be reset
}

func TestRUCollectingCompactAndReportConsistency(t *testing.T) {
	// This case mixes multiple users/SQLs and two evicted users with maxUsers=2,maxSQLs=1.
	// We only assert normalized non-empty output to validate compact+report path consistency.
	collecting := newRUCollecting()
	add := func(user, sql, plan string, ts uint64, ru float64) {
		collecting.add(ts, stmtstats.RUKey{
			User:       user,
			SQLDigest:  stmtstats.BinaryDigest(sql),
			PlanDigest: stmtstats.BinaryDigest(plan),
		}, &stmtstats.RUIncrement{
			TotalRU:      ru,
			ExecCount:    1,
			ExecDuration: 10,
		})
	}
	add("u1", "s1", "p1", 0, 100)
	add("u1", "s2", "p2", 15, 80)
	add("u2", "s1", "p1", 0, 70)
	add("u2", "s2", "p2", 30, 60)
	add("u3", "s1", "p1", 0, 50) // evicted by maxUsers=2
	add("u4", "s1", "p1", 0, 10) // evicted by maxUsers=2

	maxUsers := 2
	maxSQLsPerUser := 1
	keyspace := []byte("ks")

	compacted := collecting.compactWithLimits(maxUsers, maxSQLsPerUser)
	require.NotNil(t, compacted)

	fromCompact := compacted.toTopRURecords(keyspace)
	require.NotEmpty(t, normalizeTopRURecords(fromCompact))
}

func TestCompactWithLimitsPreExistingOthersRecAndEvictedSQL(t *testing.T) {
	// Precondition: user already has othersRec, then one SQL is evicted by per-user top1.
	// Contract: pre-existing othersRec and evicted SQL RU are merged into one others record.
	collecting := newRUCollectingWithCaps(10, 10)
	u1 := newUserRUCollectingWithCap("u1", 10)
	u1.add(1000, stmtstats.BinaryDigest("sql-top"), stmtstats.BinaryDigest("plan-top"), &stmtstats.RUIncrement{
		TotalRU:      100,
		ExecCount:    1,
		ExecDuration: 10,
	})
	u1.add(1001, stmtstats.BinaryDigest("sql-evicted"), stmtstats.BinaryDigest("plan-evicted"), &stmtstats.RUIncrement{
		TotalRU:      40,
		ExecCount:    1,
		ExecDuration: 10,
	})
	u1.addOthers(1002, &stmtstats.RUIncrement{
		TotalRU:      7,
		ExecCount:    1,
		ExecDuration: 10,
	})
	collecting.users["u1"] = u1

	compacted := collecting.compactWithLimits(1, 1)
	require.NotNil(t, compacted)
	require.Nil(t, compacted.othersUser)

	compactedU1, ok := compacted.users["u1"]
	require.True(t, ok)
	require.Len(t, compactedU1.records, 1)
	for _, rec := range compactedU1.records {
		require.Equal(t, stmtstats.BinaryDigest("sql-top"), rec.sqlDigest)
		require.Equal(t, stmtstats.BinaryDigest("plan-top"), rec.planDigest)
		require.Equal(t, 100.0, rec.totalRU)
	}
	require.NotNil(t, compactedU1.othersRec)
	require.Empty(t, compactedU1.othersRec.sqlDigest)
	require.Empty(t, compactedU1.othersRec.planDigest)
	require.Equal(t, 47.0, compactedU1.othersRec.totalRU) // pre-existing 7 + evicted SQL 40
}

func TestCompactWithLimitsOthersUserAndEvictedUsersBothPresent(t *testing.T) {
	// Precondition: both pre-existing othersUser and newly evicted users exist.
	// Contract: both sources merge into one othersUser.othersRec without leaking normal records.
	collecting := newRUCollectingWithCaps(10, 10)

	u1 := newUserRUCollectingWithCap("u1", 10)
	u1.add(2000, stmtstats.BinaryDigest("sql-top"), stmtstats.BinaryDigest("plan-top"), &stmtstats.RUIncrement{
		TotalRU:      100,
		ExecCount:    1,
		ExecDuration: 10,
	})
	collecting.users["u1"] = u1

	u2 := newUserRUCollectingWithCap("u2", 10)
	u2.add(2000, stmtstats.BinaryDigest("sql-u2"), stmtstats.BinaryDigest("plan-u2"), &stmtstats.RUIncrement{
		TotalRU:      30,
		ExecCount:    1,
		ExecDuration: 10,
	})
	collecting.users["u2"] = u2

	preOthers := newUserRUCollectingWithCap(keyRUOthersUser, 10)
	preOthers.add(2000, stmtstats.BinaryDigest("sql-pre-others"), stmtstats.BinaryDigest("plan-pre-others"), &stmtstats.RUIncrement{
		TotalRU:      6,
		ExecCount:    1,
		ExecDuration: 10,
	})
	preOthers.addOthers(2001, &stmtstats.RUIncrement{
		TotalRU:      4,
		ExecCount:    1,
		ExecDuration: 10,
	})
	collecting.othersUser = preOthers

	compacted := collecting.compactWithLimits(1, 1)
	require.NotNil(t, compacted)
	require.Len(t, compacted.users, 1)
	require.Contains(t, compacted.users, "u1")

	require.NotNil(t, compacted.othersUser)
	require.Equal(t, keyRUOthersUser, compacted.othersUser.user)
	require.Empty(t, compacted.othersUser.records)
	require.NotNil(t, compacted.othersUser.othersRec)
	require.Empty(t, compacted.othersUser.othersRec.sqlDigest)
	require.Empty(t, compacted.othersUser.othersRec.planDigest)
	require.Equal(t, 40.0, compacted.othersUser.othersRec.totalRU) // pre-existing 6+4 + evicted user 30
}

func TestCompactWithLimitsOnlyOthersUserNonEmpty(t *testing.T) {
	collecting := newRUCollectingWithCaps(10, 10)
	collecting.othersUser = newUserRUCollectingWithCap(keyRUOthersUser, 10)
	collecting.othersUser.addOthers(3000, &stmtstats.RUIncrement{
		TotalRU:      11,
		ExecCount:    1,
		ExecDuration: 10,
	})

	compacted := collecting.compactWithLimits(1, 1)
	require.NotNil(t, compacted)
	require.Empty(t, compacted.users)
	require.NotNil(t, compacted.othersUser)
	require.NotNil(t, compacted.othersUser.othersRec)
	require.Equal(t, 11.0, compacted.othersUser.othersRec.totalRU)
}

func TestCompactWithLimitsSingleUserSingleSQL(t *testing.T) {
	collecting := newRUCollectingWithCaps(10, 10)
	u1 := newUserRUCollectingWithCap("u1", 10)
	u1.add(4000, stmtstats.BinaryDigest("sql-only"), stmtstats.BinaryDigest("plan-only"), &stmtstats.RUIncrement{
		TotalRU:      88,
		ExecCount:    1,
		ExecDuration: 10,
	})
	collecting.users["u1"] = u1

	compacted := collecting.compactWithLimits(1, 1)
	require.NotNil(t, compacted)
	require.Nil(t, compacted.othersUser)
	require.Len(t, compacted.users, 1)

	compactedU1, ok := compacted.users["u1"]
	require.True(t, ok)
	require.Len(t, compactedU1.records, 1)
	require.Nil(t, compactedU1.othersRec)
}

func TestCompactWithLimitsOnlyOthersUserLegacyRecords(t *testing.T) {
	// Compatibility case: legacy others data may sit in othersUser.records with nil digests.
	// Contract: compaction folds it into othersUser.othersRec.
	collecting := newRUCollectingWithCaps(10, 10)
	legacyOthers := newUserRUCollectingWithCap(keyRUOthersUser, 10)
	legacyRec := newOthersRURecord()
	legacyRec.add(5000, 13, 2, 30)
	legacyOthers.records[othersKey] = legacyRec
	legacyOthers.totalRU = legacyRec.totalRU
	collecting.othersUser = legacyOthers

	compacted := collecting.compactWithLimits(1, 1)
	require.NotNil(t, compacted)
	require.Empty(t, compacted.users)
	require.NotNil(t, compacted.othersUser)
	require.Empty(t, compacted.othersUser.records)
	require.NotNil(t, compacted.othersUser.othersRec)
	require.Empty(t, compacted.othersUser.othersRec.sqlDigest)
	require.Empty(t, compacted.othersUser.othersRec.planDigest)
	require.Equal(t, 13.0, compacted.othersUser.othersRec.totalRU)
}

func normalizeTopRURecords(records []tipb.TopRURecord) []string {
	// Normalize record/item order so assertions are stable across map iteration order.
	out := make([]string, 0, len(records))
	for _, rec := range records {
		items := make([]string, 0, len(rec.Items))
		for _, item := range rec.Items {
			items = append(items, fmt.Sprintf("%d|%.6f|%d|%d", item.TimestampSec, item.TotalRu, item.ExecCount, item.ExecDuration))
		}
		sort.Strings(items)
		out = append(out, fmt.Sprintf("%s|%x|%x|%x|%s", rec.User, rec.SqlDigest, rec.PlanDigest, rec.KeyspaceName, strings.Join(items, ",")))
	}
	sort.Strings(out)
	return out
}

func TestRUItemsSort(t *testing.T) {
	items := ruItems{
		{timestamp: 1002},
		{timestamp: 1000},
		{timestamp: 1001},
	}

	// Items should be sortable by timestamp ascending
	require.False(t, items.Less(0, 1)) // 1002 > 1000
	require.True(t, items.Less(1, 0))  // 1000 < 1002
}

func TestRUItemsToProto(t *testing.T) {
	items := ruItems{
		{timestamp: 1000, totalRU: 10, execCount: 1, execDuration: 100},
		{timestamp: 1001, totalRU: 20, execCount: 2, execDuration: 200},
	}

	proto := items.toProto()
	require.Len(t, proto, 2)
	require.Equal(t, uint64(1000), proto[0].TimestampSec)
	require.Equal(t, uint64(1001), proto[1].TimestampSec)
}

func TestRUCollectingSameBucketSameKeyAccumulates(t *testing.T) {
	// Two batches with same (user,sql,plan,timestamp) should coalesce into one item.
	// Contract: RU/duration add up, while begin-based ExecCount is not double-counted.
	collecting := newRUCollecting()
	key := stmtstats.RUKey{
		User:       "u1",
		SQLDigest:  stmtstats.BinaryDigest("sql1"),
		PlanDigest: stmtstats.BinaryDigest("plan1"),
	}

	collecting.addBatch(1000, stmtstats.RUIncrementMap{
		key: {
			TotalRU:      10,
			ExecCount:    1,
			ExecDuration: 100,
		},
	})
	collecting.addBatch(1000, stmtstats.RUIncrementMap{
		key: {
			TotalRU:      7,
			ExecCount:    0,
			ExecDuration: 40,
		},
	})

	compacted := collecting.compactWithLimits(maxTopUsers, maxTopSQLsPerUser)
	var records []tipb.TopRURecord
	if compacted != nil {
		records = compacted.toTopRURecords([]byte("ks"))
	}
	require.Len(t, records, 1)
	require.Equal(t, "u1", records[0].User)
	require.Equal(t, []byte("sql1"), records[0].SqlDigest)
	require.Equal(t, []byte("plan1"), records[0].PlanDigest)
	require.Len(t, records[0].Items, 1)
	require.Equal(t, uint64(1000), records[0].Items[0].TimestampSec)
	require.InDelta(t, 17.0, records[0].Items[0].TotalRu, 1e-9)
	require.Equal(t, uint64(1), records[0].Items[0].ExecCount)
	require.Equal(t, uint64(140), records[0].Items[0].ExecDuration)
}

func TestEmptyRUCollecting(t *testing.T) {
	collecting := newRUCollecting()
	compacted := collecting.compactWithLimits(maxTopUsers, maxTopSQLsPerUser)
	var records []tipb.TopRURecord
	if compacted != nil {
		records = compacted.toTopRURecords([]byte("keyspace"))
	}
	require.Nil(t, records)
}

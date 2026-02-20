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
	rec := newRURecord([]byte("sql1"), []byte("plan1"))

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
	rec1 := newRURecord([]byte("sql1"), []byte("plan1"))
	rec1.add(1000, 10.0, 1, 100)
	rec1.add(1001, 20.0, 2, 200)

	rec2 := newRURecord([]byte("sql1"), []byte("plan1"))
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
		rec := newRURecord([]byte(fmt.Sprintf("sql%d", i)), nil)
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
		sqlDigest := []byte(fmt.Sprintf("sql%d", i))
		ru := float64(i + 1) // 1, 2, 3, ..., numSQLs
		user.add(1000, sqlDigest, nil, &stmtstats.RUIncrement{TotalRU: ru, ExecCount: 1, ExecDuration: 100})
	}

	require.Len(t, user.records, numSQLs)

	// Get report records - should apply TopN
	reportRecords := user.getReportRecords()

	// Should have maxTopSQLsPerUser + 1 (for "others")
	require.Len(t, reportRecords, maxTopSQLsPerUser+1)

	// Find the "others" record (nil sqlDigest)
	var othersRec *ruRecord
	normalCount := 0
	for _, rec := range reportRecords {
		if rec.sqlDigest == nil {
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
		sqlDigest := []byte(fmt.Sprintf("sql%d", i))
		user.add(1000, sqlDigest, nil, &stmtstats.RUIncrement{TotalRU: 1.0, ExecCount: 1, ExecDuration: 10})
	}

	require.Len(t, user.records, maxPreTopNSQLsPerUser)
	require.NotNil(t, user.othersRec)
	require.Equal(t, float64(extra), user.othersRec.totalRU)
}

func TestRUCollectingHybridTopN(t *testing.T) {
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

	// Get report records
	records := collecting.getReportRecords([]byte("test-keyspace"))

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
	fromWrapper := collecting.getReportRecordsWithLimits(keyspace, maxUsers, maxSQLsPerUser)
	require.Equal(t, normalizeTopRURecords(fromWrapper), normalizeTopRURecords(fromCompact))
}

func normalizeTopRURecords(records []tipb.TopRURecord) []string {
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

	records := collecting.getReportRecords([]byte("ks"))
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
	records := collecting.getReportRecords([]byte("keyspace"))
	require.Nil(t, records)
}

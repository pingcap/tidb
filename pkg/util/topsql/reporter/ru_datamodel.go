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
	"bytes"
	"sort"
	"sync"

	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
)

// TopN limits for RU aggregation (Phase 2 Decision A: Hybrid TopN).
// These values are implementation-defined per design doc.
const (
	// maxTopUsers is the maximum number of users to keep in global TopN.
	maxTopUsers = 200
	// maxTopSQLsPerUser is the maximum number of SQLs to keep per user.
	maxTopSQLsPerUser = 200
	// keyRUOthersUser is the special user key for aggregated "others" users.
	// Use a non-empty sentinel to avoid collision with real empty user names.
	keyRUOthersUser = "<others>"

	// Phase 3: Pre-TopN memory bounding caps.
	// These caps are applied during collection to prevent unbounded memory growth
	// before TopN filtering is applied at report time.
	// Use 2x the TopN limits to allow some headroom before eviction.
	maxPreTopNUsers       = maxTopUsers * 2       // 400 users max during collection
	maxPreTopNSQLsPerUser = maxTopSQLsPerUser * 2 // 400 SQLs per user max during collection
)

// ruItem represents RU statistics for a single timestamp.
// Parallel to tsItem in datamodel.go but for RU data.
type ruItem struct {
	timestamp    uint64
	totalRU      float64
	execCount    uint64
	execDuration uint64
}

// toProto converts ruItem to protobuf representation.
func (i *ruItem) toProto() *tipb.TopRURecordItem {
	return &tipb.TopRURecordItem{
		TimestampSec: i.timestamp,
		TotalRu:      i.totalRU,
		ExecCount:    i.execCount,
		ExecDuration: i.execDuration,
	}
}

// ruItems is a sortable list of ruItem, sorted by timestamp (asc).
type ruItems []ruItem

func (rs ruItems) Len() int           { return len(rs) }
func (rs ruItems) Less(i, j int) bool { return rs[i].timestamp < rs[j].timestamp }
func (rs ruItems) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }

// toProto converts ruItems to protobuf representation.
func (rs ruItems) toProto() []*tipb.TopRURecordItem {
	if len(rs) == 0 {
		return nil
	}
	items := make([]*tipb.TopRURecordItem, 0, len(rs))
	for _, item := range rs {
		items = append(items, item.toProto())
	}
	return items
}

// ruRecord represents RU statistics for a single (sql_digest, plan_digest) combination.
// Used within a user's SQL tracking.
type ruRecord struct {
	sqlDigest  []byte
	planDigest []byte
	items      ruItems
	tsIndex    map[uint64]int // timestamp => index in items
	totalRU    float64        // cumulative RU for TopN sorting
}

func newRURecord(sqlDigest, planDigest []byte) *ruRecord {
	return &ruRecord{
		sqlDigest:  sqlDigest,
		planDigest: planDigest,
		items:      make(ruItems, 0, 64),
		tsIndex:    make(map[uint64]int, 64),
	}
}

// add adds RU increment data for a specific timestamp.
func (r *ruRecord) add(timestamp uint64, totalRU float64, execCount, execDuration uint64) {
	if idx, ok := r.tsIndex[timestamp]; ok {
		r.items[idx].totalRU += totalRU
		r.items[idx].execCount += execCount
		r.items[idx].execDuration += execDuration
	} else {
		r.tsIndex[timestamp] = len(r.items)
		r.items = append(r.items, ruItem{
			timestamp:    timestamp,
			totalRU:      totalRU,
			execCount:    execCount,
			execDuration: execDuration,
		})
	}
	r.totalRU += totalRU
}

func (r *ruRecord) addIncr(timestamp uint64, incr *stmtstats.RUIncrement) {
	if incr == nil {
		return
	}
	r.add(timestamp, incr.TotalRU, incr.ExecCount, incr.ExecDuration)
}

// merge merges another ruRecord into this one.
func (r *ruRecord) merge(other *ruRecord) {
	if other == nil {
		return
	}
	for _, item := range other.items {
		r.add(item.timestamp, item.totalRU, item.execCount, item.execDuration)
	}
}

// ruRecords is a sortable list of ruRecord pointers, sorted by totalRU (desc).
type ruRecords []*ruRecord

func (rs ruRecords) Len() int           { return len(rs) }
func (rs ruRecords) Less(i, j int) bool { return rs[i].totalRU > rs[j].totalRU } // DESC
func (rs ruRecords) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }

// topN returns top n records by totalRU and the evicted records.
func (rs ruRecords) topN(n int) (top, evicted ruRecords) {
	if len(rs) <= n {
		return rs, nil
	}
	if err := quickselect.QuickSelect(rs, n); err != nil {
		return rs, nil
	}
	return rs[:n], rs[n:]
}

// userRUCollecting tracks RU data for a single user with per-SQL TopN.
//
// Phase 3: Pre-TopN memory bounding is applied during collection.
// When the number of SQLs exceeds maxPreTopNSQLsPerUser, new SQLs are
// merged directly into the "others SQL" bucket to prevent unbounded growth.
type userRUCollecting struct {
	user               string
	records            map[string]*ruRecord // sqlPlanKey => ruRecord
	othersRec          *ruRecord            // Phase 3: Pre-aggregated "others SQL" record
	keyBuf             *bytes.Buffer
	totalRU            float64 // cumulative RU for user-level TopN sorting
	preTopNSQLsPerUser int
}

func newUserRUCollecting(user string) *userRUCollecting {
	return newUserRUCollectingWithCap(user, maxPreTopNSQLsPerUser)
}

func newUserRUCollectingWithCap(user string, preTopNSQLsPerUser int) *userRUCollecting {
	if preTopNSQLsPerUser <= 0 {
		preTopNSQLsPerUser = maxPreTopNSQLsPerUser
	}
	return &userRUCollecting{
		user:               user,
		records:            make(map[string]*ruRecord),
		keyBuf:             bytes.NewBuffer(make([]byte, 0, 64)),
		preTopNSQLsPerUser: preTopNSQLsPerUser,
	}
}

// add adds RU increment data for a specific SQL.
//
// Phase 3: Pre-TopN memory bounding.
// When the number of distinct SQLs exceeds maxPreTopNSQLsPerUser,
// new SQLs are merged into "others SQL" to bound memory usage.
func (u *userRUCollecting) add(timestamp uint64, sqlDigest, planDigest []byte, incr *stmtstats.RUIncrement) {
	if incr == nil {
		return
	}

	key := encodeKey(u.keyBuf, sqlDigest, planDigest)
	rec, ok := u.records[key]
	if ok {
		rec.addIncr(timestamp, incr)
		u.totalRU += incr.TotalRU
		return
	}

	// Phase 3: Check pre-TopN cap before adding new SQL
	if len(u.records) >= u.preTopNSQLsPerUser {
		// At capacity - merge into "others SQL" instead
		if u.othersRec == nil {
			u.othersRec = newRURecord(nil, nil) // nil digests = "others SQL"
		}
		u.othersRec.addIncr(timestamp, incr)
		u.totalRU += incr.TotalRU
		return
	}

	rec = newRURecord(sqlDigest, planDigest)
	u.records[key] = rec
	rec.addIncr(timestamp, incr)
	u.totalRU += incr.TotalRU
}

// addOthers adds RU increment data into this user's aggregated "others SQL" bucket.
func (u *userRUCollecting) addOthers(timestamp uint64, incr *stmtstats.RUIncrement) {
	u.add(timestamp, nil, nil, incr)
}

// addItem converts a ruItem into RU increment and aggregates it into "others SQL".
func (u *userRUCollecting) addItem(item ruItem) {
	u.addOthers(item.timestamp, &stmtstats.RUIncrement{
		TotalRU:      item.totalRU,
		ExecCount:    item.execCount,
		ExecDuration: item.execDuration,
	})
}

// getReportRecordsWithLimit returns TopN SQL records for this user, with evicted SQLs merged into "others".
func (u *userRUCollecting) getReportRecordsWithLimit(topNSQLsPerUser int) []*ruRecord {
	if topNSQLsPerUser <= 0 {
		topNSQLsPerUser = maxTopSQLsPerUser
	}
	if len(u.records) == 0 && u.othersRec == nil {
		return nil
	}

	// Extract all records
	allRecords := make(ruRecords, 0, len(u.records))
	for _, rec := range u.records {
		allRecords = append(allRecords, rec)
	}

	// Apply TopN filtering
	top, evicted := allRecords.topN(topNSQLsPerUser)

	// Phase 3: Start with pre-aggregated "others" from collection phase
	var othersRec *ruRecord
	if u.othersRec != nil {
		othersRec = u.othersRec
	}

	// Merge evicted into "others SQL"
	if len(evicted) > 0 {
		if othersRec == nil {
			othersRec = newRURecord(nil, nil) // nil digests = "others SQL"
		}
		for _, rec := range evicted {
			othersRec.merge(rec)
		}
	}

	if othersRec != nil {
		top = append(top, othersRec)
	}

	return top
}

// getReportRecords returns TopN SQL records for this user, with evicted SQLs merged into "others".
// Returns a slice of ruRecord ready for proto conversion.
//
// Phase 3: Pre-aggregated "others SQL" from collection phase (u.othersRec) is merged
// with any SQLs evicted during TopN filtering.
func (u *userRUCollecting) getReportRecords() []*ruRecord {
	return u.getReportRecordsWithLimit(maxTopSQLsPerUser)
}

// userRUCollectings is a sortable list of userRUCollecting pointers, sorted by totalRU (desc).
type userRUCollectings []*userRUCollecting

func (us userRUCollectings) Len() int           { return len(us) }
func (us userRUCollectings) Less(i, j int) bool { return us[i].totalRU > us[j].totalRU } // DESC
func (us userRUCollectings) Swap(i, j int)      { us[i], us[j] = us[j], us[i] }

// topN returns top n users by totalRU and the evicted users.
func (us userRUCollectings) topN(n int) (top, evicted userRUCollectings) {
	if len(us) <= n {
		return us, nil
	}
	if err := quickselect.QuickSelect(us, n); err != nil {
		return us, nil
	}
	return us[:n], us[n:]
}

// ruCollecting is the top-level RU data collector implementing Hybrid TopN (Decision A).
// It maintains global TopN users, with per-user TopN SQLs.
//
// Phase 3: Pre-TopN memory bounding is applied during collection.
// When the number of users exceeds maxPreTopNUsers, new users are
// merged directly into the "others user" bucket to prevent unbounded growth.
type ruCollecting struct {
	mu                 sync.Mutex
	users              map[string]*userRUCollecting // user => userRUCollecting
	othersUser         *userRUCollecting            // Phase 3: Pre-aggregated "others user"
	preTopNUsers       int
	preTopNSQLsPerUser int
}

func newRUCollecting() *ruCollecting {
	return newRUCollectingWithCaps(maxPreTopNUsers, maxPreTopNSQLsPerUser)
}

func newRUCollectingWithCaps(preTopNUsers, preTopNSQLsPerUser int) *ruCollecting {
	if preTopNUsers <= 0 {
		preTopNUsers = maxPreTopNUsers
	}
	if preTopNSQLsPerUser <= 0 {
		preTopNSQLsPerUser = maxPreTopNSQLsPerUser
	}
	return &ruCollecting{
		users:              make(map[string]*userRUCollecting),
		preTopNUsers:       preTopNUsers,
		preTopNSQLsPerUser: preTopNSQLsPerUser,
	}
}

// add adds RU increment data from aggregator.
//
// Phase 3: Pre-TopN memory bounding.
// When the number of distinct users exceeds maxPreTopNUsers,
// new users are merged into "others user" to bound memory usage.
func (c *ruCollecting) add(timestamp uint64, key stmtstats.RUKey, incr *stmtstats.RUIncrement) {
	user := key.User
	userCollecting, ok := c.users[user]
	if !ok {
		// Phase 3: Check pre-TopN cap before adding new user
		if len(c.users) >= c.preTopNUsers {
			// At capacity - merge into "others user" instead
			if c.othersUser == nil {
				c.othersUser = newUserRUCollectingWithCap(keyRUOthersUser, c.preTopNSQLsPerUser)
			}
			// Merge into "others user"'s "others SQL" (nil digests)
			c.othersUser.addOthers(timestamp, incr)
			return
		}
		userCollecting = newUserRUCollectingWithCap(user, c.preTopNSQLsPerUser)
		c.users[user] = userCollecting
	}
	// Convert BinaryDigest (string) to []byte for storage
	userCollecting.add(timestamp, []byte(key.SQLDigest), []byte(key.PlanDigest), incr)
}

// addRecordItem adds one report item back into collecting. It preserves special handling for others user/SQL.
func (c *ruCollecting) addRecordItem(timestamp uint64, user string, sqlDigest, planDigest []byte, incr *stmtstats.RUIncrement) {
	if incr == nil {
		return
	}
	if user == keyRUOthersUser {
		if c.othersUser == nil {
			c.othersUser = newUserRUCollectingWithCap(keyRUOthersUser, c.preTopNSQLsPerUser)
		}
		c.othersUser.addOthers(timestamp, incr)
		return
	}

	userCollecting, ok := c.users[user]
	if !ok {
		if len(c.users) >= c.preTopNUsers {
			if c.othersUser == nil {
				c.othersUser = newUserRUCollectingWithCap(keyRUOthersUser, c.preTopNSQLsPerUser)
			}
			c.othersUser.addOthers(timestamp, incr)
			return
		}
		userCollecting = newUserRUCollectingWithCap(user, c.preTopNSQLsPerUser)
		c.users[user] = userCollecting
	}
	if len(sqlDigest) == 0 && len(planDigest) == 0 {
		userCollecting.addOthers(timestamp, incr)
		return
	}
	userCollecting.add(timestamp, sqlDigest, planDigest, incr)
}

// addBatch adds a batch of RU increments for a given timestamp.
// addBatch is thread-safe: called from collectRUWorker goroutine.
func (c *ruCollecting) addBatch(timestamp uint64, increments stmtstats.RUIncrementMap) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, incr := range increments {
		c.add(timestamp, key, incr)
	}
}

// take takes all collected data and returns a new ruCollecting, resetting internal state.
// take is thread-safe: called from collectWorker goroutine on report ticker.
func (c *ruCollecting) take() *ruCollecting {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := &ruCollecting{
		users:              c.users,
		othersUser:         c.othersUser,
		preTopNUsers:       c.preTopNUsers,
		preTopNSQLsPerUser: c.preTopNSQLsPerUser,
	}
	c.users = make(map[string]*userRUCollecting)
	c.othersUser = nil
	return result
}

// getReportRecordsWithLimits applies two-level TopN filtering and returns records ready for reporting.
func (c *ruCollecting) getReportRecordsWithLimits(keyspaceName []byte, maxUsers, maxSQLsPerUser int) []tipb.TopRURecord {
	if maxUsers <= 0 {
		maxUsers = maxTopUsers
	}
	if maxSQLsPerUser <= 0 {
		maxSQLsPerUser = maxTopSQLsPerUser
	}
	if len(c.users) == 0 && c.othersUser == nil {
		return nil
	}

	// Extract all users
	allUsers := make(userRUCollectings, 0, len(c.users))
	for _, userCollecting := range c.users {
		allUsers = append(allUsers, userCollecting)
	}

	// Apply global TopN user filtering
	topUsers, evictedUsers := allUsers.topN(maxUsers)

	// Collect records from top users
	var result []tipb.TopRURecord
	for _, userCollecting := range topUsers {
		userRecords := userCollecting.getReportRecordsWithLimit(maxSQLsPerUser)
		for _, rec := range userRecords {
			// Sort items by timestamp before converting to proto
			sort.Sort(rec.items)
			result = append(result, tipb.TopRURecord{
				KeyspaceName: keyspaceName,
				User:         userCollecting.user,
				SqlDigest:    rec.sqlDigest,
				PlanDigest:   rec.planDigest,
				Items:        rec.items.toProto(),
			})
		}
	}

	// Phase 3: Start with pre-aggregated "others user" from collection phase
	var othersUser *userRUCollecting
	if c.othersUser != nil {
		othersUser = c.othersUser
	}

	// Merge evicted users into "others user"
	if len(evictedUsers) > 0 {
		if othersUser == nil {
			othersUser = newUserRUCollectingWithCap(keyRUOthersUser, c.preTopNSQLsPerUser)
		}
		for _, evictedUser := range evictedUsers {
			// Merge all SQLs from evicted user into others user's "others SQL"
			for _, rec := range evictedUser.records {
				for _, item := range rec.items {
					othersUser.addItem(item)
				}
			}
			// Phase 3: Also merge evicted user's pre-aggregated "others SQL"
			if evictedUser.othersRec != nil {
				for _, item := range evictedUser.othersRec.items {
					othersUser.addItem(item)
				}
			}
		}
	}

	// Output "others user" record if we have any
	if othersUser != nil {
		// Get the "others SQL" records from "others user"
		othersRecords := othersUser.getReportRecordsWithLimit(maxSQLsPerUser)
		for _, rec := range othersRecords {
			sort.Sort(rec.items)
			result = append(result, tipb.TopRURecord{
				KeyspaceName: keyspaceName,
				User:         keyRUOthersUser,
				SqlDigest:    nil,
				PlanDigest:   nil,
				Items:        rec.items.toProto(),
			})
		}
	}

	return result
}

// getReportRecords applies two-level TopN filtering and returns records ready for reporting.
// Level 1: Global TopN users (200)
// Level 2: Per-user TopN SQLs (200)
// Evicted users are merged into "others user", evicted SQLs into "others SQL".
//
// Phase 3: Pre-aggregated "others user" from collection phase (c.othersUser) is merged
// with any users evicted during TopN filtering.
func (c *ruCollecting) getReportRecords(keyspaceName []byte) []tipb.TopRURecord {
	return c.getReportRecordsWithLimits(keyspaceName, maxTopUsers, maxTopSQLsPerUser)
}

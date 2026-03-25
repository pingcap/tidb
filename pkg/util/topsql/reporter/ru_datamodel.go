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
	"sort"
	"sync"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/wangjohn/quickselect"
	"go.uber.org/zap"
)

// TopN limits for RU aggregation.
//
// Two "others" buckets are used to bound cardinality:
// 1. "others user" (othersUserWireLabel = "_TIDB_TOPRU_OTHERS_USER") for evicted users.
// 2. "others SQL" (nil sqlDigest + nil planDigest) for evicted SQLs per user.
const (
	// maxTopUsers is the maximum number of users to keep in global TopN.
	maxTopUsers = 200
	// maxTopSQLsPerUser is the maximum number of SQLs to keep per user.
	maxTopSQLsPerUser = 200
	// othersUserWireLabel is the wire/output label for aggregated "others user".
	// It is only used when encoding to TopRURecord output.
	// Under current runtime invariant, real user values come from vars.User.String()
	// (shape: "user@host" or ""), so this label does not collide with runtime users.
	othersUserWireLabel = "_TIDB_TOPRU_OTHERS_USER"

	// Pre-TopN memory caps used during collection.
	maxPreTopNUsers       = maxTopUsers * 2       // 400 users max during collection.
	maxPreTopNSQLsPerUser = maxTopSQLsPerUser * 2 // 400 SQLs per user max during collection.
)

// ruItem stores RU statistics for one timestamp.
type ruItem struct {
	timestamp    uint64
	totalRU      float64
	execCount    uint64
	execDuration uint64
}

// toProto converts ruItem to protobuf.
func (i *ruItem) toProto() tipb.TopRURecordItem {
	return tipb.TopRURecordItem{
		TimestampSec: i.timestamp,
		TotalRu:      i.totalRU,
		ExecCount:    i.execCount,
		ExecDuration: i.execDuration,
	}
}

// ruItems is sorted by timestamp in ascending order.
type ruItems []ruItem

func (rs ruItems) Len() int           { return len(rs) }
func (rs ruItems) Less(i, j int) bool { return rs[i].timestamp < rs[j].timestamp }
func (rs ruItems) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }

// toProto converts ruItems to protobuf.
func (rs ruItems) toProto() []*tipb.TopRURecordItem {
	if len(rs) == 0 {
		return nil
	}
	vals := make([]tipb.TopRURecordItem, len(rs))
	items := make([]*tipb.TopRURecordItem, len(rs))
	for i := range rs {
		vals[i] = rs[i].toProto()
		items[i] = &vals[i]
	}
	return items
}

type sqlPlanKey struct {
	sqlDigest  stmtstats.BinaryDigest
	planDigest stmtstats.BinaryDigest
}

// othersKey is the sentinel key for the aggregated "others SQL" bucket.
// It uses the zero value (empty sql/plan digest), matching legacy encodeKey(nil, nil).
var othersKey = sqlPlanKey{}

func makeKey(sqlDigest, planDigest stmtstats.BinaryDigest) sqlPlanKey {
	return sqlPlanKey{sqlDigest: sqlDigest, planDigest: planDigest}
}

func isOthersKey(key sqlPlanKey) bool {
	return key == othersKey
}

// ruRecord stores RU statistics for one (sql_digest, plan_digest).
type ruRecord struct {
	sqlDigest  stmtstats.BinaryDigest
	planDigest stmtstats.BinaryDigest
	items      ruItems
	totalRU    float64 // cumulative RU for TopN sorting
}

func newRURecord(sqlDigest, planDigest stmtstats.BinaryDigest) *ruRecord {
	return &ruRecord{
		sqlDigest:  sqlDigest,
		planDigest: planDigest,
		items:      make(ruItems, 0, 4),
	}
}

func newOthersRURecord() *ruRecord {
	return newRURecord("", "")
}

// add appends an RU increment for a timestamp.
func (r *ruRecord) add(timestamp uint64, totalRU float64, execCount, execDuration uint64) {
	for idx := range r.items {
		if r.items[idx].timestamp == timestamp {
			r.items[idx].totalRU += totalRU
			r.items[idx].execCount += execCount
			r.items[idx].execDuration += execDuration
			r.totalRU += totalRU
			return
		}
	}

	r.items = append(r.items, ruItem{
		timestamp:    timestamp,
		totalRU:      totalRU,
		execCount:    execCount,
		execDuration: execDuration,
	})
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

// mergeWithTimestamp merges another ruRecord with all timestamps rewritten to ts.
func (r *ruRecord) mergeWithTimestamp(other *ruRecord, ts uint64) {
	if other == nil {
		return
	}
	for _, item := range other.items {
		r.add(ts, item.totalRU, item.execCount, item.execDuration)
	}
}

// ruRecords is sorted by totalRU in descending order.
type ruRecords []*ruRecord

func (rs ruRecords) Len() int           { return len(rs) }
func (rs ruRecords) Less(i, j int) bool { return rs[i].totalRU > rs[j].totalRU } // DESC
func (rs ruRecords) Swap(i, j int)      { rs[i], rs[j] = rs[j], rs[i] }

// topN returns top-n records by totalRU and evicted records.
func (rs ruRecords) topN(n int) (top, evicted ruRecords) {
	if len(rs) <= n {
		return rs, nil
	}
	if err := quickselect.QuickSelect(rs, n); err != nil {
		logutil.BgLogger().Warn("[top-sql] quickselect failed on all ru records, returning unsorted",
			zap.Int("total", len(rs)), zap.Int("topN", n), zap.Error(err))
		return rs, nil
	}
	return rs[:n], rs[n:]
}

// userRUCollecting tracks RU data for one user with per-user SQL TopN.
type userRUCollecting struct {
	records   map[sqlPlanKey]*ruRecord // sqlPlanKey => ruRecord
	othersRec *ruRecord                // Pre-aggregated "others SQL" record
	// user is the real user key for entries stored in ruCollecting.users.
	// Do not use this field to infer whether it is synthetic othersUser.
	user               string
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
		records:            make(map[sqlPlanKey]*ruRecord, preTopNSQLsPerUser),
		preTopNSQLsPerUser: preTopNSQLsPerUser,
	}
}

func newOthersUserRUCollectingWithCap(preTopNSQLsPerUser int) *userRUCollecting {
	return newUserRUCollectingWithCap("", preTopNSQLsPerUser)
}

// add adds RU increments for one SQL.
// When SQL count reaches the pre-cap, new SQLs are merged into "others SQL".
func (u *userRUCollecting) add(timestamp uint64, sqlDigest, planDigest stmtstats.BinaryDigest, incr *stmtstats.RUIncrement) {
	if incr == nil {
		return
	}

	key := makeKey(sqlDigest, planDigest)
	if isOthersKey(key) {
		u.addOthers(timestamp, incr)
		return
	}

	rec, ok := u.records[key]
	if ok {
		rec.addIncr(timestamp, incr)
		u.totalRU += incr.TotalRU
		return
	}

	// At capacity, merge into "others SQL".
	if len(u.records) >= u.preTopNSQLsPerUser {
		u.addOthers(timestamp, incr)
		return
	}

	rec = newRURecord(sqlDigest, planDigest)
	u.records[key] = rec
	rec.addIncr(timestamp, incr)
	u.totalRU += incr.TotalRU
}

// addOthers adds RU increments into this user's "others SQL" bucket.
func (u *userRUCollecting) addOthers(timestamp uint64, incr *stmtstats.RUIncrement) {
	if incr == nil {
		return
	}

	if u.othersRec == nil {
		u.othersRec = newOthersRURecord()
		// Compatibility: fold any legacy nil-digest record into othersRec.
		if rec, ok := u.records[othersKey]; ok {
			u.othersRec.merge(rec)
			delete(u.records, othersKey)
		}
	}

	u.othersRec.addIncr(timestamp, incr)
	u.totalRU += incr.TotalRU
}

// getReportRecordsWithLimit returns TopN SQL records for this user.
func (u *userRUCollecting) getReportRecordsWithLimit(topNSQLsPerUser int) []*ruRecord {
	if topNSQLsPerUser <= 0 {
		topNSQLsPerUser = maxTopSQLsPerUser
	}
	if len(u.records) == 0 && u.othersRec == nil {
		return nil
	}

	// Fast path: no need to apply TopN when we are strictly under the limit
	// and have no pre-aggregated "others SQL". In this case all records are
	// already part of the final TopN set and we can avoid quickselect.
	if len(u.records) <= topNSQLsPerUser && u.othersRec == nil {
		top := make([]*ruRecord, 0, len(u.records))
		for _, rec := range u.records {
			top = append(top, rec)
		}
		return top
	}

	// Extract all records.
	allRecords := make(ruRecords, 0, len(u.records))
	for _, rec := range u.records {
		allRecords = append(allRecords, rec)
	}

	// Apply TopN filtering.
	top, evicted := allRecords.topN(topNSQLsPerUser)

	// Start with pre-aggregated "others" from collection phase.
	var othersRec *ruRecord
	if u.othersRec != nil {
		othersRec = u.othersRec
	}

	// Merge evicted records into "others SQL".
	if len(evicted) > 0 {
		if othersRec == nil {
			othersRec = newOthersRURecord()
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

// mergeRecord merges srcRec into this user's records.
// It falls back to othersRec when at capacity or when key is othersKey.
func (u *userRUCollecting) mergeRecord(key sqlPlanKey, srcRec *ruRecord, targetTs uint64, rewriteTs bool) {
	if srcRec == nil || len(srcRec.items) == 0 {
		return
	}

	// "others SQL" path.
	if isOthersKey(key) {
		if u.othersRec == nil {
			u.othersRec = newOthersRURecord()
		}
		u.mergeRecordInto(u.othersRec, srcRec, targetTs, rewriteTs)
		return
	}

	// Normal SQL path.
	dstRec, ok := u.records[key]
	if !ok {
		// Check pre-TopN cap before adding a new SQL.
		if len(u.records) >= u.preTopNSQLsPerUser {
			if u.othersRec == nil {
				u.othersRec = newOthersRURecord()
			}
			u.mergeRecordInto(u.othersRec, srcRec, targetTs, rewriteTs)
			return
		}
		dstRec = newRURecord(key.sqlDigest, key.planDigest)
		u.records[key] = dstRec
	}
	u.mergeRecordInto(dstRec, srcRec, targetTs, rewriteTs)
}

// mergeRecordInto merges srcRec into dstRec and updates totalRU.
func (u *userRUCollecting) mergeRecordInto(dstRec, srcRec *ruRecord, targetTs uint64, rewriteTs bool) {
	if rewriteTs {
		dstRec.mergeWithTimestamp(srcRec, targetTs)
	} else {
		dstRec.merge(srcRec)
	}
	u.totalRU += srcRec.totalRU
}

// userRUCollectings is sorted by totalRU in descending order.
type userRUCollectings []*userRUCollecting

func (us userRUCollectings) Len() int           { return len(us) }
func (us userRUCollectings) Less(i, j int) bool { return us[i].totalRU > us[j].totalRU } // DESC
func (us userRUCollectings) Swap(i, j int)      { us[i], us[j] = us[j], us[i] }

// topN returns top-n users by totalRU and evicted users.
func (us userRUCollectings) topN(n int) (top, evicted userRUCollectings) {
	if len(us) <= n {
		return us, nil
	}
	if err := quickselect.QuickSelect(us, n); err != nil {
		logutil.BgLogger().Warn("[top-sql] quickselect failed on all ru records, returning unsorted",
			zap.Int("total", len(us)), zap.Int("topN", n), zap.Error(err))
		return us, nil
	}
	return us[:n], us[n:]
}

// ruCollecting is the top-level RU collector.
// It keeps global TopN users with per-user SQL TopN.
type ruCollecting struct {
	users map[string]*userRUCollecting // user => userRUCollecting
	// othersUser stores the synthetic global "others user" bucket.
	// Its identity is determined by this field location, not by userRUCollecting.user value.
	othersUser         *userRUCollecting
	preTopNUsers       int
	preTopNSQLsPerUser int
	mu                 sync.Mutex
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
		users:              make(map[string]*userRUCollecting, preTopNUsers),
		preTopNUsers:       preTopNUsers,
		preTopNSQLsPerUser: preTopNSQLsPerUser,
	}
}

// add adds RU increments from aggregator.
// When user count reaches the pre-cap, new users are merged into "others user".
func (c *ruCollecting) add(timestamp uint64, key stmtstats.RUKey, incr *stmtstats.RUIncrement) {
	user := key.User
	userCollecting, ok := c.users[user]
	if !ok {
		// At capacity, merge into "others user".
		if len(c.users) >= c.preTopNUsers {
			c.getOrCreateOthersUser().addOthers(timestamp, incr)
			return
		}
		userCollecting = newUserRUCollectingWithCap(user, c.preTopNSQLsPerUser)
		c.users[user] = userCollecting
	}
	userCollecting.add(timestamp, key.SQLDigest, key.PlanDigest, incr)
}

// addBatch adds a batch of RU increments for a given timestamp.
// It is called from collectRUWorker.
func (c *ruCollecting) addBatch(timestamp uint64, increments stmtstats.RUIncrementMap) {
	for key, incr := range increments {
		c.add(timestamp, key, incr)
	}
}

// take returns collected data and resets internal state.
func (c *ruCollecting) take() *ruCollecting {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := &ruCollecting{
		users:              c.users,
		othersUser:         c.othersUser,
		preTopNUsers:       c.preTopNUsers,
		preTopNSQLsPerUser: c.preTopNSQLsPerUser,
	}
	c.users = make(map[string]*userRUCollecting, c.preTopNUsers)
	c.othersUser = nil
	return result
}

func normalizeTopNLimits(maxUsers, maxSQLsPerUser int) (normalizedMaxUsers, normalizedMaxSQLsPerUser int) {
	normalizedMaxUsers = maxUsers
	normalizedMaxSQLsPerUser = maxSQLsPerUser
	if normalizedMaxUsers <= 0 {
		normalizedMaxUsers = maxTopUsers
	}
	if normalizedMaxSQLsPerUser <= 0 {
		normalizedMaxSQLsPerUser = maxTopSQLsPerUser
	}
	return normalizedMaxUsers, normalizedMaxSQLsPerUser
}

// toTopRURecords converts current collecting data to proto records.
// It does not apply TopN filtering.
func (c *ruCollecting) toTopRURecords(keyspaceName []byte) []tipb.TopRURecord {
	if len(c.users) == 0 && c.othersUser == nil {
		return nil
	}

	// Pre-compute total record count to allocate result slice once.
	totalRecords := 0
	for _, userCollecting := range c.users {
		totalRecords += len(userCollecting.records)
		if userCollecting.othersRec != nil {
			totalRecords++
		}
	}
	if c.othersUser != nil && c.othersUser.othersRec != nil {
		totalRecords++
	}
	result := make([]tipb.TopRURecord, 0, totalRecords)
	for _, userCollecting := range c.users {
		for _, rec := range userCollecting.records {
			if len(rec.items) > 1 {
				sort.Sort(rec.items)
			}
			var sqlDigest []byte
			var planDigest []byte
			if len(rec.sqlDigest) > 0 {
				sqlDigest = []byte(rec.sqlDigest)
			}
			if len(rec.planDigest) > 0 {
				planDigest = []byte(rec.planDigest)
			}
			result = append(result, tipb.TopRURecord{
				KeyspaceName: keyspaceName,
				User:         userCollecting.user,
				SqlDigest:    sqlDigest,
				PlanDigest:   planDigest,
				Items:        rec.items.toProto(),
			})
		}
		if userCollecting.othersRec != nil {
			if len(userCollecting.othersRec.items) > 1 {
				sort.Sort(userCollecting.othersRec.items)
			}
			result = append(result, tipb.TopRURecord{
				KeyspaceName: keyspaceName,
				User:         userCollecting.user,
				SqlDigest:    nil,
				PlanDigest:   nil,
				Items:        userCollecting.othersRec.items.toProto(),
			})
		}
	}
	if c.othersUser != nil && c.othersUser.othersRec != nil {
		if len(c.othersUser.othersRec.items) > 1 {
			sort.Sort(c.othersUser.othersRec.items)
		}
		result = append(result, tipb.TopRURecord{
			KeyspaceName: keyspaceName,
			User:         othersUserWireLabel,
			SqlDigest:    nil,
			PlanDigest:   nil,
			Items:        c.othersUser.othersRec.items.toProto(),
		})
	}
	return result
}

// mergeUserIntoOthers merges one user's records into dst othersRec.
func mergeUserIntoOthers(dst, src *userRUCollecting) {
	if dst == nil || src == nil {
		return
	}
	for _, rec := range src.records {
		dst.mergeRecord(othersKey, rec, 0, false)
	}
	if src.othersRec != nil {
		dst.mergeRecord(othersKey, src.othersRec, 0, false)
	}
}

// compactWithLimits applies TopN limits and returns a compacted ruCollecting.
func (c *ruCollecting) compactWithLimits(maxUsers, maxSQLsPerUser int) *ruCollecting {
	maxUsers, maxSQLsPerUser = normalizeTopNLimits(maxUsers, maxSQLsPerUser)
	if len(c.users) == 0 && c.othersUser == nil {
		return nil
	}

	// Fast path: everything is already within the final TopN bounds and there is
	// no pre-aggregated "others user" or "others SQL". In this case compacting
	// would be a no-op, so we can return the original collecting.
	//
	// NOTE: fast path may return c itself. Callers must treat the returned
	// collecting as read-only (or ownership-transferred) and must not mutate it.
	if len(c.users) <= maxUsers && c.othersUser == nil {
		underSQLCap := true
		for _, u := range c.users {
			if len(u.records) > maxSQLsPerUser || u.othersRec != nil {
				underSQLCap = false
				break
			}
		}
		if underSQLCap {
			return c
		}
	}

	// Extract all users.
	allUsers := make(userRUCollectings, 0, len(c.users))
	for _, userCollecting := range c.users {
		allUsers = append(allUsers, userCollecting)
	}

	// Apply global TopN user filtering.
	topUsers, evictedUsers := allUsers.topN(maxUsers)

	// Build result snapshot with top users.
	result := newRUCollectingWithCaps(maxUsers, maxSQLsPerUser)
	for _, userCollecting := range topUsers {
		// Apply per-user SQL TopN and get compacted records.
		userRecords := userCollecting.getReportRecordsWithLimit(maxSQLsPerUser)
		compactedUser := newUserRUCollectingWithCap(userCollecting.user, maxSQLsPerUser)
		// Directly assign records to the fresh compactedUser.
		for _, rec := range userRecords {
			if len(rec.sqlDigest) == 0 && len(rec.planDigest) == 0 {
				// "others SQL" record.
				if compactedUser.othersRec == nil {
					compactedUser.othersRec = rec
				} else {
					compactedUser.othersRec.merge(rec)
				}
			} else {
				// Normal SQL record.
				key := makeKey(rec.sqlDigest, rec.planDigest)
				compactedUser.records[key] = rec
			}
			compactedUser.totalRU += rec.totalRU
		}
		result.users[compactedUser.user] = compactedUser
	}

	// Merge evicted users into "others user".
	var othersUser *userRUCollecting
	if c.othersUser != nil {
		othersUser = newOthersUserRUCollectingWithCap(maxSQLsPerUser)
		mergeUserIntoOthers(othersUser, c.othersUser)
	}
	if len(evictedUsers) > 0 {
		if othersUser == nil {
			othersUser = newOthersUserRUCollectingWithCap(maxSQLsPerUser)
		}
		for _, evictedUser := range evictedUsers {
			mergeUserIntoOthers(othersUser, evictedUser)
		}
	}
	result.othersUser = othersUser

	return result
}

// getOrCreateUser returns userRUCollecting for real users and creates it if needed.
// It also returns whether the user overflows current user capacity.
func (c *ruCollecting) getOrCreateUser(user string) (u *userRUCollecting, overflow bool) {
	u, ok := c.users[user]
	if !ok {
		if len(c.users) >= c.preTopNUsers {
			return nil, true
		}
		u = newUserRUCollectingWithCap(user, c.preTopNSQLsPerUser)
		c.users[user] = u
	}
	return u, false
}

// getOrCreateOthersUser returns othersUser and creates it if needed.
func (c *ruCollecting) getOrCreateOthersUser() *userRUCollecting {
	if c.othersUser == nil {
		// Identity of synthetic global othersUser comes from being stored at
		// c.othersUser, not from userRUCollecting.user value.
		c.othersUser = newOthersUserRUCollectingWithCap(c.preTopNSQLsPerUser)
	}
	return c.othersUser
}

// mergeFrom merges data from src into the current ruCollecting.
// If rewriteTimestamp is true, all timestamps are rewritten to targetTimestamp.
// Callers must ensure external synchronization.
func (c *ruCollecting) mergeFrom(src *ruCollecting, targetTimestamp uint64, rewriteTimestamp bool) {
	if src == nil {
		return
	}

	// Merge regular users.
	for _, srcUser := range src.users {
		dstUser, overflow := c.getOrCreateUser(srcUser.user)
		if overflow {
			// When dst user capacity is full, fold the entire src user footprint into
			// synthetic global othersUser instead of creating a new real-user bucket.
			dstOthersUser := c.getOrCreateOthersUser()
			for _, srcRec := range srcUser.records {
				dstOthersUser.mergeRecord(othersKey, srcRec, targetTimestamp, rewriteTimestamp)
			}
			if srcUser.othersRec != nil {
				dstOthersUser.mergeRecord(othersKey, srcUser.othersRec, targetTimestamp, rewriteTimestamp)
			}
			continue
		}
		for _, srcRec := range srcUser.records {
			key := makeKey(srcRec.sqlDigest, srcRec.planDigest)
			dstUser.mergeRecord(key, srcRec, targetTimestamp, rewriteTimestamp)
		}
		// Also merge user's othersRec.
		if srcUser.othersRec != nil {
			dstUser.mergeRecord(othersKey, srcUser.othersRec, targetTimestamp, rewriteTimestamp)
		}
	}

	// Merge "others user".
	if src.othersUser != nil {
		dstOthersUser := c.getOrCreateOthersUser()
		// Compatibility: merge potential legacy records map into othersUser.
		for _, srcRec := range src.othersUser.records {
			dstOthersUser.mergeRecord(othersKey, srcRec, targetTimestamp, rewriteTimestamp)
		}
		if src.othersUser.othersRec != nil {
			dstOthersUser.mergeRecord(othersKey, src.othersUser.othersRec, targetTimestamp, rewriteTimestamp)
		}
	}
}

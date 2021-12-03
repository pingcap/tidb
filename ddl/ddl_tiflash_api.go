// Copyright 2016 PingCAP, Inc.
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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/domain/infosync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl/placement"
	"go.uber.org/zap"

	//ddlutil "github.com/pingcap/tidb/ddl/util"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/gcutil"
)

// PollTiFlashReplicaStatusContext records status for each TiFlash replica.
type PollTiFlashReplicaStatusContext struct {
	ID             int64
	Count          uint64
	LocationLabels []string
	Available      bool
	HighPriority   bool
}

// PollTiFlashReplicaStatusBackoff records backoff for each TiFlash Table
type PollTiFlashReplicaStatusBackoff struct {
	Counter   int
	Threshold int
}

var (
	// PollTiFlashReplicaStatusBackoffMaxTick is the max tick before we try to update TiFlash replica availability for one table.
	PollTiFlashReplicaStatusBackoffMaxTick = 60
	// PollTiFlashReplicaStatusBackoffMinTick is the max tick before we try to update TiFlash replica availability for one table.
	PollTiFlashReplicaStatusBackoffMinTick  = 2
	pollTiFlashReplicaStatusBackoffCapacity = 1000
)

// NewPollTiFlashReplicaStatusBackoff create an instance with the smallest interval.
func NewPollTiFlashReplicaStatusBackoff() PollTiFlashReplicaStatusBackoff {
	return PollTiFlashReplicaStatusBackoff{
		Counter:   1,
		Threshold: PollTiFlashReplicaStatusBackoffMinTick,
	}
}

// Tick will increase Counter, and check if Threshold meets.
func (b *PollTiFlashReplicaStatusBackoff) Tick() bool {
	if b.Threshold < PollTiFlashReplicaStatusBackoffMinTick {
		b.Threshold = PollTiFlashReplicaStatusBackoffMinTick
	}
	if b.Threshold > PollTiFlashReplicaStatusBackoffMaxTick {
		b.Threshold = PollTiFlashReplicaStatusBackoffMaxTick
	}
	log.Info("Tick", zap.Int("Counter", b.Counter), zap.Int("Threshold", b.Threshold))
	defer func() {
		b.Counter += 1
		b.Counter %= b.Threshold
	}()
	if b.Counter%b.Threshold == 0 {
		return true
	}
	return false
}

// Backoff will increase Threshold
func (b *PollTiFlashReplicaStatusBackoff) Backoff() {
	if b.Threshold < PollTiFlashReplicaStatusBackoffMinTick {
		b.Threshold = PollTiFlashReplicaStatusBackoffMinTick
	}
	if b.Threshold > PollTiFlashReplicaStatusBackoffMaxTick/2 {
		b.Threshold = PollTiFlashReplicaStatusBackoffMaxTick
		return
	}
	b.Threshold *= 2
	b.Counter = 1
}

func makeBaseRule() placement.Rule {
	return placement.Rule{
		GroupID:  "tiflash",
		ID:       "",
		Index:    0,
		Override: true,
		Role:     placement.Learner,
		Count:    2,
		Constraints: []placement.Constraint{
			{
				Key:    "engine",
				Op:     placement.In,
				Values: []string{"tiflash"},
			},
		},
	}
}

// MakeNewRule creates a pd rule for TiFlash.
func MakeNewRule(ID int64, Count uint64, LocationLabels []string) *placement.Rule {
	ruleID := fmt.Sprintf("table-%v-r", ID)
	startKey := tablecodec.GenTableRecordPrefix(ID)
	endKey := tablecodec.EncodeTablePrefix(ID + 1)
	startKey = codec.EncodeBytes([]byte{}, startKey)
	endKey = codec.EncodeBytes([]byte{}, endKey)

	ruleNew := makeBaseRule()
	ruleNew.ID = ruleID
	ruleNew.StartKeyHex = startKey.String()
	ruleNew.EndKeyHex = endKey.String()
	ruleNew.Count = int(Count)
	ruleNew.LocationLabels = LocationLabels

	return &ruleNew
}

func getTiflashHTTPAddr(host string, statusAddr string) (string, error) {
	configURL := fmt.Sprintf("%s://%s/config",
		util.InternalHTTPSchema(),
		statusAddr,
	)
	resp, err := util.InternalHTTPClient().Get(configURL)
	if err != nil {
		return "", errors.Trace(err)
	}

	defer func() {
		resp.Body.Close()
	}()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return "", errors.Trace(err)
	}

	var j map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &j)
	if err != nil {
		return "", errors.Trace(err)
	}

	engineStore, ok := j["engine-store"].(map[string]interface{})
	if !ok {
		return "", errors.New("Error json")
	}
	port64, ok := engineStore["http_port"].(float64)
	if !ok {
		return "", errors.New("Error json")
	}
	port := int(port64)

	addr := fmt.Sprintf("%v:%v", host, port)
	return addr, nil
}

// GetTiFlashReplicaInfo parses model.TableInfo into []PollTiFlashReplicaStatusContext.
func GetTiFlashReplicaInfo(tblInfo *model.TableInfo, tableList *[]PollTiFlashReplicaStatusContext) {
	if tblInfo.TiFlashReplica == nil {
		// reject tables that has no tiflash replica such like `INFORMATION_SCHEMA`
		return
	}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			log.Debug(fmt.Sprintf("Table %v has partition %v\n", tblInfo.ID, p.ID))
			*tableList = append(*tableList, PollTiFlashReplicaStatusContext{p.ID,
				tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID), false})
		}
		// partitions that in adding mid-state
		for _, p := range pi.AddingDefinitions {
			log.Debug(fmt.Sprintf("Table %v has partition %v\n", tblInfo.ID, p.ID))
			*tableList = append(*tableList, PollTiFlashReplicaStatusContext{p.ID, tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID), true})
		}
	} else {
		log.Debug(fmt.Sprintf("Table %v has no partition\n", tblInfo.ID))
		*tableList = append(*tableList, PollTiFlashReplicaStatusContext{tblInfo.ID, tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.Available, false})
	}
}

// TODO test _update_http_port, since we have no etcdCli
func (d *ddl) UpdateTiFlashHTTPAddress(store *helper.StoreStat) error {
	addrAndPort := strings.Split(store.Store.StatusAddress, ":")
	if len(addrAndPort) < 2 {
		return errors.New("Can't get TiFlash Address from PD")
	}
	httpAddr, err := getTiflashHTTPAddr(addrAndPort[0], store.Store.StatusAddress)
	if err != nil {
		return errors.Trace(err)
	}
	// Report to pd
	key := fmt.Sprintf("/tiflash/cluster/http_port/%v", store.Store.Address)
	if d.etcdCli == nil {
		return errors.New("no etcdCli in ddl")
	}
	origin := ""
	resp, err := d.etcdCli.Get(d.ctx, key)
	if err != nil {
		// If there is no key,
		for _, kv := range resp.Kvs {
			if string(kv.Key) == key {
				origin = string(kv.Value)
				break
			}
		}
	}
	if origin != httpAddr {
		// TODO add lease ttl
		log.Warn(fmt.Sprintf("Update status addr to %v\n", httpAddr))
		// TODO this may fail with no error
		_, err := d.etcdCli.Put(d.ctx, key, httpAddr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) PollTiFlashReplicaStatus(ctx sessionctx.Context, handlePd bool, backoffs *map[int64]*PollTiFlashReplicaStatusBackoff) (bool, error) {
	allReplicaReady := true
	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return false, errors.New("Can not get Helper")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}

	// TODO Is there any ways we can get all TiFlash stores without send request to PD?
	tikvStats, err := tikvHelper.GetStoresStat()
	if err != nil {
		return false, errors.Trace(err)
	}
	tiflashStores := make(map[int64]helper.StoreStat)
	for _, store := range tikvStats.Stores {
		for _, l := range store.Store.Labels {
			if l.Key == "engine" && l.Value == "tiflash" {
				tiflashStores[store.Store.ID] = store
				log.Debug("Find tiflash store", zap.Int64("id", store.Store.ID), zap.String("Address", store.Store.Address), zap.String("StatusAddress", store.Store.StatusAddress))
			}
		}
	}

	for _, store := range tiflashStores {
		s := store
		err := d.UpdateTiFlashHTTPAddress(&s)
		log.Error("Update TiFlash status address failed", zap.Error(err))
	}

	// Main body of table_update
	schema := d.GetInfoSchemaWithInterceptor(ctx)
	if schema == nil {
		return false, errors.New("Schema is nil")
	}

	// Compute table_list
	var tableList = make([]PollTiFlashReplicaStatusContext, 0)

	for _, db := range schema.AllSchemas() {
		tbls := schema.SchemaTables(db.Name)
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			GetTiFlashReplicaInfo(tblInfo, &tableList)
		}
	}

	// Removed pd rule handling to somewhere else
	if handlePd {
		if err := HandlePlacementRuleRoutine(ctx, d, tableList); err != nil {
			log.Error("handle placement rule routine error", zap.Error(err))
		}
	}

	for _, tb := range tableList {
		// For every region in each table, if it has one replica, we reckon it ready.
		// TODO Can we batch request table?
		available := tb.Available
		failpoint.Inject("PollTiFlashReplicaStatusReplacePrevAvailableValue", func(val failpoint.Value) {
			available = val.(bool)
		})
		if !available {
			bo, ok := (*backoffs)[tb.ID]
			if !ok {
				// Small table may be already ready at first check later.
				// so we omit assigning into `backoffs` map for the first time.
			} else {
				if !bo.Tick() {
					// Skip
					log.Info("Escape checking available status", zap.Int64("tableId", tb.ID))
					continue
				}
			}

			allReplicaReady = false

			// We don't need to set accelerate schedule, since it is already done in DDL.
			// Compute sync data process by request TiFlash.
			regionReplica := make(map[int64]int)
			for _, store := range tiflashStores {
				statURL := fmt.Sprintf("%s://%s/tiflash/sync-status/%d",
					util.InternalHTTPSchema(),
					store.Store.StatusAddress,
					tb.ID,
				)
				resp, err := util.InternalHTTPClient().Get(statURL)
				if err != nil {
					continue
				}

				defer func() {
					resp.Body.Close()
				}()

				reader := bufio.NewReader(resp.Body)
				ns, _, _ := reader.ReadLine()
				n, err := strconv.ParseInt(string(ns), 10, 64)
				if err != nil {
					return false, errors.Trace(err)
				}
				for i := int64(0); i < n; i++ {
					rs, _, _ := reader.ReadLine()
					// For (`table`, `store`), has region `r`
					r, err := strconv.ParseInt(strings.Trim(string(rs), "\r\n \t"), 10, 32)
					if err != nil {
						return false, errors.Trace(err)
					}
					if i, ok := regionReplica[r]; ok {
						regionReplica[r] = i + 1
					} else {
						regionReplica[r] = 1
					}
				}
			}

			// TODO It will be better if we can get `regionCount` directly from TiDB.
			var stats helper.PDRegionStats
			if err = tikvHelper.GetPDRegionRecordStats(tb.ID, &stats); err != nil {
				return false, errors.Trace(err)
			}

			regionCount := stats.Count
			flashRegionCount := len(regionReplica)
			avail := regionCount == flashRegionCount
			failpoint.Inject("PollTiFlashReplicaStatusReplaceCurAvailableValue", func(val failpoint.Value) {
				avail = val.(bool)
			})

			if !avail {
				bo, ok := (*backoffs)[tb.ID]
				if ok {
					log.Info("TiFlash replica is not ready, trigger backoff", zap.Int64("tableId", tb.ID))
					bo.Backoff()
				} else {
					// If the table is not available at first check, it should be added into `backoffs`
					log.Info("TiFlash replica is not ready, add to backoffs", zap.Int64("tableId", tb.ID))
					if len(*backoffs) < pollTiFlashReplicaStatusBackoffCapacity {
						newBackoff := NewPollTiFlashReplicaStatusBackoff()
						(*backoffs)[tb.ID] = &newBackoff
					}
				}
				log.Info("Update tiflash replica sync process", zap.Int64("id", tb.ID), zap.Int("region need", regionCount), zap.Int("region ready", flashRegionCount))
				err = infosync.UpdateTiFlashTableSyncProgress(context.Background(), tb.ID, float64(flashRegionCount)/float64(regionCount))
			} else {
				log.Info("Tiflash replica is available", zap.Int64("id", tb.ID), zap.Int("region need", regionCount))
				delete(*backoffs, tb.ID)
				err = infosync.DeleteTiFlashTableSyncProgress(tb.ID)
			}
			if err := d.UpdateTableReplicaInfo(ctx, tb.ID, avail); err != nil {
				log.Error("UpdateTableReplicaInfo error when updating TiFlash replica status", zap.Error(err))
			}
		}
	}

	return allReplicaReady, nil
}

// GetDropOrTruncateTableInfoFromJobs gets the dropped/truncated table information from DDL jobs,
// it will use the `start_ts` of DDL job as snapshot to get the dropped/truncated table information.
func getDropOrTruncateTableInfoFromJobsByStore(jobs []*model.Job, gcSafePoint uint64, store *kv.Storage, fn func(*model.Job, *model.TableInfo) (bool, error)) (bool, error) {
	for _, job := range jobs {
		// Check GC safe point for getting snapshot infoSchema.
		err := gcutil.ValidateSnapshotWithGCSafePoint(job.StartTS, gcSafePoint)
		if err != nil {
			return false, err
		}
		if job.Type != model.ActionDropTable && job.Type != model.ActionTruncateTable {
			continue
		}

		snapMeta := meta.NewSnapshotMeta((*store).GetSnapshot(kv.NewVersion(job.StartTS)))
		tbl, err := snapMeta.GetTable(job.SchemaID, job.TableID)
		if err != nil {
			if meta.ErrDBNotExists.Equal(err) {
				// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
				// then can't find the table from the snapshot info-schema. Should just ignore error here,
				// see more in TestParallelDropSchemaAndDropTable.
				continue
			}
			return false, err
		}
		if tbl == nil {
			// The dropped/truncated DDL maybe execute failed that caused by the parallel DDL execution,
			// then can't find the table from the snapshot info-schema. Should just ignore error here,
			// see more in TestParallelDropSchemaAndDropTable.
			continue
		}
		finish, err := fn(job, tbl)
		if err != nil || finish {
			return finish, err
		}
	}
	return false, nil
}

func getDropOrTruncateTableTiflash(ctx sessionctx.Context, currentSchema infoschema.InfoSchema, tikvHelper *helper.Helper, replicaInfos *[]PollTiFlashReplicaStatusContext) error {
	store := tikvHelper.Store.(kv.Storage)

	txn, err := store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	gcSafePoint, err := gcutil.GetGCSafePoint(ctx)
	if err != nil {
		return err
	}
	uniqueIDMap := make(map[int64]struct{})
	handleJobAndTableInfo := func(job *model.Job, tblInfo *model.TableInfo) (bool, error) {
		// Avoid duplicate table ID info.
		if _, ok := currentSchema.TableByID(tblInfo.ID); ok {
			return false, nil
		}
		if _, ok := uniqueIDMap[tblInfo.ID]; ok {
			return false, nil
		}
		uniqueIDMap[tblInfo.ID] = struct{}{}
		GetTiFlashReplicaInfo(tblInfo, replicaInfos)
		return false, nil
	}
	fn := func(jobs []*model.Job) (bool, error) {
		return getDropOrTruncateTableInfoFromJobsByStore(jobs, gcSafePoint, &store, handleJobAndTableInfo)
	}

	err = admin.IterAllDDLJobs(txn, fn)
	if err != nil {
		if terror.ErrorEqual(variable.ErrSnapshotTooOld, err) {
			// The err indicate that current ddl job and remain DDL jobs was been deleted by GC,
			// just ignore the error and return directly.
			return nil
		}
		return err
	}
	return nil
}

// HandlePlacementRuleRoutine fetch all rules from pd, and remove all obsolete rules.
func HandlePlacementRuleRoutine(ctx sessionctx.Context, d *ddl, tableList []PollTiFlashReplicaStatusContext) error {
	// TODO Is it OK to do this in `doGCPlacementRules`, rather than looping?
	currentSchema := d.GetInfoSchemaWithInterceptor(ctx)

	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Can not get Helper")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}

	allRulesArr, err := tikvHelper.GetGroupRules("tiflash")
	if err != nil {
		return errors.Trace(err)
	}
	allRules := make(map[string]placement.Rule)
	for _, r := range allRulesArr {
		allRules[r.ID] = r
	}

	// Cover getDropOrTruncateTableTiflash
	if err := getDropOrTruncateTableTiflash(ctx, currentSchema, tikvHelper, &tableList); err != nil {
		// may fail when no `tikv_gc_safe_point` available, should return in order to remove valid pd rules.
		log.Error("getDropOrTruncateTableTiflash returns error", zap.Error(err))
		return errors.Trace(err)
	}
	for _, tb := range tableList {
		// For every region in each table, if it has one replica, we reckon it ready.
		// TODO Can we batch request table?
		// Implement _check_and_make_rule
		ruleID := fmt.Sprintf("table-%v-r", tb.ID)
		if _, ok := allRules[ruleID]; !ok {
			// Mostly because of a previous failure of setting pd rule.
			log.Warn(fmt.Sprintf("Table %v exists, but there are no rule for it", tb.ID))
			newRule := MakeNewRule(tb.ID, tb.Count, tb.LocationLabels)
			err := tikvHelper.SetPlacementRule(*newRule)
			if err != nil {
				log.Warn("SetPlacementRule fails")
			}
		}
		// For every existing table, we do not remove their rules.
		delete(allRules, ruleID)
	}

	// Remove rules of non-existing table
	for _, v := range allRules {
		log.Info("remove tiflash rule", zap.String("id", v.ID))
		if err := tikvHelper.DeletePlacementRule("tiflash", v.ID); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Copyright 2022 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/placement"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// TiFlashReplicaStatus records status for each TiFlash replica.
type TiFlashReplicaStatus struct {
	ID             int64
	Count          uint64
	LocationLabels []string
	Available      bool
	HighPriority   bool
}

// TiFlashManagementContext is the context for TiFlash Replica Management
type TiFlashManagementContext struct {
	TiFlashStores             map[int64]helper.StoreStat
	HandlePdCounter           uint64
	UpdateTiFlashStoreCounter uint64
	UpdateMap                 map[int64]bool
}

// NewTiFlashManagementContext creates an instance for TiFlashManagementContext.
func NewTiFlashManagementContext() *TiFlashManagementContext {
	return &TiFlashManagementContext{
		HandlePdCounter:           0,
		UpdateTiFlashStoreCounter: 0,
		TiFlashStores:             make(map[int64]helper.StoreStat),
		UpdateMap:                 make(map[int64]bool),
	}
}

var (
	// PollTiFlashInterval is the interval between every pollTiFlashReplicaStatus call.
	PollTiFlashInterval = 2 * time.Second
	// PullTiFlashPdTick indicates the number of intervals before we fully sync all TiFlash pd rules and tables.
	PullTiFlashPdTick = atomicutil.NewUint64(30 * 5)
	// UpdateTiFlashStoreTick indicates the number of intervals before we fully update TiFlash stores.
	UpdateTiFlashStoreTick = atomicutil.NewUint64(5)
)

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

// GetTiFlashReplicaInfo parses model.TableInfo into []TiFlashReplicaStatus.
func GetTiFlashReplicaInfo(tblInfo *model.TableInfo, tableList *[]TiFlashReplicaStatus) {
	if tblInfo.TiFlashReplica == nil {
		// reject tables that has no tiflash replica such like `INFORMATION_SCHEMA`
		return
	}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			logutil.BgLogger().Debug(fmt.Sprintf("Table %v has partition %v\n", tblInfo.ID, p.ID))
			*tableList = append(*tableList, TiFlashReplicaStatus{p.ID,
				tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID), false})
		}
		// partitions that in adding mid-state
		for _, p := range pi.AddingDefinitions {
			logutil.BgLogger().Debug(fmt.Sprintf("Table %v has partition %v\n", tblInfo.ID, p.ID))
			*tableList = append(*tableList, TiFlashReplicaStatus{p.ID, tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID), true})
		}
	} else {
		logutil.BgLogger().Debug(fmt.Sprintf("Table %v has no partition\n", tblInfo.ID))
		*tableList = append(*tableList, TiFlashReplicaStatus{tblInfo.ID, tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.Available, false})
	}
}

// UpdateTiFlashHTTPAddress report TiFlash's StatusAddress's port to Pd's etcd.
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
		return errors.Trace(err)
	}
	// Try to update.
	for _, kv := range resp.Kvs {
		if string(kv.Key) == key {
			origin = string(kv.Value)
			break
		}
	}
	if origin != httpAddr {
		logutil.BgLogger().Warn(fmt.Sprintf("Update status addr of %v from %v to %v", key, origin, httpAddr))
		err := ddlutil.PutKVToEtcd(d.ctx, d.etcdCli, 1, key, httpAddr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func updateTiFlashStores(pollTiFlashContext *TiFlashManagementContext) error {
	// We need the up-to-date information about TiFlash stores.
	// Since TiFlash Replica synchronize may happen immediately after new TiFlash stores are added.
	tikvStats, err := infosync.GetTiFlashStoresStat(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	pollTiFlashContext.TiFlashStores = make(map[int64]helper.StoreStat)
	for _, store := range tikvStats.Stores {
		for _, l := range store.Store.Labels {
			if l.Key == "engine" && l.Value == "tiflash" {
				pollTiFlashContext.TiFlashStores[store.Store.ID] = store
				logutil.BgLogger().Debug("Found tiflash store", zap.Int64("id", store.Store.ID), zap.String("Address", store.Store.Address), zap.String("StatusAddress", store.Store.StatusAddress))
			}
		}
	}
	logutil.BgLogger().Info("updateTiFlashStores finished", zap.Int("TiFlash store count", len(pollTiFlashContext.TiFlashStores)))
	return nil
}

func (d *ddl) pollTiFlashReplicaStatus(ctx sessionctx.Context, pollTiFlashContext *TiFlashManagementContext) (bool, error) {
	allReplicaReady := true
	defer func() {
		pollTiFlashContext.HandlePdCounter += 1
		pollTiFlashContext.HandlePdCounter %= PullTiFlashPdTick.Load()
	}()

	updateTiFlash := pollTiFlashContext.UpdateTiFlashStoreCounter%UpdateTiFlashStoreTick.Load() == 0
	if updateTiFlash {
		if err := updateTiFlashStores(pollTiFlashContext); err != nil {
			// If we failed to get from pd, retry everytime.
			pollTiFlashContext.UpdateTiFlashStoreCounter = 0
			return false, errors.Trace(err)
		}
	}
	pollTiFlashContext.UpdateTiFlashStoreCounter += 1
	pollTiFlashContext.UpdateTiFlashStoreCounter %= UpdateTiFlashStoreTick.Load()

	// The following loop updates TiFlash store's status address.
	for _, store := range pollTiFlashContext.TiFlashStores {
		s := store
		if err := d.UpdateTiFlashHTTPAddress(&s); err != nil {
			logutil.BgLogger().Error("Update TiFlash status address failed", zap.Error(err))
		}
	}

	// Start to process every table.
	schema := d.GetInfoSchemaWithInterceptor(ctx)
	if schema == nil {
		return false, errors.New("Schema is nil")
	}

	var tableList = make([]TiFlashReplicaStatus, 0)

	// Collect TiFlash Replica info, for every table.
	for _, db := range schema.AllSchemas() {
		tbls := schema.SchemaTables(db.Name)
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			GetTiFlashReplicaInfo(tblInfo, &tableList)
		}
	}

	for _, tb := range tableList {
		// For every region in each table, if it has one replica, we reckon it ready.
		// These request can be batched as an optimization.
		available := tb.Available
		failpoint.Inject("PollTiFlashReplicaStatusReplacePrevAvailableValue", func(val failpoint.Value) {
			available = val.(bool)
		})
		// We only check unavailable tables here, so doesn't include blocked add partition case.
		if !available {
			allReplicaReady = false

			// We don't need to set accelerate schedule for this table, since it is already done in DDL, when
			// 1. Add partition
			// 2. Set TiFlash replica

			// Compute sync data process by request TiFlash.
			regionReplica := make(map[int64]int)
			for _, store := range pollTiFlashContext.TiFlashStores {
				err := helper.CollectTiFlashStatus(store.Store.StatusAddress, tb.ID, &regionReplica)
				if err != nil {
					return allReplicaReady, errors.Trace(err)
				}
			}

			logutil.BgLogger().Info("CollectTiFlashStatus", zap.Any("regionReplica", regionReplica), zap.Int64("tb.ID", tb.ID))

			var stats helper.PDRegionStats
			if err := infosync.GetTiFlashPDRegionRecordStats(context.Background(), tb.ID, &stats); err != nil {
				return allReplicaReady, errors.Trace(err)
			}
			regionCount := stats.Count
			flashRegionCount := len(regionReplica)
			avail := regionCount == flashRegionCount
			failpoint.Inject("PollTiFlashReplicaStatusReplaceCurAvailableValue", func(val failpoint.Value) {
				avail = val.(bool)
			})

			if !avail {
				logutil.BgLogger().Info("Tiflash replica is not available", zap.Int64("id", tb.ID), zap.Uint64("region need", uint64(regionCount)), zap.Uint64("region have", uint64(flashRegionCount)))
				err := infosync.UpdateTiFlashTableSyncProgress(context.Background(), tb.ID, float64(flashRegionCount)/float64(regionCount))
				if err != nil {
					return false, errors.Trace(err)
				}
			} else {
				logutil.BgLogger().Info("Tiflash replica is available", zap.Int64("id", tb.ID), zap.Uint64("region need", uint64(regionCount)))
				err := infosync.DeleteTiFlashTableSyncProgress(tb.ID)
				if err != nil {
					return false, errors.Trace(err)
				}
			}
			// Will call `onUpdateFlashReplicaStatus` to update `TiFlashReplica`.
			if err := d.UpdateTableReplicaInfo(ctx, tb.ID, avail); err != nil {
				logutil.BgLogger().Error("UpdateTableReplicaInfo error when updating TiFlash replica status", zap.Error(err))
			}
		}
	}

	return allReplicaReady, nil
}

func getDropOrTruncateTableTiflash(ctx sessionctx.Context, currentSchema infoschema.InfoSchema, tikvHelper *helper.Helper, replicaInfos *[]TiFlashReplicaStatus) error {
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
		getTable := func(StartTS uint64, SchemaID int64, TableID int64) (*model.TableInfo, error) {
			snapMeta := meta.NewSnapshotMeta(store.GetSnapshot(kv.NewVersion(StartTS)))
			if err != nil {
				return nil, err
			}
			tbl, err := snapMeta.GetTable(SchemaID, TableID)
			return tbl, err
		}
		return GetDropOrTruncateTableInfoFromJobsByStore(jobs, gcSafePoint, getTable, handleJobAndTableInfo)
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

// HandlePlacementRuleRoutine fetch all rules from pd, remove all obsolete rules.
// It handles rare situation, when we fail to alter pd rules.
func HandlePlacementRuleRoutine(ctx sessionctx.Context, d *ddl, tableList []TiFlashReplicaStatus) error {
	c := context.Background()
	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return errors.New("Can not get Helper")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}

	allRulesArr, err := infosync.GetTiFlashGroupRules(c, "tiflash")
	if err != nil {
		return errors.Trace(err)
	}
	allRules := make(map[string]placement.TiFlashRule)
	for _, r := range allRulesArr {
		allRules[r.ID] = r
	}

	start := time.Now()
	originLen := len(tableList)
	currentSchema := d.GetInfoSchemaWithInterceptor(ctx)
	if err := getDropOrTruncateTableTiflash(ctx, currentSchema, tikvHelper, &tableList); err != nil {
		// may fail when no `tikv_gc_safe_point` available, should return in order to remove valid pd rules.
		logutil.BgLogger().Error("getDropOrTruncateTableTiflash returns error", zap.Error(err))
		return errors.Trace(err)
	}
	elapsed := time.Since(start)
	logutil.BgLogger().Info("getDropOrTruncateTableTiflash cost", zap.Duration("time", elapsed), zap.Int("updated", len(tableList)-originLen))
	for _, tb := range tableList {
		// For every region in each table, if it has one replica, we reckon it ready.
		ruleID := fmt.Sprintf("table-%v-r", tb.ID)
		if _, ok := allRules[ruleID]; !ok {
			// Mostly because of a previous failure of setting pd rule.
			logutil.BgLogger().Warn(fmt.Sprintf("Table %v exists, but there are no rule for it", tb.ID))
			newRule := infosync.MakeNewRule(tb.ID, tb.Count, tb.LocationLabels)
			_ = infosync.SetTiFlashPlacementRule(context.Background(), *newRule)
		}
		// For every existing table, we do not remove their rules.
		delete(allRules, ruleID)
	}

	// Remove rules of non-existing table
	for _, v := range allRules {
		logutil.BgLogger().Info("Remove TiFlash rule", zap.String("id", v.ID))
		if err := infosync.DeleteTiFlashPlacementRule(c, "tiflash", v.ID); err != nil {
			logutil.BgLogger().Warn("delete TiFlash pd rule failed", zap.Error(err), zap.String("ruleID", v.ID))
		}
	}

	return nil
}

func (d *ddl) PollTiFlashRoutine() {
	pollTiflashContext := NewTiFlashManagementContext()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-time.After(PollTiFlashInterval):
		}
		if d.IsTiFlashPollEnabled() {
			if d.sessPool == nil {
				logutil.BgLogger().Error("failed to get sessionPool for pollTiFlashReplicaStatus")
				return
			}
			failpoint.Inject("BeforePollTiFlashReplicaStatusLoop", func() {
				failpoint.Continue()
			})
			sctx, err := d.sessPool.get()
			if err == nil {
				if d.ownerManager.IsOwner() {
					_, err := d.pollTiFlashReplicaStatus(sctx, pollTiflashContext)
					if err != nil {
						logutil.BgLogger().Warn("pollTiFlashReplicaStatus returns error", zap.Error(err))
					}
				}
				d.sessPool.put(sctx)
			} else {
				if sctx != nil {
					d.sessPool.put(sctx)
				}
				logutil.BgLogger().Error("failed to get session for pollTiFlashReplicaStatus", zap.Error(err))
			}
		}
	}
}

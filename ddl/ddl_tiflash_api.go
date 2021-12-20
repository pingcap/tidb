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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/logutil"
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

// PollTiFlashBackoffElement records backoff for each TiFlash Table.
type PollTiFlashBackoffElement struct {
	Counter   int
	Threshold int
}

// PollTiFlashBackoffContext is a collection of all backoff states.
type PollTiFlashBackoffContext struct {
	MinTick  int
	MaxTick  int
	Capacity int
	elements map[int64]*PollTiFlashBackoffElement
}

// TiFlashManagementContext is the context for TiFlash Replica Management
type TiFlashManagementContext struct {
	BackoffContext            PollTiFlashBackoffContext
	TiFlashStores             map[int64]helper.StoreStat
	HandlePdCounter           int
	UpdateTiFlashStoreCounter int
	mu                        sync.Mutex
	UpdateMap                 map[int64]bool
}

// NewTiFlashManagementContext creates an instance for TiFlashManagementContext.
func NewTiFlashManagementContext() *TiFlashManagementContext {
	return &TiFlashManagementContext{
		HandlePdCounter:           0,
		UpdateTiFlashStoreCounter: 0,
		TiFlashStores:             make(map[int64]helper.StoreStat),
		BackoffContext:            NewPollTiFlashBackoffContext(PollTiFlashBackoffMinTick, PollTiFlashBackoffMaxTick, PollTiFlashBackoffCapacity),
		UpdateMap:                 make(map[int64]bool),
	}
}

// Get returns pointer to inner PollTiFlashBackoffElement.
func (b *PollTiFlashBackoffContext) Get(index int64) (*PollTiFlashBackoffElement, bool) {
	res, ok := b.elements[index]
	return res, ok
}

// Set will replace inner *PollTiFlashBackoffElement at index, or create if there are enough room.
func (b *PollTiFlashBackoffContext) Set(index int64, bo *PollTiFlashBackoffElement) bool {
	_, ok := b.elements[index]
	if ok || b.Len() < b.Capacity {
		b.elements[index] = bo
		return true
	}
	return false
}

// Remove removes table from backoff context.
func (b *PollTiFlashBackoffContext) Remove(index int64) {
	delete(b.elements, index)
}

// Len gets size of PollTiFlashBackoffContext.
func (b *PollTiFlashBackoffContext) Len() int {
	return len(b.elements)
}

var (
	// PollTiFlashInterval is the interval between every PollTiFlashReplicaStatus call.
	PollTiFlashInterval = 1 * time.Second
	// PullTiFlashPdTick indicates the number of intervals before we fully sync all TiFlash pd rules and tables.
	PullTiFlashPdTick = 60 * 5
	// UpdateTiFlashStoreTick indicates the number of intervals before we fully update TiFlash stores.
	UpdateTiFlashStoreTick = 10
	// ReschePullTiFlash is set true, so we do a fully sync, regardless of PullTiFlashPdTick.
	// Set to be true, when last TiFlash pd rule fails.
	ReschePullTiFlash = uint32(0)
	// PollTiFlashBackoffMaxTick is the max tick before we try to update TiFlash replica availability for one table.
	PollTiFlashBackoffMaxTick int = 10
	// PollTiFlashBackoffMinTick is the min tick before we try to update TiFlash replica availability for one table.
	PollTiFlashBackoffMinTick int = 1
	// PollTiFlashBackoffCapacity is the cache size of backoff struct.
	PollTiFlashBackoffCapacity int = 1000
)

// NewPollTiFlashBackoffElement initialize backoff element for a TiFlash table.
func NewPollTiFlashBackoffElement() PollTiFlashBackoffElement {
	return PollTiFlashBackoffElement{
		Counter:   1,
		Threshold: PollTiFlashBackoffMinTick,
	}
}

// NewPollTiFlashBackoffContext creates an instance of PollTiFlashBackoffContext.
func NewPollTiFlashBackoffContext(MinTick, MaxTick, Capacity int) PollTiFlashBackoffContext {
	return PollTiFlashBackoffContext{
		MinTick:  MinTick,
		MaxTick:  MaxTick,
		Capacity: Capacity,
		elements: make(map[int64]*PollTiFlashBackoffElement),
	}
}

// Tick will increase Counter, and check if Threshold meets.
func (b *PollTiFlashBackoffElement) Tick(ctx *PollTiFlashBackoffContext) bool {
	if b.Threshold < ctx.MinTick {
		b.Threshold = ctx.MinTick
	}
	if b.Threshold > ctx.MaxTick {
		b.Threshold = ctx.MaxTick
	}
	defer func() {
		b.Counter += 1
		b.Counter %= b.Threshold
	}()
	return b.Counter%b.Threshold == 0
}

// Grow will increase Threshold, and reset counter
func (b *PollTiFlashBackoffElement) Grow(ctx *PollTiFlashBackoffContext) {
	if b.Threshold < ctx.MinTick {
		b.Threshold = ctx.MinTick
	}
	if b.Threshold > ctx.MaxTick/2 {
		b.Threshold = ctx.MaxTick
		return
	}
	if b.Threshold < 4 {
		b.Threshold += 1
	} else {
		b.Threshold *= 2
	}
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
		// If there is no key,
		for _, kv := range resp.Kvs {
			if string(kv.Key) == key {
				origin = string(kv.Value)
				break
			}
		}
	}
	if origin != httpAddr {
		logutil.BgLogger().Warn(fmt.Sprintf("Update status addr to %v", httpAddr))
		err := ddlutil.PutKVToEtcd(d.ctx, d.etcdCli, 1, key, httpAddr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func updateTiFlashStores(tikvHelper *helper.Helper, pollTiFlashContext *TiFlashManagementContext) error {
	// We need the up-to-date information about TiFlash stores.
	// Since TiFlash Replica synchronize may happen immediately after new TiFlash stores are added.
	tikvStats, err := tikvHelper.GetStoresStat()
	if err != nil {
		return errors.Trace(err)
	}
	for _, store := range tikvStats.Stores {
		for _, l := range store.Store.Labels {
			if l.Key == "engine" && l.Value == "tiflash" {
				pollTiFlashContext.TiFlashStores[store.Store.ID] = store
				logutil.BgLogger().Debug("Find tiflash store", zap.Int64("id", store.Store.ID), zap.String("Address", store.Store.Address), zap.String("StatusAddress", store.Store.StatusAddress))
			}
		}
	}
	return nil
}

func (d *ddl) PollTiFlashReplicaStatus(ctx sessionctx.Context, pollTiFlashContext *TiFlashManagementContext) (bool, error) {
	allReplicaReady := true
	defer func() {
		pollTiFlashContext.HandlePdCounter += 1
		pollTiFlashContext.HandlePdCounter %= PullTiFlashPdTick
	}()

	tikvStore, ok := ctx.GetStore().(helper.Storage)
	if !ok {
		return false, errors.New("Can not get Helper")
	}
	tikvHelper := &helper.Helper{
		Store:       tikvStore,
		RegionCache: tikvStore.GetRegionCache(),
	}

	updateTiFlash := pollTiFlashContext.UpdateTiFlashStoreCounter%UpdateTiFlashStoreTick == 0
	if updateTiFlash {
		if err := updateTiFlashStores(tikvHelper, pollTiFlashContext); err != nil {
			// If we failed to get from pd, retry everytime.
			pollTiFlashContext.UpdateTiFlashStoreCounter = 0
			return false, errors.Trace(err)
		}
		pollTiFlashContext.UpdateTiFlashStoreCounter += 1
		pollTiFlashContext.UpdateTiFlashStoreCounter %= UpdateTiFlashStoreTick
	}

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

	// Missing/Removed pd rule handling.

	handlePd := pollTiFlashContext.HandlePdCounter%PullTiFlashPdTick == 0
	if atomic.CompareAndSwapUint32(&ReschePullTiFlash, 0, 1) {
		// This is because last pd rule failed.
		handlePd = true
	}
	if handlePd {
		if err := HandlePlacementRuleRoutine(ctx, d, tableList); err != nil {
			logutil.BgLogger().Error("handle placement rule routine error", zap.Error(err))
		}
	}

	for _, tb := range tableList {
		// For every region in each table, if it has one replica, we reckon it ready.
		// These request can be batched as an optimization.
		available := tb.Available
		failpoint.Inject("PollTiFlashReplicaStatusReplacePrevAvailableValue", func(val failpoint.Value) {
			available = val.(bool)
		})
		duplicate := false
		pollTiFlashContext.mu.Lock()
		if a, ok := pollTiFlashContext.UpdateMap[tb.ID]; ok {
			// If there is already pending job, we shall finish it first.
			// TODO Maybe we can make immediate status check here
			logutil.BgLogger().Info("TiFlash replica is pending, wait for it", zap.Int64("tableID", tb.ID), zap.Bool("available", a))
			duplicate = true
		}
		pollTiFlashContext.mu.Unlock()
		if !available && !duplicate {
			bo, ok := pollTiFlashContext.BackoffContext.Get(tb.ID)
			if !ok {
				// Small table may be already ready at first check later.
				// so we omit assigning into `backoffs` map for the first time.
			} else {
				if !bo.Tick(&pollTiFlashContext.BackoffContext) {
					// Skip
					logutil.BgLogger().Info("Escape checking available status", zap.Int64("tableId", tb.ID))
					continue
				}
			}

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

			logutil.BgLogger().Info("CollectTiFlashStatus", zap.Any("regionReplica", regionReplica))
			// Get most up-to-date replica count from pd.

			//var stats helper.PDRegionStats
			//if err := tikvHelper.GetPDRegionRecordStats(tb.ID, &stats); err != nil {
			//	return false, errors.Trace(err)
			//}

			regionCount := tb.Count
			flashRegionCount := uint64(len(regionReplica))
			avail := regionCount == flashRegionCount
			failpoint.Inject("PollTiFlashReplicaStatusReplaceCurAvailableValue", func(val failpoint.Value) {
				avail = val.(bool)
			})

			if !avail {
				bo, ok := pollTiFlashContext.BackoffContext.Get(tb.ID)
				if ok {
					logutil.BgLogger().Info("TiFlash replica is not ready, grow interval", zap.Int64("tableId", tb.ID), zap.Uint64("region need", regionCount), zap.Uint64("region ready", flashRegionCount))
					bo.Grow(&pollTiFlashContext.BackoffContext)
				} else {
					// If the table is not available at first check, it should be added into `backoffs`
					newBackoff := NewPollTiFlashBackoffElement()
					if pollTiFlashContext.BackoffContext.Set(tb.ID, &newBackoff) {
						logutil.BgLogger().Info("TiFlash replica is not ready, queuing", zap.Int64("tableId", tb.ID), zap.Uint64("region need", regionCount), zap.Uint64("region ready", flashRegionCount))
					} else {
						logutil.BgLogger().Warn("Too many jobs in backoff queue", zap.Int64("tableId", tb.ID), zap.Uint64("region need", regionCount), zap.Uint64("region ready", flashRegionCount))
					}
				}
				err := infosync.UpdateTiFlashTableSyncProgress(context.Background(), tb.ID, float64(flashRegionCount)/float64(regionCount))
				if err != nil {
					return false, errors.Trace(err)
				}
			} else {
				logutil.BgLogger().Info("Tiflash replica is available", zap.Int64("id", tb.ID), zap.Uint64("region need", regionCount))
				pollTiFlashContext.BackoffContext.Remove(tb.ID)
				err := infosync.DeleteTiFlashTableSyncProgress(tb.ID)
				if err != nil {
					return false, errors.Trace(err)
				}
			}
			// Will call `onUpdateFlashReplicaStatus` to update `TiFlashReplica`.
			//if err := d.UpdateTableReplicaInfo(ctx, tb.ID, avail); err != nil {
			//	logutil.BgLogger().Error("UpdateTableReplicaInfo error when updating TiFlash replica status", zap.Error(err))
			//}
			pollTiFlashContext.mu.Lock()
			oldAvailable, ok := pollTiFlashContext.UpdateMap[tb.ID]
			if !ok || oldAvailable != avail {
				// We witnessed a TiFlash status update.
				logutil.BgLogger().Info("Instantly changing TiFlash status updating", zap.Int64("tableID", tb.ID), zap.Bool("value", avail))
			}
			pollTiFlashContext.UpdateMap[tb.ID] = avail
			pollTiFlashContext.mu.Unlock()
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

// HandlePlacementRuleRoutine fetch all rules from pd, remove all obsolete rules, and add all missing rules.
// It handles rare situation, when we fail to alter pd rules.
func HandlePlacementRuleRoutine(ctx sessionctx.Context, d *ddl, tableList []TiFlashReplicaStatus) error {
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

	start := time.Now()
	// Cover getDropOrTruncateTableTiflash
	if err := getDropOrTruncateTableTiflash(ctx, currentSchema, tikvHelper, &tableList); err != nil {
		// may fail when no `tikv_gc_safe_point` available, should return in order to remove valid pd rules.
		logutil.BgLogger().Error("getDropOrTruncateTableTiflash returns error", zap.Error(err))
		return errors.Trace(err)
	}
	elapsed := time.Since(start)
	logutil.BgLogger().Info("getDropOrTruncateTableTiflash cost", zap.Duration("time", elapsed))
	for _, tb := range tableList {
		// For every region in each table, if it has one replica, we reckon it ready.
		ruleID := fmt.Sprintf("table-%v-r", tb.ID)
		if _, ok := allRules[ruleID]; !ok {
			// Mostly because of a previous failure of setting pd rule.
			logutil.BgLogger().Warn(fmt.Sprintf("Table %v exists, but there are no rule for it", tb.ID))
			newRule := MakeNewRule(tb.ID, tb.Count, tb.LocationLabels)
			err := tikvHelper.SetPlacementRule(*newRule)
			if err != nil {
				logutil.BgLogger().Warn("SetPlacementRule fails")
			}
		}
		// For every existing table, we do not remove their rules.
		delete(allRules, ruleID)
	}

	// Remove rules of non-existing table
	for _, v := range allRules {
		logutil.BgLogger().Info("Remove TiFlash rule", zap.String("id", v.ID))
		if err := tikvHelper.DeletePlacementRule("tiflash", v.ID); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (d *ddl) PollTiFlashRoutine() {
	pollTiflashContext := NewTiFlashManagementContext()
	go func() {
		for {
			pollTiflashContext.mu.Lock()
			var id int64 = -1
			avail := false
			// We can take arbitrary elements from `pollTiflashContext.UpdateMap`
			for id, avail = range pollTiflashContext.UpdateMap {
				break
			}
			pollTiflashContext.mu.Unlock()
			if id != -1 {
				sctx, _ := d.sessPool.get()
				err := d.UpdateTableReplicaInfo(sctx, id, avail)
				if err != nil {
					// This may because some table no longer exists, so we don't retry.
					// This is safe, because if this table is detected to be non-existing, the loop will retry.
					logutil.BgLogger().Warn("Error Handle UpdateTableReplicaInfo", zap.Int64("tid", id), zap.Error(err))
				} else {
					logutil.BgLogger().Info("Finish Handle UpdateTableReplicaInfo", zap.Int64("tid", id))
				}
				d.sessPool.put(sctx)
				pollTiflashContext.mu.Lock()
				delete(pollTiflashContext.UpdateMap, id)
				pollTiflashContext.mu.Unlock()
			} else {
				select {
				case <-d.ctx.Done():
					logutil.BgLogger().Info("Quit consumer")
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}()
	for {
		if d.sessPool == nil {
			logutil.BgLogger().Error("failed to get sessionPool for PollTiFlashReplicaStatus")
			return
		}
		failpoint.Inject("BeforePollTiFlashReplicaStatusLoop", func() {
			failpoint.Continue()
		})
		if d.IsTiFlashPollEnabled() {
			sctx, err := d.sessPool.get()
			if err == nil {
				if d.ownerManager.IsOwner() {
					_, err := d.PollTiFlashReplicaStatus(sctx, pollTiflashContext)
					if err != nil {
						logutil.BgLogger().Warn("PollTiFlashReplicaStatus returns error", zap.Error(err))
					}
				}
				d.sessPool.put(sctx)
			} else {
				if sctx != nil {
					d.sessPool.put(sctx)
				}
				logutil.BgLogger().Error("failed to get session for PollTiFlashReplicaStatus", zap.Error(err))
			}
		}

		select {
		case <-d.ctx.Done():
			return
		case <-time.After(PollTiFlashInterval):
		}
	}
}

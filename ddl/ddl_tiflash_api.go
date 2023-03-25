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
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

// TiFlashReplicaStatus records status for each TiFlash replica.
type TiFlashReplicaStatus struct {
	ID                    int64
	Count                 uint64
	LocationLabels        []string
	Available             bool
	LogicalTableAvailable bool
	HighPriority          bool
	IsPartition           bool
}

// TiFlashTick is type for backoff threshold.
type TiFlashTick float64

// PollTiFlashBackoffElement records backoff for each TiFlash Table.
// `Counter` increases every `Tick`, if it reached `Threshold`, it will be reset to 0 while `Threshold` grows.
// `TotalCounter` records total `Tick`s this element has since created.
type PollTiFlashBackoffElement struct {
	Counter      int
	Threshold    TiFlashTick
	TotalCounter int
}

// NewPollTiFlashBackoffElement initialize backoff element for a TiFlash table.
func NewPollTiFlashBackoffElement() *PollTiFlashBackoffElement {
	return &PollTiFlashBackoffElement{
		Counter:      0,
		Threshold:    PollTiFlashBackoffMinTick,
		TotalCounter: 0,
	}
}

// PollTiFlashBackoffContext is a collection of all backoff states.
type PollTiFlashBackoffContext struct {
	MinThreshold TiFlashTick
	MaxThreshold TiFlashTick
	// Capacity limits tables a backoff pool can handle, in order to limit handling of big tables.
	Capacity int
	Rate     TiFlashTick
	elements map[int64]*PollTiFlashBackoffElement
}

// NewPollTiFlashBackoffContext creates an instance of PollTiFlashBackoffContext.
func NewPollTiFlashBackoffContext(MinThreshold, MaxThreshold TiFlashTick, Capacity int, Rate TiFlashTick) (*PollTiFlashBackoffContext, error) {
	if MaxThreshold < MinThreshold {
		return nil, fmt.Errorf("`MaxThreshold` should always be larger than `MinThreshold`")
	}
	if MinThreshold < 1 {
		return nil, fmt.Errorf("`MinThreshold` should not be less than 1")
	}
	if Capacity < 0 {
		return nil, fmt.Errorf("negative `Capacity`")
	}
	if Rate <= 1 {
		return nil, fmt.Errorf("`Rate` should always be larger than 1")
	}
	return &PollTiFlashBackoffContext{
		MinThreshold: MinThreshold,
		MaxThreshold: MaxThreshold,
		Capacity:     Capacity,
		elements:     make(map[int64]*PollTiFlashBackoffElement),
		Rate:         Rate,
	}, nil
}

// TiFlashManagementContext is the context for TiFlash Replica Management
type TiFlashManagementContext struct {
	TiFlashStores map[int64]helper.StoreStat
	PollCounter   uint64
	Backoff       *PollTiFlashBackoffContext
	// tables waiting for updating progress after become available.
	UpdatingProgressTables *list.List
}

// AvailableTableID is the table id info of available table for waiting to update TiFlash replica progress.
type AvailableTableID struct {
	ID          int64
	IsPartition bool
}

// Tick will first check increase Counter.
// It returns:
// 1. A bool indicates whether threshold is grown during this tick.
// 2. A bool indicates whether this ID exists.
// 3. A int indicates how many ticks ID has counted till now.
func (b *PollTiFlashBackoffContext) Tick(ID int64) (bool, bool, int) {
	e, ok := b.Get(ID)
	if !ok {
		return false, false, 0
	}
	grew := e.MaybeGrow(b)
	e.Counter++
	e.TotalCounter++
	return grew, true, e.TotalCounter
}

// NeedGrow returns if we need to grow.
// It is exported for testing.
func (e *PollTiFlashBackoffElement) NeedGrow() bool {
	return e.Counter >= int(e.Threshold)
}

func (e *PollTiFlashBackoffElement) doGrow(b *PollTiFlashBackoffContext) {
	if e.Threshold < b.MinThreshold {
		e.Threshold = b.MinThreshold
	}
	if e.Threshold*b.Rate > b.MaxThreshold {
		e.Threshold = b.MaxThreshold
	} else {
		e.Threshold *= b.Rate
	}
	e.Counter = 0
}

// MaybeGrow grows threshold and reset counter when needed.
func (e *PollTiFlashBackoffElement) MaybeGrow(b *PollTiFlashBackoffContext) bool {
	if !e.NeedGrow() {
		return false
	}
	e.doGrow(b)
	return true
}

// Remove will reset table from backoff.
func (b *PollTiFlashBackoffContext) Remove(ID int64) bool {
	_, ok := b.elements[ID]
	delete(b.elements, ID)
	return ok
}

// Get returns pointer to inner PollTiFlashBackoffElement.
// Only exported for test.
func (b *PollTiFlashBackoffContext) Get(ID int64) (*PollTiFlashBackoffElement, bool) {
	res, ok := b.elements[ID]
	return res, ok
}

// Put will record table into backoff pool, if there is enough room, or returns false.
func (b *PollTiFlashBackoffContext) Put(ID int64) bool {
	_, ok := b.elements[ID]
	if ok {
		return true
	} else if b.Len() < b.Capacity {
		b.elements[ID] = NewPollTiFlashBackoffElement()
		return true
	}
	return false
}

// Len gets size of PollTiFlashBackoffContext.
func (b *PollTiFlashBackoffContext) Len() int {
	return len(b.elements)
}

// NewTiFlashManagementContext creates an instance for TiFlashManagementContext.
func NewTiFlashManagementContext() (*TiFlashManagementContext, error) {
	c, err := NewPollTiFlashBackoffContext(PollTiFlashBackoffMinTick, PollTiFlashBackoffMaxTick, PollTiFlashBackoffCapacity, PollTiFlashBackoffRate)
	if err != nil {
		return nil, err
	}
	return &TiFlashManagementContext{
		PollCounter:            0,
		TiFlashStores:          make(map[int64]helper.StoreStat),
		Backoff:                c,
		UpdatingProgressTables: list.New(),
	}, nil
}

var (
	// PollTiFlashInterval is the interval between every pollTiFlashReplicaStatus call.
	PollTiFlashInterval = 2 * time.Second
	// PullTiFlashPdTick indicates the number of intervals before we fully sync all TiFlash pd rules and tables.
	PullTiFlashPdTick = atomicutil.NewUint64(30 * 5)
	// UpdateTiFlashStoreTick indicates the number of intervals before we fully update TiFlash stores.
	UpdateTiFlashStoreTick = atomicutil.NewUint64(5)
	// PollTiFlashBackoffMaxTick is the max tick before we try to update TiFlash replica availability for one table.
	PollTiFlashBackoffMaxTick TiFlashTick = 10
	// PollTiFlashBackoffMinTick is the min tick before we try to update TiFlash replica availability for one table.
	PollTiFlashBackoffMinTick TiFlashTick = 1
	// PollTiFlashBackoffCapacity is the cache size of backoff struct.
	PollTiFlashBackoffCapacity int = 1000
	// PollTiFlashBackoffRate is growth rate of exponential backoff threshold.
	PollTiFlashBackoffRate TiFlashTick = 1.5
	// RefreshProgressMaxTableCount is the max count of table to refresh progress after available each poll.
	RefreshProgressMaxTableCount uint64 = 1000
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

// LoadTiFlashReplicaInfo parses model.TableInfo into []TiFlashReplicaStatus.
func LoadTiFlashReplicaInfo(tblInfo *model.TableInfo, tableList *[]TiFlashReplicaStatus) {
	if tblInfo.TiFlashReplica == nil {
		// reject tables that has no tiflash replica such like `INFORMATION_SCHEMA`
		return
	}
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			logutil.BgLogger().Debug(fmt.Sprintf("Table %v has partition %v\n", tblInfo.ID, p.ID))
			*tableList = append(*tableList, TiFlashReplicaStatus{p.ID,
				tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID), tblInfo.TiFlashReplica.Available, false, true})
		}
		// partitions that in adding mid-state
		for _, p := range pi.AddingDefinitions {
			logutil.BgLogger().Debug(fmt.Sprintf("Table %v has partition adding %v\n", tblInfo.ID, p.ID))
			*tableList = append(*tableList, TiFlashReplicaStatus{p.ID, tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.IsPartitionAvailable(p.ID), tblInfo.TiFlashReplica.Available, true, true})
		}
	} else {
		logutil.BgLogger().Debug(fmt.Sprintf("Table %v has no partition\n", tblInfo.ID))
		*tableList = append(*tableList, TiFlashReplicaStatus{tblInfo.ID, tblInfo.TiFlashReplica.Count, tblInfo.TiFlashReplica.LocationLabels, tblInfo.TiFlashReplica.Available, tblInfo.TiFlashReplica.Available, false, false})
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
	// If MockTiFlash is not set, will issue a MockTiFlashError here.
	if err != nil {
		return err
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
	logutil.BgLogger().Debug("updateTiFlashStores finished", zap.Int("TiFlash store count", len(pollTiFlashContext.TiFlashStores)))
	return nil
}

func pollAvailableTableProgress(schemas infoschema.InfoSchema, ctx sessionctx.Context, pollTiFlashContext *TiFlashManagementContext) {
	pollMaxCount := RefreshProgressMaxTableCount
	failpoint.Inject("PollAvailableTableProgressMaxCount", func(val failpoint.Value) {
		pollMaxCount = uint64(val.(int))
	})
	for element := pollTiFlashContext.UpdatingProgressTables.Front(); element != nil && pollMaxCount > 0; pollMaxCount-- {
		availableTableID := element.Value.(AvailableTableID)
		var table table.Table
		if availableTableID.IsPartition {
			table, _, _ = schemas.FindTableByPartitionID(availableTableID.ID)
			if table == nil {
				logutil.BgLogger().Info("get table by partition failed, may be dropped or truncated",
					zap.Int64("partitionID", availableTableID.ID),
				)
				pollTiFlashContext.UpdatingProgressTables.Remove(element)
				element = element.Next()
				continue
			}
		} else {
			var ok bool
			table, ok = schemas.TableByID(availableTableID.ID)
			if !ok {
				logutil.BgLogger().Info("get table id failed, may be dropped or truncated",
					zap.Int64("tableID", availableTableID.ID),
				)
				pollTiFlashContext.UpdatingProgressTables.Remove(element)
				element = element.Next()
				continue
			}
		}

		tableInfo := table.Meta()
		if tableInfo.TiFlashReplica == nil {
			logutil.BgLogger().Info("table has no TiFlash replica",
				zap.Int64("tableID or partitionID", availableTableID.ID),
				zap.Bool("IsPartition", availableTableID.IsPartition),
			)
			pollTiFlashContext.UpdatingProgressTables.Remove(element)
			element = element.Next()
			continue
		}
		progress, err := infosync.CalculateTiFlashProgress(availableTableID.ID, tableInfo.TiFlashReplica.Count, pollTiFlashContext.TiFlashStores)
		if err != nil {
			logutil.BgLogger().Error("get tiflash sync progress failed",
				zap.Error(err),
				zap.Int64("tableID", availableTableID.ID),
				zap.Bool("IsPartition", availableTableID.IsPartition),
			)
			if intest.InTest {
				// In the test, the server cannot start up because the port is occupied.
				// Although the port is random. so we need to quickly return when to
				// fail to get tiflash sync.
				// https://github.com/pingcap/tidb/issues/39949
				panic(err)
			}
			continue
		}
		err = infosync.UpdateTiFlashProgressCache(availableTableID.ID, progress)
		if err != nil {
			logutil.BgLogger().Error("update tiflash sync progress cache failed",
				zap.Error(err),
				zap.Int64("tableID", availableTableID.ID),
				zap.Bool("IsPartition", availableTableID.IsPartition),
				zap.Float64("progress", progress),
			)
			continue
		}
		next := element.Next()
		pollTiFlashContext.UpdatingProgressTables.Remove(element)
		element = next
	}
}

func (d *ddl) refreshTiFlashTicker(ctx sessionctx.Context, pollTiFlashContext *TiFlashManagementContext) error {
	if pollTiFlashContext.PollCounter%UpdateTiFlashStoreTick.Load() == 0 {
		if err := updateTiFlashStores(pollTiFlashContext); err != nil {
			// If we failed to get from pd, retry everytime.
			pollTiFlashContext.PollCounter = 0
			return err
		}
	}

	failpoint.Inject("OneTiFlashStoreDown", func() {
		for storeID, store := range pollTiFlashContext.TiFlashStores {
			store.Store.StateName = "Down"
			pollTiFlashContext.TiFlashStores[storeID] = store
			break
		}
	})
	pollTiFlashContext.PollCounter++

	// Start to process every table.
	schema := d.GetInfoSchemaWithInterceptor(ctx)
	if schema == nil {
		return errors.New("Schema is nil")
	}

	pollAvailableTableProgress(schema, ctx, pollTiFlashContext)

	var tableList = make([]TiFlashReplicaStatus, 0)

	// Collect TiFlash Replica info, for every table.
	for _, db := range schema.AllSchemas() {
		tbls := schema.SchemaTables(db.Name)
		for _, tbl := range tbls {
			tblInfo := tbl.Meta()
			LoadTiFlashReplicaInfo(tblInfo, &tableList)
		}
	}

	failpoint.Inject("waitForAddPartition", func(val failpoint.Value) {
		for _, phyTable := range tableList {
			is := d.infoCache.GetLatest()
			_, ok := is.TableByID(phyTable.ID)
			if !ok {
				tb, _, _ := is.FindTableByPartitionID(phyTable.ID)
				if tb == nil {
					logutil.BgLogger().Info("[ddl] waitForAddPartition")
					sleepSecond := val.(int)
					time.Sleep(time.Duration(sleepSecond) * time.Second)
				}
			}
		}
	})

	needPushPending := false
	if pollTiFlashContext.UpdatingProgressTables.Len() == 0 {
		needPushPending = true
	}

	for _, tb := range tableList {
		// For every region in each table, if it has one replica, we reckon it ready.
		// These request can be batched as an optimization.
		available := tb.Available
		failpoint.Inject("PollTiFlashReplicaStatusReplacePrevAvailableValue", func(val failpoint.Value) {
			available = val.(bool)
		})
		// We only check unavailable tables here, so doesn't include blocked add partition case.
		if !available && !tb.LogicalTableAvailable {
			enabled, inqueue, _ := pollTiFlashContext.Backoff.Tick(tb.ID)
			if inqueue && !enabled {
				logutil.BgLogger().Info("Escape checking available status due to backoff", zap.Int64("tableId", tb.ID))
				continue
			}

			progress, err := infosync.CalculateTiFlashProgress(tb.ID, tb.Count, pollTiFlashContext.TiFlashStores)
			if err != nil {
				logutil.BgLogger().Error("get tiflash sync progress failed",
					zap.Error(err),
					zap.Int64("tableID", tb.ID),
				)
				continue
			}

			err = infosync.UpdateTiFlashProgressCache(tb.ID, progress)
			if err != nil {
				logutil.BgLogger().Error("get tiflash sync progress from cache failed",
					zap.Error(err),
					zap.Int64("tableID", tb.ID),
					zap.Bool("IsPartition", tb.IsPartition),
					zap.Float64("progress", progress),
				)
				continue
			}

			avail := progress == 1
			failpoint.Inject("PollTiFlashReplicaStatusReplaceCurAvailableValue", func(val failpoint.Value) {
				avail = val.(bool)
			})

			if !avail {
				logutil.BgLogger().Info("Tiflash replica is not available", zap.Int64("tableID", tb.ID), zap.Float64("progress", progress))
				pollTiFlashContext.Backoff.Put(tb.ID)
			} else {
				logutil.BgLogger().Info("Tiflash replica is available", zap.Int64("tableID", tb.ID), zap.Float64("progress", progress))
				pollTiFlashContext.Backoff.Remove(tb.ID)
			}
			failpoint.Inject("skipUpdateTableReplicaInfoInLoop", func() {
				failpoint.Continue()
			})
			// Will call `onUpdateFlashReplicaStatus` to update `TiFlashReplica`.
			if err := d.UpdateTableReplicaInfo(ctx, tb.ID, avail); err != nil {
				if infoschema.ErrTableNotExists.Equal(err) && tb.IsPartition {
					// May be due to blocking add partition
					logutil.BgLogger().Info("updating TiFlash replica status err, maybe false alarm by blocking add", zap.Error(err), zap.Int64("tableID", tb.ID), zap.Bool("isPartition", tb.IsPartition))
				} else {
					logutil.BgLogger().Error("updating TiFlash replica status err", zap.Error(err), zap.Int64("tableID", tb.ID), zap.Bool("isPartition", tb.IsPartition))
				}
			}
		} else {
			if needPushPending {
				pollTiFlashContext.UpdatingProgressTables.PushFront(AvailableTableID{tb.ID, tb.IsPartition})
			}
		}
	}

	return nil
}

func (d *ddl) PollTiFlashRoutine() {
	pollTiflashContext, err := NewTiFlashManagementContext()
	if err != nil {
		logutil.BgLogger().Fatal("TiFlashManagement init failed", zap.Error(err))
	}

	hasSetTiFlashGroup := false
	nextSetTiFlashGroupTime := time.Now()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-time.After(PollTiFlashInterval):
		}
		if d.IsTiFlashPollEnabled() {
			if d.sessPool == nil {
				logutil.BgLogger().Error("failed to get sessionPool for refreshTiFlashTicker")
				return
			}
			failpoint.Inject("BeforeRefreshTiFlashTickeLoop", func() {
				failpoint.Continue()
			})

			if !hasSetTiFlashGroup && !time.Now().Before(nextSetTiFlashGroupTime) {
				// We should set tiflash rule group a higher index than other placement groups to forbid override by them.
				// Once `SetTiFlashGroupConfig` succeed, we do not need to invoke it again. If failed, we should retry it util success.
				if err = infosync.SetTiFlashGroupConfig(d.ctx); err != nil {
					logutil.BgLogger().Warn("SetTiFlashGroupConfig failed", zap.Error(err))
					nextSetTiFlashGroupTime = time.Now().Add(time.Minute)
				} else {
					hasSetTiFlashGroup = true
				}
			}

			sctx, err := d.sessPool.get()
			if err == nil {
				if d.ownerManager.IsOwner() {
					err := d.refreshTiFlashTicker(sctx, pollTiflashContext)
					if err != nil {
						switch err.(type) {
						case *infosync.MockTiFlashError:
							// If we have not set up MockTiFlash instance, for those tests without TiFlash, just suppress.
						default:
							logutil.BgLogger().Warn("refreshTiFlashTicker returns error", zap.Error(err))
						}
					}
				} else {
					infosync.CleanTiFlashProgressCache()
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

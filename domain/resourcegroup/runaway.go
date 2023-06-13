// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package resourcegroup

import (
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
	"github.com/ngaut/pools"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/util/dbterror/exeerrors"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

const (
	maxWatchListCap           = 10000
	maxWatchRecordChannelSize = 1024
)

type RunawayWatchRecord struct {
	ResourceGroupName string
	StartTime         time.Time
	EndTime           time.Time
	Type              string
	SqlText           string
	PlanDigest        string
	From              uint64
}

type sessionPool interface {
	Get() (pools.Resource, error)
	Put(pools.Resource)
}

type RunawayManager struct {
	resourceGroupCtl *rmclient.ResourceGroupsController
	watchList        *ttlcache.Cache[string, struct{}]
	serverID         uint64

	pool sessionPool

	watchRecordCache chan *RunawayWatchRecord
	flush            chan struct{}
}

func NewRunawayManager(resourceGroupCtl *rmclient.ResourceGroupsController, serverID uint64, pool sessionPool) *RunawayManager {
	watchList := ttlcache.New[string, struct{}](ttlcache.WithCapacity[string, struct{}](maxWatchListCap))
	go watchList.Start()
	return &RunawayManager{
		resourceGroupCtl: resourceGroupCtl,
		watchList:        watchList,
		watchRecordCache: make(chan *RunawayWatchRecord, maxWatchRecordChannelSize),
		serverID:         serverID,
		flush:            make(chan struct{}),
		pool:             pool,
	}
}

func (rm *RunawayManager) DeriveChecker(resourceGroupName string, originalSql string, planDigest string) *RunawayChecker {
	meta, err := rm.resourceGroupCtl.GetResourceGroup(resourceGroupName)
	if err != nil || meta == nil || meta.RunawaySettings == nil {
		return nil
	}

	return newRunawayChecker(rm, resourceGroupName, meta.RunawaySettings, originalSql, planDigest)
}

func (rm *RunawayManager) MarkRunaway(resourceGroupName, convict, originalSql, planDigest, watchType string, ttl time.Duration) {
	rm.watchList.Set(resourceGroupName+"/"+convict, struct{}{}, ttl)
	now := time.Now()
	if len(rm.watchRecordCache) >= maxWatchRecordChannelSize/2 {
		select {
		case rm.flush <- struct{}{}:
		default:
		}
	}
	go func() {
		rm.watchRecordCache <- &RunawayWatchRecord{
			ResourceGroupName: resourceGroupName,
			StartTime:         now,
			EndTime:           now.Add(ttl),
			Type:              watchType,
			SqlText:           originalSql,
			PlanDigest:        planDigest,
			From:              rm.serverID,
		}
	}()
}

func (rm *RunawayManager) FlushWatchRecords() (ret []*RunawayWatchRecord) {
	ret = make([]*RunawayWatchRecord, 0)
	for {
		if len(ret) > maxWatchRecordChannelSize {
			break
		}
		select {
		case record := <-rm.watchRecordCache:
			ret = append(ret, record)
		default:
			return ret
		}
	}
	return ret
}

func (rm *RunawayManager) FlushChannel() <-chan struct{} {
	return rm.flush
}

func (rm *RunawayManager) ExamineWatchList(resourceGroupName string, convict string) bool {
	return rm.watchList.Get(resourceGroupName+"/"+convict) != nil
}

func (rm *RunawayManager) Stop() {
	rm.watchList.Stop()
}

type RunawayChecker struct {
	manager           *RunawayManager
	resourceGroupName string
	originalSql       string
	planDigest        string
	setting           *rmpb.RunawaySettings
	deadline          time.Time
	marked            atomic.Bool
}

func newRunawayChecker(manager *RunawayManager, resourceGroupName string, setting *rmpb.RunawaySettings, originalSql string, planDigest string) *RunawayChecker {
	return &RunawayChecker{
		manager:           manager,
		resourceGroupName: resourceGroupName,
		originalSql:       originalSql,
		planDigest:        planDigest,
		deadline:          time.Now().Add(time.Duration(setting.Rule.ExecElapsedTimeMs) * time.Millisecond),
		setting:           setting,
		marked:            atomic.Bool{},
	}
}

// BeforeExecutor checks whether query is in watch list before executing and after compiling.
func (r *RunawayChecker) BeforeExecutor() error {
	if r == nil {
		return nil
	}
	result := r.manager.ExamineWatchList(r.resourceGroupName, r.getConvictName())
	if result {
		r.marked.Store(result)
		switch r.setting.Action {
		case rmpb.RunawayAction_Kill:
			return exeerrors.ErrResourceGroupQueryRunawayQuarantine
		case rmpb.RunawayAction_CoolDown:
			return nil
		case rmpb.RunawayAction_DryRun:
			return nil
		default:
		}
	}
	return nil
}

// BeforeCopRequest checks runaway and modifies the request if necessary before sending coprocessor request.
func (r *RunawayChecker) BeforeCopRequest(req *tikvrpc.Request) error {
	if r == nil {
		return nil
	}
	marked := r.marked.Load()
	if !marked {
		until := time.Until(r.deadline)
		// execution time exceeds the threshold, mark the query as runaway
		if until <= 0 {
			r.marked.Store(true)
			r.markRunaway()
		} else {
			if r.setting.Action == rmpb.RunawayAction_Kill {
				// if the execution time is close to the threshold, set a timeout
				if until < tikv.ReadTimeoutMedium {
					req.Context.MaxExecutionDurationMs = uint64(until.Milliseconds())
				}
			}
			return nil
		}
	}
	switch r.setting.Action {
	case rmpb.RunawayAction_Kill:
		return exeerrors.ErrResourceGroupQueryRunaway
	case rmpb.RunawayAction_CoolDown:
		req.ResourceControlContext.OverridePriority = 1 // set priority to lowest
		return nil
	case rmpb.RunawayAction_DryRun:
		return nil
	default:
		return nil
	}
}

func (r *RunawayChecker) AfterCopRequest() {
	if r == nil {
		return
	}
	// Do not perform action here as it may be the last cop request and just let it finish. If it's not the last cop request, action would be performed in `BeforeCopRequest` when handling the next cop request.
	// Here only marks the query as runaway
	if !r.marked.Load() && r.deadline.Before(time.Now()) {
		r.marked.Store(true)
		r.markRunaway()
	}
}

func (r *RunawayChecker) markRunaway() {
	if r.setting.Watch == nil {
		return
	}
	r.manager.MarkRunaway(r.resourceGroupName, r.getConvictName(), r.originalSql, r.planDigest, r.setting.Watch.Type.String(), time.Duration(r.setting.Watch.LastingDurationMs)*time.Millisecond)
}

func (r *RunawayChecker) getConvictName() string {
	if r.setting.Watch == nil {
		return ""
	}
	switch r.setting.Watch.Type {
	case rmpb.RunawayWatchType_Similar:
		return r.planDigest
	case rmpb.RunawayWatchType_Exact:
		return r.originalSql
	default:
		return ""
	}
}

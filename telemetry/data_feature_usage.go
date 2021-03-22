// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package telemetry

import (
	"context"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/atomic"
)

type featureUsageInfo struct {
	AsyncCommitUsed  bool                       `json:"asyncCommitUsed"`
	CoprCacheUsed    []*CoprCacheUsedWindowItem `json:"coprCacheUsed"`
	ClusterIndexUsed map[string]bool            `json:"clusterIndexUsed"`
	TiFlashUsed      []*TiFlashUsageItem        `json:"tiFlashUsed"`
}

// CoprocessorCacheTelemetry is to save the global coprocessor cache telemetry data.
var CoprocessorCacheTelemetry = struct {
	MinuteWindow []CoprCacheUsedWindowItem
	Lock         sync.RWMutex
}{Lock: sync.RWMutex{}}

// CoprCacheUsedWindowItem is the coprocessor cache telemetry data struct.
type CoprCacheUsedWindowItem struct {
	P0   atomic.Uint64 `json:"gte0"`
	P1   atomic.Uint64 `json:"gte1"`
	P10  atomic.Uint64 `json:"gte10"`
	P20  atomic.Uint64 `json:"gte20"`
	P40  atomic.Uint64 `json:"gte40"`
	P80  atomic.Uint64 `json:"gte80"`
	P100 atomic.Uint64 `json:"gte100"`

	BeginAt *time.Time `json:"beginAt"`
}

// TiFlashUsageTelemetry is to save the global TiFlash usage telemetry data.
var TiFlashUsageTelemetry = struct {
	MinuteWindow []TiFlashUsageItem
	Lock         sync.RWMutex
}{Lock: sync.RWMutex{}}

// TiFlashUsageItem is the TiFlash usage telemetry data struct.
type TiFlashUsageItem struct {
	TotalNumbers                   atomic.Uint64 `json:"totalNumbers"`
	TiFlashPushDownNumbers         atomic.Uint64 `json:"tiFlashPushDownNumbers"`
	TiFlashExchangePushDownNumbers atomic.Uint64 `json:"tiFlashExchangePushDownNumbers"`

	BeginAt *time.Time `json:"beginAt"`
}

func getTelemetryFeatureUsageInfo(ctx sessionctx.Context) (*featureUsageInfo, error) {
	// init
	usageInfo := featureUsageInfo{
		CoprCacheUsed:    make([]*CoprCacheUsedWindowItem, 0, UpdateInterval/time.Hour),
		ClusterIndexUsed: make(map[string]bool),
		TiFlashUsed:      make([]*TiFlashUsageItem, 0, UpdateInterval/time.Hour),
	}

	// coprocessor cache
	CoprocessorCacheTelemetry.Lock.Lock()
	maxLen := 0
	for _, window := range CoprocessorCacheTelemetry.MinuteWindow {
		timeSince := time.Since(*window.BeginAt)
		if timeSince >= UpdateInterval {
			continue
		}
		maxLen = mathutil.Max(maxLen, int(timeSince/time.Hour))
		i := maxLen - int(timeSince/time.Hour)
		if len(usageInfo.CoprCacheUsed) <= i {
			usageInfo.CoprCacheUsed = append(usageInfo.CoprCacheUsed, &CoprCacheUsedWindowItem{})
		}
		usageInfo.CoprCacheUsed[i].P0.Add(window.P0.Load())
		usageInfo.CoprCacheUsed[i].P1.Add(window.P1.Load())
		usageInfo.CoprCacheUsed[i].P10.Add(window.P10.Load())
		usageInfo.CoprCacheUsed[i].P20.Add(window.P20.Load())
		usageInfo.CoprCacheUsed[i].P40.Add(window.P40.Load())
		usageInfo.CoprCacheUsed[i].P80.Add(window.P80.Load())
		usageInfo.CoprCacheUsed[i].P100.Add(window.P100.Load())
		if usageInfo.CoprCacheUsed[i].BeginAt == nil {
			usageInfo.CoprCacheUsed[i].BeginAt = window.BeginAt
		}
	}
	CoprocessorCacheTelemetry.Lock.Unlock()

	// TiFlash usage
	TiFlashUsageTelemetry.Lock.Lock()
	maxLen = 0
	for _, window := range TiFlashUsageTelemetry.MinuteWindow {
		timeSince := time.Since(*window.BeginAt)
		if timeSince >= UpdateInterval {
			continue
		}
		maxLen = mathutil.Max(maxLen, int(timeSince/time.Hour))
		i := maxLen - int(timeSince/time.Hour)
		if len(usageInfo.TiFlashUsed) <= i {
			usageInfo.TiFlashUsed = append(usageInfo.TiFlashUsed, &TiFlashUsageItem{})
		}
		usageInfo.TiFlashUsed[i].TotalNumbers.Add(window.TotalNumbers.Load())
		usageInfo.TiFlashUsed[i].TiFlashPushDownNumbers.Add(window.TiFlashPushDownNumbers.Load())
		usageInfo.TiFlashUsed[i].TiFlashExchangePushDownNumbers.Add(window.TiFlashExchangePushDownNumbers.Load())
		if usageInfo.TiFlashUsed[i].BeginAt == nil {
			usageInfo.TiFlashUsed[i].BeginAt = window.BeginAt
		}
	}
	TiFlashUsageTelemetry.Lock.Unlock()

	// cluster index
	exec := ctx.(sqlexec.RestrictedSQLExecutor)
	stmt, err := exec.ParseWithParams(context.TODO(), `
		SELECT sha2(TABLE_NAME, 256) name, TIDB_PK_TYPE
		FROM information_schema.tables
		WHERE table_schema not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql')
		limit 10000`)
	if err != nil {
		return nil, err
	}
	rows, _, err := exec.ExecRestrictedStmt(context.TODO(), stmt)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if row.Len() < 2 {
			continue
		}
		isClustered := false
		if row.GetString(1) == "CLUSTERED" {
			isClustered = true
		}
		usageInfo.ClusterIndexUsed[row.GetString(0)] = isClustered
	}

	// async commit
	stmt, err = exec.ParseWithParams(context.TODO(), `show config where name = 'storage.enable-async-apply-prewrite'`)
	if err != nil {
		return nil, err
	}
	rows, _, err = exec.ExecRestrictedStmt(context.TODO(), stmt)
	if err != nil {
		return nil, err
	}
	if len(rows) > 0 {
		if rows[0].GetString(3) == "true" {
			usageInfo.AsyncCommitUsed = true
		}
	}

	return &usageInfo, nil
}

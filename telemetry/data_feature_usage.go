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

// FeatureTaskChan is the update task channel for telemetry feature data.
var FeatureTaskChan = make(chan *FeatureTask, 1<<16)

// FeatureTask is the update task for telemetry feature data.
type FeatureTask struct {
	TiFlashPushDown         bool
	TiFLashExchangePushDown bool
	CoprRespTimes           uint64
	CoprCacheHitNum         uint64
}

// UpdateFeature update feature data for one task.
func UpdateFeature(task *FeatureTask) {
	CoprocessorCacheTelemetry.Lock.Lock()
	length := len(CoprocessorCacheTelemetry.MinuteWindow)
	if length == 0 || time.Since(*CoprocessorCacheTelemetry.MinuteWindow[length-1].BeginAt) >= time.Minute {
		var i int
		for i = 0; i < length && time.Since(*CoprocessorCacheTelemetry.MinuteWindow[i].BeginAt) >= ReportInterval; i++ {
		}
		CoprocessorCacheTelemetry.MinuteWindow = CoprocessorCacheTelemetry.MinuteWindow[i:]
		CoprocessorCacheTelemetry.MinuteWindow = append(CoprocessorCacheTelemetry.MinuteWindow, CoprCacheUsedWindowItem{})
		length -= i - 1
		CoprocessorCacheTelemetry.MinuteWindow[length-1].BeginAt = new(time.Time)
		*CoprocessorCacheTelemetry.MinuteWindow[length-1].BeginAt = time.Now()
	}
	if task.CoprRespTimes > 0 {
		ratio := float64(task.CoprCacheHitNum) / float64(task.CoprRespTimes)
		switch {
		case ratio >= 0:
			CoprocessorCacheTelemetry.MinuteWindow[length-1].P0.Add(1)
			fallthrough
		case ratio >= 0.01:
			CoprocessorCacheTelemetry.MinuteWindow[length-1].P1.Add(1)
			fallthrough
		case ratio >= 0.1:
			CoprocessorCacheTelemetry.MinuteWindow[length-1].P10.Add(1)
			fallthrough
		case ratio >= 0.2:
			CoprocessorCacheTelemetry.MinuteWindow[length-1].P20.Add(1)
			fallthrough
		case ratio >= 0.4:
			CoprocessorCacheTelemetry.MinuteWindow[length-1].P40.Add(1)
			fallthrough
		case ratio >= 0.8:
			CoprocessorCacheTelemetry.MinuteWindow[length-1].P80.Add(1)
			fallthrough
		case ratio >= 1:
			CoprocessorCacheTelemetry.MinuteWindow[length-1].P100.Add(1)
		}
	} else {
		CoprocessorCacheTelemetry.MinuteWindow[length-1].P0.Add(1)
	}
	CoprocessorCacheTelemetry.Lock.Unlock()

	TiFlashUsageTelemetry.Lock.Lock()
	length = len(TiFlashUsageTelemetry.MinuteWindow)
	if length == 0 || time.Since(*TiFlashUsageTelemetry.MinuteWindow[length-1].BeginAt) >= time.Minute {
		var i int
		for i = 0; i < length && time.Since(*TiFlashUsageTelemetry.MinuteWindow[i].BeginAt) >= ReportInterval; i++ {
		}
		TiFlashUsageTelemetry.MinuteWindow = TiFlashUsageTelemetry.MinuteWindow[i:]
		TiFlashUsageTelemetry.MinuteWindow = append(TiFlashUsageTelemetry.MinuteWindow, TiFlashUsageItem{})
		length -= i - 1
		TiFlashUsageTelemetry.MinuteWindow[length-1].BeginAt = new(time.Time)
		*TiFlashUsageTelemetry.MinuteWindow[length-1].BeginAt = time.Now()
	}
	TiFlashUsageTelemetry.MinuteWindow[length-1].TotalNumbers.Add(1)
	if task.TiFlashPushDown {
		TiFlashUsageTelemetry.MinuteWindow[length-1].TiFlashPushDownNumbers.Add(1)
	}
	if task.TiFLashExchangePushDown {
		TiFlashUsageTelemetry.MinuteWindow[length-1].TiFlashExchangePushDownNumbers.Add(1)
	}
	TiFlashUsageTelemetry.Lock.Unlock()
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
		CoprCacheUsed:    make([]*CoprCacheUsedWindowItem, 0, ReportInterval/time.Hour),
		ClusterIndexUsed: make(map[string]bool),
		TiFlashUsed:      make([]*TiFlashUsageItem, 0, ReportInterval/time.Hour),
	}

	// coprocessor cache
	CoprocessorCacheTelemetry.Lock.Lock()
	maxLen := 0
	for _, window := range CoprocessorCacheTelemetry.MinuteWindow {
		timeSince := time.Since(*window.BeginAt)
		if timeSince >= ReportInterval {
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
		if timeSince >= ReportInterval {
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
		SELECT left(sha2(TABLE_NAME, 256), 6) name, TIDB_PK_TYPE
		FROM information_schema.tables
		WHERE table_schema not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql')
		ORDER BY name
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

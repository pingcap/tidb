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

package ttlworker

import (
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

const jobManagerLoopTickerInterval = 10 * time.Second

const updateInfoSchemaCacheInterval = 2 * time.Minute
const updateTTLTableStatusCacheInterval = 2 * time.Minute

const ttlInternalSQLTimeout = 30 * time.Second
const resizeWorkersInterval = 30 * time.Second
const splitScanCount = 64
const ttlJobTimeout = 6 * time.Hour

const taskManagerLoopTickerInterval = time.Minute
const ttlTaskHeartBeatTickerInterval = time.Minute
const ttlGCInterval = 10 * time.Minute

func getCheckJobInterval() time.Duration {
	failpoint.Inject("check-job-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return jobManagerLoopTickerInterval
}

func getHeartbeatInterval() time.Duration {
	failpoint.Inject("heartbeat-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return jobManagerLoopTickerInterval
}

func getJobManagerLoopSyncTimerInterval() time.Duration {
	failpoint.Inject("sync-timer", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return time.Second
}

func getUpdateInfoSchemaCacheInterval() time.Duration {
	failpoint.Inject("update-info-schema-cache-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return updateInfoSchemaCacheInterval
}

func getUpdateTTLTableStatusCacheInterval() time.Duration {
	failpoint.Inject("update-status-table-cache-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return updateTTLTableStatusCacheInterval
}

func getResizeWorkersInterval() time.Duration {
	failpoint.Inject("resize-workers-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return resizeWorkersInterval
}

func getTaskManagerLoopCheckTaskInterval() time.Duration {
	failpoint.Inject("check-task-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return time.Second * 5
}

func getTaskManagerLoopTickerInterval() time.Duration {
	failpoint.Inject("task-manager-loop-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return taskManagerLoopTickerInterval
}

func getTaskManagerHeartBeatInterval() time.Duration {
	failpoint.Inject("task-manager-heartbeat-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return ttlTaskHeartBeatTickerInterval
}

func getCheckJobTriggeredInterval() time.Duration {
	failpoint.Inject("check-job-triggered-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return 2 * time.Second
}

func getTTLGCInterval() time.Duration {
	failpoint.Inject("gc-interval", func(val failpoint.Value) time.Duration {
		return time.Duration(val.(int))
	})
	return ttlGCInterval
}

func getScanSplitCnt(store kv.Storage) int {
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return splitScanCount
	}

	if cache := tikvStore.GetRegionCache(); cache != nil {
		if tikvCnt := len(cache.GetStoresByType(tikvrpc.TiKV)); tikvCnt > splitScanCount {
			return tikvCnt
		}
	}

	return splitScanCount
}

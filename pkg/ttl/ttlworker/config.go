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
	if val, _err_ := failpoint.Eval(_curpkg_("check-job-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return jobManagerLoopTickerInterval
}

func getHeartbeatInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("heartbeat-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return jobManagerLoopTickerInterval
}

func getJobManagerLoopSyncTimerInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("sync-timer")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return time.Second
}

func getUpdateInfoSchemaCacheInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("update-info-schema-cache-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return updateInfoSchemaCacheInterval
}

func getUpdateTTLTableStatusCacheInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("update-status-table-cache-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return updateTTLTableStatusCacheInterval
}

func getResizeWorkersInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("resize-workers-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return resizeWorkersInterval
}

func getTaskManagerLoopCheckTaskInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("check-task-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return time.Second * 5
}

func getTaskManagerLoopTickerInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("task-manager-loop-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return taskManagerLoopTickerInterval
}

func getTaskManagerHeartBeatInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("task-manager-heartbeat-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return ttlTaskHeartBeatTickerInterval
}

func getCheckJobTriggeredInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("check-job-triggered-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
	return 2 * time.Second
}

func getTTLGCInterval() time.Duration {
	if val, _err_ := failpoint.Eval(_curpkg_("gc-interval")); _err_ == nil {
		return time.Duration(val.(int))
	}
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

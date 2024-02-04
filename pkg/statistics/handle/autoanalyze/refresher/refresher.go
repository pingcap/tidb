// Copyright 2024 PingCAP, Inc.
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

package refresher

import (
	"container/heap"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/priorityqueue"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"go.uber.org/zap"
)

// Refresher provides methods to refresh stats info.
type Refresher struct {
	statsHandle    statstypes.StatsHandle
	sysProcTracker sessionctx.SysProcTracker

	jobs *priorityqueue.AnalysisQueue
}

// NewRefresher creates a new Refresher and starts the goroutine.
func NewRefresher(
	statsHandle statstypes.StatsHandle,
	sysProcTracker sessionctx.SysProcTracker,
) (*Refresher, error) {
	r := &Refresher{
		statsHandle:    statsHandle,
		sysProcTracker: sysProcTracker,
	}

	return r, nil
}

func (r *Refresher) pickOneTableAndAnalyzeByPriority() {
	se, err := r.statsHandle.SPool().Get()
	if err != nil {
		statslogutil.StatsLogger().Error(
			"Get session context failed",
			zap.Error(err),
		)
		return
	}
	defer r.statsHandle.SPool().Put(se)
	sctx := se.(sessionctx.Context)
	// Pick the table with the highest weight.
	for r.jobs.Len() > 0 {
		job := heap.Pop(r.jobs).(*priorityqueue.TableAnalysisJob)
		if valid, failReason := job.IsValidToAnalyze(
			sctx,
		); !valid {
			statslogutil.StatsLogger().Info(
				"Table is not ready to analyze",
				zap.String("failReason", failReason),
				zap.Any("job", job),
			)
			continue
		}
		statslogutil.StatsLogger().Info(
			"Auto analyze triggered",
			zap.Any("job", job),
		)
		err = job.Execute(
			r.statsHandle,
			r.sysProcTracker,
		)
		if err != nil {
			statslogutil.StatsLogger().Error(
				"Execute auto analyze job failed",
				zap.Any("job", job),
				zap.Error(err),
			)
		}
		// Only analyze one table each time.
		return
	}
}

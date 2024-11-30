// Copyright 2024 PingCAP, Inc.
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

package context

import (
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/deeptest"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ppcpuusage"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	tikvstore "github.com/tikv/client-go/v2/kv"
)

func TestContextDetach(t *testing.T) {
	sqlkiller := &sqlkiller.SQLKiller{Signal: 1}
	sqlCPUUsages := &ppcpuusage.SQLCPUUsages{}
	kvVars := &tikvstore.Variables{
		BackoffLockFast: 1,
		BackOffWeight:   2,
		Killed:          &sqlkiller.Signal,
	}
	warnHandler := contextutil.NewStaticWarnHandler(5)

	obj := &DistSQLContext{
		WarnHandler:            warnHandler,
		InRestrictedSQL:        true,
		EnabledRateLimitAction: true,
		EnableChunkRPC:         true,
		OriginalSQL:            "a",
		KVVars:                 kvVars,
		KvExecCounter:          &stmtstats.KvExecCounter{},
		SessionMemTracker:      &memory.Tracker{},

		Location:         time.Local,
		RuntimeStatsColl: &execdetails.RuntimeStatsColl{},
		SQLKiller:        sqlkiller,
		CPUUsage:         sqlCPUUsages,
		ErrCtx:           errctx.NewContextWithLevels(errctx.LevelMap{errctx.LevelWarn}, warnHandler),

		// TiFlash related configurations
		TiFlashReplicaRead:                   tiflash.ClosestAdaptive,
		TiFlashMaxThreads:                    1,
		TiFlashMaxBytesBeforeExternalJoin:    1,
		TiFlashMaxBytesBeforeExternalGroupBy: 1,
		TiFlashMaxBytesBeforeExternalSort:    1,
		TiFlashMaxQueryMemoryPerNode:         1,
		TiFlashQuerySpillRatio:               1.0,

		DistSQLConcurrency:            1,
		ReplicaReadType:               kv.ReplicaReadFollower,
		WeakConsistency:               true,
		RCCheckTS:                     true,
		NotFillCache:                  true,
		TaskID:                        1,
		Priority:                      mysql.HighPriority,
		EnablePaging:                  true,
		MinPagingSize:                 1,
		MaxPagingSize:                 1,
		RequestSourceType:             "a",
		ExplicitRequestSourceType:     "b",
		StoreBatchSize:                1,
		ResourceGroupName:             "c",
		LoadBasedReplicaReadThreshold: time.Second,
		TiKVClientReadTimeout:         1,
		MaxExecutionTime:              1,

		ReplicaClosestReadThreshold: 1,
		ConnectionID:                1,
		SessionAlias:                "c",
	}

	obj.AppendWarning(errors.New("test warning"))
	deeptest.AssertRecursivelyNotEqual(t, obj, &DistSQLContext{},
		deeptest.WithIgnorePath([]string{
			"$.Client",
			"$.ErrCtx.levelMap*",
			"$.WarnHandler",
			// The following fields are on session but safe to be used concurrently
			"$.ResourceGroupTagger",
			// The following fields are on stmtctx and will be recreated before the new statement
			"$.RunawayChecker",
			"$.ExecDetails",
		}))

	staticObj := obj.Detach()

	deeptest.AssertDeepClonedEqual(t, obj, staticObj,
		deeptest.WithIgnorePath([]string{
			"$.ErrCtx.warnHandler",
		}),
		deeptest.WithPointerComparePath([]string{
			"$.SQLKiller",
			// The following fields are on stmtctx and will be recreated before the new statement,
			// so keep the same pointer is fine
			"$.RunawayChecker",
			"$.ExecDetails",
			"$.KVVars.Killed",
			"$.KvExecCounter",
			"$.SessionMemTracker",
			"$.Location",
			"$.RuntimeStatsColl",
			"$.WarnHandler",
			"$.ResourceGroupTagger",
		}))
}

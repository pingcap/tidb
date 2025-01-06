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
	"time"

	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/ppcpuusage"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"go.uber.org/atomic"
)

// DistSQLContext provides all information needed by using functions in `distsql`
type DistSQLContext struct {
	WarnHandler contextutil.WarnAppender

	InRestrictedSQL bool
	Client          kv.Client

	EnabledRateLimitAction bool
	EnableChunkRPC         bool
	OriginalSQL            string
	KVVars                 *tikvstore.Variables
	KvExecCounter          *stmtstats.KvExecCounter
	SessionMemTracker      *memory.Tracker

	Location         *time.Location
	RuntimeStatsColl *execdetails.RuntimeStatsColl
	SQLKiller        *sqlkiller.SQLKiller
	CPUUsage         *ppcpuusage.SQLCPUUsages
	ErrCtx           errctx.Context

	// TiFlash related configurations
	TiFlashReplicaRead                   tiflash.ReplicaRead
	TiFlashMaxThreads                    int64
	TiFlashMaxBytesBeforeExternalJoin    int64
	TiFlashMaxBytesBeforeExternalGroupBy int64
	TiFlashMaxBytesBeforeExternalSort    int64
	TiFlashMaxQueryMemoryPerNode         int64
	TiFlashQuerySpillRatio               float64

	DistSQLConcurrency            int
	ReplicaReadType               kv.ReplicaReadType
	WeakConsistency               bool
	RCCheckTS                     bool
	NotFillCache                  bool
	TaskID                        uint64
	Priority                      mysql.PriorityEnum
	ResourceGroupTagger           *kv.ResourceGroupTagBuilder
	EnablePaging                  bool
	MinPagingSize                 int
	MaxPagingSize                 int
	RequestSourceType             string
	ExplicitRequestSourceType     string
	StoreBatchSize                int
	ResourceGroupName             string
	LoadBasedReplicaReadThreshold time.Duration
	RunawayChecker                resourcegroup.RunawayChecker
	TiKVClientReadTimeout         uint64
	MaxExecutionTime              uint64

	ReplicaClosestReadThreshold int64
	ConnectionID                uint64
	SessionAlias                string

	ExecDetails *execdetails.SyncExecDetails

	// Only one cop-reader can use lite worker at the same time. Using lite-worker in multiple readers will affect the concurrent execution of readers.
	TryCopLiteWorker atomic.Uint32
}

// AppendWarning appends the warning to the warning handler.
func (dctx *DistSQLContext) AppendWarning(warn error) {
	dctx.WarnHandler.AppendWarning(warn)
}

// Detach detaches this context from the session context.
//
// NOTE: Though this session context can be used parallelly with this context after calling
// it, the `StatementContext` cannot. The session context should create a new `StatementContext`
// before executing another statement.
func (dctx *DistSQLContext) Detach() *DistSQLContext {
	newCtx := *dctx

	// TODO: using the same `SQLKiller` is actually not that meaningful. The `SQLKiller` will be reset before the
	// execution of each statements, so that if the running statement is killed, the background cursor will also
	// be affected (but it's not guaranteed). However, we don't provide an interface to `KILL` the background
	// cursor, so that it's still good to provide at least one way to stop it.
	// In the future, we should provide a more constant behavior for killing the cursor.
	newCtx.SQLKiller = dctx.SQLKiller
	newCPUUsages := new(ppcpuusage.SQLCPUUsages)
	newCPUUsages.SetCPUUsages(dctx.CPUUsage.GetCPUUsages())
	newCtx.CPUUsage = newCPUUsages
	newCtx.KVVars = new(tikvstore.Variables)
	*newCtx.KVVars = *dctx.KVVars
	newCtx.KVVars.Killed = &newCtx.SQLKiller.Signal
	return &newCtx
}

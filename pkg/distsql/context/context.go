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

	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/nocopy"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/topsql/stmtstats"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// DistSQLContext provides all information needed by using functions in `distsql`
type DistSQLContext struct {
	// TODO: provide a `Clone` to copy this struct.
	// The life cycle of some fields in this struct cannot be extended. For example, some fields will be recycled before
	// the next execution. They'll need to be handled specially.
	_ nocopy.NoCopy

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
	ResourceGroupTagger           tikvrpc.ResourceGroupTagger
	EnablePaging                  bool
	MinPagingSize                 int
	MaxPagingSize                 int
	RequestSourceType             string
	ExplicitRequestSourceType     string
	StoreBatchSize                int
	ResourceGroupName             string
	LoadBasedReplicaReadThreshold time.Duration
	RunawayChecker                *resourcegroup.RunawayChecker
	TiKVClientReadTimeout         uint64

	ReplicaClosestReadThreshold int64
	ConnectionID                uint64
	SessionAlias                string

	ExecDetails *execdetails.SyncExecDetails
}

// AppendWarning appends the warning to the warning handler.
func (dctx *DistSQLContext) AppendWarning(warn error) {
	dctx.WarnHandler.AppendWarning(warn)
}

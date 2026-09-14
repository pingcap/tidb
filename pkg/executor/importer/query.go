// Copyright 2026 PingCAP, Inc.
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

package importer

import (
	"context"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
)

// QueryPlan records SQL and its source metadata after tenant privilege checks.
// The worker optimizes it using statistics loaded from the target keyspace.
type QueryPlan struct {
	MemoryQuota int64
	CurrentDB   string
	Timestamp   int64
	Keyspace    string
	Databases   []*model.DBInfo
	// DBInfo.Deprecated.Tables and TableInfo.DBID are not serialized.
	// Persist table definitions grouped by database ID explicitly.
	Tables        map[int64][]*model.TableInfo
	SQL           string
	SessionVars   map[string]string
	PushDownFlags uint64
}

// QueryRuntime supplies resources owned by the task, not by the source session.
type QueryRuntime struct {
	// TotalMemoryLimit covers query readers and operators together.
	TotalMemoryLimit int64
	// Session is exclusively owned by this attempt. The caller must close or
	// destroy it on every exit path after RunImportQuery returns.
	Session     sessionctx.Context
	SessionPool util.DestroyableSessionPool
	Storage     storeapi.Storage
	Prefix      string
	MemoryLimit int64
}

// RunImportQuery executes a complete query attempt and streams owned chunks to
// output. The caller owns the channel and session; the function closes its executor
// before returning. Registration by executor avoids a package import cycle.
var RunImportQuery func(context.Context, *QueryPlan, QueryRuntime, chan<- QueryChunk) error

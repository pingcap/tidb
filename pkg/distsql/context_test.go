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

package distsql

import (
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
)

// NewDistSQLContextForTest creates a new dist sql context for test
func NewDistSQLContextForTest() *distsqlctx.DistSQLContext {
	return &distsqlctx.DistSQLContext{
		WarnHandler:                          contextutil.NewFuncWarnAppenderForTest(func(level string, err error) {}),
		TiFlashMaxThreads:                    vardef.DefTiFlashMaxThreads,
		TiFlashMaxBytesBeforeExternalJoin:    vardef.DefTiFlashMaxBytesBeforeExternalJoin,
		TiFlashMaxBytesBeforeExternalGroupBy: vardef.DefTiFlashMaxBytesBeforeExternalGroupBy,
		TiFlashMaxBytesBeforeExternalSort:    vardef.DefTiFlashMaxBytesBeforeExternalSort,
		TiFlashMaxQueryMemoryPerNode:         vardef.DefTiFlashMemQuotaQueryPerNode,
		TiFlashQuerySpillRatio:               vardef.DefTiFlashQuerySpillRatio,

		DistSQLConcurrency: vardef.DefDistSQLScanConcurrency,
		MinPagingSize:      vardef.DefMinPagingSize,
		MaxPagingSize:      vardef.DefMaxPagingSize,
		ResourceGroupName:  "default",

		ErrCtx: errctx.NewContext(contextutil.IgnoreWarn),
	}
}

// DefaultDistSQLContext is an empty distsql context used for testing, which doesn't have a client and cannot be used to
// send requests.
var DefaultDistSQLContext = NewDistSQLContextForTest()

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
	"github.com/pingcap/tidb/pkg/errctx"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
)

// RangeFallbackHandler is used to handle range fallback.
// If there are too many ranges, it'll fallback and add a warning.
type RangeFallbackHandler interface {
	RecordRangeFallback(rangeMaxSize int64)
	SetSkipPlanCache(err error)
	contextutil.WarnHandler
}

// RangerContext is the context used to build range.
type RangerContext struct {
	TypeCtx types.Context
	ErrCtx  errctx.Context
	ExprCtx exprctx.BuildContext
	RangeFallbackHandler
	OptimizerFixControl      map[uint64]string
	UseCache                 bool
	InPreparedPlanBuilding   bool
	RegardNULLAsPoint        bool
	OptPrefixIndexSingleScan bool
}

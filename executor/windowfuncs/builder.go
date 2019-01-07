// Copyright 2019 PingCAP, Inc.
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

package windowfuncs

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
)

// Build builds window functions according to the window functions description.
func Build(sctx sessionctx.Context, windowFuncDesc *aggregation.WindowFuncDesc, ordinal int) (WindowFunc, error) {
	aggDesc := aggregation.NewAggFuncDesc(sctx, windowFuncDesc.Name, windowFuncDesc.Args, false)
	agg := aggfuncs.Build(sctx, aggDesc, ordinal)
	if agg == nil {
		return nil, errors.New("window evaluator only support aggregation functions without frame now")
	}
	return &aggWithoutFrame{agg: agg}, nil
}

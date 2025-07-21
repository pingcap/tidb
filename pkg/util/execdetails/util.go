// Copyright 2025 PingCAP, Inc.
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

package execdetails

import (
	"context"

	"github.com/tikv/client-go/v2/util"
)

// ContextWithInitializedExecDetails returns a context with initialized stmt execution, execution and resource usage details.
func ContextWithInitializedExecDetails(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, StmtExecDetailKey, &StmtExecDetails{})
	ctx = context.WithValue(ctx, util.ExecDetailsKey, &util.ExecDetails{})
	ctx = context.WithValue(ctx, util.RUDetailsCtxKey, util.NewRUDetails())
	return ctx
}

// GetExecDetailsFromContext gets stmt execution, execution and resource usage details from context.
func GetExecDetailsFromContext(ctx context.Context) (stmtDetail StmtExecDetails, tikvExecDetail util.ExecDetails, ruDetails *util.RUDetails) {
	stmtDetailRaw := ctx.Value(StmtExecDetailKey)
	if stmtDetailRaw != nil {
		stmtDetail = *(stmtDetailRaw.(*StmtExecDetails))
	}
	tikvExecDetailRaw := ctx.Value(util.ExecDetailsKey)
	if tikvExecDetailRaw != nil {
		tikvExecDetail = *(tikvExecDetailRaw.(*util.ExecDetails))
	}
	ruDetails = util.NewRUDetails()
	if ruDetailsVal := ctx.Value(util.RUDetailsCtxKey); ruDetailsVal != nil {
		ruDetails = ruDetailsVal.(*util.RUDetails)
	}

	return
}

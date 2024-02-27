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

package contextimpl

import (
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// sessionctx.Context + *ExprCtxExtendedImpl should implement `expression.BuildContext`
// Only used to assert `ExprCtxExtendedImpl` should implement all methods not in `sessionctx.Context`
var _ exprctx.BuildContext = struct {
	sessionctx.Context
	*ExprCtxExtendedImpl
}{}

// ExprCtxExtendedImpl extends the sessionctx.Context to implement `expression.BuildContext`
type ExprCtxExtendedImpl struct {
	sctx sessionctx.Context
}

// NewExprExtendedImpl creates a new ExprCtxExtendedImpl.
func NewExprExtendedImpl(sctx sessionctx.Context) *ExprCtxExtendedImpl {
	return &ExprCtxExtendedImpl{sctx: sctx}
}

// SQLMode returns the sql mode
func (ctx *ExprCtxExtendedImpl) SQLMode() mysql.SQLMode {
	return ctx.sctx.GetSessionVars().SQLMode
}

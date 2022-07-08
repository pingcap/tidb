// Copyright 2022 PingCAP, Inc.
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

package sessiontxn

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/tikv/client-go/v2/oracle"
)

// ConstantFuture implements oracle.Future
type ConstantFuture uint64

// Wait returns a constant ts
func (n ConstantFuture) Wait() (uint64, error) {
	return uint64(n), nil
}

// FuncFuture implements oracle.Future
type FuncFuture func() (uint64, error)

// Wait returns a ts got from the func
func (f FuncFuture) Wait() (uint64, error) {
	return f()
}

// NewOracleFuture creates new future according to the scope and the session context
func NewOracleFuture(ctx context.Context, sctx sessionctx.Context, scope string) oracle.Future {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("sessiontxn.NewOracleFuture", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	oracleStore := sctx.GetStore().GetOracle()
	option := &oracle.Option{TxnScope: scope}

	if sctx.GetSessionVars().LowResolutionTSO {
		return oracleStore.GetLowResolutionTimestampAsync(ctx, option)
	}
	return oracleStore.GetTimestampAsync(ctx, option)
}

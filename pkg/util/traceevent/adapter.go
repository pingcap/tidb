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

package traceevent

import (
	"context"

	"github.com/pingcap/tidb/pkg/util/tracing"
	"github.com/tikv/client-go/v2/trace"
	"go.uber.org/zap"
)

// RegisterWithClientGo registers TiDB's trace event handlers with client-go.
// This should be called once during TiDB initialization.
func RegisterWithClientGo() {
	trace.SetTraceEventFunc(handleClientGoTraceEvent)
	trace.SetIsCategoryEnabledFunc(handleClientGoIsCategoryEnabled)
	trace.SetTraceControlExtractor(handleTraceControlExtractor)
}

// handleClientGoTraceEvent is the function called by client-go to emit trace events.
func handleClientGoTraceEvent(ctx context.Context, category trace.Category, name string, fields ...zap.Field) {
	cat := mapCategory(category)
	if !IsEnabled(cat) {
		return
	}
	// Include original category value for unknown categories to aid debugging
	if cat == tracing.UnknownClient {
		fields = append(fields, zap.Uint32("client_go_category", uint32(category)))
	}
	TraceEvent(ctx, cat, name, fields...)
}

// handleClientGoIsCategoryEnabled is the function called by client-go to check category enablement.
func handleClientGoIsCategoryEnabled(category trace.Category) bool {
	cat := mapCategory(category)
	return IsEnabled(cat)
}

// handleTraceControlExtractor is called by client-go to extract trace control flags from context.
// For now, this is a placeholder that returns FlagTiKVCategoryRequest (the default).
// The actual extraction logic will be added in a future PR.
func handleTraceControlExtractor(ctx context.Context) trace.TraceControlFlags {
	// TODO: Implement actual extraction logic based on trace ID or other context information.
	// Future implementation might:
	// - Check if trace ID matches certain patterns
	// - Look up trace ID in a configuration map
	// - Extract flags from context values
	// - Conditionally enable/disable flags based on runtime state
	// For now, return FlagTiKVCategoryRequest (the default), preserving existing behavior.
	return trace.FlagTiKVCategoryRequest
}

func mapCategory(category trace.Category) TraceCategory {
	switch category {
	case trace.CategoryTxn2PC:
		return tracing.Txn2PC
	case trace.CategoryTxnLockResolve:
		return tracing.TxnLockResolve
	case trace.CategoryKVRequest:
		return tracing.KvRequest
	default:
		return tracing.UnknownClient
	}
}

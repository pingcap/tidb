// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
)

// HandleDDLEventWithTxn wraps the common pattern of handling DDL events with a transaction
func HandleDDLEventWithTxn(h *handle.Handle, event *notifier.SchemaChangeEvent) error {
	return statsutil.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDDLNotifier)
		return h.HandleDDLEvent(ctx, sctx, event)
	}, statsutil.FlagWrapTxn)
}

// HandleNextDDLEventWithTxn handles the next DDL event from the channel with a transaction
func HandleNextDDLEventWithTxn(h *handle.Handle) error {
	return HandleDDLEventWithTxn(h, <-h.DDLEventCh())
}

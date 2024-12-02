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

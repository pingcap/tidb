package utils

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

const (
	ActionAddMV     = 146
	ActionDropMV    = 147
	ActionAlterMV   = 148
	ActionAddMVLOG  = 149
	ActionDropMVLOG = 150
)

// RegisterMVDDLEventHandler registers a DDL event handler for MV-related events.
func RegisterMVDDLEventHandler(ddlNotifier *notifier.DDLNotifier) {
	if ddlNotifier == nil {
		return
	}
	ddlNotifier.RegisterHandler(notifier.MVJobsHandlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		case ActionAddMV, ActionDropMVLOG, ActionAlterMV, ActionAddMVLOG, ActionDropMV:
			mvDDLEventCh.Wake()
		}
		return nil
	})
}

package utils

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

const (
	ActionAddMV     = 46
	ActionDropMV    = 47
	ActionAlterMV   = 48
	ActionAddMVLOG  = 49
	ActionDropMVLOG = 50
)

// RegisterTableDDLEventHandler registers a DDL notifier handler for table create/alter/drop events.
// The caller must pass a unique handlerID and register it before the notifier starts.
// The handler receives the action type, and table info pointers may be nil depending on the action.
func RegisterTableDDLEventHandler(
	ddlNotifier *notifier.DDLNotifier,
	handlerID notifier.HandlerID,
	onEvent func(action model.ActionType, tableInfo *model.TableInfo, oldTableInfo *model.TableInfo),
) {
	ddlNotifier.RegisterHandler(handlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		if onEvent == nil {
			return nil
		}
		action := event.GetType()
		switch action {
		case ActionAddMV:
			onEvent(action, event.GetCreateTableInfo(), nil)
		case ActionAddMVLOG:
			onEvent(action, nil, event.GetDropTableInfo())
		case ActionDropMV:
			onEvent(action, nil, event.GetDropTableInfo())
		case ActionDropMVLOG:
			onEvent(action, event.GetCreateTableInfo(), nil)
		case ActionAlterMV:
			onEvent(action, event.GetCreateTableInfo(), event.GetOldTableInfo())
		default:
		}
		return nil
	})
}

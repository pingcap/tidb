package utils

import (
	"context"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

// RegisterCreateDropTableHandler registers a DDL notifier handler for table create/drop events.
// The caller must pass a unique handlerID and register it before the notifier starts.
func RegisterCreateDropTableHandler(
	ddlNotifier *notifier.DDLNotifier,
	handlerID notifier.HandlerID,
	onCreate func(*model.TableInfo),
	onDrop func(*model.TableInfo),
) {
	ddlNotifier.RegisterHandler(handlerID, func(_ context.Context, _ sessionctx.Context, event *notifier.SchemaChangeEvent) error {
		switch event.GetType() {
		case model.ActionCreateTable:
			if onCreate != nil {
				onCreate(event.GetCreateTableInfo())
			}
		case model.ActionDropTable:
			if onDrop != nil {
				onDrop(event.GetDropTableInfo())
			}
		}
		return nil
	})
}

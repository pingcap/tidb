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
	"time"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
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

// FindEvent finds and returns the first event of the specified type from the event channel.
// It blocks until an event of the specified type is found.
func FindEvent(eventCh <-chan *notifier.SchemaChangeEvent, eventType model.ActionType) *notifier.SchemaChangeEvent {
	// Find the target event.
	for {
		event := <-eventCh
		if event.GetType() == eventType {
			return event
		}
	}
}

// FindEventWithTimeout finds and returns the first event of the specified type from the event channel.
// It returns nil if no matching event is found within the specified timeout (in seconds).
func FindEventWithTimeout(eventCh <-chan *notifier.SchemaChangeEvent, eventType model.ActionType, timeout int) *notifier.SchemaChangeEvent {
	ticker := time.NewTicker(time.Second * time.Duration(timeout))
	defer ticker.Stop()
	// Find the target event.
	for {
		select {
		case event := <-eventCh:
			if event.GetType() == eventType {
				return event
			}
		case <-ticker.C:
			return nil
		}
	}
}

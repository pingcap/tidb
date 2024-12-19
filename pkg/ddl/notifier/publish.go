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

package notifier

import (
	"context"

	sess "github.com/pingcap/tidb/pkg/ddl/session"
)

// PubSchemeChangeToStore publishes schema changes to the store to notify
// subscribers on the Store. It stages changes in given `se` so they will be
// visible when `se` further commits. When the schema change is not from
// multi-schema change DDL, `multiSchemaChangeSeq` is -1. Otherwise,
// `multiSchemaChangeSeq` is the sub-job index of the multi-schema change DDL.
func PubSchemeChangeToStore(
	ctx context.Context,
	se *sess.Session,
	ddlJobID int64,
	multiSchemaChangeSeq int64,
	event *SchemaChangeEvent,
	store Store,
) error {
	change := &schemaChange{
		ddlJobID:             ddlJobID,
		multiSchemaChangeSeq: multiSchemaChangeSeq,
		event:                event,
	}
	return store.Insert(ctx, se, change)
}

// schemaChange is the Golang representation of the persistent data. (ddlJobID,
// multiSchemaChangeSeq) should be unique in the cluster.
type schemaChange struct {
	ddlJobID             int64
	multiSchemaChangeSeq int64
	event                *SchemaChangeEvent
	processedByFlag      uint64
}

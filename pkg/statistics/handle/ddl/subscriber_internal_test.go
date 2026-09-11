// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestSubscriberHandlesMaterializedViewMetadataEvents(t *testing.T) {
	mvInfo := &model.TableInfo{ID: 1, Name: ast.NewCIStr("mv")}
	mlogInfo := &model.TableInfo{ID: 2, Name: ast.NewCIStr("$mlog$t")}
	oldMVInfo := mvInfo.Clone()
	oldMLogInfo := mlogInfo.Clone()

	tests := []struct {
		name  string
		event *notifier.SchemaChangeEvent
	}{
		{
			name:  "refresh",
			event: notifier.NewAlterMaterializedViewRefreshEvent(mvInfo, oldMVInfo),
		},
		{
			name:  "attributes",
			event: notifier.NewAlterMaterializedViewAttributesEvent(mvInfo, oldMVInfo),
		},
		{
			name:  "log purge",
			event: notifier.NewAlterMaterializedViewLogPurgeEvent(mlogInfo, oldMLogInfo),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub := newSubscriber(nil)
			require.NoError(t, sub.handle(context.Background(), nil, tt.event))
		})
	}
}

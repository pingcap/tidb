// Copyright 2019 PingCAP, Inc.
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

package plugin_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestExportManifest(t *testing.T) {
	callRecorder := struct {
		OnInitCalled      bool
		NotifyEventCalled bool
	}{}
	manifest := &plugin.AuditManifest{
		Manifest: plugin.Manifest{
			Kind:    plugin.Authentication,
			Name:    "test audit",
			Version: 1,
			OnInit: func(ctx context.Context, manifest *plugin.Manifest) error {
				callRecorder.OnInitCalled = true
				return nil
			},
		},
		OnGeneralEvent: func(ctx context.Context, sctx *variable.SessionVars, event plugin.GeneralEvent, cmd string) {
			callRecorder.NotifyEventCalled = true
		},
	}
	exported := plugin.ExportManifest(manifest)
	err := exported.OnInit(context.Background(), exported)
	require.NoError(t, err)
	audit := plugin.DeclareAuditManifest(exported)
	audit.OnGeneralEvent(context.Background(), nil, plugin.Completed, "QUERY")
	require.True(t, callRecorder.NotifyEventCalled)
	require.True(t, callRecorder.OnInitCalled)
}

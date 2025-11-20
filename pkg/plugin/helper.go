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

package plugin

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

// DeclareAuditManifest declares manifest as AuditManifest.
func DeclareAuditManifest(m *Manifest) *AuditManifest {
	return (*AuditManifest)(unsafe.Pointer(m))
}

// DeclareAuthenticationManifest declares manifest as AuthenticationManifest.
func DeclareAuthenticationManifest(m *Manifest) *AuthenticationManifest {
	return (*AuthenticationManifest)(unsafe.Pointer(m))
}

// DeclareSchemaManifest declares manifest as SchemaManifest.
func DeclareSchemaManifest(m *Manifest) *SchemaManifest {
	return (*SchemaManifest)(unsafe.Pointer(m))
}

// DeclareDaemonManifest declares manifest as DaemonManifest.
func DeclareDaemonManifest(m *Manifest) *DaemonManifest {
	return (*DaemonManifest)(unsafe.Pointer(m))
}

// ID present plugin identity.
type ID string

// Decode decodes a plugin id into name, version parts.
func (n ID) Decode() (name string, version string, err error) {
	index := strings.LastIndex(string(n), "-")
	if index == -1 {
		err = errInvalidPluginID.GenWithStackByArgs(string(n))
		return
	}
	name = string(n)[:index]
	version = string(n)[index+1:]
	return
}

// LoadPluginForTest loads a test plugin with given onGeneralEvent callback.
func LoadPluginForTest(t *testing.T, onGeneralEvent func(context.Context, *variable.SessionVars, GeneralEvent, string)) {
	ctx := context.Background()
	pluginName := "audit_test"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := Config{
		Plugins:    []string{pluginSign},
		PluginDir:  "",
		EnvVersion: map[string]uint16{"go": 1112},
	}

	validate := func(_ context.Context, _ *Manifest) error {
		return nil
	}
	onInit := func(_ context.Context, _ *Manifest) error {
		return nil
	}
	onShutdown := func(_ context.Context, _ *Manifest) error {
		return nil
	}
	onConnectionEvent := func(_ context.Context, _ ConnectionEvent, _ *variable.ConnectionInfo) error {
		return nil
	}

	// setup load test hook.
	loadOne := func(_ *Plugin, _ string, _ ID) (manifest func() *Manifest, err error) {
		return func() *Manifest {
			m := &AuditManifest{
				Manifest: Manifest{
					Kind:       Audit,
					Name:       pluginName,
					Version:    pluginVersion,
					OnInit:     onInit,
					OnShutdown: onShutdown,
					Validate:   validate,
				},
				OnGeneralEvent:    onGeneralEvent,
				OnConnectionEvent: onConnectionEvent,
			}
			return ExportManifest(m)
		}, nil
	}
	SetTestHook(loadOne)

	// trigger load.
	err := Load(ctx, cfg)
	require.NoErrorf(t, err, "load plugin [%s] fail, error [%s]\n", pluginSign, err)

	err = Init(ctx, cfg)
	require.NoErrorf(t, err, "init plugin [%s] fail, error [%s]\n", pluginSign, err)
}

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
// See the License for the specific language governing permissions and
// limitations under the License.

package plugin

import (
	"context"
	"io"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/stretchr/testify/assert"
)

func TestLoadPluginSuccess(t *testing.T) {
	ctx := context.Background()

	pluginName := "tplugin"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := Config{
		Plugins:    []string{pluginSign},
		PluginDir:  "",
		EnvVersion: map[string]uint16{"go": 1112},
	}

	// setup load test hook.
	SetTestHook(func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error) {
		return func() *Manifest {
			m := &AuditManifest{
				Manifest: Manifest{
					Kind:    Authentication,
					Name:    pluginName,
					Version: pluginVersion,
					OnInit: func(ctx context.Context, manifest *Manifest) error {
						return nil
					},
					OnShutdown: func(ctx context.Context, manifest *Manifest) error {
						return nil
					},
					Validate: func(ctx context.Context, manifest *Manifest) error {
						return nil
					},
				},
				OnGeneralEvent: func(ctx context.Context, sctx *variable.SessionVars, event GeneralEvent, cmd string) {
				},
			}
			return ExportManifest(m)
		}, nil
	})
	defer func() {
		testHook = nil
	}()

	// trigger load.
	err := Load(ctx, cfg)
	assert.NoErrorf(t, err, "load plugin [%s] fail", pluginSign)

	err = Init(ctx, cfg)
	assert.NoErrorf(t, err, "init plugin [%s] fail", pluginSign)

	// load all.
	ps := GetAll()
	assert.Lenf(t, ps, 1, "loaded plugins is empty")
	assert.Truef(t, IsEnable(Authentication), "plugin is not enabled")

	// find plugin by type and name
	p := Get(Authentication, "tplugin")
	assert.NotNilf(t, p, "tplugin can not be load")
	p = Get(Authentication, "tplugin2")
	assert.Nilf(t, p, "found miss plugin")
	p = getByName("tplugin")
	assert.NotNilf(t, p, "can not find miss plugin")

	// foreach plugin
	err = ForeachPlugin(Authentication, func(plugin *Plugin) error {
		return nil
	})
	assert.NoErrorf(t, err, "foreach error %v", err)
	err = ForeachPlugin(Authentication, func(plugin *Plugin) error {
		return io.EOF
	})
	assert.Equalf(t, io.EOF, err, "foreach should return EOF error")

	Shutdown(ctx)
}

func TestLoadPluginSkipError(t *testing.T) {
	ctx := context.Background()

	pluginName := "tplugin"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := Config{
		Plugins:      []string{pluginSign, pluginSign, "notExists-2"},
		PluginDir:    "",
		EnvVersion:   map[string]uint16{"go": 1112},
		SkipWhenFail: true,
	}

	// setup load test hook.
	SetTestHook(func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error) {
		return func() *Manifest {
			m := &AuditManifest{
				Manifest: Manifest{
					Kind:    Audit,
					Name:    pluginName,
					Version: pluginVersion,
					OnInit: func(ctx context.Context, manifest *Manifest) error {
						return io.EOF
					},
					OnShutdown: func(ctx context.Context, manifest *Manifest) error {
						return io.EOF
					},
					Validate: func(ctx context.Context, manifest *Manifest) error {
						return io.EOF
					},
				},
				OnGeneralEvent: func(ctx context.Context, sctx *variable.SessionVars, event GeneralEvent, cmd string) {
				},
			}
			return ExportManifest(m)
		}, nil
	})
	defer func() {
		testHook = nil
	}()

	// trigger load.
	err := Load(ctx, cfg)
	assert.NoErrorf(t, err, "load plugin [%s] fail", pluginSign)

	err = Init(ctx, cfg)
	assert.NoErrorf(t, err, "init plugin [%s] fail", pluginSign)
	assert.Falsef(t, IsEnable(Audit), "plugin is enabled")

	// load all.
	ps := GetAll()
	assert.Lenf(t, ps, 1, "loaded plugins is empty")

	// find plugin by type and name
	p := Get(Audit, "tplugin")
	assert.NotNilf(t, p, "tplugin can not be load")
	p = Get(Audit, "tplugin2")
	assert.Nilf(t, p, "found miss plugin")
	p = getByName("tplugin")
	assert.NotNilf(t, p, "can not find miss plugin")
	p = getByName("not exists")
	assert.Nilf(t, p, "got not exists plugin")

	// foreach plugin
	readyCount := 0
	err = ForeachPlugin(Audit, func(plugin *Plugin) error {
		readyCount++
		return nil
	})
	assert.NoErrorf(t, err, "foreach meet error %v", err)
	assert.Equalf(t, 0, readyCount, "validate fail can be load but no ready")

	Shutdown(ctx)
}

func TestLoadFail(t *testing.T) {
	ctx := context.Background()

	pluginName := "tplugin"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := Config{
		Plugins:      []string{pluginSign, pluginSign, "notExists-2"},
		PluginDir:    "",
		EnvVersion:   map[string]uint16{"go": 1112},
		SkipWhenFail: false,
	}

	// setup load test hook.
	SetTestHook(func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error) {
		return func() *Manifest {
			m := &AuditManifest{
				Manifest: Manifest{
					Kind:    Audit,
					Name:    pluginName,
					Version: pluginVersion,
					OnInit: func(ctx context.Context, manifest *Manifest) error {
						return io.EOF
					},
					OnShutdown: func(ctx context.Context, manifest *Manifest) error {
						return io.EOF
					},
					Validate: func(ctx context.Context, manifest *Manifest) error {
						return io.EOF
					},
				},
				OnGeneralEvent: func(ctx context.Context, sctx *variable.SessionVars, event GeneralEvent, cmd string) {
				},
			}
			return ExportManifest(m)
		}, nil
	})
	defer func() {
		testHook = nil
	}()

	err := Load(ctx, cfg)
	assert.Errorf(t, err, "load plugin should fail")
}

func TestPluginsClone(t *testing.T) {
	ps := &plugins{
		plugins: map[Kind][]Plugin{
			Audit: {{}},
		},
		versions: map[string]uint16{
			"whitelist": 1,
		},
		dyingPlugins: []Plugin{{}},
	}
	cps := ps.clone()
	ps.dyingPlugins = append(ps.dyingPlugins, Plugin{})
	ps.versions["w"] = 2
	as := ps.plugins[Audit]
	ps.plugins[Audit] = append(as, Plugin{})

	msg := "clone plugins failure"
	assert.Lenf(t, cps.plugins, 1, msg)
	assert.Lenf(t, cps.plugins[Audit], 1, msg)
	assert.Lenf(t, cps.versions, 1, msg)
	assert.Equalf(t, uint16(1), cps.versions["whitelist"], msg)
	assert.Lenf(t, cps.dyingPlugins, 1, msg)
}

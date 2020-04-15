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

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
)

func TestT(t *testing.T) {
	check.TestingT(t)
}

func TestLoadPluginSuccess(t *testing.T) {
	ctx := context.Background()

	pluginName := "tplugin"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := Config{
		Plugins:        []string{pluginSign},
		PluginDir:      "",
		GlobalSysVar:   &variable.SysVars,
		PluginVarNames: &variable.PluginVarNames,
		EnvVersion:     map[string]uint16{"go": 1112},
	}

	// setup load test hook.
	testHook = &struct{ loadOne loadFn }{loadOne: func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error) {
		return func() *Manifest {
			m := &AuditManifest{
				Manifest: Manifest{
					Kind:    Authentication,
					Name:    pluginName,
					Version: pluginVersion,
					SysVars: map[string]*variable.SysVar{pluginName + "_key": {Scope: variable.ScopeGlobal, Name: pluginName + "_key", Value: "v1"}},
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
	}}
	defer func() {
		testHook = nil
	}()

	// trigger load.
	err := Load(ctx, cfg)
	if err != nil {
		t.Errorf("load plugin [%s] fail", pluginSign)
	}

	err = Init(ctx, cfg)
	if err != nil {
		t.Errorf("init plugin [%s] fail", pluginSign)
	}

	// load all.
	ps := GetAll()
	if len(ps) != 1 {
		t.Errorf("loaded plugins is empty")
	}

	// find plugin by type and name
	p := Get(Authentication, "tplugin")
	if p == nil {
		t.Errorf("tplugin can not be load")
	}
	p = Get(Authentication, "tplugin2")
	if p != nil {
		t.Errorf("found miss plugin")
	}
	p = getByName("tplugin")
	if p == nil {
		t.Errorf("can not find miss plugin")
	}

	// foreach plugin
	err = ForeachPlugin(Authentication, func(plugin *Plugin) error {
		return nil
	})
	if err != nil {
		t.Errorf("foreach error %v", err)
	}
	err = ForeachPlugin(Authentication, func(plugin *Plugin) error {
		return io.EOF
	})
	if err != io.EOF {
		t.Errorf("foreach should return EOF error")
	}

	Shutdown(ctx)
}

func TestLoadPluginSkipError(t *testing.T) {
	ctx := context.Background()

	pluginName := "tplugin"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := Config{
		Plugins:        []string{pluginSign, pluginSign, "notExists-2"},
		PluginDir:      "",
		PluginVarNames: &variable.PluginVarNames,
		EnvVersion:     map[string]uint16{"go": 1112},
		SkipWhenFail:   true,
	}

	// setup load test hook.
	testHook = &struct{ loadOne loadFn }{loadOne: func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error) {
		return func() *Manifest {
			m := &AuditManifest{
				Manifest: Manifest{
					Kind:    Audit,
					Name:    pluginName,
					Version: pluginVersion,
					SysVars: map[string]*variable.SysVar{pluginName + "_key": {Scope: variable.ScopeGlobal, Name: pluginName + "_key", Value: "v1"}},
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
	}}
	defer func() {
		testHook = nil
	}()

	// trigger load.
	err := Load(ctx, cfg)
	if err != nil {
		t.Errorf("load plugin [%s] fail %v", pluginSign, err)
	}

	err = Init(ctx, cfg)
	if err != nil {
		t.Errorf("init plugin [%s] fail", pluginSign)
	}

	// load all.
	ps := GetAll()
	if len(ps) != 1 {
		t.Errorf("loaded plugins is empty")
	}

	// find plugin by type and name
	p := Get(Audit, "tplugin")
	if p == nil {
		t.Errorf("tplugin can not be load")
	}
	p = Get(Audit, "tplugin2")
	if p != nil {
		t.Errorf("found miss plugin")
	}
	p = getByName("tplugin")
	if p == nil {
		t.Errorf("can not find miss plugin")
	}
	p = getByName("not exists")
	if p != nil {
		t.Errorf("got not exists plugin")
	}

	// foreach plugin
	readyCount := 0
	err = ForeachPlugin(Authentication, func(plugin *Plugin) error {
		readyCount++
		return nil
	})
	if err != nil {
		t.Errorf("foreach meet error %v", err)
	}
	if readyCount != 0 {
		t.Errorf("validate fail can be load but no ready")
	}

	Shutdown(ctx)
}

func TestLoadFail(t *testing.T) {
	ctx := context.Background()

	pluginName := "tplugin"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := Config{
		Plugins:        []string{pluginSign, pluginSign, "notExists-2"},
		PluginDir:      "",
		PluginVarNames: &variable.PluginVarNames,
		EnvVersion:     map[string]uint16{"go": 1112},
		SkipWhenFail:   false,
	}

	// setup load test hook.
	testHook = &struct{ loadOne loadFn }{loadOne: func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error) {
		return func() *Manifest {
			m := &AuditManifest{
				Manifest: Manifest{
					Kind:    Audit,
					Name:    pluginName,
					Version: pluginVersion,
					SysVars: map[string]*variable.SysVar{pluginName + "_key": {Scope: variable.ScopeGlobal, Name: pluginName + "_key", Value: "v1"}},
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
	}}
	defer func() {
		testHook = nil
	}()

	err := Load(ctx, cfg)
	if err == nil {
		t.Errorf("load plugin should fail")
	}
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

	if len(cps.plugins) != 1 || len(cps.plugins[Audit]) != 1 || len(cps.versions) != 1 || len(cps.dyingPlugins) != 1 {
		t.Errorf("clone plugins failure")
	}
}

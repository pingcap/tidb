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
	"io"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestLoadStaticRegisteredPlugin(t *testing.T) {
	StaticPlugins.Clear()
	defer StaticPlugins.Clear()

	// error should return when plugin not found
	_, err := loadOne("/fake/path", "tpluginstatic-1")
	require.EqualError(t, err, `plugin.Open("/fake/path/tpluginstatic-1.so"): realpath failed`)

	m := &AuditManifest{
		Manifest: Manifest{
			Kind:    Authentication,
			Name:    "tpluginstatic",
			Version: 1,
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

	require.NoError(t, StaticPlugins.Add(m.Name, func() *Manifest {
		return ExportManifest(m)
	}))

	// static plugin can be load
	plugin, err := loadOne("/fake/path", "tpluginstatic-1")
	require.NoError(t, err)
	require.NotNil(t, plugin)
	require.Equal(t, uintptr(unsafe.Pointer(m)), uintptr(unsafe.Pointer(plugin.Manifest)))

	// static plugin do not check version
	plugin, err = loadOne("/fake/path", "tpluginstatic-2")
	require.NoError(t, err)
	require.NotNil(t, plugin)
	require.Equal(t, uintptr(unsafe.Pointer(m)), uintptr(unsafe.Pointer(plugin.Manifest)))

	// static plugins has a higher priority
	SetTestHook(func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error) {
		return func() *Manifest {
			m2 := &AuditManifest{
				Manifest: Manifest{
					Kind:    Authentication,
					Name:    m.Name,
					Version: 1,
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
			return ExportManifest(m2)
		}, nil
	})
	plugin, err = loadOne("/fake/path", "tpluginstatic-1")
	require.NoError(t, err)
	require.NotNil(t, plugin)
	require.Equal(t, uintptr(unsafe.Pointer(m)), uintptr(unsafe.Pointer(plugin.Manifest)))
}

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
	require.NoError(t, err)

	err = Init(ctx, cfg)
	require.NoError(t, err)

	// load all.
	ps := GetAll()
	require.Len(t, ps, 1)
	require.True(t, IsEnable(Authentication))

	// find plugin by type and name
	p := Get(Authentication, "tplugin")
	require.NotNil(t, p)
	p = Get(Authentication, "tplugin2")
	require.Nil(t, p)
	p = getByName("tplugin")
	require.NotNil(t, p)

	// foreach plugin
	err = ForeachPlugin(Authentication, func(plugin *Plugin) error {
		return nil
	})
	require.NoError(t, err)
	err = ForeachPlugin(Authentication, func(plugin *Plugin) error {
		return io.EOF
	})
	require.Equal(t, io.EOF, err)

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
	require.NoError(t, err)

	err = Init(ctx, cfg)
	require.NoError(t, err)
	require.False(t, IsEnable(Audit))

	// load all.
	ps := GetAll()
	require.Len(t, ps, 1)

	// find plugin by type and name
	p := Get(Audit, "tplugin")
	require.NotNil(t, p)
	p = Get(Audit, "tplugin2")
	require.Nil(t, p)
	p = getByName("tplugin")
	require.NotNil(t, p)
	p = getByName("not exists")
	require.Nil(t, p)

	// foreach plugin
	readyCount := 0
	err = ForeachPlugin(Audit, func(plugin *Plugin) error {
		readyCount++
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 0, readyCount)

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
	require.Error(t, err)
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

	require.Len(t, cps.plugins, 1)
	require.Len(t, cps.plugins[Audit], 1)
	require.Len(t, cps.versions, 1)
	require.Equal(t, uint16(1), cps.versions["whitelist"])
	require.Len(t, cps.dyingPlugins, 1)
}

func TestPluginWatcherLoop(t *testing.T) {
	// exit when ctx done
	ctx, cancel := context.WithCancel(context.Background())
	watcher := &flushWatcher{
		ctx: ctx,
		manifest: &Manifest{
			Name: "test",
		},
	}
	ch := make(chan clientv3.WatchResponse)
	var cancelled atomic.Bool
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancelled.Store(true)
		cancel()
	}()
	exit := watcher.watchLoopWithChan(ch)
	require.True(t, exit)
	require.True(t, cancelled.Load())

	// exit when ch closed
	watcher = &flushWatcher{
		ctx: context.Background(),
		manifest: &Manifest{
			Name: "test",
		},
	}

	var closed atomic.Bool
	ch = make(chan clientv3.WatchResponse)
	go func() {
		time.Sleep(10 * time.Millisecond)
		closed.Store(true)
		close(ch)
	}()
	exit = watcher.watchLoopWithChan(ch)
	require.False(t, exit)
	require.True(t, cancelled.Load())
}

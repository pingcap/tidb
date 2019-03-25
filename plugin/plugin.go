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
	"path/filepath"
	gplugin "plugin"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// pluginGlobal holds all global variables for plugin.
var pluginGlobal copyOnWriteContext

// copyOnWriteContext wraps a context follow COW idiom.
type copyOnWriteContext struct {
	tiPlugins unsafe.Pointer // *plugins
}

// plugins collects loaded plugins info.
type plugins struct {
	plugins      map[Kind][]Plugin
	versions     map[string]uint16
	dyingPlugins []Plugin
}

// clone deep copies plugins info.
func (p *plugins) clone() *plugins {
	np := &plugins{
		plugins:  make(map[Kind][]Plugin, len(p.plugins)),
		versions: make(map[string]uint16, len(p.versions)),
	}
	for key, value := range p.plugins {
		np.plugins[key] = append([]Plugin(nil), value...)
	}
	for key, value := range p.versions {
		np.versions[key] = value
	}
	for key, value := range p.dyingPlugins {
		np.dyingPlugins[key] = value
	}
	return np
}

// add adds a plugin to loaded plugin collection.
func (p plugins) add(plugin *Plugin) {
	plugins, ok := p.plugins[plugin.Kind]
	if !ok {
		plugins = make([]Plugin, 0)
	}
	plugins = append(plugins, *plugin)
	p.plugins[plugin.Kind] = plugins
	p.versions[plugin.Name] = plugin.Version
}

// plugins got plugin in COW context.
func (p copyOnWriteContext) plugins() *plugins {
	return (*plugins)(atomic.LoadPointer(&p.tiPlugins))
}

// Config presents the init configuration for plugin framework.
type Config struct {
	Plugins        []string
	PluginDir      string
	GlobalSysVar   *map[string]*variable.SysVar
	PluginVarNames *[]string
	SkipWhenFail   bool
	EnvVersion     map[string]uint16
	EtcdClient     *clientv3.Client
}

// Plugin presents a TiDB plugin.
type Plugin struct {
	*Manifest
	library *gplugin.Plugin
	State   State
	Path    string
}

type validateMode int

const (
	initMode validateMode = iota
	reloadMode
)

func (p *Plugin) validate(ctx context.Context, tiPlugins *plugins, mode validateMode) error {
	if mode == reloadMode {
		var oldPlugin *Plugin
		for i, item := range tiPlugins.plugins[p.Kind] {
			if item.Name == p.Name {
				oldPlugin = &tiPlugins.plugins[p.Kind][i]
				break
			}
		}
		if oldPlugin == nil {
			return errUnsupportedReloadPlugin.GenWithStackByArgs(p.Name)
		}
		if len(p.SysVars) != len(oldPlugin.SysVars) {
			return errUnsupportedReloadPluginVar.GenWithStackByArgs("")
		}
		for varName, varVal := range p.SysVars {
			if oldPlugin.SysVars[varName] == nil || *oldPlugin.SysVars[varName] != *varVal {
				return errUnsupportedReloadPluginVar.GenWithStackByArgs(varVal)
			}
		}
	}
	if p.RequireVersion != nil {
		for component, reqVer := range p.RequireVersion {
			if ver, ok := tiPlugins.versions[component]; !ok || ver < reqVer {
				return errRequireVersionCheckFail.GenWithStackByArgs(p.Name, component, reqVer, ver)
			}
		}
	}
	if p.SysVars != nil {
		for varName := range p.SysVars {
			if !strings.HasPrefix(varName, p.Name) {
				return errInvalidPluginSysVarName.GenWithStackByArgs(p.Name, varName, p.Name)
			}
		}
	}
	if p.Manifest.Validate != nil {
		if err := p.Manifest.Validate(ctx, p.Manifest); err != nil {
			return err
		}
	}
	return nil
}

// Load load plugin by config param.
// This method need be called before domain init to inject global variable info during bootstrap.
func Load(ctx context.Context, cfg Config) (err error) {
	tiPlugins := &plugins{
		plugins:      make(map[Kind][]Plugin),
		versions:     make(map[string]uint16),
		dyingPlugins: make([]Plugin, 0),
	}

	// Setup component version info for plugin running env.
	for component, version := range cfg.EnvVersion {
		tiPlugins.versions[component] = version
	}

	// Load plugin dl & manifest.
	for _, pluginID := range cfg.Plugins {
		var pName string
		pName, _, err = ID(pluginID).Decode()
		if err != nil {
			err = errors.Trace(err)
			return
		}
		// Check duplicate.
		_, dup := tiPlugins.versions[pName]
		if dup {
			if cfg.SkipWhenFail {
				logutil.Logger(ctx).Warn("duplicate load %s and ignored", zap.String("pluginName", pName))
				continue
			}
			err = errDuplicatePlugin.GenWithStackByArgs(pluginID)
			return
		}
		// Load dl.
		var plugin Plugin
		plugin, err = loadOne(cfg.PluginDir, ID(pluginID))
		if err != nil {
			if cfg.SkipWhenFail {
				logutil.Logger(ctx).Warn("load plugin failure and ignored", zap.String("pluginID", pluginID), zap.Error(err))
				continue
			}
			return
		}
		tiPlugins.add(&plugin)
	}

	// Cross validate & Load plugins.
	for kind := range tiPlugins.plugins {
		for i := range tiPlugins.plugins[kind] {
			if err = tiPlugins.plugins[kind][i].validate(ctx, tiPlugins, initMode); err != nil {
				if cfg.SkipWhenFail {
					logutil.Logger(ctx).Warn("validate plugin fail and disable plugin",
						zap.String("plugin", tiPlugins.plugins[kind][i].Name), zap.Error(err))
					tiPlugins.plugins[kind][i].State = Disable
					err = nil
					continue
				}
				return
			}
			if cfg.GlobalSysVar != nil {
				for key, value := range tiPlugins.plugins[kind][i].SysVars {
					(*cfg.GlobalSysVar)[key] = value
					if value.Scope != variable.ScopeSession && cfg.PluginVarNames != nil {
						*cfg.PluginVarNames = append(*cfg.PluginVarNames, key)
					}
				}
			}
		}
	}
	pluginGlobal = copyOnWriteContext{tiPlugins: unsafe.Pointer(tiPlugins)}
	err = nil
	return
}

// Init initializes the loaded plugin by config param.
// This method must be called after `Load` but before any other plugin method call, so it call got TiDB domain info.
func Init(ctx context.Context, cfg Config) (err error) {
	tiPlugins := pluginGlobal.plugins()
	if tiPlugins == nil {
		return nil
	}
	for kind := range tiPlugins.plugins {
		for i := range tiPlugins.plugins[kind] {
			p := tiPlugins.plugins[kind][i]
			if err = p.OnInit(ctx, p.Manifest); err != nil {
				if cfg.SkipWhenFail {
					logutil.Logger(ctx).Warn("call Plugin OnInit failure, err: %v",
						zap.String("plugin", p.Name), zap.Error(err))
					tiPlugins.plugins[kind][i].State = Disable
					err = nil
					continue
				}
				return
			}
			if p.OnFlush != nil && cfg.EtcdClient != nil {
				const pluginWatchPrefix = "/tidb/plugins/"
				ctx, cancel := context.WithCancel(context.Background())
				watcher := &flushWatcher{
					ctx:      ctx,
					cancel:   cancel,
					path:     pluginWatchPrefix + tiPlugins.plugins[kind][i].Name,
					etcd:     cfg.EtcdClient,
					manifest: tiPlugins.plugins[kind][i].Manifest,
				}
				tiPlugins.plugins[kind][i].flushWatcher = watcher
				go util.WithRecovery(watcher.watchLoop, nil)
			}
			tiPlugins.plugins[kind][i].State = Ready
		}
	}
	return
}

type flushWatcher struct {
	ctx      context.Context
	cancel   context.CancelFunc
	path     string
	etcd     *clientv3.Client
	manifest *Manifest
}

func (w *flushWatcher) watchLoop() {
	watchChan := w.etcd.Watch(w.ctx, w.path)
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-watchChan:
			err := w.manifest.OnFlush(w.ctx, w.manifest)
			if err != nil {
				logutil.Logger(context.Background()).Error("notify plugin flush event failed", zap.String("plugin", w.manifest.Name), zap.Error(err))
			}
		}
	}
}

func loadOne(dir string, pluginID ID) (plugin Plugin, err error) {
	plugin.Path = filepath.Join(dir, string(pluginID)+LibrarySuffix)
	plugin.library, err = gplugin.Open(plugin.Path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	manifestSym, err := plugin.library.Lookup(ManifestSymbol)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	manifest, ok := manifestSym.(func() *Manifest)
	if !ok {
		err = errInvalidPluginManifest.GenWithStackByArgs(string(pluginID))
		return
	}
	pName, pVersion, err := pluginID.Decode()
	if err != nil {
		err = errors.Trace(err)
		return
	}
	plugin.Manifest = manifest()
	if plugin.Name != pName {
		err = errInvalidPluginName.GenWithStackByArgs(string(pluginID), plugin.Name)
		return
	}
	if strconv.Itoa(int(plugin.Version)) != pVersion {
		err = errInvalidPluginVersion.GenWithStackByArgs(string(pluginID))
		return
	}
	return
}

// Shutdown cleanups all plugin resources.
// Notice: it just cleanups the resource of plugin, but cannot unload plugins(limited by go plugin).
func Shutdown(ctx context.Context) {
	for {
		tiPlugins := pluginGlobal.plugins()
		if tiPlugins == nil {
			return
		}
		for _, plugins := range tiPlugins.plugins {
			for _, p := range plugins {
				p.State = Dying
				if p.flushWatcher != nil {
					p.flushWatcher.cancel()
				}
				if err := p.OnShutdown(ctx, p.Manifest); err != nil {
					logutil.Logger(ctx).Error("call OnShutdown for failure",
						zap.String("plugin", p.Name), zap.Error(err))
				}
			}
		}
		if atomic.CompareAndSwapPointer(&pluginGlobal.tiPlugins, unsafe.Pointer(tiPlugins), nil) {
			return
		}
	}
}

// Get finds and returns plugin by kind and name parameters.
func Get(kind Kind, name string) *Plugin {
	plugins := pluginGlobal.plugins()
	if plugins == nil {
		return nil
	}
	for _, p := range plugins.plugins[kind] {
		if p.Name == name {
			return &p
		}
	}
	return nil
}

// ForeachPlugin loops all ready plugins.
func ForeachPlugin(kind Kind, fn func(plugin *Plugin) error) error {
	plugins := pluginGlobal.plugins()
	if plugins == nil {
		return nil
	}
	for i := range plugins.plugins[kind] {
		p := &plugins.plugins[kind][i]
		if p.State != Ready {
			continue
		}
		err := fn(p)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetAll finds and returns all plugins.
func GetAll() map[Kind][]Plugin {
	plugins := pluginGlobal.plugins()
	if plugins == nil {
		return nil
	}
	return plugins.plugins
}

// NotifyFlush notify plugins to do flush logic.
func NotifyFlush(dom *domain.Domain, pluginName string) error {
	p := getByName(pluginName)
	if p == nil || p.Manifest.flushWatcher == nil || p.State != Ready {
		return errors.Errorf("plugin %s doesn't exists or unsupported flush or doesn't start with PD", pluginName)
	}
	_, err := dom.GetEtcdClient().KV.Put(context.Background(), p.Manifest.flushWatcher.path, "")
	if err != nil {
		return err
	}
	return nil
}

func getByName(pluginName string) *Plugin {
	for _, plugins := range GetAll() {
		for _, p := range plugins {
			if p.Name == pluginName {
				return &p
			}
		}
	}
	return nil
}

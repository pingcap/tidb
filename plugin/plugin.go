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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/v4/domain"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
	"github.com/pingcap/tidb/v4/util"
	"github.com/pingcap/tidb/v4/util/logutil"
	"go.etcd.io/etcd/clientv3"
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
		plugins:      make(map[Kind][]Plugin, len(p.plugins)),
		versions:     make(map[string]uint16, len(p.versions)),
		dyingPlugins: make([]Plugin, len(p.dyingPlugins)),
	}
	for key, value := range p.plugins {
		np.plugins[key] = append([]Plugin(nil), value...)
	}
	for key, value := range p.versions {
		np.versions[key] = value
	}
	copy(np.dyingPlugins, p.dyingPlugins)
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
	library  *gplugin.Plugin
	Path     string
	Disabled uint32
	State    State
}

// StateValue returns readable state string.
func (p *Plugin) StateValue() string {
	flag := "enable"
	if atomic.LoadUint32(&p.Disabled) == 1 {
		flag = "disable"
	}
	return p.State.String() + "-" + flag
}

// DisableFlag changes the disable flag of plugin.
func (p *Plugin) DisableFlag(disable bool) {
	if disable {
		atomic.StoreUint32(&p.Disabled, 1)
	} else {
		atomic.StoreUint32(&p.Disabled, 0)
	}
}

func (p *Plugin) validate(ctx context.Context, tiPlugins *plugins) error {
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
		versions:     make(map[string]uint16, len(cfg.EnvVersion)),
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
			if err = tiPlugins.plugins[kind][i].validate(ctx, tiPlugins); err != nil {
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
					plugin:   &tiPlugins.plugins[kind][i],
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
	plugin   *Plugin
}

func (w *flushWatcher) watchLoop() {
	watchChan := w.etcd.Watch(w.ctx, w.path)
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-watchChan:
			disabled, err := w.getPluginDisabledFlag()
			if err != nil {
				logutil.BgLogger().Error("get plugin disabled flag failure", zap.String("plugin", w.manifest.Name), zap.Error(err))
			}
			if disabled {
				atomic.StoreUint32(&w.manifest.flushWatcher.plugin.Disabled, 1)
			} else {
				atomic.StoreUint32(&w.manifest.flushWatcher.plugin.Disabled, 0)
			}
			err = w.manifest.OnFlush(w.ctx, w.manifest)
			if err != nil {
				logutil.BgLogger().Error("notify plugin flush event failed", zap.String("plugin", w.manifest.Name), zap.Error(err))
			}
		}
	}
}

func (w *flushWatcher) getPluginDisabledFlag() (bool, error) {
	if w == nil || w.etcd == nil {
		return true, errors.New("etcd is need to get plugin enable status")
	}
	resp, err := w.etcd.Get(context.Background(), w.manifest.flushWatcher.path)
	if err != nil {
		return true, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return false, nil
	}
	return string(resp.Kvs[0].Value) == "1", nil
}

type loadFn func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error)

var testHook *struct {
	loadOne loadFn
}

func loadOne(dir string, pluginID ID) (plugin Plugin, err error) {
	pName, pVersion, err := pluginID.Decode()
	if err != nil {
		err = errors.Trace(err)
		return
	}
	var manifest func() *Manifest
	if testHook == nil {
		manifest, err = loadManifestByGoPlugin(&plugin, dir, pluginID)
	} else {
		manifest, err = testHook.loadOne(&plugin, dir, pluginID)
	}
	if err != nil {
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

func loadManifestByGoPlugin(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error) {
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
	var ok bool
	manifest, ok = manifestSym.(func() *Manifest)
	if !ok {
		err = errInvalidPluginManifest.GenWithStackByArgs(string(pluginID))
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
				if p.OnShutdown == nil {
					continue
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
		if atomic.LoadUint32(&p.Disabled) == 1 {
			continue
		}
		err := fn(p)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsEnable checks plugin's enable state.
func IsEnable(kind Kind) bool {
	plugins := pluginGlobal.plugins()
	if plugins == nil {
		return false
	}
	for i := range plugins.plugins[kind] {
		p := &plugins.plugins[kind][i]
		if p.State == Ready && atomic.LoadUint32(&p.Disabled) != 1 {
			return true
		}
	}
	return false
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
	_, err := dom.GetEtcdClient().KV.Put(context.Background(), p.Manifest.flushWatcher.path, strconv.Itoa(int(p.Disabled)))
	if err != nil {
		return err
	}
	return nil
}

// ChangeDisableFlagAndFlush changes plugin disable flag and notify other nodes to do same change.
func ChangeDisableFlagAndFlush(dom *domain.Domain, pluginName string, disable bool) error {
	p := getByName(pluginName)
	if p == nil || p.Manifest.flushWatcher == nil || p.State != Ready {
		return errors.Errorf("plugin %s doesn't exists or unsupported flush or doesn't start with PD", pluginName)
	}
	disableInt := uint32(0)
	if disable {
		disableInt = 1
	}
	atomic.StoreUint32(&p.Disabled, disableInt)
	_, err := dom.GetEtcdClient().KV.Put(context.Background(), p.Manifest.flushWatcher.path, strconv.Itoa(int(disableInt)))
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

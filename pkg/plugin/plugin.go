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
	"maps"
	"path/filepath"
	gplugin "plugin"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
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
// nolint: unused
func (p *plugins) clone() *plugins {
	np := &plugins{
		plugins:      make(map[Kind][]Plugin, len(p.plugins)),
		versions:     maps.Clone(p.versions),
		dyingPlugins: make([]Plugin, len(p.dyingPlugins)),
	}
	for key, value := range p.plugins {
		np.plugins[key] = slices.Clone(value)
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
	if p.versions == nil {
		p.versions = make(map[string]uint16, 1)
	}
	p.versions[plugin.Name] = plugin.Version
}

// plugins got plugin in COW context.
func (p copyOnWriteContext) plugins() *plugins {
	return (*plugins)(atomic.LoadPointer(&p.tiPlugins))
}

// Config presents the init configuration for plugin framework.
type Config struct {
	Plugins      []string
	PluginDir    string
	SkipWhenFail bool
	EnvVersion   map[string]uint16
	EtcdClient   *clientv3.Client
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
		plugins: make(map[Kind][]Plugin),
		// Setup component version info for plugin running env.
		versions:     maps.Clone(cfg.EnvVersion),
		dyingPlugins: make([]Plugin, 0),
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
					//nolint: ineffassign
					err = nil
					continue
				}
				return
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
				if err = watcher.refreshPluginState(); err != nil {
					if cfg.SkipWhenFail {
						tiPlugins.plugins[kind][i].State = Disable
						err = nil
						go util.WithRecovery(watcher.watchLoop, nil)
						continue
					}
					return
				}
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

func (w *flushWatcher) refreshPluginState() error {
	disabled, err := w.getPluginDisabledFlag()
	if err != nil {
		logutil.BgLogger().Error("get plugin disabled flag failure", zap.String("plugin", w.manifest.Name), zap.Error(err))
		return err
	}
	if disabled {
		atomic.StoreUint32(&w.manifest.flushWatcher.plugin.Disabled, 1)
	} else {
		atomic.StoreUint32(&w.manifest.flushWatcher.plugin.Disabled, 0)
	}
	err = w.manifest.OnFlush(w.ctx, w.manifest)
	if err != nil {
		logutil.BgLogger().Error("plugin flush event failed", zap.String("plugin", w.manifest.Name), zap.Error(err))
		return err
	}
	return nil
}
func (w *flushWatcher) watchLoop() {
	const reWatchInterval = time.Second * 5
	logutil.BgLogger().Info("plugin flushWatcher loop started", zap.String("plugin", w.manifest.Name))
	for w.ctx.Err() == nil {
		ch := w.etcd.Watch(w.ctx, w.path)
		if exit := w.watchLoopWithChan(ch); exit {
			break
		}

		logutil.BgLogger().Info(
			"plugin flushWatcher old chan closed, restart loop later",
			zap.String("plugin", w.manifest.Name),
			zap.Duration("after", reWatchInterval))
		time.Sleep(reWatchInterval)
	}
}

func (w *flushWatcher) watchLoopWithChan(ch clientv3.WatchChan) (exit bool) {
	for {
		select {
		case <-w.ctx.Done():
			return true
		case _, ok := <-ch:
			if !ok {
				return false
			}
			logutil.BgLogger().Info("plugin flushWatcher detected event to reload plugin config", zap.String("plugin", w.manifest.Name))
			_ = w.refreshPluginState()
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

type staticPlugins struct {
	sync.Mutex
	plugins map[string]func() *Manifest
}

// Add adds a new plugin
func (p *staticPlugins) Add(pluginName string, plugin func() *Manifest) error {
	p.Lock()
	defer p.Unlock()
	if p.plugins == nil {
		p.plugins = make(map[string]func() *Manifest)
	}

	if _, ok := p.plugins[pluginName]; ok {
		return errors.Errorf("plugin with name '%s' has already been registered to static plugins", pluginName)
	}

	p.plugins[pluginName] = plugin
	return nil
}

// Get returns a registered plugin
func (p *staticPlugins) Get(pluginName string) (m func() *Manifest, ok bool) {
	p.Lock()
	m, ok = p.plugins[pluginName]
	p.Unlock()
	return
}

// Clear clears all registered plugins
func (p *staticPlugins) Clear() {
	p.Lock()
	p.plugins = nil
	p.Unlock()
}

// StaticPlugins is a registry to register plugins for other packages without loading go plugin so
var StaticPlugins staticPlugins

func loadOne(dir string, pluginID ID) (plugin Plugin, err error) {
	pName, pVersion, err := pluginID.Decode()
	if err != nil {
		err = errors.Trace(err)
		return
	}

	log.Info("start load plugin manifest", zap.String("plugin", string(pluginID)), zap.String("name", pName), zap.String("version", pVersion))
	var manifest func() *Manifest
	var fromStatic bool
	if manifest, fromStatic = StaticPlugins.Get(pName); fromStatic {
		log.Info("plugin discovered from static plugins, use static one", zap.String("plugin", string(pluginID)))
	} else if testHook == nil {
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
	if !fromStatic && strconv.Itoa(int(plugin.Version)) != pVersion {
		err = errInvalidPluginVersion.GenWithStackByArgs(string(pluginID))
		return
	}
	return
}

// SetTestHook for uint test in custom plugin.
func SetTestHook(fn func(plugin *Plugin, dir string, pluginID ID) (manifest func() *Manifest, err error)) {
	testHook = &struct{ loadOne loadFn }{loadOne: fn}
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

func (p *Plugin) supportsFlush(pluginName string) error {
	if p == nil {
		return errors.Errorf("plugin '%s' not found", pluginName)
	}
	if p.State != Ready {
		return errors.Errorf("plugin '%s' is not ready", pluginName)
	}
	if p.Manifest.flushWatcher == nil {
		return errors.Errorf("plugin %s does not support flush, or PD is not available", pluginName)
	}
	return nil
}

// NotifyFlush notify plugins to do flush logic.
func NotifyFlush(dom *domain.Domain, pluginName string) error {
	p := getByName(pluginName)
	if err := p.supportsFlush(pluginName); err != nil {
		return err
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
	if err := p.supportsFlush(pluginName); err != nil {
		return err
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

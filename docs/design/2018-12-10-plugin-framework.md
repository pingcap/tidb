# Proposal: Support Plugin

- Author(s):     [lysu](https://github.com/lysu)
- Last updated:  2018-12-10
- Discussion at:

## Abstract

This proposal proposes to introduce plugin framework to TiDB to support TiDB plugin development.

## Background

There are many cool customize requirements need be address but not convenient to merge to TiDB mainly repo, and Go 1.9+ introduce new plugin support, we can add plugin framework to TiDB to make them can be address, and attract more people to mutual build TiDB ecosystem.

## Proposal

Add a plugin framework to TiDB.

## Rationale

Add a plugin framework base on Go's plugin support, but support uniform plugin manifest, package and flexible SPI.

## Implementation

### Go Plugin

We build plugin framework based on Go's plugin support, so let's see "what is Go's plugin supported?" at first.

Go's plugin support is simple, just as document at https://golang.org/pkg/plugin/, we can build and use plugin in three step.

- build plugin via `go build -buildmode=plugin` in `main` package to make plugin `.so`
- using `plugin.Open` to `dlopen` plugin's `.so`
- using `plugin.Lookup` to `dlsym` to find symbol in plugin `.so`

There are another "undocument" but important concept: `pluginpath` also need in the spotlight, just as previous said we let our plugin code into `main` package then `go build -buildmode=plugin` to build a plugin, `pluginpath` is the package path for a plugin after plugin packaged. e.g. we have a method named `DoIt` and `pluginpath` be `pkg1` then we can use `nm` to see method name be `pluginpath.DoIt`. 

`pluginpath` can be given by `-ldflags -pluginpath=[path-value]` or generate by [go build](https://github.com/golang/go/blob/3b137dd2df19c261a007b8a620a2182cd679d700/src/cmd/go/internal/work/gc.go#L389)(for 1.11.1 be package name if build with package folder or be content hash if build with source file).

So we load a Go plugin with same pluginpath twice, second `Load` call will got a error, Go plugin use `pluginpath` to detect duplicate load.

Last things we need take care is Go plugin's dependence, at first, almost plugins need depend on TiDB code to do its logic. Go runtime require runtime hash and link time hash for dependence package is equal. so we no need take care plugin depend on TiDB some component but its code changed, and we need release new plugin whenever TiDB new version be released.

### TiDB Plugin

Go plugin give us a good start point, but we need do something to let plugin be more uniform and easy to use.

#### Manifest

Addition to Go plugin, we need some meta info to self-describe plugin, then TiDB can know how to work with plugin, we need those info:

- Plugin name: we need reload plugin, so we need load same plugin with different version that more higher level then `pluginpath`.
- Plugin version: plugin version make us more easy to maintenance.
- Simple Dependence check: Go help use check build version, but in real world that's common a plugin rely on b plugin's some new logic, we try to maintenance a simple Required-Version relationship between different plugins.
- Configuration: plugin is a standalone module, so every plugin will introduce special sysVars that just like normal MySQL variables, user can use those variables to tweak plugin behaviors just like normal MySQL variables does.
- Stats: plugin will introduce new stats info, TiDB use prometheus, so plugin can take free to push metric to prometheus.
- Plugin Category and Flexible SPI: TiDB can have limited plugin category and each of them accept same but abstract SPI that need plugin to implement.

All of above construct the plugin metadata or we normally call it --- `Manifest`, it describe the metadata and how other can use it by plugin itself.

We just use Go plugin to load plugin and plugin give us a `Manifest`, then just use manifest to interact with plugin.(Only load/lookup is heavy cgo call, later call manifest is normal golang method call).

#### SPI

`Manifest` is base struct for all other sub manifest, caller can use `Kind` and `DeclareXXManifest` to convert them to sub manifest.

Manifest provides common meta data:

- Kind: plugin's category, now we have audit, authentication..and easy to add more.
- Name: name for plugin that use to identity plugin, so it can not duplicate with other plugin.
- Version: one plugin we can load multiple versions into TiDB but just active one of them to support hot-fix or hot-upgrade
- RequireVersion: will make a simple relationship between different plugins
- SysVars: define the plugin's configuration info

Manifest also provide three lifecycle extension point: 

- Validate: called after load all plugins but before onInit, so it can do cross plugins check before init.
- OnInit: let plugin can prepare resource before real work.
- OnShutDown: let plugin can cleanup some outer resources before die.

so we can image a common manifest code like this:

```
type Manifest struct {
	Kind           Kind
	Name           string
	Description    string
	Version        uint16
	RequireVersion map[string]uint16
	License        string
	BuildTime      string
	SysVars        map[string]*variable.SysVar
	Validate       func(ctx context.Context, manifest *Manifest) error
	OnInit         func(ctx context.Context) error
	OnShutdown     func(ctx context.Context) error
}
```

base on `Kind` we can define other subManifest for authentication plugin, audit plugin and so on.

Every subManifest will have a `Manifest` anonymous field as FIRST field in struct definition, so every subManifest can use as `Manifest`(by `unsafe.Pointer` cast), for example a audit plugin' manifest will like this:

```
type AuditManifest struct {
	Manifest
	NotifyEvent func(ctx context.Context) error
}
```

Why we chose embedded struct + unsafe.Pointer cast instead of interface way in here is first way is more flexible then fixed interface and more efficient to access data member, at last we also provide the package tools and helper method to hide those detail to plugin developer. 

#### Package tool

In this proposal, we add a simple tool `cmd/pluginpkg` to help use package a plugin, and also uniform the package format.

Plugin's develop event no longer need take care previous Manifest and so on, developer just provide a `manifest.toml` configuration file like this in package:

```
name = "conn_ip_example"
kind = "Audit"
description = "just a test"
version = "2"
license = ""
sysVars = [
    {name="conn_ip_example_test_variable", scope="Global", value="2"},
    {name="conn_ip_example_test_variable2", scope="Session", value="2"},
]
validate = "Validate"
onInit = "OnInit"
onShutdown = "OnShutdown"
export = [
    {extPoint="NotifyEvent", impl="NotifyEvent"}
]
```

- name: name of plugin, it must be unique in loaded TiDB instance
- kind: kind of plugin, it will determine the call-point in TiDB, package tool also base on it to generate different manifest
- version: version of plugin, for same plugin and same version only be load once
- description: description of plugin usage
- license: license of plugin, it will display in `show plugins`'s result
- sysVars: define the variable needed by this plugin with name, scope and default value.
- validate: specify the callback function used to validate before load, e.g. auth plugin check `-with-skip-grant-tables` configuration
- onInit: specify the callback function used to init plugin before it join real work.
- onShutdown: the callback function will be called when plugin shutdown to release outer resource hold by plugin, normally TiDB shutdown.
- export: define callback list for the special kind plugins, e.g. for auth plugin it use a `NotifyEvent` method to implement `notifyEvent` extension point.

`pluginpkg` will generate code and build as Go plugin, using plugin pkg we also control the plugin binary's format:

- plugin file name be `[pluginName]-[version].so`, so we can know plugin's version from filename.
- `pluginpath` will be `[pluginName]-[version]`, then we can load same plugin in different version in same host program
- package tool also add some build time and misc info into Manifest info

Package tools adds a abstract layer over manifest, so we can change manifest easier in future if needed. 

#### Plugin Point

In TiDB code, we can add new plugin point in everywhere and:

- call `plugin.GetByKind` or `plugin.Get` to find matched plugins
- call `plugin.Declare[Kind]Manifest` to cast Manifest to special kind
- call extension point method for special manifest

we can see a simple example in `clientConn#Run` and `conn_ip_example` plugin implement. 

#### Configuration

Every plugin has its own configurations, TiDB plugin use system variable to handle configuration management requirement.

In `manifest.toml`, we can use `sysVar` field to provide plugin's variable name and its default value. Plugin's system variable will be registered as TiDB system variable, so user can read/modify variable just like normal system variable.

Plugin's variable name must use plugin name as prefix. at last, plugin can not be reload if we change plugin's sysVar(include default value, add or remove variable)

We implement it by add plugin variable into `variable.SysVars` before `bootstrap`, so later `doDMLWorker` will handle them just as normal sysVars, and change `loadCommonGlobalVarsSQL` to load them.(can not unload plugin and can not modify sysVar during reload make this implement easier) 

#### Dependency

Go's plugin mechanism will check all dependency package hash to ensure link time and run time use some version([see code](https://github.com/golang/go/blob/50bd1c4d4eb4fac8ddeb5f063c099daccfb71b26/src/runtime/plugin.go#L52)), so we no longer need to take care compile package dependency.

but for real world, there are maybe logic dependency between plugin. for example, some guy write a authorization plugin but it rely on vault plugin and only works if vault enable but isn't direct rely on vault plugin's source code.

In `manifest.toml`, we can use `requireVersion` to declare A plugin require B plugin in X version, then plugin runtime will check it during load or reload phase.

### Reload

Go plugin doesn't support unload a plugin, but it can not stop us to load multiple version plugin into host program and framework ensure last reload one will be active, and others isn't unload but disable.

So, we can reload plugin with different version that be packaged by `pluginpkg` to modify plugin's implement logic, although we can not change the plugin's meta info(e.g. sysVars) now, I think it's still useful.

#### Management

For add a plugin to TiDB, we need:

- add `-plugin-dir` as start argument to specify the folder contains plugins, e.g. '-plugin-dir=/data/deploy/tidb/plugin'
- add `-plugin-load` as start argument to specify the plugin id(name "-" version) that need be load, e.g. '-plugin-load=conn_limit-1'

then start TiDB will load and enable plugins.

we can see all plugin info by:

```
mysql> show plugins;
+-----------------+--------+-------+----------------------------------------------------+---------+---------+
| Name            | Status | Type  | Library                                            | License | Version |
+-----------------+--------+-------+----------------------------------------------------+---------+---------+
| conn_limit-1    | Ready  | Audit | /data/deploy/tidb/plugin/conn_limit-1.so           |         | 1       |
+-----------------+--------+-------+----------------------------------------------------+---------+---------+
1 row in set (0.00 sec)
```

to reload a load plugin, just use

```
mysql> admin plugins reload conn_limit-2;
```

### Limitations

there are some limit in plugin, so it can not do:

- unload plugin, once plugin load into TiDB never be unload until server restarted, but we can reload plugin in limited situation to hot fix plugin bug.
- read sysVars in OnInit will got unexpected value, but can access `Manifest` to got default value
- reload can not change the sysVar's default value or add/remove variable 
- build plugin need TiDB source code tree, it's different to MySQL that can build plugin standalone(expect Information Schema and Storage Engine plugins)


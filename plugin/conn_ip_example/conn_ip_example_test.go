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

package main

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
)

type testConnIPExampleSuite struct{}

var _ = SerialSuites(&testConnIPExampleSuite{})

func (s *testConnIPExampleSuite) TestLoadPlugin(c *C) {
	ctx := context.Background()
	pluginName := "conn_ip_example"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := plugin.Config{
		Plugins:        []string{pluginSign},
		PluginDir:      "",
		PluginVarNames: &variable.PluginVarNames,
		EnvVersion:     map[string]uint16{"go": 1112},
	}

	// setup load test hook.
	loadOne := func(p *plugin.Plugin, dir string, pluginID plugin.ID) (manifest func() *plugin.Manifest, err error) {
		return func() *plugin.Manifest {
			m := &plugin.AuditManifest{
				Manifest: plugin.Manifest{
					Kind:       plugin.Audit,
					Name:       pluginName,
					Version:    pluginVersion,
					SysVars:    map[string]*variable.SysVar{pluginName + "_key": {Scope: variable.ScopeGlobal, Name: pluginName + "_key", Value: "v1"}},
					OnInit:     OnInit,
					OnShutdown: OnShutdown,
					Validate:   Validate,
				},
				OnGeneralEvent:    OnGeneralEvent,
				OnConnectionEvent: OnConnectionEvent,
			}
			return plugin.ExportManifest(m)
		}, nil
	}
	plugin.SetTestHook(loadOne)

	// trigger load.
	err := plugin.Load(ctx, cfg)
	if err != nil {
		log.Fatal(fmt.Sprintf("load plugin [%s] fail, error [%s]\n", pluginSign, err))
	}

	err = plugin.Init(ctx, cfg)
	if err != nil {
		log.Fatal(fmt.Sprintf("init plugin [%s] fail, error [%s]\n", pluginSign, err))
	}

	err = plugin.ForeachPlugin(plugin.Audit, func(auditPlugin *plugin.Plugin) error {
		plugin.DeclareAuditManifest(auditPlugin.Manifest).OnGeneralEvent(context.Background(), nil, plugin.Log, "QUERY")
		return nil
	})
	if err != nil {
		log.Fatal(fmt.Sprintf("query event fail, error [%s]\n", err))
	}

	connectionNum := 5
	for i := 0; i < connectionNum; i++ {
		err = plugin.ForeachPlugin(plugin.Audit, func(auditPlugin *plugin.Plugin) error {
			return plugin.DeclareAuditManifest(auditPlugin.Manifest).OnConnectionEvent(context.Background(), plugin.Connected, &variable.ConnectionInfo{Host: "localhost"})
		})
		if err != nil {
			log.Fatal(fmt.Sprintf("OnConnectionEvent error [%s]\n", err))
		}
	}
	// accumulator of connection must be connectionNum(5).
	c.Assert(atomic.LoadInt32(&connection), Equals, int32(connectionNum))
	plugin.Shutdown(context.Background())
	// after shutdown, accumulator of connection must be clear.
	c.Assert(atomic.LoadInt32(&connection), Equals, int32(0))

	// Output:
	//## conn_ip_example Validate called ##
	//---- context: context.Background
	//---- read cfg in validate [key: conn_ip_example_key, value: v1]
	//## conn_ip_example OnInit called ##
	//---- context: context.Background
	//---- read cfg in init [key: conn_ip_example_key, value: v1]
	//## conn_ip_example OnGeneralEvent called ##
	//---- new connection by %!s(<nil>)
	//---- event: Log
	//---- cmd: QUERY
	//## conn_ip_example onConnectionEvent called ##
	//---- conenct event: Connected, reason: []
	//---- connection host: localhost
	//## conn_ip_example onConnectionEvent called ##
	//---- conenct event: Connected, reason: []
	//---- connection host: localhost
	//## conn_ip_example onConnectionEvent called ##
	//---- conenct event: Connected, reason: []
	//---- connection host: localhost
	//## conn_ip_example onConnectionEvent called ##
	//---- conenct event: Connected, reason: []
	//---- connection host: localhost
	//## conn_ip_example onConnectionEvent called ##
	//---- conenct event: Connected, reason: []
	//---- connection host: localhost
	//## conn_ip_examples OnShutdown called ##
	//---- context: context.Background
	//---- read cfg in shutdown [key: conn_ip_example_key, value: v1]
}

func TestT(t *testing.T) {
	TestingT(t)
}

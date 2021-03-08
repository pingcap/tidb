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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
	"strconv"
	"testing"
)

type testConnIpExampleSuite struct{}

var _ = SerialSuites(&testConnIpExampleSuite{})

func (s *testConnIpExampleSuite) TestLoadPlugin(c *C) {
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
		fmt.Printf("load plugin [%s] fail\n", pluginSign)
		fmt.Print(err)
	}

	err = plugin.Init(ctx, cfg)
	if err != nil {
		fmt.Printf("init plugin [%s] fail\n", pluginSign)
	}

	err = plugin.ForeachPlugin(plugin.Audit, func(auditPlugin *plugin.Plugin) error {
		plugin.DeclareAuditManifest(auditPlugin.Manifest).OnGeneralEvent(context.Background(), nil, plugin.Log, "QUERY")
		return nil
	})
	connectionNum := 5
	for i := 0; i < connectionNum; i++ {
		err = plugin.ForeachPlugin(plugin.Audit, func(auditPlugin *plugin.Plugin) error {
			err = plugin.DeclareAuditManifest(auditPlugin.Manifest).OnConnectionEvent(context.Background(), plugin.Connected, &variable.ConnectionInfo{Host: "localhost"})
			if err != nil {
				fmt.Printf("OnConnectionEvent error [%s]\n", err)
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	// accumulator of connection must be connectionNum(5).
	c.Assert(connection, Equals, connectionNum)
	plugin.Shutdown(context.Background())
	// after shutdown, accumulator of connection must be clear.
	c.Assert(connection, Equals, 0)

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
	//## conn_ip_examples OnShutdown called ##
	//---- context: context.Background
	//---- read cfg in shutdown [key: conn_ip_example_key, value: v1]
}

func TestT(t *testing.T) {
	TestingT(t)
}

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

package main_test

import (
	"context"

	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/sessionctx/variable"
)

func ExampleLoadRunShutdownPlugin() {
	ctx := context.Background()
	var pluginVarNames []string
	cfg := plugin.Config{
		Plugins:        []string{"conn_ip_example-1"},
		PluginDir:      "/home/robi/Code/go/src/github.com/pingcap/tidb/plugin/conn_ip_example",
		GlobalSysVar:   &variable.SysVars,
		PluginVarNames: &pluginVarNames,
	}

	err := plugin.Init(ctx, cfg)
	if err != nil {
		panic(err)
	}

	ps := plugin.GetByKind(plugin.Audit)
	for _, auditPlugin := range ps {
		if auditPlugin.State != plugin.Ready {
			continue
		}
		plugin.DeclareAuditManifest(auditPlugin.Manifest).NotifyEvent(context.Background(), nil)
	}

	plugin.Shutdown(context.Background())
}

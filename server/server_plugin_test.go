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

package server

import (
	"context"
	"database/sql"
	"strconv"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
)

type ServerPluginTestSuite struct {
	server  *Server
	store   kv.Storage
	domain  *domain.Domain
	tidbdrv *TiDBDriver
}

var _ = Suite(new(ServerPluginTestSuite))

func (ts *ServerPluginTestSuite) SetUpSuite(c *C) {
	initPlugin(c)
	var err error
	ts.store, err = mockstore.NewMockTikvStore()
	session.DisableStats4Test()
	c.Assert(err, IsNil)
	ts.domain, err = session.BootstrapSession(ts.store)
	c.Assert(err, IsNil)
	ts.tidbdrv = NewTiDBDriver(ts.store)
	cfg := config.NewConfig()
	cfg.Port = 4001
	cfg.Status.ReportStatus = true
	cfg.Status.StatusPort = 10090
	cfg.Performance.TCPKeepAlive = true

	server, err := NewServer(cfg, ts.tidbdrv)
	c.Assert(err, IsNil)
	ts.server = server
	go ts.server.Run()
	waitUntilServerOnline(cfg.Status.StatusPort)
}

func (ts *ServerPluginTestSuite) TearDownSuite(c *C) {
	ts.server.KillAllConnections()
	ts.server.TryGracefulDown()
	if ts.domain != nil {
		ts.domain.Close()
	}
	if ts.store != nil {
		ts.store.Close()
	}
	if ts.server != nil {
		ts.server.Close()
	}
	plugin.Shutdown(context.Background())
}

func (ts *ServerPluginTestSuite) TestPlugin(c *C) {
	db, err := sql.Open("mysql", getDSN())
	c.Assert(err, IsNil, Commentf("Error connecting"))
	defer db.Close()
	dbt := &DBTest{c, db}
	res := dbt.mustExec("use test")
	count, err := res.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(count, Equals, int64(10090))
}

func initPlugin(c *C) {
	ctx := context.Background()

	pluginName := "tplugin"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	cfg := plugin.Config{
		Plugins:        []string{pluginSign},
		PluginDir:      "",
		GlobalSysVar:   &variable.SysVars,
		PluginVarNames: &variable.PluginVarNames,
		EnvVersion:     map[string]uint16{"go": 1112},
	}
	var testHook plugin.LoadFn

	hook := func() *plugin.Manifest {
		affectedRowsPlugin := &plugin.AuditManifest{
			Manifest: plugin.Manifest{
				Kind:    plugin.Audit,
				Name:    pluginName,
				Version: pluginVersion,
				SysVars: map[string]*variable.SysVar{pluginName + "_key": {Scope: variable.ScopeGlobal, Name: pluginName + "_key", Value: "v1"}},
				OnInit: func(ctx context.Context, manifest *plugin.Manifest) error {
					return nil
				},
				OnShutdown: func(ctx context.Context, manifest *plugin.Manifest) error {
					return nil
				},
				Validate: func(ctx context.Context, manifest *plugin.Manifest) error {
					return nil
				},
			},
			OnGeneralEvent: func(ctx context.Context, sctx *variable.SessionVars, event plugin.GeneralEvent, cmd string) {
				sctx.StmtCtx.AddAffectedRows(10090)
			},
			OnConnectionEvent: func(ctx context.Context, identity *auth.UserIdentity, event plugin.ConnectionEvent, info *variable.ConnectionInfo) error {
				return nil
			},
		}
		return plugin.ExportManifest(affectedRowsPlugin)
	}

	testHook = func(plugin *plugin.Plugin, dir string, pluginID plugin.ID) (manifest func() *plugin.Manifest, err error) {
		return hook, nil
	}
	// setup load test hook.
	plugin.SetTestHook(testHook)

	// trigger load.
	err := plugin.Load(ctx, cfg)
	c.Assert(err, IsNil)

	err = plugin.Init(ctx, cfg)
	c.Assert(err, IsNil)

	// load all.
	ps := plugin.GetAll()
	c.Assert(len(ps), Equals, 1)
}

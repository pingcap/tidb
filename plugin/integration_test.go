// Copyright 2021 PingCAP, Inc.
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

package plugin_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/plugin"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

type testAuditLogSuite struct {
	cluster testutils.Cluster
	store   kv.Storage
	dom     *domain.Domain

	bytes.Buffer
}

func (s *testAuditLogSuite) setup(t *testing.T) {
	pluginName := "test_audit_log"
	pluginVersion := uint16(1)
	pluginSign := pluginName + "-" + strconv.Itoa(int(pluginVersion))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.Plugin.Load = pluginSign
	})

	// setup load test hook.
	loadOne := func(p *plugin.Plugin, dir string, pluginID plugin.ID) (manifest func() *plugin.Manifest, err error) {
		return func() *plugin.Manifest {
			m := &plugin.AuditManifest{
				Manifest: plugin.Manifest{
					Kind:       plugin.Audit,
					Name:       pluginName,
					Version:    pluginVersion,
					OnInit:     OnInit,
					OnShutdown: OnShutdown,
					Validate:   Validate,
				},
				OnGeneralEvent:    s.OnGeneralEvent,
				OnConnectionEvent: OnConnectionEvent,
			}
			return plugin.ExportManifest(m)
		}, nil
	}
	plugin.SetTestHook(loadOne)

	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	require.NoError(t, err)
	s.store = store
	session.SetSchemaLease(0)
	session.DisableStats4Test()

	d, err := session.BootstrapSession(s.store)
	require.NoError(t, err)
	d.SetStatsUpdating(true)
	s.dom = d
}

func (s *testAuditLogSuite) teardown() {
	s.dom.Close()
	s.store.Close()
}

func TestAuditLog(t *testing.T) {
	var s testAuditLogSuite
	s.setup(t)
	defer s.teardown()

	var buf1 bytes.Buffer
	tk := testkit.NewAsyncTestKit(t, s.store)
	ctx := tk.OpenSession(context.Background(), "test")
	buf1.WriteString("Use use `test`\n") // Workaround for the testing framework.

	tk.MustExec(ctx, "use test")
	buf1.WriteString("Use use `test`\n")

	tk.MustExec(ctx, "create table t (id int primary key, a int, b int unique)")
	buf1.WriteString("CreateTable create table `t` ( `id` int primary key , `a` int , `b` int unique )\n")

	tk.MustExec(ctx, "create view v1 as select * from t where id > 2")
	buf1.WriteString("CreateView create view `v1` as select * from `t` where `id` > ?\n")

	tk.MustExec(ctx, "drop view v1")
	buf1.WriteString("DropView drop view `v1`\n")

	tk.MustExec(ctx, "create session binding for select * from t where b = 123 using select * from t ignore index(b) where b = 123")
	buf1.WriteString("CreateBinding create session binding for select * from `t` where `b` = ? using select * from `t` where `b` = ?\n")

	tk.MustExec(ctx, "prepare mystmt from 'select ? as num from DUAL'")
	buf1.WriteString("Prepare prepare `mystmt` from ?\n")

	tk.MustExec(ctx, "set @number = 5")
	buf1.WriteString("Set set @number = ?\n")

	tk.MustExec(ctx, "execute mystmt using @number")
	buf1.WriteString("Select select ? as `num` from dual\n")

	tk.MustQuery(ctx, "trace format = 'row' select * from t")
	buf1.WriteString("Trace trace format = ? select * from `t`\n")

	tk.MustExec(ctx, "shutdown")
	buf1.WriteString("Shutdown shutdown\n")

	require.Equal(t, buf1.String(), s.Buffer.String())
}

func Validate(ctx context.Context, m *plugin.Manifest) error {
	return nil
}

// OnInit implements TiDB plugin's OnInit SPI.
func OnInit(ctx context.Context, manifest *plugin.Manifest) error {
	return nil
}

// OnShutdown implements TiDB plugin's OnShutdown SPI.
func OnShutdown(ctx context.Context, manifest *plugin.Manifest) error {
	return nil
}

// OnGeneralEvent implements TiDB Audit plugin's OnGeneralEvent SPI.
func (s *testAuditLogSuite) OnGeneralEvent(ctx context.Context, sctx *variable.SessionVars, event plugin.GeneralEvent, cmd string) {
	if sctx != nil {
		normalized, _ := sctx.StmtCtx.SQLDigest()
		fmt.Fprintln(&s.Buffer, sctx.StmtCtx.StmtType, normalized)
	}
}

// OnConnectionEvent implements TiDB Audit plugin's OnConnectionEvent SPI.
func OnConnectionEvent(ctx context.Context, event plugin.ConnectionEvent, info *variable.ConnectionInfo) error {
	return nil
}

// Copyright 2026 PingCAP, Inc.
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

package admintest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/kvproto/pkg/meta_storagepb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/client/opt"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
)

type ruVersionResourceGroupProvider struct {
	rmclient.ResourceGroupProvider
	config *rmclient.Config
}

func (p *ruVersionResourceGroupProvider) Get(context.Context, []byte, ...opt.MetaStorageOption) (*meta_storagepb.GetResponse, error) {
	value, err := json.Marshal(p.config)
	if err != nil {
		return nil, err
	}
	return &meta_storagepb.GetResponse{
		Kvs: []*meta_storagepb.KeyValue{{Value: value}},
	}, nil
}

func setDomainRUVersionForTest(t *testing.T, dom *domain.Domain, version rmclient.RUVersion) {
	t.Helper()
	cfg := rmclient.DefaultConfig()
	cfg.RUVersionPolicy = &rmclient.RUVersionPolicy{Default: version}
	baseProvider, ok := infosync.NewMockResourceManagerClient(1).(rmclient.ResourceGroupProvider)
	require.True(t, ok)
	controller, err := rmclient.NewResourceGroupController(
		context.Background(), 1,
		&ruVersionResourceGroupProvider{ResourceGroupProvider: baseProvider, config: cfg},
		nil, 1,
	)
	require.NoError(t, err)
	oldController := dom.ResourceGroupsController()
	t.Cleanup(func() {
		dom.SetResourceGroupsController(oldController)
	})
	dom.SetResourceGroupsController(controller)
}

func TestAdminShowDDLJobsRU(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("DDL job RU is only displayed in NextGen")
	}
	store, dom := testkit.CreateMockStoreAndDomain(t)
	setDomainRUVersionForTest(t, dom, rmclient.RUVersionV2)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_admin_show_ddl_jobs_ru (a int)")
	tk.MustExec("alter table t_admin_show_ddl_jobs_ru add column b int")

	row := tk.MustQuery("admin show ddl jobs 1").Rows()[0]
	require.Equal(t, "add column", row[3])
	jobID, err := strconv.ParseInt(row[0].(string), 10, 64)
	require.NoError(t, err)
	job, err := ddl.GetHistoryJobByID(tk.Session(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Positive(t, job.RU)
	require.Equal(t, fmt.Sprintf("RU=%.2f", job.RU), row[12])

	tk.MustExec("alter table t_admin_show_ddl_jobs_ru add column c int, add column d int")
	rows := tk.MustQuery("admin show ddl jobs 1").Rows()
	require.Len(t, rows, 3)
	jobID, err = strconv.ParseInt(rows[0][0].(string), 10, 64)
	require.NoError(t, err)
	job, err = ddl.GetHistoryJobByID(tk.Session(), jobID)
	require.NoError(t, err)
	require.NotNil(t, job)
	require.Equal(t, fmt.Sprintf("RU=%.2f", job.RU), rows[0][12])
	for _, subjobRow := range rows[1:] {
		require.Contains(t, subjobRow[3], "/* subjob */")
		require.Empty(t, subjobRow[12])
	}
}

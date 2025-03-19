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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/mock"
	pdhttp "github.com/tikv/pd/client/http"
)

type MockDistributePDCli struct {
	pdhttp.Client
	mock.Mock
	jobs []map[string]any
}

func (cli *MockDistributePDCli) GetSchedulerConfig(ctx context.Context, schedulerName string) (any, error) {
	args := cli.Called(ctx, schedulerName)
	return cli.jobs, args.Error(1)
}

func (cli *MockDistributePDCli) CreateSchedulerWithInput(ctx context.Context, name string, input map[string]any) error {
	args := cli.Called(ctx, name, input)
	input["job-id"] = uint64(len(cli.jobs) + 1)
	cli.jobs = append(cli.jobs, input)
	return args.Error(0)
}

func TestShowDistributionJobs(t *testing.T) {
	re := require.New(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cli := &MockDistributePDCli{}
	recoverCli := infosync.SetPDHttpCliForTest(cli)
	defer recoverCli()
	mockGetSchedulerConfig := func(schedulerName string) *mock.Call {
		return cli.On("GetSchedulerConfig", mock.Anything, schedulerName).
			Return(nil, nil)
	}
	job := map[string]any{}
	job["job-id"] = uint64(1)
	job["alias"] = strings.Join([]string{"test", "test", "partition(P0,P1)"}, ".")
	job["engine"] = "tikv"
	job["rule"] = "leader"
	job["create-time"] = time.Now().Add(-time.Minute)
	job["start-time"] = time.Now().Add(-time.Second * 30)
	job["finish-time"] = time.Now().Add(-time.Second * 10)
	job["status"] = "finish"
	jobs := make([]map[string]any, 0)
	jobs = append(jobs, job)
	cli.jobs = jobs
	mockGetSchedulerConfig("balance-range-scheduler")

	ret := tk.MustQuery("show distribution jobs").Rows()
	re.Len(ret, 1)
	re.Len(ret[0], 10)

	tk.MustQuery("show distribution jobs where `job_id`=1").Check(tk.MustQuery("show distribution job 1").Rows())
	tk.MustQuery("show distribution jobs where `job_id`=0").Check(tk.MustQuery("show distribution job 0").Rows())
}

func TestDistributeTable(t *testing.T) {
	re := require.New(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	database := "test"
	tk.MustExec(fmt.Sprintf("use %s", database))

	cli := &MockDistributePDCli{}
	recoverCli := infosync.SetPDHttpCliForTest(cli)
	defer recoverCli()
	mockCreateSchedulerWithInput := func(tblName, partition string, schedulerName string, config map[string]any) *mock.Call {
		is := tk.Session().GetDomainInfoSchema()
		tbl, err := is.TableInfoByName(ast.NewCIStr(database), ast.NewCIStr(tblName))
		require.NoError(t, err)
		tblID := tbl.ID
		if partition != "" {
			tblID = tbl.GetPartitionInfo().GetPartitionIDByName(partition)
		}

		startKey, endKey := tablecodec.GetTableHandleKeyRange(tblID)
		config["start-key"] = startKey
		config["end-key"] = endKey
		return cli.On("CreateSchedulerWithInput", mock.Anything, schedulerName, config).
			Return(nil)
	}
	mockGetSchedulerConfig := func(schedulerName string) *mock.Call {
		return cli.On("GetSchedulerConfig", mock.Anything, schedulerName).
			Return(nil, nil)
	}
	table := "t1"
	partition := ""

	alias := strings.Join([]string{database, table, partition}, ".")
	config := map[string]any{
		"alias":  alias,
		"engine": "tikv",
		"rule":   "leader",
	}
	tk.MustExec("create table t1(a int)")
	mockCreateSchedulerWithInput(table, partition, "balance-range-scheduler", config)
	mockGetSchedulerConfig("balance-range-scheduler")
	ret := tk.MustQuery(fmt.Sprintf("distribute table %s rule=leader engine=tikv", table)).Rows()
	re.Len(ret, 1)
	re.Equal("1", ret[0][0].(string))
}

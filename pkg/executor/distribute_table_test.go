// Copyright 2025 PingCAP, Inc.
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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
	now := time.Now()
	job["create-time"] = now.Add(-time.Minute)
	job["start-time"] = now.Add(-time.Second * 30)
	job["finish-time"] = now.Add(-time.Second * 10)
	job["status"] = "finish"
	jobs := make([]map[string]any, 0)
	jobs = append(jobs, job)
	cli.jobs = jobs
	mockGetSchedulerConfig("balance-range-scheduler")

	tk.MustQuery("show distribution jobs").Check(testkit.Rows(
		fmt.Sprintf("1 test test partition(P0,P1) tikv leader finish %s %s %s",
			now.Add(-time.Minute).Format("2006-01-02 15:04:05"),
			now.Add(-time.Second*30).Format("2006-01-02 15:04:05"),
			now.Add(-time.Second*10).Format("2006-01-02 15:04:05"))))

	tk.MustQuery("show distribution jobs where `job_id`=1").Check(tk.MustQuery("show distribution job 1").Rows())
	tk.MustQuery("show distribution jobs where `job_id`=0").Check(tk.MustQuery("show distribution job 0").Rows())
}

func TestDistributeTable(t *testing.T) {
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
	tk.MustQuery(fmt.Sprintf("distribute table %s rule=leader engine=tikv", table)).Check(testkit.Rows("1"))

	// test for incorrect arguments
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule=leader engine=tiflash", table),
		"[planner:1210]Incorrect arguments to tiflash only supports learner")
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule=leader engine=titan", table),
		"[planner:1210]Incorrect arguments to engine must be one of tikv, tiflash")
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule=witness engine=tikv", table),
		"[planner:1210]Incorrect arguments to rule must be one of leader, follower, learner")
}

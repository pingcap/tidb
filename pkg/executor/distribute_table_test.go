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
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/codec"
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
	data, _ := json.Marshal(cli.jobs)
	var jobList any
	json.Unmarshal(data, &jobList)
	return jobList, args.Error(1)
}

func (cli *MockDistributePDCli) CreateSchedulerWithInput(ctx context.Context, name string, input map[string]any) error {
	args := cli.Called(ctx, name, input)
	input["job-id"] = uint64(len(cli.jobs) + 1)
	input["status"] = "pending"
	cli.jobs = append(cli.jobs, input)
	return args.Error(0)
}

func (cli *MockDistributePDCli) GetRegionDistributionByKeyRange(ctx context.Context, keyRange *pdhttp.KeyRange, engine string) (*pdhttp.RegionDistributions, error) {
	args := cli.Called(ctx, keyRange, engine)
	return args.Get(0).(*pdhttp.RegionDistributions), args.Error(1)
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
	mockGetSchedulerConfig("balance-range-scheduler")
	require.Empty(t, tk.MustQuery("show distribution jobs").Rows())

	job := map[string]any{}
	job["job-id"] = float64(1)
	job["alias"] = strings.Join([]string{"test", "test", "partition(P0,P1)"}, ".")
	job["engine"] = "tikv"
	job["rule"] = "leader"
	now := time.Now()
	layout := "2006-01-02T15:04:05.999999-07:00"
	job["create"] = now.Add(-time.Minute).Format(layout)
	job["start"] = now.Add(-time.Second * 30).Format(layout)
	job["status"] = "finish"
	jobs := make([]map[string]any, 0)
	jobs = append(jobs, job)
	cli.jobs = jobs

	tk.MustQuery("show distribution jobs").Check(testkit.Rows(
		fmt.Sprintf("1 test test partition(P0,P1) tikv leader finish %s %s <nil>",
			now.Add(-time.Minute).Format("2006-01-02 15:04:05"),
			now.Add(-time.Second*30).Format("2006-01-02 15:04:05"))))

	tk.MustQuery("show distribution jobs where `job_id`=1").Check(tk.MustQuery("show distribution jobs").Rows())
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

		startKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(tblID))
		endKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(tblID+1))
		config["start-key"] = url.QueryEscape(string(startKey))
		config["end-key"] = url.QueryEscape(string(endKey))
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
	mockGetSchedulerConfig("balance-range-scheduler")
	mockCreateSchedulerWithInput(table, partition, "balance-range-scheduler", config)
	tk.MustQuery(fmt.Sprintf("distribute table %s rule=leader engine=tikv", table)).Check(testkit.Rows("1"))
	// create new scheduler with the same inputs
	mockCreateSchedulerWithInput(table, partition, "balance-range-scheduler", config)
	tk.MustQuery(fmt.Sprintf("distribute table %s rule=leader engine=tikv", table)).Check(testkit.Rows("2"))

	// test for incorrect arguments
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule=leader engine=tiflash", table),
		"[planner:1210]Incorrect arguments to the rule of tiflash must be learner")
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule=leader engine=titan", table),
		"[planner:1210]Incorrect arguments to engine must be tikv or tiflash")
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule=witness engine=tikv", table),
		"[planner:1210]Incorrect arguments to rule must be leader, follower or learner")
}

func TestShowTableDistributions(t *testing.T) {
	re := require.New(t)
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cli := &MockDistributePDCli{}
	recoverCli := infosync.SetPDHttpCliForTest(cli)
	defer recoverCli()
	mockGetDistributions := func(tblName, partition string, distributions *pdhttp.RegionDistributions) *mock.Call {
		is := tk.Session().GetDomainInfoSchema()
		tbl, err := is.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr(tblName))
		require.NoError(t, err)
		tblID := tbl.ID
		if partition != "" {
			tblID = tbl.GetPartitionInfo().GetPartitionIDByName(partition)
		}
		startKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(tblID))
		endKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(tblID+1))
		keyRange := pdhttp.NewKeyRange(startKey, endKey)
		return cli.On("GetRegionDistributionByKeyRange", mock.Anything, keyRange, "").
			Return(distributions, nil)
	}
	distributions := &pdhttp.RegionDistributions{}
	distributions.RegionDistributions = append(distributions.RegionDistributions, &pdhttp.RegionDistribution{
		StoreID:               1,
		EngineType:            "tikv",
		RegionLeaderCount:     1,
		RegionPeerCount:       3,
		RegionWriteBytes:      100,
		RegionWriteKeys:       10,
		RegionWriteQuery:      1,
		RegionLeaderReadBytes: 1000,
		RegionLeaderReadKeys:  100,
		RegionLeaderReadQuery: 10,
		RegionPeerReadKeys:    10000,
		RegionPeerReadBytes:   1000,
		RegionPeerReadQuery:   100,
	})
	tk.MustExec("create table t1(a int)")
	mockGetDistributions("t1", "", distributions)
	ret := tk.MustQuery("show table t1 distributions").Rows()
	re.Len(ret, 1)
	re.Len(ret[0], 13)

	// test for partition table distributions
	tk.MustExec("create table tp1 (id int) PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100)," +
		"PARTITION p1 VALUES LESS THAN (1000)," +
		"PARTITION p2 VALUES LESS THAN (10000)" +
		")")
	mockGetDistributions("tp1", "p0", distributions)
	mockGetDistributions("tp1", "p1", distributions)
	tk.MustQuery("show table tp1 partition(p0) distributions").Check(testkit.Rows("1 tikv 1 3 100 10 1 " +
		"1000 100 10 1000 10000 100"))
	tk.MustQuery("show table tp1 partition(p0,p1) distributions").Check(testkit.Rows("1 tikv 1 3 100 10 1 "+
		"1000 100 10 1000 10000 100", "1 tikv 1 3 100 10 1 1000 100 10 1000 10000 100"))
}

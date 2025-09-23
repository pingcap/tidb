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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

var schedulerName = "balance-range-scheduler"

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

func (cli *MockDistributePDCli) CancelSchedulerJob(ctx context.Context, schedulerName string, jobID uint64) error {
	args := cli.Called(ctx, schedulerName, jobID)
	if args.Error(0) != nil {
		return args.Error(0)
	}
	for _, job := range cli.jobs {
		if job["job-id"] == float64(jobID) {
			return nil
		}
	}
	return errors.New("job not found")
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
	mockGetSchedulerConfig := func() *mock.Call {
		return cli.On("GetSchedulerConfig", mock.Anything, schedulerName).
			Return(nil, nil)
	}
	mockGetSchedulerConfig()
	require.Empty(t, tk.MustQuery("show distribution jobs").Rows())

	job := map[string]any{}
	job["job-id"] = float64(1)
	job["alias"] = strings.Join([]string{"test", "test", "partition(P0,P1)"}, ".")
	job["engine"] = "tikv"
	job["rule"] = "leader-scatter"
	now := time.Now()
	job["create"] = now.Add(-time.Minute)
	job["start"] = now.Add(-time.Second * 30)
	job["status"] = "finished"
	job["timeout"] = 30 * time.Minute
	jobs := make([]map[string]any, 0)
	jobs = append(jobs, job)
	cli.jobs = jobs

	tk.MustQuery("show distribution jobs").Check(testkit.Rows(
		fmt.Sprintf("1 test test partition(P0,P1) tikv leader-scatter finished 30m0s %s %s <nil>",
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
	mockCreateSchedulerWithInput := func(tblName string, config map[string]any, partitions []string) *mock.Call {
		is := tk.Session().GetDomainInfoSchema()
		tbl, err := is.TableInfoByName(pmodel.NewCIStr(database), pmodel.NewCIStr(tblName))
		require.NoError(t, err)
		tblID := tbl.ID
		pi := tbl.GetPartitionInfo()
		pids := make([]int64, 0)
		if pi == nil {
			pids = append(pids, tblID)
		} else {
			if len(partitions) > 0 {
				for _, partition := range partitions {
					pids = append(pids, pi.GetPartitionIDByName(partition))
				}
			} else {
				for _, partition := range pi.Definitions {
					pids = append(pids, partition.ID)
				}
			}
		}
		starts := make([]string, 0)
		ends := make([]string, 0)
		slices.Sort(pids)
		for i, pid := range pids {
			if i == 0 || pid != pids[i-1]+1 {
				startKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(pid))
				endKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(pid+1))
				starts = append(starts, url.QueryEscape(string(startKey)))
				ends = append(ends, url.QueryEscape(string(endKey)))
			} else {
				endKey := codec.EncodeBytes([]byte{}, tablecodec.GenTablePrefix(pid+1))
				ends[len(starts)-1] = url.QueryEscape(string(endKey))
			}
		}

		config["start-key"] = strings.Join(starts, ",")
		config["end-key"] = strings.Join(ends, ",")
		return cli.On("CreateSchedulerWithInput", mock.Anything, schedulerName, config).
			Return(nil)
	}
	mockGetSchedulerConfig := func(schedulerName string) *mock.Call {
		return cli.On("GetSchedulerConfig", mock.Anything, schedulerName).
			Return(nil, nil)
	}
	table := "t1"
	partition := ""

	config := map[string]any{
		"alias":  strings.Join([]string{database, table, partition}, "."),
		"engine": "tikv",
		"rule":   "leader-scatter",
	}
	tk.MustExec(`create table t1 (
		c1 int, c2 int, c3 int
	)
	partition by range( c1 ) (
		partition p0 values less than (1024),
		partition p1 values less than (2048),
		partition p2 values less than (3072)
	   );`)
	mockGetSchedulerConfig("balance-range-scheduler")
	mockCreateSchedulerWithInput(table, config, nil)
	tk.MustQuery(fmt.Sprintf("distribute table %s rule='leader-scatter' engine='tikv'", table)).Check(testkit.Rows("1"))
	// test for multi partitions
	partition = "partition(p1,p2)"
	config["alias"] = strings.Join([]string{database, table, partition}, ".")
	mockCreateSchedulerWithInput(table, config, []string{"p1", "p2"})
	tk.MustQuery(fmt.Sprintf("distribute table %s partition (p1,p2) rule='leader-scatter' engine='tikv'", table)).Check(testkit.Rows("2"))
	// test for unordered partitions
	tk.MustQuery(fmt.Sprintf("distribute table %s partition (p2,p1) rule='leader-scatter' engine='tikv'", table)).Check(testkit.Rows("3"))

	// test for timeout
	partition = "partition(p0)"
	config["alias"] = strings.Join([]string{database, table, partition}, ".")
	config["timeout"] = "30m"
	mockCreateSchedulerWithInput(table, config, []string{"p0"})
	tk.MustQuery(fmt.Sprintf("distribute table %s partition(p0) rule='leader-scatter' engine='tikv' timeout='30m'", table)).Check(testkit.Rows("4"))

	partition = "partition(p0,p2)"
	config["alias"] = strings.Join([]string{database, table, partition}, ".")
	config["timeout"] = "30m"
	mockCreateSchedulerWithInput(table, config, []string{"p0", "p2"})
	tk.MustQuery(fmt.Sprintf("distribute table %s partition(p0,p2) rule='leader-scatter' engine='tikv' timeout='30m'", table)).Check(testkit.Rows("5"))

	// test for incorrect arguments
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule='leader-scatter' engine='tiflash'", table),
		"[planner:1210]Incorrect arguments to the rule of tiflash must be learner-scatter")
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule='leader-scatter' engine='titan'", table),
		"[planner:1210]Incorrect arguments to engine must be tikv or tiflash")
	tk.MustGetErrMsg(fmt.Sprintf("distribute table %s rule='witness' engine='tikv'", table),
		"[planner:1210]Incorrect arguments to rule must be leader-scatter, peer-scatter or learner-scatter")
}

func TestShowTableDistributions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cli := &MockDistributePDCli{}
	recoverCli := infosync.SetPDHttpCliForTest(cli)
	defer recoverCli()
	mockGetDistributions := func(tblName, partition string, distributions *pdhttp.RegionDistributions) *mock.Call {
		is := tk.Session().GetDomainInfoSchema()
		tbl, err := is.TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr(tblName))
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
	mockDistribution := func(storeID uint64) *pdhttp.RegionDistribution {
		return &pdhttp.RegionDistribution{
			StoreID:               storeID,
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
		}
	}
	distributions.RegionDistributions = append(distributions.RegionDistributions, mockDistribution(1), mockDistribution(2))
	tk.MustExec("create table t1(a int)")
	mockGetDistributions("t1", "", distributions)
	tk.MustQuery("show table t1 distributions").Check(testkit.Rows("t1 1 tikv 1 3 100 10 1 "+
		"1000 100 10 1000 10000 100", "t1 2 tikv 1 3 100 10 1 1000 100 10 1000 10000 100"))

	// test for partition table distributions
	tk.MustExec("create table tp1 (id int) PARTITION BY RANGE (id) (" +
		"PARTITION p0 VALUES LESS THAN (100)," +
		"PARTITION p1 VALUES LESS THAN (1000)," +
		"PARTITION p2 VALUES LESS THAN (10000)" +
		")")
	mockGetDistributions("tp1", "p0", distributions)
	mockGetDistributions("tp1", "p1", distributions)
	tk.MustQuery("show table tp1 partition(p0) distributions").Check(testkit.Rows("p0 1 tikv 1 3 100 10 1 "+
		"1000 100 10 1000 10000 100", "p0 2 tikv 1 3 100 10 1 1000 100 10 1000 10000 100"))
	tk.MustQuery("show table tp1 partition(p0,p1) distributions").Check(testkit.Rows("p0 1 tikv 1 3 100 10 1 "+
		"1000 100 10 1000 10000 100", "p0 2 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
		"p1 1 tikv 1 3 100 10 1 1000 100 10 1000 10000 100", "p1 2 tikv 1 3 100 10 1 1000 100 10 1000 10000 100",
	))
}

func TestCancelDistributionJob(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	cli := &MockDistributePDCli{}
	recoverCli := infosync.SetPDHttpCliForTest(cli)
	defer recoverCli()
	mockCancelJobID := func(jobID uint64) *mock.Call {
		return cli.On("CancelSchedulerJob", mock.Anything, schedulerName, jobID).
			Return(nil)
	}
	mockCancelJobID(1)
	// cancel job failed because job not found
	_, err := tk.Exec("cancel distribution job 1")
	require.ErrorContains(t, err, "job not found")

	// cancel job successfully
	job := map[string]any{
		"job-id": float64(1),
	}
	cli.jobs = append(cli.jobs, job)
	_, err = tk.Exec("cancel distribution job 1")
	require.NoError(t, err)
}

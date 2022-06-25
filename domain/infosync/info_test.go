// Copyright 2020 PingCAP, Inc.
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

package infosync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/placement"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit/testsetup"
	util2 "github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestTopology(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTest(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	currentID := "test"

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)

	client := cluster.RandClient()

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockServerInfo", "return(true)"))
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/domain/infosync/mockServerInfo")
		require.NoError(t, err)
	}()

	info, err := GlobalInfoSyncerInit(ctx, currentID, func() uint64 { return 1 }, client, false)
	require.NoError(t, err)

	err = info.newTopologySessionAndStoreServerInfo(ctx, util2.NewSessionDefaultRetryCnt)
	require.NoError(t, err)

	topology, err := info.getTopologyFromEtcd(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1282967700), topology.StartTimestamp)

	v, ok := topology.Labels["foo"]
	require.True(t, ok)
	require.Equal(t, "bar", v)
	require.Equal(t, info.getTopologyInfo(), *topology)

	nonTTLKey := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, info.info.IP, info.info.Port)
	ttlKey := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, info.info.IP, info.info.Port)

	err = util.DeleteKeyFromEtcd(nonTTLKey, client, util2.NewSessionDefaultRetryCnt, time.Second)
	require.NoError(t, err)

	// Refresh and re-test if the key exists
	err = info.RestartTopology(ctx)
	require.NoError(t, err)

	topology, err = info.getTopologyFromEtcd(ctx)
	require.NoError(t, err)

	s, err := os.Executable()
	require.NoError(t, err)

	dir := path.Dir(s)
	require.Equal(t, dir, topology.DeployPath)
	require.Equal(t, int64(1282967700), topology.StartTimestamp)
	require.Equal(t, info.getTopologyInfo(), *topology)

	// check ttl key
	ttlExists, err := info.ttlKeyExists(ctx)
	require.NoError(t, err)
	require.True(t, ttlExists)

	err = util.DeleteKeyFromEtcd(ttlKey, client, util2.NewSessionDefaultRetryCnt, time.Second)
	require.NoError(t, err)

	err = info.updateTopologyAliveness(ctx)
	require.NoError(t, err)

	ttlExists, err = info.ttlKeyExists(ctx)
	require.NoError(t, err)
	require.True(t, ttlExists)
}

func (is *InfoSyncer) getTopologyFromEtcd(ctx context.Context) (*TopologyInfo, error) {
	key := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, is.info.IP, is.info.Port)
	resp, err := is.etcdCli.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("not-exists")
	}
	if len(resp.Kvs) != 1 {
		return nil, errors.New("resp.Kvs error")
	}
	var ret TopologyInfo
	err = json.Unmarshal(resp.Kvs[0].Value, &ret)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func (is *InfoSyncer) ttlKeyExists(ctx context.Context) (bool, error) {
	key := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, is.info.IP, is.info.Port)
	resp, err := is.etcdCli.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) >= 2 {
		return false, errors.New("too many arguments in resp.Kvs")
	}
	return len(resp.Kvs) == 1, nil
}

func TestPutBundlesRetry(t *testing.T) {
	_, err := GlobalInfoSyncerInit(context.TODO(), "test", func() uint64 { return 1 }, nil, false)
	require.NoError(t, err)

	bundle, err := placement.NewBundleFromOptions(&model.PlacementSettings{PrimaryRegion: "r1", Regions: "r1,r2"})
	require.NoError(t, err)
	bundle = bundle.Reset(placement.RuleIndexTable, []int64{1024})

	t.Run("serviceErrorShouldNotRetry", func(t *testing.T) {
		require.NoError(t, PutRuleBundles(context.TODO(), []*placement.Bundle{{ID: bundle.ID}}))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError"))
		}()

		err := PutRuleBundlesWithRetry(context.TODO(), []*placement.Bundle{bundle}, 3, time.Millisecond)
		require.Error(t, err)
		require.Equal(t, "[domain:8243]mock service error", err.Error())

		got, err := GetRuleBundle(context.TODO(), bundle.ID)
		require.NoError(t, err)
		require.True(t, got.IsEmpty())
	})

	t.Run("nonServiceErrorShouldRetry", func(t *testing.T) {
		require.NoError(t, PutRuleBundles(context.TODO(), []*placement.Bundle{{ID: bundle.ID}}))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError", "3*return(false)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError"))
		}()

		err := PutRuleBundlesWithRetry(context.TODO(), []*placement.Bundle{bundle}, 3, time.Millisecond)
		require.NoError(t, err)

		got, err := GetRuleBundle(context.TODO(), bundle.ID)
		require.NoError(t, err)

		gotJSON, err := json.Marshal(got)
		require.NoError(t, err)

		expectJSON, err := json.Marshal(bundle)
		require.NoError(t, err)

		require.Equal(t, expectJSON, gotJSON)
	})

	t.Run("nonServiceErrorRetryAndFail", func(t *testing.T) {
		require.NoError(t, PutRuleBundles(context.TODO(), []*placement.Bundle{{ID: bundle.ID}}))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError", "4*return(false)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/domain/infosync/putRuleBundlesError"))
		}()

		err := PutRuleBundlesWithRetry(context.TODO(), []*placement.Bundle{bundle}, 3, time.Millisecond)
		require.Error(t, err)
		require.Equal(t, "mock other error", err.Error())

		got, err := GetRuleBundle(context.TODO(), bundle.ID)
		require.NoError(t, err)
		require.True(t, got.IsEmpty())
	})
}

func TestTiFlashManager(t *testing.T) {
	ctx := context.Background()
	_, err := GlobalInfoSyncerInit(ctx, "test", func() uint64 { return 1 }, nil, false)
	tiflash := NewMockTiFlash()
	SetMockTiFlash(tiflash)

	require.NoError(t, err)

	// SetTiFlashPlacementRule/GetTiFlashGroupRules
	rule := MakeNewRule(1, 2, []string{"a"})
	require.NoError(t, SetTiFlashPlacementRule(ctx, *rule))
	rules, err := GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	require.Equal(t, 1, len(rules))
	require.Equal(t, "table-1-r", rules[0].ID)
	require.Equal(t, 2, rules[0].Count)
	require.Equal(t, []string{"a"}, rules[0].LocationLabels)
	require.Equal(t, false, rules[0].Override, false)
	require.Equal(t, placement.RuleIndexTiFlash, rules[0].Index)

	// PostTiFlashAccelerateSchedule
	require.Nil(t, PostTiFlashAccelerateSchedule(ctx, 1))
	z, ok := tiflash.SyncStatus[1]
	require.Equal(t, true, ok)
	require.Equal(t, true, z.Accel)

	// GetTiFlashStoresStat
	stats, err := GetTiFlashStoresStat(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Count)

	// DeleteTiFlashPlacementRule
	require.NoError(t, DeleteTiFlashPlacementRule(ctx, "tiflash", rule.ID))
	rules, err = GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	require.Equal(t, 0, len(rules))

	// ConfigureTiFlashPDForTable
	require.Nil(t, ConfigureTiFlashPDForTable(1, 2, &[]string{"a"}))
	rules, err = GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	require.Equal(t, 1, len(rules))

	// ConfigureTiFlashPDForPartitions
	ConfigureTiFlashPDForPartitions(true, &[]model.PartitionDefinition{
		{
			ID:       2,
			Name:     model.NewCIStr("p"),
			LessThan: []string{},
		},
	}, 3, &[]string{}, 100)
	rules, err = GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	// Have table 1 and 2
	require.Equal(t, 2, len(rules))
	z, ok = tiflash.SyncStatus[2]
	require.Equal(t, true, ok)
	require.Equal(t, true, z.Accel)

	CloseTiFlashManager(ctx)
}

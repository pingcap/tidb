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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestPutBundlesRetry(t *testing.T) {
	_, err := GlobalInfoSyncerInit(context.TODO(), "test", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, false, nil)
	require.NoError(t, err)

	bundle, err := placement.NewBundleFromOptions(&model.PlacementSettings{PrimaryRegion: "r1", Regions: "r1,r2"})
	require.NoError(t, err)
	bundle = bundle.Reset(placement.RuleIndexTable, []int64{1024})

	t.Run("serviceErrorShouldNotRetry", func(t *testing.T) {
		require.NoError(t, PutRuleBundles(context.TODO(), []*placement.Bundle{{ID: bundle.ID}}))
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError", "1*return(true)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError"))
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
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError", "3*return(false)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError"))
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
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError", "4*return(false)"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/domain/infosync/putRuleBundlesError"))
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
	_, err := GlobalInfoSyncerInit(ctx, "test", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, false, nil)
	tiflash := NewMockTiFlash()
	SetMockTiFlash(tiflash)

	require.NoError(t, err)

	// SetTiFlashPlacementRule/GetTiFlashGroupRules
	rule := MakeNewRule(1, 2, []string{"a"})
	require.NoError(t, SetTiFlashPlacementRule(ctx, rule))
	rules, err := GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	require.Equal(t, 1, len(rules))
	require.Equal(t, "table-1-r", rules[0].ID)
	require.Equal(t, 2, rules[0].Count)
	require.Equal(t, []string{"a"}, rules[0].LocationLabels)
	require.Equal(t, false, rules[0].Override, false)
	require.Equal(t, placement.RuleIndexTiFlash, rules[0].Index)

	// GetTiFlashStoresStat
	stats, err := GetTiFlashStoresStat(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, stats.Count)

	// DeleteTiFlashPlacementRules
	require.NoError(t, DeleteTiFlashPlacementRules(ctx, []int64{1}))
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
			Name:     ast.NewCIStr("p1"),
			LessThan: []string{},
		},
		{
			ID:       3,
			Name:     ast.NewCIStr("p2"),
			LessThan: []string{},
		},
	}, 3, &[]string{}, 100)
	rules, err = GetTiFlashGroupRules(ctx, "tiflash")
	require.NoError(t, err)
	// Have table a and partitions p1, p2
	require.Equal(t, 3, len(rules))
	z, ok := tiflash.SyncStatus[2]
	require.Equal(t, true, ok)
	require.Equal(t, true, z.Accel)
	z, ok = tiflash.SyncStatus[3]
	require.Equal(t, true, ok)
	require.Equal(t, true, z.Accel)

	CloseTiFlashManager(ctx)
}

func TestInfoSyncerMarshal(t *testing.T) {
	info := &serverinfo.ServerInfo{
		StaticInfo: serverinfo.StaticInfo{
			VersionInfo: serverinfo.VersionInfo{
				Version: "8.8.8",
				GitHash: "123456",
			},
			ID:             "tidb1",
			IP:             "127.0.0.1",
			Port:           4000,
			StatusPort:     10080,
			Lease:          "1s",
			StartTimestamp: 10000,
			ServerIDGetter: func() uint64 { return 0 },
			JSONServerID:   1,
		},
		DynamicInfo: serverinfo.DynamicInfo{
			Labels: map[string]string{"zone": "ap-northeast-1a"},
		},
	}
	data, err := json.Marshal(info)
	require.NoError(t, err)
	require.Equal(t, data, []byte(`{"version":"8.8.8","git_hash":"123456",`+
		`"ddl_id":"tidb1","ip":"127.0.0.1","listening_port":4000,"status_port":10080,"lease":"1s","start_timestamp":10000,`+
		`"server_id":1,"labels":{"zone":"ap-northeast-1a"}}`))
	var decodeInfo *serverinfo.ServerInfo
	err = json.Unmarshal(data, &decodeInfo)
	require.NoError(t, err)
	require.Nil(t, decodeInfo.ServerIDGetter)
	require.Equal(t, info.Version, decodeInfo.Version)
	require.Equal(t, info.GitHash, decodeInfo.GitHash)
	require.Equal(t, info.ID, decodeInfo.ID)
	require.Equal(t, info.IP, decodeInfo.IP)
	require.Equal(t, info.Port, decodeInfo.Port)
	require.Equal(t, info.StatusPort, decodeInfo.StatusPort)
	require.Equal(t, info.Lease, decodeInfo.Lease)
	require.Equal(t, info.StartTimestamp, decodeInfo.StartTimestamp)
	require.Equal(t, info.JSONServerID, decodeInfo.JSONServerID)
	require.Equal(t, info.Labels, decodeInfo.Labels)
}

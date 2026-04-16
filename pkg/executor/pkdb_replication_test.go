package executor

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/logreplicationpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

func TestScanReplStateCF(t *testing.T) {
	t.Skip("this is a manual test")

	ctx := context.Background()
	replicaPDAddr := "127.0.0.1:12379"
	cli, err := rawkv.NewClient(ctx, strings.Split(replicaPDAddr, ","), config.Security{})
	require.NoError(t, err)
	defer cli.Close()

	startKey := []byte{}

	minSafeTs := ^uint64(0)
	_, values, err := cli.Scan(ctx, startKey, nil, 1000, rawkv.SetColumnFamily("lr-state"))
	require.NoError(t, err)
	for _, v := range values {
		pb := &logreplicationpb.LogReplicationState{}
		err = pb.Unmarshal(v)
		require.NoError(t, err)
		if pb.SafeTs == 0 {
			t.Logf("got safe-ts = 0. pb: %#v", pb)
		} else {
			minSafeTs = min(minSafeTs, pb.SafeTs)
		}
	}
	t.Logf("minSafeTs = %d", minSafeTs)
}

func TestValidateTiKVConfigEnabledForLogReplication(t *testing.T) {
	cases := []struct {
		name        string
		cluster     string
		instances   []string
		configs     map[string]map[string]bool
		wantErr     bool
		wantContain []string
	}{
		{
			name:      "all enabled",
			cluster:   "current",
			instances: []string{"tikv-1", "tikv-2"},
			configs: map[string]map[string]bool{
				tikvConfigReplicatorEnabledKey: {
					"tikv-1": true,
					"tikv-2": true,
				},
				tikvConfigLogArchiveEnabledKey: {
					"tikv-1": true,
					"tikv-2": true,
				},
			},
			wantErr: false,
		},
		{
			name:      "disabled config",
			cluster:   "current",
			instances: []string{"tikv-1", "tikv-2"},
			configs: map[string]map[string]bool{
				tikvConfigReplicatorEnabledKey: {
					"tikv-1": true,
					"tikv-2": false,
				},
				tikvConfigLogArchiveEnabledKey: {
					"tikv-1": true,
					"tikv-2": true,
				},
			},
			wantErr: true,
			wantContain: []string{
				"current cluster",
				tikvConfigReplicatorEnabledKey,
				tikvConfigLogArchiveEnabledKey,
			},
		},
		{
			name:      "missing config",
			cluster:   "source",
			instances: []string{"tikv-1"},
			configs: map[string]map[string]bool{
				tikvConfigReplicatorEnabledKey: {
					"tikv-1": true,
				},
			},
			wantErr: true,
			wantContain: []string{
				"source cluster",
				tikvConfigReplicatorEnabledKey,
				tikvConfigLogArchiveEnabledKey,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateTiKVConfigEnabled(tc.cluster, tc.instances, tc.configs)
			if tc.wantErr {
				require.Error(t, err)
				for _, contain := range tc.wantContain {
					require.Contains(t, err.Error(), contain)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

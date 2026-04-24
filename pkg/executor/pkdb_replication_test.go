package executor

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/kvproto/pkg/logreplicationpb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
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

type mockWorkflowListResult struct {
	workflows []*pdpb.WorkflowInfo
	err       error
}

type mockWorkflowLister struct {
	results []mockWorkflowListResult
	calls   int
}

func (m *mockWorkflowLister) ListLogReplWorkflows(context.Context) ([]*pdpb.WorkflowInfo, error) {
	if len(m.results) == 0 {
		return nil, nil
	}
	idx := m.calls
	if idx >= len(m.results) {
		idx = len(m.results) - 1
	}
	result := m.results[idx]
	m.calls++
	return result.workflows, result.err
}

func TestPollWorkflowCompleteRetriesListErrors(t *testing.T) {
	lister := &mockWorkflowLister{
		results: []mockWorkflowListResult{
			{err: context.DeadlineExceeded},
			{err: context.DeadlineExceeded},
			{workflows: []*pdpb.WorkflowInfo{{Id: 42, State: "COMPLETED"}}},
		},
	}

	err := pollWorkflowComplete(context.Background(), lister, 42)
	require.NoError(t, err)
	require.Equal(t, 3, lister.calls)
}

func TestPollWorkflowCompleteReturnsWorkflowNotCancelableOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		lister := &mockWorkflowLister{
			results: []mockWorkflowListResult{
				{workflows: []*pdpb.WorkflowInfo{{Id: 42, State: "IN_PROGRESS"}}},
			},
		}
		time.AfterFunc(10*time.Millisecond, cancel)

		err := pollWorkflowComplete(ctx, lister, 42)
		require.ErrorContains(t, err, "workflow 42 cannot be cancelled at the moment")
		require.GreaterOrEqual(t, lister.calls, 1)
	})
}

func TestPollWorkflowCompleteReturnsErrorOnCancelledState(t *testing.T) {
	lister := &mockWorkflowLister{
		results: []mockWorkflowListResult{
			{workflows: []*pdpb.WorkflowInfo{{Id: 42, State: "CANCELLED"}}},
		},
	}

	err := pollWorkflowComplete(context.Background(), lister, 42)
	require.ErrorContains(t, err, "workflow 42 is cancelled")
	require.Equal(t, 1, lister.calls)
}

func TestCreateLogReplicationExecNextDetached(t *testing.T) {
	col := &expression.Column{RetType: types.NewFieldType(mysql.TypeLonglong)}
	e := &CreateLogReplicationExec{
		BaseExecutor: exec.NewBaseExecutor(mock.NewContext(), expression.NewSchema(col), 0),
		Detached:     true,
		workflowID:   42,
	}

	req := e.NewChunk()
	err := e.Next(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 1, req.NumRows())
	require.Equal(t, uint64(42), req.GetRow(0).GetUint64(0))

	err = e.Next(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, 0, req.NumRows())
}

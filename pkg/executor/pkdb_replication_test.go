package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
)

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
	opts    []pd.ListLogReplWorkflowsOptions
}

func (m *mockWorkflowLister) ListLogReplWorkflows(
	_ context.Context,
	opts pd.ListLogReplWorkflowsOptions,
) ([]*pdpb.WorkflowInfo, error) {
	m.opts = append(m.opts, opts)
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

	err := pollWorkflowComplete(context.Background(), lister, 42, &sqlkiller.SQLKiller{})
	require.NoError(t, err)
	require.Equal(t, 3, lister.calls)
	require.Equal(t, []pd.ListLogReplWorkflowsOptions{
		{StartWorkflowID: 42, EndWorkflowID: 43, Limit: 1},
		{StartWorkflowID: 42, EndWorkflowID: 43, Limit: 1},
		{StartWorkflowID: 42, EndWorkflowID: 43, Limit: 1},
	}, lister.opts)
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

		err := pollWorkflowComplete(ctx, lister, 42, &sqlkiller.SQLKiller{})
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

	err := pollWorkflowComplete(context.Background(), lister, 42, &sqlkiller.SQLKiller{})
	require.ErrorContains(t, err, "workflow 42 is cancelled")
	require.Equal(t, 1, lister.calls)
}

func TestPollWorkflowCompleteReturnsCleanedWorkflowError(t *testing.T) {
	lister := &mockWorkflowLister{
		results: []mockWorkflowListResult{
			{workflows: nil},
		},
	}

	err := pollWorkflowComplete(context.Background(), lister, 42, &sqlkiller.SQLKiller{})
	require.ErrorIs(t, err, errLogReplWorkflowCleaned)
	require.ErrorContains(t, err, "log replication workflow has been cleaned: workflow 42")
	require.Equal(t, 1, lister.calls)
}

func TestPollWorkflowCompleteGetKilled(t *testing.T) {
	killer := sqlkiller.SQLKiller{}
	killer.SendKillSignal(sqlkiller.QueryInterrupted)
	err := pollWorkflowComplete(context.Background(), nil, 42, &killer)
	require.ErrorContains(t, err, "Query execution was interrupted")
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

type mockStoreLister struct {
	stores []*metapb.Store
	err    error
}

func (m mockStoreLister) GetAllStores(context.Context, ...opt.GetStoreOption) ([]*metapb.Store, error) {
	return m.stores, m.err
}

func TestFetchTiKVConfigFromPD(t *testing.T) {
	tikv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"replicator":{"enable":true},"raft-engine":{"enable-log-archive":true}}`))
	}))
	defer tikv1.Close()

	tikv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"replicator":{"enable":false},"raft-engine":{"enable-log-archive":true}}`))
	}))
	defer tikv2.Close()

	stores := []*metapb.Store{
		{
			Id:            1,
			Address:       "tikv-2:20160",
			StatusAddress: strings.TrimPrefix(tikv2.URL, "http://"),
			State:         metapb.StoreState_Up,
		},
		{
			Id:            2,
			Address:       "tikv-1:20160",
			StatusAddress: strings.TrimPrefix(tikv1.URL, "http://"),
			State:         metapb.StoreState_Up,
		},
	}

	instances, configValues, err := fetchTiKVConfigFromPD(context.Background(), "source", mockStoreLister{stores: stores}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"tikv-1:20160", "tikv-2:20160"}, instances)
	require.Equal(t, map[string]map[string]bool{
		tikvConfigReplicatorEnabledKey: {
			"tikv-1:20160": true,
			"tikv-2:20160": false,
		},
		tikvConfigLogArchiveEnabledKey: {
			"tikv-1:20160": true,
			"tikv-2:20160": true,
		},
	}, configValues)
}

func TestFetchTiKVConfigFromPDRejectsNonUpStore(t *testing.T) {
	stores := []*metapb.Store{
		{
			Id:            3,
			Address:       "tikv-offline:20160",
			StatusAddress: "127.0.0.1:65535",
			State:         metapb.StoreState_Offline,
		},
	}

	_, _, err := fetchTiKVConfigFromPD(context.Background(), "source", mockStoreLister{stores: stores}, nil)
	require.ErrorContains(t, err, "source TiKV store 3 is not in Up state")
}

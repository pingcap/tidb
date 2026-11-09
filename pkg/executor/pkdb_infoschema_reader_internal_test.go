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

package executor

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
)

type workflowHistoryStorage struct {
	kv.Storage
	pdClient pd.Client
}

func (s *workflowHistoryStorage) GetPDClient() pd.Client {
	return s.pdClient
}

func (*workflowHistoryStorage) GetPDHTTPClient() pdhttp.Client {
	return nil
}

type workflowHistoryPDClient struct {
	pd.Client

	calls []pd.ListLogReplWorkflowsOptions
}

func (c *workflowHistoryPDClient) ListLogReplWorkflows(
	_ context.Context,
	opts pd.ListLogReplWorkflowsOptions,
) ([]*pdpb.WorkflowInfo, error) {
	c.calls = append(c.calls, opts)
	return nil, nil
}

func TestSetDataForLogReplWorkflowHistoryGlobalPassesOptions(t *testing.T) {
	pdCli := &workflowHistoryPDClient{}
	store := &workflowHistoryStorage{pdClient: pdCli}
	dom := domain.NewDomain(store, time.Second, time.Second, time.Second, nil, nil)
	t.Cleanup(dom.Close)
	sctx := mock.NewContext()
	sctx.BindDomain(dom)

	cases := []struct {
		name      string
		extractor *plannercore.LogReplWorkflowHistoryGlobalExtractor
		expected  []pd.ListLogReplWorkflowsOptions
	}{
		{
			name: "start end and limit",
			extractor: &plannercore.LogReplWorkflowHistoryGlobalExtractor{
				StartWorkflowID: 10,
				EndWorkflowID:   20,
				Limit:           11,
			},
			expected: []pd.ListLogReplWorkflowsOptions{{
				StartWorkflowID: 10,
				EndWorkflowID:   20,
				Limit:           11,
			}},
		},
		{
			name: "workflow id bounds are passed without limit",
			extractor: &plannercore.LogReplWorkflowHistoryGlobalExtractor{
				StartWorkflowID: 10,
				EndWorkflowID:   20,
			},
			expected: []pd.ListLogReplWorkflowsOptions{{
				StartWorkflowID: 10,
				EndWorkflowID:   20,
			}},
		},
		{
			name: "skip request",
			extractor: &plannercore.LogReplWorkflowHistoryGlobalExtractor{
				SkipRequest: true,
			},
		},
	}

	for _, ca := range cases {
		t.Run(ca.name, func(t *testing.T) {
			pdCli.calls = nil
			retriever := &memtableRetriever{extractor: ca.extractor}

			err := retriever.setDataForLogReplWorkflowHistoryGlobal(context.Background(), sctx)
			require.NoError(t, err)
			require.Equal(t, ca.expected, pdCli.calls)
		})
	}
}

// Copyright 2022 PingCAP, Inc.
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

package task

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func newMockStreamMgr(pdCli pd.Client, httpCli *http.Client) *streamMgr {
	pdController := &pdutil.PdController{}
	pdController.SetPDClient(pdCli)
	mgr := &conn.Mgr{PdController: pdController}
	return &streamMgr{mgr: mgr, httpCli: httpCli}
}

func TestStreamStartChecks(t *testing.T) {
	cases := []struct {
		stores        []*metapb.Store
		content       []string
		supportStream bool
	}{
		{
			stores: []*metapb.Store{
				{
					Id:    1,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "engine",
							Value: "tiflash",
						},
					},
				},
			},
			content: []string{""},
			// no tikv detected in this case, so support is false.
			supportStream: false,
		},
		{
			stores: []*metapb.Store{
				{
					Id:    1,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "engine",
							Value: "tikv",
						},
					},
				},
			},
			content: []string{
				"{\"log-level\": \"debug\", \"backup-stream\": {\"enable\": true}}",
			},
			// one tikv detected in this case and `enable-streaming` is true.
			supportStream: true,
		},
		{
			stores: []*metapb.Store{
				{
					Id:    1,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "engine",
							Value: "tikv",
						},
					},
				},
				{
					Id:    2,
					State: metapb.StoreState_Up,
					Labels: []*metapb.StoreLabel{
						{
							Key:   "engine",
							Value: "tikv",
						},
					},
				},
			},
			content: []string{
				"{\"log-level\": \"debug\", \"backup-stream\": {\"enable\": true}}",
				"{\"log-level\": \"debug\", \"backup-stream\": {\"enable\": false}}",
			},
			// two tikv detected in this case and one of them's `enable-streaming` is false.
			supportStream: false,
		},
	}

	ctx := context.Background()
	for _, ca := range cases {
		pdCli := utils.FakePDClient{Stores: ca.stores}
		require.Equal(t, len(ca.content), len(ca.stores))
		count := 0
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch strings.TrimSpace(r.URL.Path) {
			case "/config":
				_, _ = fmt.Fprint(w, ca.content[count])
			default:
				http.NotFoundHandler().ServeHTTP(w, r)
			}
			count++
		}))

		for _, s := range ca.stores {
			s.StatusAddress = mockServer.URL
		}

		httpCli := mockServer.Client()
		sMgr := newMockStreamMgr(pdCli, httpCli)
		support, err := sMgr.checkRequirements(ctx)
		require.NoError(t, err)
		require.Equal(t, ca.supportStream, support)
		mockServer.Close()
	}
}

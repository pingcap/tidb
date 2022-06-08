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
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/pdutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
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
				"{\"log-level\": \"debug\", \"log-backup\": {\"enable\": true}}",
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
				"{\"log-level\": \"debug\", \"log-backup\": {\"enable\": true}}",
				"{\"log-level\": \"debug\", \"log-backup\": {\"enable\": false}}",
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

func TestShiftTS(t *testing.T) {
	var startTS uint64 = 433155751280640000
	shiftTS := ShiftTS(startTS)
	require.Equal(t, true, shiftTS < startTS)

	delta := oracle.GetTimeFromTS(startTS).Sub(oracle.GetTimeFromTS(shiftTS))
	require.Equal(t, delta, streamShiftDuration)
}

func TestCheckLogRange(t *testing.T) {
	cases := []struct {
		restoreFrom uint64
		restoreTo   uint64
		logMinTS    uint64
		logMaxTS    uint64
		result      bool
	}{
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   99,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 1,
			restoreTo:   99,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   10,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    11,
			restoreFrom: 10,
			restoreTo:   99,
			logMaxTS:    100,
			result:      false,
		},
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   9,
			logMaxTS:    100,
			result:      false,
		},
		{
			logMinTS:    1,
			restoreFrom: 9,
			restoreTo:   99,
			logMaxTS:    99,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 9,
			restoreTo:   99,
			logMaxTS:    98,
			result:      false,
		},
	}

	for _, c := range cases {
		err := checkLogRange(c.restoreFrom, c.restoreTo, c.logMinTS, c.logMaxTS)
		if c.result {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

type fakeResolvedInfo struct {
	storeID    int64
	resolvedTS uint64
}

func fakeMetaFiles(ctx context.Context, tempDir string, infos []fakeResolvedInfo) error {
	backupMetaDir := filepath.Join(tempDir, restore.GetStreamBackupMetaPrefix())
	s, err := storage.NewLocalStorage(backupMetaDir)
	if err != nil {
		return errors.Trace(err)
	}

	for _, info := range infos {
		meta := &backuppb.Metadata{
			StoreId:    info.storeID,
			ResolvedTs: info.resolvedTS,
		}
		buff, err := meta.Marshal()
		if err != nil {
			return errors.Trace(err)
		}
		filename := fmt.Sprintf("%d_%d.meta", info.storeID, info.resolvedTS)
		if err = s.WriteFile(ctx, filename, buff); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func TestGetGlobalResolvedTS(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpdir)
	require.Nil(t, err)

	stores := []fakeResolvedInfo{
		{
			storeID:    1,
			resolvedTS: 100,
		},
		{
			storeID:    2,
			resolvedTS: 101,
		},
		{
			storeID:    1,
			resolvedTS: 70,
		},
	}

	err = fakeMetaFiles(ctx, tmpdir, stores)
	require.Nil(t, err)
	globalResolvedTS, err := getGlobalResolvedTS(ctx, s)
	require.Nil(t, err)
	require.Equal(t, uint64(100), globalResolvedTS)
}

func TestGetGlobalResolvedTS2(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpdir)
	require.Nil(t, err)

	stores := []fakeResolvedInfo{
		{
			storeID:    1,
			resolvedTS: 95,
		},
		{
			storeID:    1,
			resolvedTS: 98,
		},
		{
			storeID:    2,
			resolvedTS: 90,
		},
		{
			storeID:    2,
			resolvedTS: 99,
		},
	}

	err = fakeMetaFiles(ctx, tmpdir, stores)
	require.Nil(t, err)
	globalResolvedTS, err := getGlobalResolvedTS(ctx, s)
	require.Nil(t, err)
	require.Equal(t, uint64(98), globalResolvedTS)
}

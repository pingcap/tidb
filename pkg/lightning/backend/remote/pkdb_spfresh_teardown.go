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

package remote

import (
	"context"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/tipb/go-tipb"
)

const spfreshTeardownPath = "/spfresh/teardown"

type spfreshTeardownRequestPoster func(
	context.Context,
	*http.Client,
	string,
	uint64,
	string,
	[]byte,
	map[string]string,
	string,
) error

// TeardownSPFreshIndex notifies a worker to tear down a SPFresh index.
func TeardownSPFreshIndex(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	keyspaceID uint32,
	tableID int64,
	indexID int64,
) error {
	return teardownSPFreshIndex(
		ctx, httpClient, workerURL, clusterID, keyspaceID, tableID, indexID,
		postSPFreshControlRequest,
	)
}

// TeardownSPFreshIndexWithShortRetry notifies a worker to tear down a SPFresh
// index, bounding retries so the DDL state machine can persist a no-progress
// step and resume on the next owner iteration.
func TeardownSPFreshIndexWithShortRetry(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	keyspaceID uint32,
	tableID int64,
	indexID int64,
) error {
	return teardownSPFreshIndex(
		ctx, httpClient, workerURL, clusterID, keyspaceID, tableID, indexID,
		postShortSPFreshControlRequest,
	)
}

func teardownSPFreshIndex(
	ctx context.Context,
	httpClient *http.Client,
	workerURL string,
	clusterID uint64,
	keyspaceID uint32,
	tableID int64,
	indexID int64,
	postRequest spfreshTeardownRequestPoster,
) error {
	if httpClient == nil {
		return errors.New("spfresh teardown requires http client")
	}
	if workerURL == "" {
		return errors.New("spfresh teardown requires worker URL")
	}
	body, err := proto.Marshal(&tipb.SPFreshTeardownRequest{
		KeyspaceId: keyspaceID,
		TableId:    tableID,
		IndexId:    indexID,
	})
	if err != nil {
		return err
	}
	headers := map[string]string{
		"Content-Type": "application/x-protobuf",
	}
	return postRequest(ctx, httpClient, workerURL, clusterID, spfreshTeardownPath, body, headers, "teardown")
}

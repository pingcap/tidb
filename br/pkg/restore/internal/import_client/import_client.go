// Copyright 2024 PingCAP, Inc.
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

package importclient

import (
	"context"
	"crypto/tls"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	gRPCBackOffMaxDelay = 3 * time.Second
)

// ImporterClient is used to import a file to TiKV.
type ImporterClient interface {
	ClearFiles(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.ClearRequest,
	) (*import_sstpb.ClearResponse, error)

	ApplyKVFile(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.ApplyRequest,
	) (*import_sstpb.ApplyResponse, error)

	DownloadSST(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.DownloadRequest,
	) (*import_sstpb.DownloadResponse, error)

	MultiIngest(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.MultiIngestRequest,
	) (*import_sstpb.IngestResponse, error)

	SetDownloadSpeedLimit(
		ctx context.Context,
		storeID uint64,
		req *import_sstpb.SetDownloadSpeedLimitRequest,
	) (*import_sstpb.SetDownloadSpeedLimitResponse, error)

	GetImportClient(
		ctx context.Context,
		storeID uint64,
	) (import_sstpb.ImportSSTClient, error)

	CloseGrpcClient() error

	CheckMultiIngestSupport(ctx context.Context, stores []uint64) error
}

type importClient struct {
	metaClient split.SplitClient
	mu         sync.Mutex
	// Notice: In order to avoid leak for BRIE via SQL, it needs to close grpc client connection before br task exits.
	// So it caches the grpc connection instead of import_sstpb.ImportSSTClient.
	// used for any request except the ingest reqeust
	conns map[uint64]*grpc.ClientConn
	// used for ingest request
	ingestConns map[uint64]*grpc.ClientConn

	tlsConf       *tls.Config
	keepaliveConf keepalive.ClientParameters
}

// NewImportClient returns a new importerClient.
func NewImportClient(metaClient split.SplitClient, tlsConf *tls.Config, keepaliveConf keepalive.ClientParameters) ImporterClient {
	return &importClient{
		metaClient:    metaClient,
		conns:         make(map[uint64]*grpc.ClientConn),
		ingestConns:   make(map[uint64]*grpc.ClientConn),
		tlsConf:       tlsConf,
		keepaliveConf: keepaliveConf,
	}
}

func (ic *importClient) ClearFiles(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.ClearRequest,
) (*import_sstpb.ClearResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.ClearFiles(ctx, req)
}

func (ic *importClient) ApplyKVFile(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.ApplyRequest,
) (*import_sstpb.ApplyResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.Apply(ctx, req)
}

func (ic *importClient) DownloadSST(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.DownloadRequest,
) (*import_sstpb.DownloadResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.Download(ctx, req)
}

func (ic *importClient) SetDownloadSpeedLimit(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.SetDownloadSpeedLimitRequest,
) (*import_sstpb.SetDownloadSpeedLimitResponse, error) {
	client, err := ic.GetImportClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.SetDownloadSpeedLimit(ctx, req)
}

func (ic *importClient) MultiIngest(
	ctx context.Context,
	storeID uint64,
	req *import_sstpb.MultiIngestRequest,
) (*import_sstpb.IngestResponse, error) {
	client, err := ic.GetIngestClient(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client.MultiIngest(ctx, req)
}

func (ic *importClient) createGrpcConn(
	ctx context.Context,
	storeID uint64,
) (*grpc.ClientConn, error) {
	store, err := ic.metaClient.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if ic.tlsConf != nil {
		opt = grpc.WithTransportCredentials(credentials.NewTLS(ic.tlsConf))
	}
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	conn, err := grpc.DialContext(
		ctx,
		addr,
		opt,
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(ic.keepaliveConf),
	)
	return conn, errors.Trace(err)
}

func (ic *importClient) cachedConnectionFrom(
	ctx context.Context,
	storeID uint64,
	caches map[uint64]*grpc.ClientConn,
) (import_sstpb.ImportSSTClient, error) {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	conn, ok := caches[storeID]
	if ok {
		return import_sstpb.NewImportSSTClient(conn), nil
	}
	conn, err := ic.createGrpcConn(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	caches[storeID] = conn
	return import_sstpb.NewImportSSTClient(conn), nil
}

func (ic *importClient) GetImportClient(
	ctx context.Context,
	storeID uint64,
) (import_sstpb.ImportSSTClient, error) {
	return ic.cachedConnectionFrom(ctx, storeID, ic.conns)
}

func (ic *importClient) GetIngestClient(
	ctx context.Context,
	storeID uint64,
) (import_sstpb.ImportSSTClient, error) {
	return ic.cachedConnectionFrom(ctx, storeID, ic.ingestConns)
}

func (ic *importClient) CloseGrpcClient() error {
	ic.mu.Lock()
	defer ic.mu.Unlock()
	for id, conn := range ic.conns {
		if err := conn.Close(); err != nil {
			return errors.Trace(err)
		}
		delete(ic.conns, id)
	}
	for id, conn := range ic.ingestConns {
		if err := conn.Close(); err != nil {
			return errors.Trace(err)
		}
		delete(ic.ingestConns, id)
	}
	return nil
}

func (ic *importClient) CheckMultiIngestSupport(ctx context.Context, stores []uint64) error {
	for _, storeID := range stores {
		_, err := ic.MultiIngest(ctx, storeID, &import_sstpb.MultiIngestRequest{})
		if err != nil {
			if s, ok := status.FromError(err); ok {
				if s.Code() == codes.Unimplemented {
					return errors.Errorf("tikv node doesn't support multi ingest. (store id %d)", storeID)
				}
			}
			return errors.Annotatef(err, "failed to check multi ingest support. (store id %d)", storeID)
		}
	}
	return nil
}

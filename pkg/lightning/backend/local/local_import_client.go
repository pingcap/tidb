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

package local

import (
	"context"
	"net"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// importClientFactory is factory to create new import client for specific store.
type importClientFactory interface {
	create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error)
	close()
}

type importClientFactoryImpl struct {
	conns           *common.GRPCConns
	splitCli        split.SplitClient
	tls             *common.TLS
	tcpConcurrency  int
	compressionType config.CompressionType
}

func newImportClientFactoryImpl(
	splitCli split.SplitClient,
	tls *common.TLS,
	tcpConcurrency int,
	compressionType config.CompressionType,
) *importClientFactoryImpl {
	return &importClientFactoryImpl{
		conns:           common.NewGRPCConns(),
		splitCli:        splitCli,
		tls:             tls,
		tcpConcurrency:  tcpConcurrency,
		compressionType: compressionType,
	}
}

func (f *importClientFactoryImpl) makeConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	store, err := f.splitCli.GetStore(ctx, storeID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var opts []grpc.DialOption
	if f.tls.TLSConfig() != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(f.tls.TLSConfig())))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	bfConf := backoff.DefaultConfig
	bfConf.MaxDelay = gRPCBackOffMaxDelay
	// we should use peer address for tiflash. for tikv, peer address is empty
	addr := store.GetPeerAddress()
	if addr == "" {
		addr = store.GetAddress()
	}
	opts = append(opts,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(unlimitedRPCRecvMsgSize)),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: bfConf}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                gRPCKeepAliveTime,
			Timeout:             gRPCKeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	switch f.compressionType {
	case config.CompressionNone:
		// do nothing
	case config.CompressionGzip:
		// Use custom compressor/decompressor to speed up compression/decompression.
		// Note that here we don't use grpc.UseCompressor option although it's the recommended way.
		// Because gprc-go uses a global registry to store compressor/decompressor, we can't make sure
		// the compressor/decompressor is not registered by other components.
		opts = append(opts, grpc.WithCompressor(&gzipCompressor{}), grpc.WithDecompressor(&gzipDecompressor{}))
	default:
		return nil, common.ErrInvalidConfig.GenWithStack("unsupported compression type %s", f.compressionType)
	}

	failpoint.Inject("LoggingImportBytes", func() {
		opts = append(opts, grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
			if err != nil {
				return nil, err
			}
			return &loggingConn{Conn: conn}, nil
		}))
	})

	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return conn, nil
}

func (f *importClientFactoryImpl) getGrpcConn(ctx context.Context, storeID uint64) (*grpc.ClientConn, error) {
	return f.conns.GetGrpcConn(ctx, storeID, f.tcpConcurrency,
		func(ctx context.Context) (*grpc.ClientConn, error) {
			return f.makeConn(ctx, storeID)
		})
}

// create creates a new import client for specific store.
func (f *importClientFactoryImpl) create(ctx context.Context, storeID uint64) (sst.ImportSSTClient, error) {
	conn, err := f.getGrpcConn(ctx, storeID)
	if err != nil {
		return nil, err
	}
	return sst.NewImportSSTClient(conn), nil
}

// close closes the factory.
func (f *importClientFactoryImpl) close() {
	f.conns.Close()
}

type loggingConn struct {
	net.Conn
}

// Write implements net.Conn.Write
func (c loggingConn) Write(b []byte) (int, error) {
	log.L().Debug("import write", zap.Int("bytes", len(b)))
	return c.Conn.Write(b)
}

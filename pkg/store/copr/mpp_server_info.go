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

package copr

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type connCacheKey struct {
	address         string
	clusterSSLCA    string
	clusterSSLCert  string
	clusterSSLKey   string
	clusterVerifyCN string
}

var connCache sync.Map

func newConnCacheKey(address string, security config.Security) connCacheKey {
	return connCacheKey{
		address:         address,
		clusterSSLCA:    security.ClusterSSLCA,
		clusterSSLCert:  security.ClusterSSLCert,
		clusterSSLKey:   security.ClusterSSLKey,
		clusterVerifyCN: strings.Join(security.ClusterVerifyCN, "\x00"),
	}
}

func getGRPCConn(address string, security config.Security) (*grpc.ClientConn, connCacheKey, error) {
	opt := grpc.WithTransportCredentials(insecure.NewCredentials())
	if len(security.ClusterSSLCA) != 0 {
		clusterSecurity := security.ClusterSecurity()
		tlsConfig, err := clusterSecurity.ToTLSConfig()
		if err != nil {
			return nil, connCacheKey{}, errors.Trace(err)
		}
		opt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	key := newConnCacheKey(address, security)
	if conn, ok := connCache.Load(key); ok {
		return conn.(*grpc.ClientConn), key, nil
	}

	conn, err := grpc.Dial(address, opt)
	if err != nil {
		return nil, connCacheKey{}, err
	}
	actual, loaded := connCache.LoadOrStore(key, conn)
	if loaded {
		if err := conn.Close(); err != nil {
			log.Warn("close duplicated grpc connection warning", zap.Error(err))
		}
		return actual.(*grpc.ClientConn), key, nil
	}
	return conn, key, nil
}

func deleteGRPCConnByKey(cacheKey connCacheKey, value any) {
	conn := value.(*grpc.ClientConn)
	if connCache.CompareAndDelete(cacheKey, conn) {
		if err := conn.Close(); err != nil {
			log.Warn("close grpc connection warning", zap.Error(err))
		}
	}
}

func closeGRPCConnsForTest() {
	connCache.Range(func(key, value any) bool {
		if err := value.(*grpc.ClientConn).Close(); err != nil {
			log.Warn("close grpc connection warning", zap.Error(err))
		}
		connCache.Delete(key)
		return true
	})
}

// GetServerInfoByGRPC fetches server info from a diagnostics service.
func GetServerInfoByGRPC(ctx context.Context, address string, tp diagnosticspb.ServerInfoType) ([]*diagnosticspb.ServerInfoItem, error) {
	security := config.GetGlobalConfig().Security
	conn, cacheKey, err := getGRPCConn(address, security)
	if err != nil {
		return nil, err
	}

	cli := diagnosticspb.NewDiagnosticsClient(conn)
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	r, err := cli.ServerInfo(ctx, &diagnosticspb.ServerInfoRequest{Tp: tp})
	if err != nil {
		deleteGRPCConnByKey(cacheKey, conn)
		return nil, err
	}
	return r.Items, nil
}

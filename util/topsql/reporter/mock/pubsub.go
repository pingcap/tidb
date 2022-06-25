// Copyright 2021 PingCAP, Inc.
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

package mock

import (
	"fmt"
	"net"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type mockPubSubServer struct {
	addr       string
	listen     net.Listener
	grpcServer *grpc.Server
}

// NewMockPubSubServer creates a mock publisher server.
func NewMockPubSubServer() (*mockPubSubServer, error) {
	addr := "127.0.0.1:0"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()

	return &mockPubSubServer{
		addr:       fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port),
		listen:     lis,
		grpcServer: server,
	}, nil
}

func (svr *mockPubSubServer) Serve() {
	err := svr.grpcServer.Serve(svr.listen)
	if err != nil {
		logutil.BgLogger().Warn("[top-sql] mock pubsub server serve failed", zap.Error(err))
	}
}

func (svr *mockPubSubServer) Server() *grpc.Server {
	return svr.grpcServer
}

func (svr *mockPubSubServer) Address() string {
	return svr.addr
}

func (svr *mockPubSubServer) Stop() {
	if svr.grpcServer != nil {
		svr.grpcServer.Stop()
	}
}

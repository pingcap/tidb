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
	"github.com/pingcap/tidb/util/topsql/reporter"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type mockPublisherServer struct {
	addr       string
	grpcServer *grpc.Server
}

// StartMockPublisherServer starts the mock publisher server.
func StartMockPublisherServer(
	planBinaryDecodeFunc func(string) (string, error),
	clientRegistry *reporter.ReportClientRegistry,
) (*mockPublisherServer, error) {
	addr := "127.0.0.1:0"
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()

	publisherServer := reporter.NewTopSQLPublisher(planBinaryDecodeFunc, clientRegistry)
	tipb.RegisterTopSQLPubSubServer(server, publisherServer)

	go func() {
		err := server.Serve(lis)
		if err != nil {
			logutil.BgLogger().Warn("[top-sql] mock publisher server serve failed", zap.Error(err))
		}
	}()

	return &mockPublisherServer{
		addr:       fmt.Sprintf("127.0.0.1:%d", lis.Addr().(*net.TCPAddr).Port),
		grpcServer: server,
	}, nil
}

func (svr *mockPublisherServer) Address() string {
	return svr.addr
}

func (svr *mockPublisherServer) Stop() {
	if svr.grpcServer != nil {
		svr.grpcServer.Stop()
	}
}

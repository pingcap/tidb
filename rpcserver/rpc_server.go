// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package rpcserver

import (
	"github.com/crazycs520/diagnostics"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// CreateTiDBRPCServer creates a TiDB rpc server.
func CreateTiDBRPCServer() *grpc.Server {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server", zap.Any("stack", v))
		}
	}()

	s := grpc.NewServer()
	diagnosticspb.RegisterDiagnosticsServer(s, &tidbRPCServer{})
	return s
}

// tidbRPCServer is TiDB RPC Server. It is use for:
// 1. diagnostics RPC services.
type tidbRPCServer struct {
	diagnostics.DiagnoseServer
}

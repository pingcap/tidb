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

package server

import (
	"context"
	"fmt"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/sysutil"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// NewRPCServer creates a new rpc server.
func NewRPCServer(security config.Security, dom *domain.Domain, sm util.SessionManager) *grpc.Server {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server", zap.Any("stack", v))
		}
	}()

	var s *grpc.Server
	if len(security.ClusterSSLCA) != 0 {
		tlsConfig, err := security.ToTLSConfig()
		if err == nil {
			s = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
		}
	}
	if s == nil {
		s = grpc.NewServer()
	}
	rpcSrv := &rpcServer{
		dom: dom,
		sm:  sm,
	}
	// For redirection the cop task.
	mocktikv.TiDBRPCServerCoprocessorHandler = rpcSrv.handleCopRequest
	diagnosticspb.RegisterDiagnosticsServer(s, rpcSrv)
	tikvpb.RegisterTikvServer(s, rpcSrv)
	return s
}

// rpcServer contains below 2 services:
// 1. Diagnose service, it's used for SQL diagnose.
// 2. Coprocessor service, it reuse the TikvServer interface, but only support the Coprocessor interface now.
// Coprocessor service will handle the cop task from other TiDB server. Currently, it's only use for read the cluster memory table.
type rpcServer struct {
	sysutil.DiagnoseServer
	tikvpb.TikvServer
	dom *domain.Domain
	sm  util.SessionManager
}

// Coprocessor implements the TiKVServer interface.
func (s *rpcServer) Coprocessor(ctx context.Context, in *coprocessor.Request) (resp *coprocessor.Response, err error) {
	resp = &coprocessor.Response{}
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server coprocessor", zap.Any("stack", v))
			resp.OtherError = fmt.Sprintf("rpc coprocessor panic, :%v", v)
		}
	}()
	resp = s.handleCopRequest(ctx, in)
	return resp, nil
}

// handleCopRequest handles the cop dag request.
func (s *rpcServer) handleCopRequest(ctx context.Context, req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	se, err := s.createSession()
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	defer se.Close()

	h := executor.NewCoprocessorDAGHandler(se)
	return h.HandleRequest(ctx, req)
}

func (s *rpcServer) createSession() (session.Session, error) {
	se, err := session.CreateSessionWithDomain(s.dom.Store(), s.dom)
	if err != nil {
		return nil, err
	}
	do := domain.GetDomain(se)
	is := do.InfoSchema()
	// TODO: Need user and host to do privilege check.
	se.GetSessionVars().TxnCtx.InfoSchema = is
	// This is for disable parallel hash agg.
	// TODO: remove this.
	se.GetSessionVars().HashAggPartialConcurrency = 1
	se.GetSessionVars().HashAggFinalConcurrency = 1
	se.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(stringutil.StringerStr("coprocessor"), -1)
	se.SetSessionManager(s.sm)
	return se, nil
}

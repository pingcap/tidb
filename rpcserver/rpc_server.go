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
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var globalDomain *domain.Domain

// SetGlobalDomain uses to set global domain, it's for avoid cycle import.
func SetGlobalDomain(do *domain.Domain) {
	globalDomain = do
}

// CreateTiDBRPCServer creates a TiDB rpc server.
func CreateTiDBRPCServer() *grpc.Server {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in RPC server", zap.Any("stack", v))
		}
	}()
	s := grpc.NewServer()
	srv := &tidbRPCServer{}
	tikvpb.RegisterTikvServer(s, srv)
	return s
}

// tidbRPCServer is TiDB RPC Server, it reuse the TikvServer interface, but only support the Coprocessor interface now.
type tidbRPCServer struct {
	tikvpb.TikvServer
}

// Coprocessor implements the TiKVServer interface.
func (c *tidbRPCServer) Coprocessor(ctx context.Context, in *coprocessor.Request) (resp *coprocessor.Response, err error) {
	resp = &coprocessor.Response{}
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server coprocessor", zap.Any("stack", v))
			resp.OtherError = fmt.Sprintf("rpc coprocessor panic, :%v", v)
		}
	}()
	resp = c.handleCopDAGRequest(ctx, in)
	return resp, nil
}

func (c *tidbRPCServer) handleCopDAGRequest(ctx context.Context, req *coprocessor.Request) *coprocessor.Response {
	return HandleCopDAGRequest(ctx, req)
}

// HandleCopDAGRequest handles the cop dag request. It's export for mockTiKV to redirect request.
func HandleCopDAGRequest(ctx context.Context, req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	se, err := createSession()
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	defer se.Close()
	return executor.HandleCopDAGRequest(ctx, se, req)
}

func createSession() (session.Session, error) {
	createSessionFunc := session.CreateSessionWithDomainFunc(globalDomain.Store())
	re, err := createSessionFunc(globalDomain)
	if err != nil {
		return nil, err
	}
	se := re.(session.Session)
	do := domain.GetDomain(se)
	is := do.InfoSchema()
	se.GetSessionVars().TxnCtx.InfoSchema = is
	se.GetSessionVars().InRestrictedSQL = true
	// This is for disable parallel hash agg.
	// TODO: remove this.
	se.GetSessionVars().HashAggPartialConcurrency = 1
	se.GetSessionVars().HashAggFinalConcurrency = 1
	se.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(stringutil.StringerStr("coprocessor"), -1)
	se.SetSessionManager(util.GetglobalSessionManager())
	return se, nil
}

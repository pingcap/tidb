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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/sysutil"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/extension"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/topsql"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/peer"
)

// NewRPCServer creates a new rpc server.
func NewRPCServer(config *config.Config, dom *domain.Domain, sm util.SessionManager) *grpc.Server {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic in TiDB RPC server", zap.Reflect("r", v),
				zap.Stack("stack trace"))
		}
	}()

	s := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    time.Duration(config.Status.GRPCKeepAliveTime) * time.Second,
			Timeout: time.Duration(config.Status.GRPCKeepAliveTimeout) * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			// Allow clients send consecutive pings in every 5 seconds.
			// The default value of MinTime is 5 minutes,
			// which is too long compared with 10 seconds of TiDB's keepalive time.
			MinTime: 5 * time.Second,
		}),
		grpc.MaxConcurrentStreams(uint32(config.Status.GRPCConcurrentStreams)),
		grpc.InitialWindowSize(int32(config.Status.GRPCInitialWindowSize)),
		grpc.MaxSendMsgSize(config.Status.GRPCMaxSendMsgSize),
	)
	rpcSrv := &rpcServer{
		DiagnosticsServer: sysutil.NewDiagnosticsServer(config.Log.File.Filename),
		dom:               dom,
		sm:                sm,
	}
	diagnosticspb.RegisterDiagnosticsServer(s, rpcSrv)
	tikvpb.RegisterTikvServer(s, rpcSrv)
	topsql.RegisterPubSubServer(s)
	return s
}

// rpcServer contains below 2 services:
// 1. Diagnose service, it's used for SQL diagnose.
// 2. Coprocessor service, it reuse the TikvServer interface, but only support the Coprocessor interface now.
// Coprocessor service will handle the cop task from other TiDB server. Currently, it's only use for read the cluster memory table.
type rpcServer struct {
	*sysutil.DiagnosticsServer
	tikvpb.TikvServer
	dom *domain.Domain
	sm  util.SessionManager
}

// Coprocessor implements the TiKVServer interface.
func (s *rpcServer) Coprocessor(ctx context.Context, in *coprocessor.Request) (resp *coprocessor.Response, err error) {
	resp = &coprocessor.Response{}
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic when RPC server handing coprocessor", zap.Reflect("r", v),
				zap.Stack("stack trace"))
			resp.OtherError = fmt.Sprintf("panic when RPC server handing coprocessor, stack:%v", v)
		}
	}()
	resp = s.handleCopRequest(ctx, in)
	return resp, nil
}

// CoprocessorStream implements the TiKVServer interface.
func (s *rpcServer) CoprocessorStream(in *coprocessor.Request, stream tikvpb.Tikv_CoprocessorStreamServer) (err error) {
	resp := &coprocessor.Response{}
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic when RPC server handing coprocessor stream", zap.Reflect("r", v),
				zap.Stack("stack trace"))
			resp.OtherError = fmt.Sprintf("panic when when RPC server handing coprocessor stream, stack:%v", v)
			err = stream.Send(resp)
			if err != nil {
				logutil.BgLogger().Error("panic when RPC server handing coprocessor stream, send response to stream error", zap.Error(err))
			}
		}
	}()

	se, err := s.createSession()
	if err != nil {
		resp.OtherError = err.Error()
		return stream.Send(resp)
	}
	defer se.Close()

	h := executor.NewCoprocessorDAGHandler(se)
	return h.HandleStreamRequest(context.Background(), in, stream)
}

// BatchCommands implements the TiKVServer interface.
func (s *rpcServer) BatchCommands(ss tikvpb.Tikv_BatchCommandsServer) error {
	defer func() {
		if v := recover(); v != nil {
			logutil.BgLogger().Error("panic when RPC server handing batch commands", zap.Reflect("r", v),
				zap.Stack("stack trace"))
		}
	}()
	for {
		reqs, err := ss.Recv()
		if err != nil {
			logutil.BgLogger().Error("RPC server batch commands receive fail", zap.Error(err))
			return err
		}

		responses := make([]*tikvpb.BatchCommandsResponse_Response, 0, len(reqs.Requests))
		for _, req := range reqs.Requests {
			var response *tikvpb.BatchCommandsResponse_Response
			switch request := req.Cmd.(type) {
			case *tikvpb.BatchCommandsRequest_Request_Coprocessor:
				cop := request.Coprocessor
				resp, err := s.Coprocessor(context.Background(), cop)
				if err != nil {
					return err
				}
				response = &tikvpb.BatchCommandsResponse_Response{
					Cmd: &tikvpb.BatchCommandsResponse_Response_Coprocessor{
						Coprocessor: resp,
					},
				}
			case *tikvpb.BatchCommandsRequest_Request_Empty:
				response = &tikvpb.BatchCommandsResponse_Response{
					Cmd: &tikvpb.BatchCommandsResponse_Response_Empty{
						Empty: &tikvpb.BatchCommandsEmptyResponse{
							TestId: request.Empty.TestId,
						},
					},
				}
			default:
				logutil.BgLogger().Info("RPC server batch commands receive unknown request", zap.Any("req", request))
				response = &tikvpb.BatchCommandsResponse_Response{
					Cmd: &tikvpb.BatchCommandsResponse_Response_Empty{
						Empty: &tikvpb.BatchCommandsEmptyResponse{},
					},
				}
			}
			responses = append(responses, response)
		}

		err = ss.Send(&tikvpb.BatchCommandsResponse{
			Responses:  responses,
			RequestIds: reqs.GetRequestIds(),
		})
		if err != nil {
			logutil.BgLogger().Error("RPC server batch commands send fail", zap.Error(err))
			return err
		}
	}
}

// handleCopRequest handles the cop dag request.
func (s *rpcServer) handleCopRequest(ctx context.Context, req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	se, err := s.createSession()
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	defer func() {
		sc := se.GetSessionVars().StmtCtx
		if sc.MemTracker != nil {
			sc.MemTracker.Detach()
		}
		se.Close()
	}()

	if p, ok := peer.FromContext(ctx); ok {
		se.GetSessionVars().SourceAddr = *p.Addr.(*net.TCPAddr)
	}

	h := executor.NewCoprocessorDAGHandler(se)
	return h.HandleRequest(ctx, req)
}

func (s *rpcServer) createSession() (session.Session, error) {
	se, err := session.CreateSessionWithDomain(s.dom.Store(), s.dom)
	if err != nil {
		return nil, err
	}
	extensions, err := extension.GetExtensions()
	if err != nil {
		return nil, err
	}
	do := domain.GetDomain(se)
	is := do.InfoSchema()
	pm := privileges.NewUserPrivileges(do.PrivilegeHandle(), extensions)
	privilege.BindPrivilegeManager(se, pm)
	vars := se.GetSessionVars()
	vars.TxnCtx.InfoSchema = is
	// This is for disable parallel hash agg.
	// TODO: remove this.
	vars.SetHashAggPartialConcurrency(1)
	vars.SetHashAggFinalConcurrency(1)
	vars.StmtCtx.InitMemTracker(memory.LabelForSQLText, -1)
	vars.StmtCtx.MemTracker.AttachTo(vars.MemTracker)
	switch variable.OOMAction.Load() {
	case variable.OOMActionCancel:
		action := &memory.PanicOnExceed{}
		vars.MemTracker.SetActionOnExceed(action)
	}
	se.SetSessionManager(s.sm)
	return se, nil
}

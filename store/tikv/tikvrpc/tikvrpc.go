// Copyright 2017 PingCAP, Inc.
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

package tikvrpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/kv"
)

// CmdType represents the concrete request type in Request or response type in Response.
type CmdType uint16

// CmdType values.
const (
	CmdGet CmdType = 1 + iota
	CmdScan
	CmdPrewrite
	CmdCommit
	CmdCleanup
	CmdBatchGet
	CmdBatchRollback
	CmdScanLock
	CmdResolveLock
	CmdGC
	CmdDeleteRange
	CmdPessimisticLock
	CmdPessimisticRollback
	CmdTxnHeartBeat
	CmdCheckTxnStatus

	CmdRawGet CmdType = 256 + iota
	CmdRawBatchGet
	CmdRawPut
	CmdRawBatchPut
	CmdRawDelete
	CmdRawBatchDelete
	CmdRawDeleteRange
	CmdRawScan

	CmdUnsafeDestroyRange

	CmdCop CmdType = 512 + iota
	CmdCopStream

	CmdMvccGetByKey CmdType = 1024 + iota
	CmdMvccGetByStartTs
	CmdSplitRegion

	CmdDebugGetRegionProperties CmdType = 2048 + iota

	CmdEmpty CmdType = 3072 + iota
)

func (t CmdType) String() string {
	switch t {
	case CmdGet:
		return "Get"
	case CmdScan:
		return "Scan"
	case CmdPrewrite:
		return "Prewrite"
	case CmdPessimisticLock:
		return "PessimisticLock"
	case CmdPessimisticRollback:
		return "PessimisticRollback"
	case CmdCommit:
		return "Commit"
	case CmdCleanup:
		return "Cleanup"
	case CmdBatchGet:
		return "BatchGet"
	case CmdBatchRollback:
		return "BatchRollback"
	case CmdScanLock:
		return "ScanLock"
	case CmdResolveLock:
		return "ResolveLock"
	case CmdGC:
		return "GC"
	case CmdDeleteRange:
		return "DeleteRange"
	case CmdRawGet:
		return "RawGet"
	case CmdRawBatchGet:
		return "RawBatchGet"
	case CmdRawPut:
		return "RawPut"
	case CmdRawBatchPut:
		return "RawBatchPut"
	case CmdRawDelete:
		return "RawDelete"
	case CmdRawBatchDelete:
		return "RawBatchDelete"
	case CmdRawDeleteRange:
		return "RawDeleteRange"
	case CmdRawScan:
		return "RawScan"
	case CmdUnsafeDestroyRange:
		return "UnsafeDestroyRange"
	case CmdCop:
		return "Cop"
	case CmdCopStream:
		return "CopStream"
	case CmdMvccGetByKey:
		return "MvccGetByKey"
	case CmdMvccGetByStartTs:
		return "MvccGetByStartTS"
	case CmdSplitRegion:
		return "SplitRegion"
	case CmdCheckTxnStatus:
		return "CheckTxnStatus"
	case CmdDebugGetRegionProperties:
		return "DebugGetRegionProperties"
	case CmdTxnHeartBeat:
		return "TxnHeartBeat"
	}
	return "Unknown"
}

// Request wraps all kv/coprocessor requests.
type Request struct {
	Type CmdType
	req  interface{}
	kvrpcpb.Context
	ReplicaReadSeed uint32
}

// NewRequest returns new kv rpc request.
func NewRequest(typ CmdType, pointer interface{}, ctxs ...kvrpcpb.Context) *Request {
	if len(ctxs) > 0 {
		return &Request{
			Type:    typ,
			req:     pointer,
			Context: ctxs[0],
		}
	}
	return &Request{
		Type: typ,
		req:  pointer,
	}
}

// NewReplicaReadRequest returns new kv rpc request with replica read.
func NewReplicaReadRequest(typ CmdType, pointer interface{}, replicaReadType kv.ReplicaReadType, replicaReadSeed uint32, ctxs ...kvrpcpb.Context) *Request {
	req := NewRequest(typ, pointer, ctxs...)
	req.ReplicaRead = replicaReadType.IsFollowerRead()
	req.ReplicaReadSeed = replicaReadSeed
	return req
}

// Get returns GetRequest in request.
func (req *Request) Get() *kvrpcpb.GetRequest {
	return req.req.(*kvrpcpb.GetRequest)
}

// Scan returns ScanRequest in request.
func (req *Request) Scan() *kvrpcpb.ScanRequest {
	return req.req.(*kvrpcpb.ScanRequest)
}

// Prewrite returns PrewriteRequest in request.
func (req *Request) Prewrite() *kvrpcpb.PrewriteRequest {
	return req.req.(*kvrpcpb.PrewriteRequest)
}

// Commit returns CommitRequest in request.
func (req *Request) Commit() *kvrpcpb.CommitRequest {
	return req.req.(*kvrpcpb.CommitRequest)
}

// Cleanup returns CleanupRequest in request.
func (req *Request) Cleanup() *kvrpcpb.CleanupRequest {
	return req.req.(*kvrpcpb.CleanupRequest)
}

// BatchGet returns BatchGetRequest in request.
func (req *Request) BatchGet() *kvrpcpb.BatchGetRequest {
	return req.req.(*kvrpcpb.BatchGetRequest)
}

// BatchRollback returns BatchRollbackRequest in request.
func (req *Request) BatchRollback() *kvrpcpb.BatchRollbackRequest {
	return req.req.(*kvrpcpb.BatchRollbackRequest)
}

// ScanLock returns ScanLockRequest in request.
func (req *Request) ScanLock() *kvrpcpb.ScanLockRequest {
	return req.req.(*kvrpcpb.ScanLockRequest)
}

// ResolveLock returns ResolveLockRequest in request.
func (req *Request) ResolveLock() *kvrpcpb.ResolveLockRequest {
	return req.req.(*kvrpcpb.ResolveLockRequest)
}

// GC returns GCRequest in request.
func (req *Request) GC() *kvrpcpb.GCRequest {
	return req.req.(*kvrpcpb.GCRequest)
}

// DeleteRange returns DeleteRangeRequest in request.
func (req *Request) DeleteRange() *kvrpcpb.DeleteRangeRequest {
	return req.req.(*kvrpcpb.DeleteRangeRequest)
}

// RawGet returns RawGetRequest in request.
func (req *Request) RawGet() *kvrpcpb.RawGetRequest {
	return req.req.(*kvrpcpb.RawGetRequest)
}

// RawBatchGet returns RawBatchGetRequest in request.
func (req *Request) RawBatchGet() *kvrpcpb.RawBatchGetRequest {
	return req.req.(*kvrpcpb.RawBatchGetRequest)
}

// RawPut returns RawPutRequest in request.
func (req *Request) RawPut() *kvrpcpb.RawPutRequest {
	return req.req.(*kvrpcpb.RawPutRequest)
}

// RawBatchPut returns RawBatchPutRequest in request.
func (req *Request) RawBatchPut() *kvrpcpb.RawBatchPutRequest {
	return req.req.(*kvrpcpb.RawBatchPutRequest)
}

// RawDelete returns PrewriteRequest in request.
func (req *Request) RawDelete() *kvrpcpb.RawDeleteRequest {
	return req.req.(*kvrpcpb.RawDeleteRequest)
}

// RawBatchDelete returns RawBatchDeleteRequest in request.
func (req *Request) RawBatchDelete() *kvrpcpb.RawBatchDeleteRequest {
	return req.req.(*kvrpcpb.RawBatchDeleteRequest)
}

// RawDeleteRange returns RawDeleteRangeRequest in request.
func (req *Request) RawDeleteRange() *kvrpcpb.RawDeleteRangeRequest {
	return req.req.(*kvrpcpb.RawDeleteRangeRequest)
}

// RawScan returns RawScanRequest in request.
func (req *Request) RawScan() *kvrpcpb.RawScanRequest {
	return req.req.(*kvrpcpb.RawScanRequest)
}

// UnsafeDestroyRange returns UnsafeDestroyRangeRequest in request.
func (req *Request) UnsafeDestroyRange() *kvrpcpb.UnsafeDestroyRangeRequest {
	return req.req.(*kvrpcpb.UnsafeDestroyRangeRequest)
}

// Cop returns coprocessor request in request.
func (req *Request) Cop() *coprocessor.Request {
	return req.req.(*coprocessor.Request)
}

// MvccGetByKey returns MvccGetByKeyRequest in request.
func (req *Request) MvccGetByKey() *kvrpcpb.MvccGetByKeyRequest {
	return req.req.(*kvrpcpb.MvccGetByKeyRequest)
}

// MvccGetByStartTs returns MvccGetByStartTsRequest in request.
func (req *Request) MvccGetByStartTs() *kvrpcpb.MvccGetByStartTsRequest {
	return req.req.(*kvrpcpb.MvccGetByStartTsRequest)
}

// SplitRegion returns SplitRegionRequest in request.
func (req *Request) SplitRegion() *kvrpcpb.SplitRegionRequest {
	return req.req.(*kvrpcpb.SplitRegionRequest)
}

// PessimisticLock returns PessimisticLockRequest in request.
func (req *Request) PessimisticLock() *kvrpcpb.PessimisticLockRequest {
	return req.req.(*kvrpcpb.PessimisticLockRequest)
}

// PessimisticRollback returns PessimisticRollbackRequest in request.
func (req *Request) PessimisticRollback() *kvrpcpb.PessimisticRollbackRequest {
	return req.req.(*kvrpcpb.PessimisticRollbackRequest)
}

// DebugGetRegionProperties returns GetRegionPropertiesRequest in request.
func (req *Request) DebugGetRegionProperties() *debugpb.GetRegionPropertiesRequest {
	return req.req.(*debugpb.GetRegionPropertiesRequest)
}

// Empty returns BatchCommandsEmptyRequest in request.
func (req *Request) Empty() *tikvpb.BatchCommandsEmptyRequest {
	return req.req.(*tikvpb.BatchCommandsEmptyRequest)
}

// CheckTxnStatus returns CheckTxnStatusRequest in request.
func (req *Request) CheckTxnStatus() *kvrpcpb.CheckTxnStatusRequest {
	return req.req.(*kvrpcpb.CheckTxnStatusRequest)
}

// TxnHeartBeat returns TxnHeartBeatRequest in request.
func (req *Request) TxnHeartBeat() *kvrpcpb.TxnHeartBeatRequest {
	return req.req.(*kvrpcpb.TxnHeartBeatRequest)
}

// ToBatchCommandsRequest converts the request to an entry in BatchCommands request.
func (req *Request) ToBatchCommandsRequest() *tikvpb.BatchCommandsRequest_Request {
	switch req.Type {
	case CmdGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Get{Get: req.Get()}}
	case CmdScan:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Scan{Scan: req.Scan()}}
	case CmdPrewrite:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Prewrite{Prewrite: req.Prewrite()}}
	case CmdCommit:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Commit{Commit: req.Commit()}}
	case CmdCleanup:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Cleanup{Cleanup: req.Cleanup()}}
	case CmdBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchGet{BatchGet: req.BatchGet()}}
	case CmdBatchRollback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchRollback{BatchRollback: req.BatchRollback()}}
	case CmdScanLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ScanLock{ScanLock: req.ScanLock()}}
	case CmdResolveLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ResolveLock{ResolveLock: req.ResolveLock()}}
	case CmdGC:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_GC{GC: req.GC()}}
	case CmdDeleteRange:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_DeleteRange{DeleteRange: req.DeleteRange()}}
	case CmdRawGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawGet{RawGet: req.RawGet()}}
	case CmdRawBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchGet{RawBatchGet: req.RawBatchGet()}}
	case CmdRawPut:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawPut{RawPut: req.RawPut()}}
	case CmdRawBatchPut:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchPut{RawBatchPut: req.RawBatchPut()}}
	case CmdRawDelete:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDelete{RawDelete: req.RawDelete()}}
	case CmdRawBatchDelete:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchDelete{RawBatchDelete: req.RawBatchDelete()}}
	case CmdRawDeleteRange:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDeleteRange{RawDeleteRange: req.RawDeleteRange()}}
	case CmdRawScan:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawScan{RawScan: req.RawScan()}}
	case CmdCop:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: req.Cop()}}
	case CmdPessimisticLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticLock{PessimisticLock: req.PessimisticLock()}}
	case CmdPessimisticRollback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticRollback{PessimisticRollback: req.PessimisticRollback()}}
	case CmdEmpty:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Empty{Empty: req.Empty()}}
	case CmdCheckTxnStatus:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_CheckTxnStatus{CheckTxnStatus: req.CheckTxnStatus()}}
	case CmdTxnHeartBeat:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_TxnHeartBeat{TxnHeartBeat: req.TxnHeartBeat()}}
	}
	return nil
}

// IsDebugReq check whether the req is debug req.
func (req *Request) IsDebugReq() bool {
	switch req.Type {
	case CmdDebugGetRegionProperties:
		return true
	}
	return false
}

// Response wraps all kv/coprocessor responses.
type Response struct {
	Resp interface{}
}

// FromBatchCommandsResponse converts a BatchCommands response to Response.
func FromBatchCommandsResponse(res *tikvpb.BatchCommandsResponse_Response) *Response {
	switch res := res.GetCmd().(type) {
	case *tikvpb.BatchCommandsResponse_Response_Get:
		return &Response{Resp: res.Get}
	case *tikvpb.BatchCommandsResponse_Response_Scan:
		return &Response{Resp: res.Scan}
	case *tikvpb.BatchCommandsResponse_Response_Prewrite:
		return &Response{Resp: res.Prewrite}
	case *tikvpb.BatchCommandsResponse_Response_Commit:
		return &Response{Resp: res.Commit}
	case *tikvpb.BatchCommandsResponse_Response_Cleanup:
		return &Response{Resp: res.Cleanup}
	case *tikvpb.BatchCommandsResponse_Response_BatchGet:
		return &Response{Resp: res.BatchGet}
	case *tikvpb.BatchCommandsResponse_Response_BatchRollback:
		return &Response{Resp: res.BatchRollback}
	case *tikvpb.BatchCommandsResponse_Response_ScanLock:
		return &Response{Resp: res.ScanLock}
	case *tikvpb.BatchCommandsResponse_Response_ResolveLock:
		return &Response{Resp: res.ResolveLock}
	case *tikvpb.BatchCommandsResponse_Response_GC:
		return &Response{Resp: res.GC}
	case *tikvpb.BatchCommandsResponse_Response_DeleteRange:
		return &Response{Resp: res.DeleteRange}
	case *tikvpb.BatchCommandsResponse_Response_RawGet:
		return &Response{Resp: res.RawGet}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchGet:
		return &Response{Resp: res.RawBatchGet}
	case *tikvpb.BatchCommandsResponse_Response_RawPut:
		return &Response{Resp: res.RawPut}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchPut:
		return &Response{Resp: res.RawBatchPut}
	case *tikvpb.BatchCommandsResponse_Response_RawDelete:
		return &Response{Resp: res.RawDelete}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchDelete:
		return &Response{Resp: res.RawBatchDelete}
	case *tikvpb.BatchCommandsResponse_Response_RawDeleteRange:
		return &Response{Resp: res.RawDeleteRange}
	case *tikvpb.BatchCommandsResponse_Response_RawScan:
		return &Response{Resp: res.RawScan}
	case *tikvpb.BatchCommandsResponse_Response_Coprocessor:
		return &Response{Resp: res.Coprocessor}
	case *tikvpb.BatchCommandsResponse_Response_PessimisticLock:
		return &Response{Resp: res.PessimisticLock}
	case *tikvpb.BatchCommandsResponse_Response_PessimisticRollback:
		return &Response{Resp: res.PessimisticRollback}
	case *tikvpb.BatchCommandsResponse_Response_Empty:
		return &Response{Resp: res.Empty}
	case *tikvpb.BatchCommandsResponse_Response_TxnHeartBeat:
		return &Response{Resp: res.TxnHeartBeat}
	case *tikvpb.BatchCommandsResponse_Response_CheckTxnStatus:
		return &Response{Resp: res.CheckTxnStatus}
	}
	return nil
}

// CopStreamResponse combinates tikvpb.Tikv_CoprocessorStreamClient and the first Recv() result together.
// In streaming API, get grpc stream client may not involve any network packet, then region error have
// to be handled in Recv() function. This struct facilitates the error handling.
type CopStreamResponse struct {
	tikvpb.Tikv_CoprocessorStreamClient
	*coprocessor.Response // The first result of Recv()
	Timeout               time.Duration
	Lease                 // Shared by this object and a background goroutine.
}

// SetContext set the Context field for the given req to the specified ctx.
func SetContext(req *Request, region *metapb.Region, peer *metapb.Peer) error {
	ctx := &req.Context
	ctx.RegionId = region.Id
	ctx.RegionEpoch = region.RegionEpoch
	ctx.Peer = peer

	switch req.Type {
	case CmdGet:
		req.Get().Context = ctx
	case CmdScan:
		req.Scan().Context = ctx
	case CmdPrewrite:
		req.Prewrite().Context = ctx
	case CmdPessimisticLock:
		req.PessimisticLock().Context = ctx
	case CmdPessimisticRollback:
		req.PessimisticRollback().Context = ctx
	case CmdCommit:
		req.Commit().Context = ctx
	case CmdCleanup:
		req.Cleanup().Context = ctx
	case CmdBatchGet:
		req.BatchGet().Context = ctx
	case CmdBatchRollback:
		req.BatchRollback().Context = ctx
	case CmdScanLock:
		req.ScanLock().Context = ctx
	case CmdResolveLock:
		req.ResolveLock().Context = ctx
	case CmdGC:
		req.GC().Context = ctx
	case CmdDeleteRange:
		req.DeleteRange().Context = ctx
	case CmdRawGet:
		req.RawGet().Context = ctx
	case CmdRawBatchGet:
		req.RawBatchGet().Context = ctx
	case CmdRawPut:
		req.RawPut().Context = ctx
	case CmdRawBatchPut:
		req.RawBatchPut().Context = ctx
	case CmdRawDelete:
		req.RawDelete().Context = ctx
	case CmdRawBatchDelete:
		req.RawBatchDelete().Context = ctx
	case CmdRawDeleteRange:
		req.RawDeleteRange().Context = ctx
	case CmdRawScan:
		req.RawScan().Context = ctx
	case CmdUnsafeDestroyRange:
		req.UnsafeDestroyRange().Context = ctx
	case CmdCop:
		req.Cop().Context = ctx
	case CmdCopStream:
		req.Cop().Context = ctx
	case CmdMvccGetByKey:
		req.MvccGetByKey().Context = ctx
	case CmdMvccGetByStartTs:
		req.MvccGetByStartTs().Context = ctx
	case CmdSplitRegion:
		req.SplitRegion().Context = ctx
	case CmdEmpty:
		req.SplitRegion().Context = ctx
	case CmdTxnHeartBeat:
		req.TxnHeartBeat().Context = ctx
	case CmdCheckTxnStatus:
		req.CheckTxnStatus().Context = ctx
	default:
		return fmt.Errorf("invalid request type %v", req.Type)
	}
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	var p interface{}
	resp := &Response{}
	switch req.Type {
	case CmdGet:
		p = &kvrpcpb.GetResponse{
			RegionError: e,
		}
	case CmdScan:
		p = &kvrpcpb.ScanResponse{
			RegionError: e,
		}
	case CmdPrewrite:
		p = &kvrpcpb.PrewriteResponse{
			RegionError: e,
		}
	case CmdPessimisticLock:
		p = &kvrpcpb.PessimisticLockResponse{
			RegionError: e,
		}
	case CmdPessimisticRollback:
		p = &kvrpcpb.PessimisticRollbackResponse{
			RegionError: e,
		}
	case CmdCommit:
		p = &kvrpcpb.CommitResponse{
			RegionError: e,
		}
	case CmdCleanup:
		p = &kvrpcpb.CleanupResponse{
			RegionError: e,
		}
	case CmdBatchGet:
		p = &kvrpcpb.BatchGetResponse{
			RegionError: e,
		}
	case CmdBatchRollback:
		p = &kvrpcpb.BatchRollbackResponse{
			RegionError: e,
		}
	case CmdScanLock:
		p = &kvrpcpb.ScanLockResponse{
			RegionError: e,
		}
	case CmdResolveLock:
		p = &kvrpcpb.ResolveLockResponse{
			RegionError: e,
		}
	case CmdGC:
		p = &kvrpcpb.GCResponse{
			RegionError: e,
		}
	case CmdDeleteRange:
		p = &kvrpcpb.DeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawGet:
		p = &kvrpcpb.RawGetResponse{
			RegionError: e,
		}
	case CmdRawBatchGet:
		p = &kvrpcpb.RawBatchGetResponse{
			RegionError: e,
		}
	case CmdRawPut:
		p = &kvrpcpb.RawPutResponse{
			RegionError: e,
		}
	case CmdRawBatchPut:
		p = &kvrpcpb.RawBatchPutResponse{
			RegionError: e,
		}
	case CmdRawDelete:
		p = &kvrpcpb.RawDeleteResponse{
			RegionError: e,
		}
	case CmdRawBatchDelete:
		p = &kvrpcpb.RawBatchDeleteResponse{
			RegionError: e,
		}
	case CmdRawDeleteRange:
		p = &kvrpcpb.RawDeleteRangeResponse{
			RegionError: e,
		}
	case CmdRawScan:
		p = &kvrpcpb.RawScanResponse{
			RegionError: e,
		}
	case CmdUnsafeDestroyRange:
		p = &kvrpcpb.UnsafeDestroyRangeResponse{
			RegionError: e,
		}
	case CmdCop:
		p = &coprocessor.Response{
			RegionError: e,
		}
	case CmdCopStream:
		p = &CopStreamResponse{
			Response: &coprocessor.Response{
				RegionError: e,
			},
		}
	case CmdMvccGetByKey:
		p = &kvrpcpb.MvccGetByKeyResponse{
			RegionError: e,
		}
	case CmdMvccGetByStartTs:
		p = &kvrpcpb.MvccGetByStartTsResponse{
			RegionError: e,
		}
	case CmdSplitRegion:
		p = &kvrpcpb.SplitRegionResponse{
			RegionError: e,
		}
	case CmdEmpty:
	case CmdTxnHeartBeat:
		p = &kvrpcpb.TxnHeartBeatResponse{
			RegionError: e,
		}
	default:
		return nil, fmt.Errorf("invalid request type %v", req.Type)
	}
	resp.Resp = p
	return resp, nil
}

type getRegionError interface {
	GetRegionError() *errorpb.Error
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	if resp.Resp == nil {
		return nil, nil
	}
	err, ok := resp.Resp.(getRegionError)
	if !ok {
		if _, isEmpty := resp.Resp.(*tikvpb.BatchCommandsEmptyResponse); isEmpty {
			return nil, nil
		}
		return nil, fmt.Errorf("invalid response type %v", resp)
	}
	return err.GetRegionError(), nil
}

// CallRPC launches a rpc call.
// ch is needed to implement timeout for coprocessor streaing, the stream object's
// cancel function will be sent to the channel, together with a lease checked by a background goroutine.
func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdGet:
		resp.Resp, err = client.KvGet(ctx, req.Get())
	case CmdScan:
		resp.Resp, err = client.KvScan(ctx, req.Scan())
	case CmdPrewrite:
		resp.Resp, err = client.KvPrewrite(ctx, req.Prewrite())
	case CmdPessimisticLock:
		resp.Resp, err = client.KvPessimisticLock(ctx, req.PessimisticLock())
	case CmdPessimisticRollback:
		resp.Resp, err = client.KVPessimisticRollback(ctx, req.PessimisticRollback())
	case CmdCommit:
		resp.Resp, err = client.KvCommit(ctx, req.Commit())
	case CmdCleanup:
		resp.Resp, err = client.KvCleanup(ctx, req.Cleanup())
	case CmdBatchGet:
		resp.Resp, err = client.KvBatchGet(ctx, req.BatchGet())
	case CmdBatchRollback:
		resp.Resp, err = client.KvBatchRollback(ctx, req.BatchRollback())
	case CmdScanLock:
		resp.Resp, err = client.KvScanLock(ctx, req.ScanLock())
	case CmdResolveLock:
		resp.Resp, err = client.KvResolveLock(ctx, req.ResolveLock())
	case CmdGC:
		resp.Resp, err = client.KvGC(ctx, req.GC())
	case CmdDeleteRange:
		resp.Resp, err = client.KvDeleteRange(ctx, req.DeleteRange())
	case CmdRawGet:
		resp.Resp, err = client.RawGet(ctx, req.RawGet())
	case CmdRawBatchGet:
		resp.Resp, err = client.RawBatchGet(ctx, req.RawBatchGet())
	case CmdRawPut:
		resp.Resp, err = client.RawPut(ctx, req.RawPut())
	case CmdRawBatchPut:
		resp.Resp, err = client.RawBatchPut(ctx, req.RawBatchPut())
	case CmdRawDelete:
		resp.Resp, err = client.RawDelete(ctx, req.RawDelete())
	case CmdRawBatchDelete:
		resp.Resp, err = client.RawBatchDelete(ctx, req.RawBatchDelete())
	case CmdRawDeleteRange:
		resp.Resp, err = client.RawDeleteRange(ctx, req.RawDeleteRange())
	case CmdRawScan:
		resp.Resp, err = client.RawScan(ctx, req.RawScan())
	case CmdUnsafeDestroyRange:
		resp.Resp, err = client.UnsafeDestroyRange(ctx, req.UnsafeDestroyRange())
	case CmdCop:
		resp.Resp, err = client.Coprocessor(ctx, req.Cop())
	case CmdCopStream:
		var streamClient tikvpb.Tikv_CoprocessorStreamClient
		streamClient, err = client.CoprocessorStream(ctx, req.Cop())
		resp.Resp = &CopStreamResponse{
			Tikv_CoprocessorStreamClient: streamClient,
		}
	case CmdMvccGetByKey:
		resp.Resp, err = client.MvccGetByKey(ctx, req.MvccGetByKey())
	case CmdMvccGetByStartTs:
		resp.Resp, err = client.MvccGetByStartTs(ctx, req.MvccGetByStartTs())
	case CmdSplitRegion:
		resp.Resp, err = client.SplitRegion(ctx, req.SplitRegion())
	case CmdEmpty:
		resp.Resp, err = &tikvpb.BatchCommandsEmptyResponse{}, nil
	case CmdCheckTxnStatus:
		resp.Resp, err = client.KvCheckTxnStatus(ctx, req.CheckTxnStatus())
	case CmdTxnHeartBeat:
		resp.Resp, err = client.KvTxnHeartBeat(ctx, req.TxnHeartBeat())
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

// CallDebugRPC launches a debug rpc call.
func CallDebugRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error) {
	resp := &Response{}
	var err error
	switch req.Type {
	case CmdDebugGetRegionProperties:
		resp.Resp, err = client.GetRegionProperties(ctx, req.DebugGetRegionProperties())
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	return resp, err
}

// Lease is used to implement grpc stream timeout.
type Lease struct {
	Cancel   context.CancelFunc
	deadline int64 // A time.UnixNano value, if time.Now().UnixNano() > deadline, cancel() would be called.
}

// Recv overrides the stream client Recv() function.
func (resp *CopStreamResponse) Recv() (*coprocessor.Response, error) {
	deadline := time.Now().Add(resp.Timeout).UnixNano()
	atomic.StoreInt64(&resp.Lease.deadline, deadline)

	ret, err := resp.Tikv_CoprocessorStreamClient.Recv()

	atomic.StoreInt64(&resp.Lease.deadline, 0) // Stop the lease check.
	return ret, errors.Trace(err)
}

// Close closes the CopStreamResponse object.
func (resp *CopStreamResponse) Close() {
	atomic.StoreInt64(&resp.Lease.deadline, 1)
	// We also call cancel here because CheckStreamTimeoutLoop
	// is not guaranteed to cancel all items when it exits.
	if resp.Lease.Cancel != nil {
		resp.Lease.Cancel()
	}
}

// CheckStreamTimeoutLoop runs periodically to check is there any stream request timeouted.
// Lease is an object to track stream requests, call this function with "go CheckStreamTimeoutLoop()"
// It is not guaranteed to call every Lease.Cancel() putting into channel when exits.
// If grpc-go supports SetDeadline(https://github.com/grpc/grpc-go/issues/2917), we can stop using this method.
func CheckStreamTimeoutLoop(ch <-chan *Lease, done <-chan struct{}) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	array := make([]*Lease, 0, 1024)

	for {
		select {
		case <-done:
		drainLoop:
			// Try my best cleaning the channel to make SendRequest which is blocking by it continues.
			for {
				select {
				case <-ch:
				default:
					break drainLoop
				}
			}
			return
		case item := <-ch:
			array = append(array, item)
		case now := <-ticker.C:
			array = keepOnlyActive(array, now.UnixNano())
		}
	}
}

// keepOnlyActive removes completed items, call cancel function for timeout items.
func keepOnlyActive(array []*Lease, now int64) []*Lease {
	idx := 0
	for i := 0; i < len(array); i++ {
		item := array[i]
		deadline := atomic.LoadInt64(&item.deadline)
		if deadline == 0 || deadline > now {
			array[idx] = array[i]
			idx++
		} else {
			item.Cancel()
		}
	}
	return array[:idx]
}

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
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/debugpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
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
	case CmdDebugGetRegionProperties:
		return "DebugGetRegionProperties"
	}
	return "Unknown"
}

// Request wraps all kv/coprocessor requests.
type Request struct {
	kvrpcpb.Context
	Type               CmdType
	Get                *kvrpcpb.GetRequest
	Scan               *kvrpcpb.ScanRequest
	Prewrite           *kvrpcpb.PrewriteRequest
	Commit             *kvrpcpb.CommitRequest
	Cleanup            *kvrpcpb.CleanupRequest
	BatchGet           *kvrpcpb.BatchGetRequest
	BatchRollback      *kvrpcpb.BatchRollbackRequest
	ScanLock           *kvrpcpb.ScanLockRequest
	ResolveLock        *kvrpcpb.ResolveLockRequest
	GC                 *kvrpcpb.GCRequest
	DeleteRange        *kvrpcpb.DeleteRangeRequest
	RawGet             *kvrpcpb.RawGetRequest
	RawBatchGet        *kvrpcpb.RawBatchGetRequest
	RawPut             *kvrpcpb.RawPutRequest
	RawBatchPut        *kvrpcpb.RawBatchPutRequest
	RawDelete          *kvrpcpb.RawDeleteRequest
	RawBatchDelete     *kvrpcpb.RawBatchDeleteRequest
	RawDeleteRange     *kvrpcpb.RawDeleteRangeRequest
	RawScan            *kvrpcpb.RawScanRequest
	UnsafeDestroyRange *kvrpcpb.UnsafeDestroyRangeRequest
	Cop                *coprocessor.Request
	MvccGetByKey       *kvrpcpb.MvccGetByKeyRequest
	MvccGetByStartTs   *kvrpcpb.MvccGetByStartTsRequest
	SplitRegion        *kvrpcpb.SplitRegionRequest

	PessimisticLock     *kvrpcpb.PessimisticLockRequest
	PessimisticRollback *kvrpcpb.PessimisticRollbackRequest

	DebugGetRegionProperties *debugpb.GetRegionPropertiesRequest
}

// ToBatchCommandsRequest converts the request to an entry in BatchCommands request.
func (req *Request) ToBatchCommandsRequest() *tikvpb.BatchCommandsRequest_Request {
	switch req.Type {
	case CmdGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Get{Get: req.Get}}
	case CmdScan:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Scan{Scan: req.Scan}}
	case CmdPrewrite:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Prewrite{Prewrite: req.Prewrite}}
	case CmdCommit:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Commit{Commit: req.Commit}}
	case CmdCleanup:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Cleanup{Cleanup: req.Cleanup}}
	case CmdBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchGet{BatchGet: req.BatchGet}}
	case CmdBatchRollback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_BatchRollback{BatchRollback: req.BatchRollback}}
	case CmdScanLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ScanLock{ScanLock: req.ScanLock}}
	case CmdResolveLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_ResolveLock{ResolveLock: req.ResolveLock}}
	case CmdGC:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_GC{GC: req.GC}}
	case CmdDeleteRange:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_DeleteRange{DeleteRange: req.DeleteRange}}
	case CmdRawGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawGet{RawGet: req.RawGet}}
	case CmdRawBatchGet:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchGet{RawBatchGet: req.RawBatchGet}}
	case CmdRawPut:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawPut{RawPut: req.RawPut}}
	case CmdRawBatchPut:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchPut{RawBatchPut: req.RawBatchPut}}
	case CmdRawDelete:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDelete{RawDelete: req.RawDelete}}
	case CmdRawBatchDelete:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawBatchDelete{RawBatchDelete: req.RawBatchDelete}}
	case CmdRawDeleteRange:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawDeleteRange{RawDeleteRange: req.RawDeleteRange}}
	case CmdRawScan:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_RawScan{RawScan: req.RawScan}}
	case CmdCop:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_Coprocessor{Coprocessor: req.Cop}}
	case CmdPessimisticLock:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticLock{PessimisticLock: req.PessimisticLock}}
	case CmdPessimisticRollback:
		return &tikvpb.BatchCommandsRequest_Request{Cmd: &tikvpb.BatchCommandsRequest_Request_PessimisticRollback{PessimisticRollback: req.PessimisticRollback}}
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
	Type CmdType
	Resp unsafe.Pointer
}

// Get returns GetResponse in response.
func (resp *Response) Get() *kvrpcpb.GetResponse {
	return (*kvrpcpb.GetResponse)(resp.Resp)
}

// Scan returns ScanResponse in response.
func (resp *Response) Scan() *kvrpcpb.ScanResponse {
	return (*kvrpcpb.ScanResponse)(resp.Resp)
}

// Prewrite returns PrewriteResponse in response.
func (resp *Response) Prewrite() *kvrpcpb.PrewriteResponse {
	return (*kvrpcpb.PrewriteResponse)(resp.Resp)
}

// Commit returns CommitResponse in response.
func (resp *Response) Commit() *kvrpcpb.CommitResponse {
	return (*kvrpcpb.CommitResponse)(resp.Resp)
}

// Cleanup returns CleanupResponse in response.
func (resp *Response) Cleanup() *kvrpcpb.CleanupResponse {
	return (*kvrpcpb.CleanupResponse)(resp.Resp)
}

// BatchGet returns BatchGetResponse in response.
func (resp *Response) BatchGet() *kvrpcpb.BatchGetResponse {
	return (*kvrpcpb.BatchGetResponse)(resp.Resp)
}

// BatchRollback returns BatchRollbackResponse in response.
func (resp *Response) BatchRollback() *kvrpcpb.BatchRollbackResponse {
	return (*kvrpcpb.BatchRollbackResponse)(resp.Resp)
}

// ScanLock returns ScanLockResponse in response.
func (resp *Response) ScanLock() *kvrpcpb.ScanLockResponse {
	return (*kvrpcpb.ScanLockResponse)(resp.Resp)
}

// ResolveLock returns ResolveLockResponse in response.
func (resp *Response) ResolveLock() *kvrpcpb.ResolveLockResponse {
	return (*kvrpcpb.ResolveLockResponse)(resp.Resp)
}

// GC returns GCResponse in response.
func (resp *Response) GC() *kvrpcpb.GCResponse {
	return (*kvrpcpb.GCResponse)(resp.Resp)
}

// DeleteRange returns DeleteRangeResponse in response.
func (resp *Response) DeleteRange() *kvrpcpb.DeleteRangeResponse {
	return (*kvrpcpb.DeleteRangeResponse)(resp.Resp)
}

// RawGet returns RawGetResponse in response.
func (resp *Response) RawGet() *kvrpcpb.RawGetResponse {
	return (*kvrpcpb.RawGetResponse)(resp.Resp)
}

// RawBatchGet returns RawBatchGetResponse in response.
func (resp *Response) RawBatchGet() *kvrpcpb.RawBatchGetResponse {
	return (*kvrpcpb.RawBatchGetResponse)(resp.Resp)
}

// RawPut returns RawPutResponse in response.
func (resp *Response) RawPut() *kvrpcpb.RawPutResponse {
	return (*kvrpcpb.RawPutResponse)(resp.Resp)
}

// RawBatchPut returns RawBatchPutResponse in response.
func (resp *Response) RawBatchPut() *kvrpcpb.RawBatchPutResponse {
	return (*kvrpcpb.RawBatchPutResponse)(resp.Resp)
}

// RawDelete returns PrewriteResponse in response.
func (resp *Response) RawDelete() *kvrpcpb.RawDeleteResponse {
	return (*kvrpcpb.RawDeleteResponse)(resp.Resp)
}

// RawBatchDelete returns RawBatchDeleteResponse in response.
func (resp *Response) RawBatchDelete() *kvrpcpb.RawBatchDeleteResponse {
	return (*kvrpcpb.RawBatchDeleteResponse)(resp.Resp)
}

// RawDeleteRange returns RawDeleteRangeResponse in response.
func (resp *Response) RawDeleteRange() *kvrpcpb.RawDeleteRangeResponse {
	return (*kvrpcpb.RawDeleteRangeResponse)(resp.Resp)
}

// RawScan returns RawScanResponse in response.
func (resp *Response) RawScan() *kvrpcpb.RawScanResponse {
	return (*kvrpcpb.RawScanResponse)(resp.Resp)
}

// UnsafeDestroyRange returns UnsafeDestroyRangeResponse in response.
func (resp *Response) UnsafeDestroyRange() *kvrpcpb.UnsafeDestroyRangeResponse {
	return (*kvrpcpb.UnsafeDestroyRangeResponse)(resp.Resp)
}

// Cop returns coprocessor Response in response.
func (resp *Response) Cop() *coprocessor.Response {
	return (*coprocessor.Response)(resp.Resp)
}

// MvccGetByKey returns MvccGetByKeyResponse in response.
func (resp *Response) MvccGetByKey() *kvrpcpb.MvccGetByKeyResponse {
	return (*kvrpcpb.MvccGetByKeyResponse)(resp.Resp)
}

// MvccGetByStartTs returns MvccGetByStartTsResponse in response.
func (resp *Response) MvccGetByStartTs() *kvrpcpb.MvccGetByStartTsResponse {
	return (*kvrpcpb.MvccGetByStartTsResponse)(resp.Resp)
}

// SplitRegion returns SplitRegionResponse in response.
func (resp *Response) SplitRegion() *kvrpcpb.SplitRegionResponse {
	return (*kvrpcpb.SplitRegionResponse)(resp.Resp)
}

// PessimisticLock returns PessimisticLockResponse in response.
func (resp *Response) PessimisticLock() *kvrpcpb.PessimisticLockResponse {
	return (*kvrpcpb.PessimisticLockResponse)(resp.Resp)
}

// PessimisticRollback returns PessimisticRollbackResponse in response.
func (resp *Response) PessimisticRollback() *kvrpcpb.PessimisticRollbackResponse {
	return (*kvrpcpb.PessimisticRollbackResponse)(resp.Resp)
}

// DebugGetRegionProperties returns GetRegionPropertiesResponse in response.
func (resp *Response) DebugGetRegionProperties() *debugpb.GetRegionPropertiesResponse {
	return (*debugpb.GetRegionPropertiesResponse)(resp.Resp)
}

// CopStream returns CopStreamResponse in response.
func (resp *Response) CopStream() *CopStreamResponse {
	return (*CopStreamResponse)(resp.Resp)
}

// FromBatchCommandsResponse converts a BatchCommands response to Response.
func FromBatchCommandsResponse(res *tikvpb.BatchCommandsResponse_Response) *Response {
	switch res := res.GetCmd().(type) {
	case *tikvpb.BatchCommandsResponse_Response_Get:
		return &Response{Type: CmdGet, Resp: unsafe.Pointer(res.Get)}
	case *tikvpb.BatchCommandsResponse_Response_Scan:
		return &Response{Type: CmdScan, Resp: unsafe.Pointer(res.Scan)}
	case *tikvpb.BatchCommandsResponse_Response_Prewrite:
		return &Response{Type: CmdPrewrite, Resp: unsafe.Pointer(res.Prewrite)}
	case *tikvpb.BatchCommandsResponse_Response_Commit:
		return &Response{Type: CmdCommit, Resp: unsafe.Pointer(res.Commit)}
	case *tikvpb.BatchCommandsResponse_Response_Cleanup:
		return &Response{Type: CmdCleanup, Resp: unsafe.Pointer(res.Cleanup)}
	case *tikvpb.BatchCommandsResponse_Response_BatchGet:
		return &Response{Type: CmdBatchGet, Resp: unsafe.Pointer(res.BatchGet)}
	case *tikvpb.BatchCommandsResponse_Response_BatchRollback:
		return &Response{Type: CmdBatchRollback, Resp: unsafe.Pointer(res.BatchRollback)}
	case *tikvpb.BatchCommandsResponse_Response_ScanLock:
		return &Response{Type: CmdScanLock, Resp: unsafe.Pointer(res.ScanLock)}
	case *tikvpb.BatchCommandsResponse_Response_ResolveLock:
		return &Response{Type: CmdResolveLock, Resp: unsafe.Pointer(res.ResolveLock)}
	case *tikvpb.BatchCommandsResponse_Response_GC:
		return &Response{Type: CmdGC, Resp: unsafe.Pointer(res.GC)}
	case *tikvpb.BatchCommandsResponse_Response_DeleteRange:
		return &Response{Type: CmdDeleteRange, Resp: unsafe.Pointer(res.DeleteRange)}
	case *tikvpb.BatchCommandsResponse_Response_RawGet:
		return &Response{Type: CmdRawGet, Resp: unsafe.Pointer(res.RawGet)}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchGet:
		return &Response{Type: CmdRawBatchGet, Resp: unsafe.Pointer(res.RawBatchGet)}
	case *tikvpb.BatchCommandsResponse_Response_RawPut:
		return &Response{Type: CmdRawPut, Resp: unsafe.Pointer(res.RawPut)}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchPut:
		return &Response{Type: CmdRawBatchPut, Resp: unsafe.Pointer(res.RawBatchPut)}
	case *tikvpb.BatchCommandsResponse_Response_RawDelete:
		return &Response{Type: CmdRawDelete, Resp: unsafe.Pointer(res.RawDelete)}
	case *tikvpb.BatchCommandsResponse_Response_RawBatchDelete:
		return &Response{Type: CmdRawBatchDelete, Resp: unsafe.Pointer(res.RawBatchDelete)}
	case *tikvpb.BatchCommandsResponse_Response_RawDeleteRange:
		return &Response{Type: CmdRawDeleteRange, Resp: unsafe.Pointer(res.RawDeleteRange)}
	case *tikvpb.BatchCommandsResponse_Response_RawScan:
		return &Response{Type: CmdRawScan, Resp: unsafe.Pointer(res.RawScan)}
	case *tikvpb.BatchCommandsResponse_Response_Coprocessor:
		return &Response{Type: CmdCop, Resp: unsafe.Pointer(res.Coprocessor)}
	case *tikvpb.BatchCommandsResponse_Response_PessimisticLock:
		return &Response{Type: CmdPessimisticLock, Resp: unsafe.Pointer(res.PessimisticLock)}
	case *tikvpb.BatchCommandsResponse_Response_PessimisticRollback:
		return &Response{Type: CmdPessimisticRollback, Resp: unsafe.Pointer(res.PessimisticRollback)}
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
		req.Get.Context = ctx
	case CmdScan:
		req.Scan.Context = ctx
	case CmdPrewrite:
		req.Prewrite.Context = ctx
	case CmdPessimisticLock:
		req.PessimisticLock.Context = ctx
	case CmdPessimisticRollback:
		req.PessimisticRollback.Context = ctx
	case CmdCommit:
		req.Commit.Context = ctx
	case CmdCleanup:
		req.Cleanup.Context = ctx
	case CmdBatchGet:
		req.BatchGet.Context = ctx
	case CmdBatchRollback:
		req.BatchRollback.Context = ctx
	case CmdScanLock:
		req.ScanLock.Context = ctx
	case CmdResolveLock:
		req.ResolveLock.Context = ctx
	case CmdGC:
		req.GC.Context = ctx
	case CmdDeleteRange:
		req.DeleteRange.Context = ctx
	case CmdRawGet:
		req.RawGet.Context = ctx
	case CmdRawBatchGet:
		req.RawBatchGet.Context = ctx
	case CmdRawPut:
		req.RawPut.Context = ctx
	case CmdRawBatchPut:
		req.RawBatchPut.Context = ctx
	case CmdRawDelete:
		req.RawDelete.Context = ctx
	case CmdRawBatchDelete:
		req.RawBatchDelete.Context = ctx
	case CmdRawDeleteRange:
		req.RawDeleteRange.Context = ctx
	case CmdRawScan:
		req.RawScan.Context = ctx
	case CmdUnsafeDestroyRange:
		req.UnsafeDestroyRange.Context = ctx
	case CmdCop:
		req.Cop.Context = ctx
	case CmdCopStream:
		req.Cop.Context = ctx
	case CmdMvccGetByKey:
		req.MvccGetByKey.Context = ctx
	case CmdMvccGetByStartTs:
		req.MvccGetByStartTs.Context = ctx
	case CmdSplitRegion:
		req.SplitRegion.Context = ctx
	default:
		return fmt.Errorf("invalid request type %v", req.Type)
	}
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	var p unsafe.Pointer
	resp := &Response{}
	resp.Type = req.Type
	switch req.Type {
	case CmdGet:
		p = unsafe.Pointer(&kvrpcpb.GetResponse{
			RegionError: e,
		})
	case CmdScan:
		p = unsafe.Pointer(&kvrpcpb.ScanResponse{
			RegionError: e,
		})
	case CmdPrewrite:
		p = unsafe.Pointer(&kvrpcpb.PrewriteResponse{
			RegionError: e,
		})
	case CmdPessimisticLock:
		p = unsafe.Pointer(&kvrpcpb.PessimisticLockResponse{
			RegionError: e,
		})
	case CmdPessimisticRollback:
		p = unsafe.Pointer(&kvrpcpb.PessimisticRollbackResponse{
			RegionError: e,
		})
	case CmdCommit:
		p = unsafe.Pointer(&kvrpcpb.CommitResponse{
			RegionError: e,
		})
	case CmdCleanup:
		p = unsafe.Pointer(&kvrpcpb.CleanupResponse{
			RegionError: e,
		})
	case CmdBatchGet:
		p = unsafe.Pointer(&kvrpcpb.BatchGetResponse{
			RegionError: e,
		})
	case CmdBatchRollback:
		p = unsafe.Pointer(&kvrpcpb.BatchRollbackResponse{
			RegionError: e,
		})
	case CmdScanLock:
		p = unsafe.Pointer(&kvrpcpb.ScanLockResponse{
			RegionError: e,
		})
	case CmdResolveLock:
		p = unsafe.Pointer(&kvrpcpb.ResolveLockResponse{
			RegionError: e,
		})
	case CmdGC:
		p = unsafe.Pointer(&kvrpcpb.GCResponse{
			RegionError: e,
		})
	case CmdDeleteRange:
		p = unsafe.Pointer(&kvrpcpb.DeleteRangeResponse{
			RegionError: e,
		})
	case CmdRawGet:
		p = unsafe.Pointer(&kvrpcpb.RawGetResponse{
			RegionError: e,
		})
	case CmdRawBatchGet:
		p = unsafe.Pointer(&kvrpcpb.RawBatchGetResponse{
			RegionError: e,
		})
	case CmdRawPut:
		p = unsafe.Pointer(&kvrpcpb.RawPutResponse{
			RegionError: e,
		})
	case CmdRawBatchPut:
		p = unsafe.Pointer(&kvrpcpb.RawBatchPutResponse{
			RegionError: e,
		})
	case CmdRawDelete:
		p = unsafe.Pointer(&kvrpcpb.RawDeleteResponse{
			RegionError: e,
		})
	case CmdRawBatchDelete:
		p = unsafe.Pointer(&kvrpcpb.RawBatchDeleteResponse{
			RegionError: e,
		})
	case CmdRawDeleteRange:
		p = unsafe.Pointer(&kvrpcpb.RawDeleteRangeResponse{
			RegionError: e,
		})
	case CmdRawScan:
		p = unsafe.Pointer(&kvrpcpb.RawScanResponse{
			RegionError: e,
		})
	case CmdUnsafeDestroyRange:
		p = unsafe.Pointer(&kvrpcpb.UnsafeDestroyRangeResponse{
			RegionError: e,
		})
	case CmdCop:
		p = unsafe.Pointer(&coprocessor.Response{
			RegionError: e,
		})
	case CmdCopStream:
		p = unsafe.Pointer(&CopStreamResponse{
			Response: &coprocessor.Response{
				RegionError: e,
			},
		})
	case CmdMvccGetByKey:
		p = unsafe.Pointer(&kvrpcpb.MvccGetByKeyResponse{
			RegionError: e,
		})
	case CmdMvccGetByStartTs:
		p = unsafe.Pointer(&kvrpcpb.MvccGetByStartTsResponse{
			RegionError: e,
		})
	case CmdSplitRegion:
		p = unsafe.Pointer(&kvrpcpb.SplitRegionResponse{
			RegionError: e,
		})
	default:
		return nil, fmt.Errorf("invalid request type %v", req.Type)
	}
	resp.Resp = p
	return resp, nil
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	var e *errorpb.Error
	switch resp.Type {
	case CmdGet:
		e = resp.Get().GetRegionError()
	case CmdScan:
		e = resp.Scan().GetRegionError()
	case CmdPessimisticLock:
		e = resp.PessimisticLock().GetRegionError()
	case CmdPessimisticRollback:
		e = resp.PessimisticRollback().GetRegionError()
	case CmdPrewrite:
		e = resp.Prewrite().GetRegionError()
	case CmdCommit:
		e = resp.Commit().GetRegionError()
	case CmdCleanup:
		e = resp.Cleanup().GetRegionError()
	case CmdBatchGet:
		e = resp.BatchGet().GetRegionError()
	case CmdBatchRollback:
		e = resp.BatchRollback().GetRegionError()
	case CmdScanLock:
		e = resp.ScanLock().GetRegionError()
	case CmdResolveLock:
		e = resp.ResolveLock().GetRegionError()
	case CmdGC:
		e = resp.GC().GetRegionError()
	case CmdDeleteRange:
		e = resp.DeleteRange().GetRegionError()
	case CmdRawGet:
		e = resp.RawGet().GetRegionError()
	case CmdRawBatchGet:
		e = resp.RawBatchGet().GetRegionError()
	case CmdRawPut:
		e = resp.RawPut().GetRegionError()
	case CmdRawBatchPut:
		e = resp.RawBatchPut().GetRegionError()
	case CmdRawDelete:
		e = resp.RawDelete().GetRegionError()
	case CmdRawBatchDelete:
		e = resp.RawBatchDelete().GetRegionError()
	case CmdRawDeleteRange:
		e = resp.RawDeleteRange().GetRegionError()
	case CmdRawScan:
		e = resp.RawScan().GetRegionError()
	case CmdUnsafeDestroyRange:
		e = resp.UnsafeDestroyRange().GetRegionError()
	case CmdCop:
		e = resp.Cop().GetRegionError()
	case CmdCopStream:
		e = resp.CopStream().GetRegionError()
	case CmdMvccGetByKey:
		e = resp.MvccGetByKey().GetRegionError()
	case CmdMvccGetByStartTs:
		e = resp.MvccGetByStartTs().GetRegionError()
	case CmdSplitRegion:
		e = resp.SplitRegion().GetRegionError()
	default:
		return nil, fmt.Errorf("invalid response type %v", resp.Type)
	}
	return e, nil
}

// CallRPC launches a rpc call.
// ch is needed to implement timeout for coprocessor streaing, the stream object's
// cancel function will be sent to the channel, together with a lease checked by a background goroutine.
func CallRPC(ctx context.Context, client tikvpb.TikvClient, req *Request) (*Response, error) {
	resp := &Response{}
	resp.Type = req.Type
	var err error
	switch req.Type {
	case CmdGet:
		var r *kvrpcpb.GetResponse
		r, err = client.KvGet(ctx, req.Get)
		resp.Resp = unsafe.Pointer(r)
	case CmdScan:
		var r *kvrpcpb.ScanResponse
		r, err = client.KvScan(ctx, req.Scan)
		resp.Resp = unsafe.Pointer(r)
	case CmdPrewrite:
		var r *kvrpcpb.PrewriteResponse
		r, err = client.KvPrewrite(ctx, req.Prewrite)
		resp.Resp = unsafe.Pointer(r)
	case CmdPessimisticLock:
		var r *kvrpcpb.PessimisticLockResponse
		r, err = client.KvPessimisticLock(ctx, req.PessimisticLock)
		resp.Resp = unsafe.Pointer(r)
	case CmdPessimisticRollback:
		var r *kvrpcpb.PessimisticRollbackResponse
		r, err = client.KVPessimisticRollback(ctx, req.PessimisticRollback)
		resp.Resp = unsafe.Pointer(r)
	case CmdCommit:
		var r *kvrpcpb.CommitResponse
		r, err = client.KvCommit(ctx, req.Commit)
		resp.Resp = unsafe.Pointer(r)
	case CmdCleanup:
		var r *kvrpcpb.CleanupResponse
		r, err = client.KvCleanup(ctx, req.Cleanup)
		resp.Resp = unsafe.Pointer(r)
	case CmdBatchGet:
		var r *kvrpcpb.BatchGetResponse
		r, err = client.KvBatchGet(ctx, req.BatchGet)
		resp.Resp = unsafe.Pointer(r)
	case CmdBatchRollback:
		var r *kvrpcpb.BatchRollbackResponse
		r, err = client.KvBatchRollback(ctx, req.BatchRollback)
		resp.Resp = unsafe.Pointer(r)
	case CmdScanLock:
		var r *kvrpcpb.ScanLockResponse
		r, err = client.KvScanLock(ctx, req.ScanLock)
		resp.Resp = unsafe.Pointer(r)
	case CmdResolveLock:
		var r *kvrpcpb.ResolveLockResponse
		r, err = client.KvResolveLock(ctx, req.ResolveLock)
		resp.Resp = unsafe.Pointer(r)
	case CmdGC:
		var r *kvrpcpb.GCResponse
		r, err = client.KvGC(ctx, req.GC)
		resp.Resp = unsafe.Pointer(r)
	case CmdDeleteRange:
		var r *kvrpcpb.DeleteRangeResponse
		r, err = client.KvDeleteRange(ctx, req.DeleteRange)
		resp.Resp = unsafe.Pointer(r)
	case CmdRawGet:
		var r *kvrpcpb.RawGetResponse
		r, err = client.RawGet(ctx, req.RawGet)
		resp.Resp = unsafe.Pointer(r)
	case CmdRawBatchGet:
		var r *kvrpcpb.RawBatchGetResponse
		r, err = client.RawBatchGet(ctx, req.RawBatchGet)
		resp.Resp = unsafe.Pointer(r)
	case CmdRawPut:
		var r *kvrpcpb.RawPutResponse
		r, err = client.RawPut(ctx, req.RawPut)
		resp.Resp = unsafe.Pointer(r)
	case CmdRawBatchPut:
		var r *kvrpcpb.RawBatchPutResponse
		r, err = client.RawBatchPut(ctx, req.RawBatchPut)
		resp.Resp = unsafe.Pointer(r)
	case CmdRawDelete:
		var r *kvrpcpb.RawDeleteResponse
		r, err = client.RawDelete(ctx, req.RawDelete)
		resp.Resp = unsafe.Pointer(r)
	case CmdRawBatchDelete:
		var r *kvrpcpb.RawBatchDeleteResponse
		r, err = client.RawBatchDelete(ctx, req.RawBatchDelete)
		resp.Resp = unsafe.Pointer(r)
	case CmdRawDeleteRange:
		var r *kvrpcpb.RawDeleteRangeResponse
		r, err = client.RawDeleteRange(ctx, req.RawDeleteRange)
		resp.Resp = unsafe.Pointer(r)
	case CmdRawScan:
		var r *kvrpcpb.RawScanResponse
		r, err = client.RawScan(ctx, req.RawScan)
		resp.Resp = unsafe.Pointer(r)
	case CmdUnsafeDestroyRange:
		var r *kvrpcpb.UnsafeDestroyRangeResponse
		r, err = client.UnsafeDestroyRange(ctx, req.UnsafeDestroyRange)
		resp.Resp = unsafe.Pointer(r)
	case CmdCop:
		var r *coprocessor.Response
		r, err = client.Coprocessor(ctx, req.Cop)
		resp.Resp = unsafe.Pointer(r)
	case CmdCopStream:
		var streamClient tikvpb.Tikv_CoprocessorStreamClient
		streamClient, err = client.CoprocessorStream(ctx, req.Cop)
		r := &CopStreamResponse{
			Tikv_CoprocessorStreamClient: streamClient,
		}
		resp.Resp = unsafe.Pointer(r)
	case CmdMvccGetByKey:
		var r *kvrpcpb.MvccGetByKeyResponse
		r, err = client.MvccGetByKey(ctx, req.MvccGetByKey)
		resp.Resp = unsafe.Pointer(r)
	case CmdMvccGetByStartTs:
		var r *kvrpcpb.MvccGetByStartTsResponse
		r, err = client.MvccGetByStartTs(ctx, req.MvccGetByStartTs)
		resp.Resp = unsafe.Pointer(r)
	case CmdSplitRegion:
		var r *kvrpcpb.SplitRegionResponse
		r, err = client.SplitRegion(ctx, req.SplitRegion)
		resp.Resp = unsafe.Pointer(r)
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
	resp := &Response{Type: req.Type}
	resp.Type = req.Type
	var err error
	switch req.Type {
	case CmdDebugGetRegionProperties:
		var debugResp *debugpb.GetRegionPropertiesResponse
		debugResp, err = client.GetRegionProperties(ctx, req.DebugGetRegionProperties)
		resp.Resp = unsafe.Pointer(debugResp)
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
}

// CheckStreamTimeoutLoop runs periodically to check is there any stream request timeouted.
// Lease is an object to track stream requests, call this function with "go CheckStreamTimeoutLoop()"
func CheckStreamTimeoutLoop(ch <-chan *Lease) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	array := make([]*Lease, 0, 1024)

	for {
		select {
		case item, ok := <-ch:
			if !ok {
				// This channel close means goroutine should return.
				return
			}
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

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
	"fmt"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
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

	CmdRawGet CmdType = 256 + iota
	CmdRawPut
	CmdRawDelete

	CmdCop CmdType = 512 + iota
)

// Request wraps all kv/coprocessor requests.
type Request struct {
	Type          CmdType
	Get           *kvrpcpb.GetRequest
	Scan          *kvrpcpb.ScanRequest
	Prewrite      *kvrpcpb.PrewriteRequest
	Commit        *kvrpcpb.CommitRequest
	Cleanup       *kvrpcpb.CleanupRequest
	BatchGet      *kvrpcpb.BatchGetRequest
	BatchRollback *kvrpcpb.BatchRollbackRequest
	ScanLock      *kvrpcpb.ScanLockRequest
	ResolveLock   *kvrpcpb.ResolveLockRequest
	GC            *kvrpcpb.GCRequest
	RawGet        *kvrpcpb.RawGetRequest
	RawPut        *kvrpcpb.RawPutRequest
	RawDelete     *kvrpcpb.RawDeleteRequest
	Cop           *coprocessor.Request
}

// GetContext returns the rpc context for the underlying concrete request.
func (req *Request) GetContext() (*kvrpcpb.Context, error) {
	var c *kvrpcpb.Context
	switch req.Type {
	case CmdGet:
		c = req.Get.GetContext()
	case CmdScan:
		c = req.Scan.GetContext()
	case CmdPrewrite:
		c = req.Prewrite.GetContext()
	case CmdCommit:
		c = req.Commit.GetContext()
	case CmdCleanup:
		c = req.Cleanup.GetContext()
	case CmdBatchGet:
		c = req.BatchGet.GetContext()
	case CmdBatchRollback:
		c = req.BatchRollback.GetContext()
	case CmdScanLock:
		c = req.ScanLock.GetContext()
	case CmdResolveLock:
		c = req.ResolveLock.GetContext()
	case CmdGC:
		c = req.GC.GetContext()
	case CmdRawGet:
		c = req.RawGet.GetContext()
	case CmdRawPut:
		c = req.RawPut.GetContext()
	case CmdRawDelete:
		c = req.RawDelete.GetContext()
	case CmdCop:
		c = req.Cop.GetContext()
	default:
		return nil, fmt.Errorf("invalid request type %v", req.Type)
	}
	return c, nil
}

// Response wraps all kv/coprocessor responses.
type Response struct {
	Type          CmdType
	Get           *kvrpcpb.GetResponse
	Scan          *kvrpcpb.ScanResponse
	Prewrite      *kvrpcpb.PrewriteResponse
	Commit        *kvrpcpb.CommitResponse
	Cleanup       *kvrpcpb.CleanupResponse
	BatchGet      *kvrpcpb.BatchGetResponse
	BatchRollback *kvrpcpb.BatchRollbackResponse
	ScanLock      *kvrpcpb.ScanLockResponse
	ResolveLock   *kvrpcpb.ResolveLockResponse
	GC            *kvrpcpb.GCResponse
	RawGet        *kvrpcpb.RawGetResponse
	RawPut        *kvrpcpb.RawPutResponse
	RawDelete     *kvrpcpb.RawDeleteResponse
	Cop           *coprocessor.Response
}

// SetContext set the Context field for the given req to the specifed ctx.
func SetContext(req *Request, ctx *kvrpcpb.Context) error {
	switch req.Type {
	case CmdGet:
		req.Get.Context = ctx
	case CmdScan:
		req.Scan.Context = ctx
	case CmdPrewrite:
		req.Prewrite.Context = ctx
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
	case CmdRawGet:
		req.RawGet.Context = ctx
	case CmdRawPut:
		req.RawPut.Context = ctx
	case CmdRawDelete:
		req.RawDelete.Context = ctx
	case CmdCop:
		req.Cop.Context = ctx
	default:
		return fmt.Errorf("invalid request type %v", req.Type)
	}
	return nil
}

// GenRegionErrorResp returns corresponding Response with specified RegionError
// according to the given req.
func GenRegionErrorResp(req *Request, e *errorpb.Error) (*Response, error) {
	resp := &Response{}
	resp.Type = req.Type
	switch req.Type {
	case CmdGet:
		resp.Get = &kvrpcpb.GetResponse{
			RegionError: e,
		}
	case CmdScan:
		resp.Scan = &kvrpcpb.ScanResponse{
			RegionError: e,
		}
	case CmdPrewrite:
		resp.Prewrite = &kvrpcpb.PrewriteResponse{
			RegionError: e,
		}
	case CmdCommit:
		resp.Commit = &kvrpcpb.CommitResponse{
			RegionError: e,
		}
	case CmdCleanup:
		resp.Cleanup = &kvrpcpb.CleanupResponse{
			RegionError: e,
		}
	case CmdBatchGet:
		resp.BatchGet = &kvrpcpb.BatchGetResponse{
			RegionError: e,
		}
	case CmdBatchRollback:
		resp.BatchRollback = &kvrpcpb.BatchRollbackResponse{
			RegionError: e,
		}
	case CmdScanLock:
		resp.ScanLock = &kvrpcpb.ScanLockResponse{
			RegionError: e,
		}
	case CmdResolveLock:
		resp.ResolveLock = &kvrpcpb.ResolveLockResponse{
			RegionError: e,
		}
	case CmdGC:
		resp.GC = &kvrpcpb.GCResponse{
			RegionError: e,
		}
	case CmdRawGet:
		resp.RawGet = &kvrpcpb.RawGetResponse{
			RegionError: e,
		}
	case CmdRawPut:
		resp.RawPut = &kvrpcpb.RawPutResponse{
			RegionError: e,
		}
	case CmdRawDelete:
		resp.RawDelete = &kvrpcpb.RawDeleteResponse{
			RegionError: e,
		}
	case CmdCop:
		resp.Cop = &coprocessor.Response{
			RegionError: e,
		}
	default:
		return nil, fmt.Errorf("invalid request type %v", req.Type)
	}
	return resp, nil
}

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() (*errorpb.Error, error) {
	var e *errorpb.Error
	switch resp.Type {
	case CmdGet:
		e = resp.Get.GetRegionError()
	case CmdScan:
		e = resp.Scan.GetRegionError()
	case CmdPrewrite:
		e = resp.Prewrite.GetRegionError()
	case CmdCommit:
		e = resp.Commit.GetRegionError()
	case CmdCleanup:
		e = resp.Cleanup.GetRegionError()
	case CmdBatchGet:
		e = resp.BatchGet.GetRegionError()
	case CmdBatchRollback:
		e = resp.BatchRollback.GetRegionError()
	case CmdScanLock:
		e = resp.ScanLock.GetRegionError()
	case CmdResolveLock:
		e = resp.ResolveLock.GetRegionError()
	case CmdGC:
		e = resp.GC.GetRegionError()
	case CmdRawGet:
		e = resp.RawGet.GetRegionError()
	case CmdRawPut:
		e = resp.RawPut.GetRegionError()
	case CmdRawDelete:
		e = resp.RawDelete.GetRegionError()
	case CmdCop:
		e = resp.Cop.GetRegionError()
	default:
		return nil, fmt.Errorf("invalid response type %v", resp.Type)
	}
	return e, nil
}

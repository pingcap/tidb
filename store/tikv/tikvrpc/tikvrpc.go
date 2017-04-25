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
func (req *Request) GetContext() *kvrpcpb.Context {
	switch req.Type {
	case CmdGet:
		return req.Get.GetContext()
	case CmdScan:
		return req.Scan.GetContext()
	case CmdPrewrite:
		return req.Prewrite.GetContext()
	case CmdCommit:
		return req.Commit.GetContext()
	case CmdCleanup:
		return req.Cleanup.GetContext()
	case CmdBatchGet:
		return req.BatchGet.GetContext()
	case CmdBatchRollback:
		return req.BatchRollback.GetContext()
	case CmdScanLock:
		return req.ScanLock.GetContext()
	case CmdResolveLock:
		return req.ResolveLock.GetContext()
	case CmdGC:
		return req.GC.GetContext()
	case CmdRawGet:
		return req.RawGet.GetContext()
	case CmdRawPut:
		return req.RawPut.GetContext()
	case CmdRawDelete:
		return req.RawDelete.GetContext()
	case CmdCop:
		return req.Cop.GetContext()
	default:
		panic(fmt.Sprintf("invalid request type %v", req.Type))
	}
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

// GetRegionError returns the RegionError of the underlying concrete response.
func (resp *Response) GetRegionError() *errorpb.Error {
	switch resp.Type {
	case CmdGet:
		return resp.Get.GetRegionError()
	case CmdScan:
		return resp.Scan.GetRegionError()
	case CmdPrewrite:
		return resp.Prewrite.GetRegionError()
	case CmdCommit:
		return resp.Commit.GetRegionError()
	case CmdCleanup:
		return resp.Cleanup.GetRegionError()
	case CmdBatchGet:
		return resp.BatchGet.GetRegionError()
	case CmdBatchRollback:
		return resp.BatchRollback.GetRegionError()
	case CmdScanLock:
		return resp.ScanLock.GetRegionError()
	case CmdResolveLock:
		return resp.ResolveLock.GetRegionError()
	case CmdGC:
		return resp.GC.GetRegionError()
	case CmdRawGet:
		return resp.RawGet.GetRegionError()
	case CmdRawPut:
		return resp.RawPut.GetRegionError()
	case CmdRawDelete:
		return resp.RawDelete.GetRegionError()
	case CmdCop:
		return resp.Cop.GetRegionError()
	default:
		panic(fmt.Sprintf("invalid response type %v", resp.Type))
	}
}

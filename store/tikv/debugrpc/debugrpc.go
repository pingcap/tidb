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

package debugrpc

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/debugpb"
)

// DebugType represents the type of request and response for debug
type DebugType uint16

// DebugType values
const (
	DebugGet DebugType = 1 + iota
	DebugRaftLog
	DebugRegionInfo
	DebugRegionSize
	DebugScanMvcc
	DebugCompact
	DebugInjectFailPoint
	DebugRecoverFailPoint
	DebugListFailPoint
	DebugGetMetrics
	DebugCheckRegionConsistency
	DebugModifyTikvConfig
	DebugGetRegionProperties
)

// Request for debug protobuf
type Request struct {
	Type                   DebugType
	Get                    *debugpb.GetRequest
	RaftLog                *debugpb.RaftLogRequest
	RegionInfo             *debugpb.RegionInfoRequest
	RegionSize             *debugpb.RegionSizeRequest
	ScanMvcc               *debugpb.ScanMvccRequest
	Compact                *debugpb.CompactRequest
	InjectFailPoint        *debugpb.InjectFailPointRequest
	RecoverFailPoint       *debugpb.RecoverFailPointRequest
	ListFailPoint          *debugpb.ListFailPointsRequest
	GetMetrics             *debugpb.GetMetricsRequest
	CheckRegionConsistency *debugpb.RegionConsistencyCheckRequest
	ModifyTikvConfig       *debugpb.ModifyTikvConfigRequest
	GetRegionProperties    *debugpb.GetRegionPropertiesRequest
}

// Response for debug protobuf
type Response struct {
	Type                   DebugType
	Get                    *debugpb.GetResponse
	RaftLog                *debugpb.RaftLogResponse
	RegionInfo             *debugpb.RegionInfoResponse
	RegionSize             *debugpb.RegionSizeResponse
	ScanMvcc               debugpb.Debug_ScanMvccClient
	Compact                *debugpb.CompactResponse
	InjectFailPoint        *debugpb.InjectFailPointResponse
	RecoverFailPoint       *debugpb.RecoverFailPointResponse
	ListFailPoint          *debugpb.ListFailPointsResponse
	GetMetrics             *debugpb.GetMetricsResponse
	CheckRegionConsistency *debugpb.RegionConsistencyCheckResponse
	ModifyTikvConfig       *debugpb.ModifyTikvConfigResponse
	GetRegionProperties    *debugpb.GetRegionPropertiesResponse
}

// CallRPC launches a rpc call.
func CallRPC(ctx context.Context, client debugpb.DebugClient, req *Request) (*Response, error) {
	resp := &Response{}
	resp.Type = req.Type
	var err error
	switch req.Type {
	case DebugGet:
		resp.Get, err = client.Get(ctx, req.Get)
	case DebugRaftLog:
		resp.RaftLog, err = client.RaftLog(ctx, req.RaftLog)
	case DebugRegionInfo:
		resp.RegionInfo, err = client.RegionInfo(ctx, req.RegionInfo)
	case DebugRegionSize:
		resp.RegionSize, err = client.RegionSize(ctx, req.RegionSize)
	case DebugScanMvcc:
		resp.ScanMvcc, err = client.ScanMvcc(ctx, req.ScanMvcc)
	case DebugCompact:
		resp.Compact, err = client.Compact(ctx, req.Compact)
	case DebugInjectFailPoint:
		resp.InjectFailPoint, err = client.InjectFailPoint(ctx, req.InjectFailPoint)
	case DebugRecoverFailPoint:
		resp.RecoverFailPoint, err = client.RecoverFailPoint(ctx, req.RecoverFailPoint)
	case DebugListFailPoint:
		resp.ListFailPoint, err = client.ListFailPoints(ctx, req.ListFailPoint)
	case DebugGetMetrics:
		resp.GetMetrics, err = client.GetMetrics(ctx, req.GetMetrics)
	case DebugCheckRegionConsistency:
		resp.CheckRegionConsistency, err = client.CheckRegionConsistency(ctx, req.CheckRegionConsistency)
	case DebugModifyTikvConfig:
		resp.ModifyTikvConfig, err = client.ModifyTikvConfig(ctx, req.ModifyTikvConfig)
	case DebugGetRegionProperties:
		resp.GetRegionProperties, err = client.GetRegionProperties(ctx, req.GetRegionProperties)
	default:
		return nil, errors.Errorf("invalid request type: %v", req.Type)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

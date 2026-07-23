package unistore

import (
	"context"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (c *pdClient) GetLogReplLocalStatus(context.Context) (*pdpb.LogReplicationLocalStatus, error) {
	return nil, status.Error(codes.Unimplemented, "mock store does not implement GetLogReplLocalStatus")
}

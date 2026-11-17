// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestReportMPPTaskStatusWithoutMeta guards the case of a ReportTaskStatusRequest
// whose `meta` field is unset. It unmarshals to a nil req.Meta, which used to
// panic inside MPPCoordinatorManager.ReportStatus and crash the whole TiDB
// process. The handler now rejects it with InvalidArgument instead.
func TestReportMPPTaskStatusWithoutMeta(t *testing.T) {
	s := &rpcServer{}

	resp, err := s.ReportMPPTaskStatus(context.Background(), &mpp.ReportTaskStatusRequest{})
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	// A nil request is rejected the same way instead of panicking.
	resp, err = s.ReportMPPTaskStatus(context.Background(), nil)
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}
func TestUnsupportedTiKVRPC(t *testing.T) {
	var srv tikvpb.TikvServer = &rpcServer{}
	ctx := context.Background()
	calls := map[string]func() error{
		"prewrite": func() error {
			_, err := srv.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{})
			return err
		},
		"commit": func() error {
			_, err := srv.KvCommit(ctx, &kvrpcpb.CommitRequest{})
			return err
		},
		"raw get": func() error {
			_, err := srv.RawGet(ctx, &kvrpcpb.RawGetRequest{})
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			var err error
			require.NotPanics(t, func() { err = call() })
			require.Equal(t, codes.Unimplemented, status.Code(err))
		})
	}
}

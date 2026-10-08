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

package diagclient

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPDRPCGuards(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		method  string
		allowed bool
	}{
		{"/pdpb.PD/GetStore", true},
		{"/keyspacepb.Keyspace/LoadKeyspace", true},
		{"/pdpb.PD/UpdateGCSafePoint", false},
		{"/pdpb.PD/UnknownMethod", false},
	} {
		t.Run(tc.method, func(t *testing.T) {
			calls := 0
			err := pdUnary(ctx, tc.method, nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
				calls++
				return status.Error(codes.Unavailable, "forwarded")
			})
			if tc.allowed {
				require.Equal(t, 1, calls)
				require.Equal(t, codes.Unavailable, status.Code(err))
			} else {
				require.Zero(t, calls)
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			}
		})
	}
	for _, tc := range []struct {
		method  string
		allowed bool
	}{
		{"/pdpb.PD/Tso", true},
		{"/tsopb.TSO/Tso", true},
		{"/meta_storagepb.MetaStorage/Watch", true},
		{"/resource_manager.ResourceManager/AcquireTokenBuckets", false},
		{"/pdpb.PD/UnknownStream", false},
	} {
		t.Run(tc.method, func(t *testing.T) {
			calls := 0
			stream, err := pdStream(ctx, nil, nil, tc.method, func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
				calls++
				return nil, status.Error(codes.Unavailable, "forwarded")
			})
			require.Nil(t, stream)
			if tc.allowed {
				require.Equal(t, 1, calls)
				require.Equal(t, codes.Unavailable, status.Code(err))
			} else {
				require.Zero(t, calls)
				require.Equal(t, codes.PermissionDenied, status.Code(err))
			}
		})
	}
}

type recordingKVClient struct {
	tikv.Client
	calls    int
	request  *tikvrpc.Request
	response *tikvrpc.Response
}

func (c *recordingKVClient) SendRequest(_ context.Context, _ string, req *tikvrpc.Request, _ time.Duration) (*tikvrpc.Response, error) {
	c.calls++
	c.request = req
	return c.response, nil
}

func (c *recordingKVClient) SendRequestAsync(_ context.Context, _ string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	c.calls++
	c.request = req
	cb.Invoke(c.response, nil)
}

func TestKVRequestGuards(t *testing.T) {
	if !intest.InTest {
		t.Skip("diagnosticmode.SetForTest requires the intest build tag")
	}
	t.Cleanup(diagnosticmode.SetForTest(false))
	backend := &recordingKVClient{response: &tikvrpc.Response{}}
	require.Same(t, backend, WrapKV(backend))
	restore := diagnosticmode.SetForTest(true)
	defer restore()
	client := WrapKV(backend)
	require.IsType(t, &KVClient{}, client)
	for _, tc := range []struct {
		name    string
		request *tikvrpc.Request
		allowed bool
	}{
		{"get", &tikvrpc.Request{Type: tikvrpc.CmdGet}, true},
		{"scan", &tikvrpc.Request{Type: tikvrpc.CmdScan}, true},
		{"prewrite", &tikvrpc.Request{Type: tikvrpc.CmdPrewrite}, false},
		{"resolve-lock", &tikvrpc.Request{Type: tikvrpc.CmdResolveLock}, false},
		{"unknown", &tikvrpc.Request{Type: tikvrpc.CmdType(65535)}, false},
		{"nil", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, asynchronous := range []bool{false, true} {
				backend.calls = 0
				var response *tikvrpc.Response
				var err error
				if asynchronous {
					callbacks := 0
					client.SendRequestAsync(context.Background(), "store", tc.request, async.NewCallback(nil, func(r *tikvrpc.Response, e error) {
						callbacks++
						response, err = r, e
					}))
					require.Equal(t, 1, callbacks)
				} else {
					response, err = client.SendRequest(context.Background(), "store", tc.request, time.Second)
				}
				if tc.allowed {
					require.NoError(t, err)
					require.Equal(t, 1, backend.calls)
					require.Same(t, tc.request, backend.request)
					require.Same(t, backend.response, response)
				} else {
					require.Equal(t, codes.PermissionDenied, status.Code(err))
					require.Nil(t, response)
					require.Zero(t, backend.calls)
				}
			}
		})
	}
}

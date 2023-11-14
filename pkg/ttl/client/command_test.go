// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

type mockCmdRequest struct {
	V1 string `json:"v_1"`
	V2 int    `json:"v_2"`
}

type mockCmdResponse struct {
	V3 string `json:"v_3"`
	V4 int    `json:"v_4"`
}

func TestCommandClient(t *testing.T) {
	integration.BeforeTestExternal(t)

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	etcd := cluster.RandClient()

	etcdCli := NewCommandClient(etcd)
	mockCli := NewMockCommandClient()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	defer cancel()

	resCh := make(chan *mockCmdResponse)
	defer close(resCh)

	for _, cli := range []CommandClient{etcdCli, mockCli} {
		var sendRequestID, recvRequestID string

		// send command
		go func() {
			var err error
			var res mockCmdResponse
			defer func() {
				resCh <- &res
			}()
			req := &mockCmdRequest{V1: "1", V2: 2}
			sendRequestID, err = cli.Command(ctx, "type1", req, &res)
			require.NoError(t, err)
			require.NotEmpty(t, sendRequestID)
		}()

		// check the received command and send response
		watcher := cli.WatchCommand(ctx)
		select {
		case cmd, ok := <-watcher:
			require.True(t, ok)
			require.NotNil(t, cmd)
			require.Equal(t, "type1", cmd.CmdType)
			recvRequestID = cmd.RequestID
			var gotReq mockCmdRequest
			require.NoError(t, json.Unmarshal(cmd.Data, &gotReq))
			require.Equal(t, "1", gotReq.V1)
			require.Equal(t, 2, gotReq.V2)
			ok, err := cli.TakeCommand(ctx, recvRequestID)
			require.NoError(t, err)
			require.True(t, ok)
			require.NoError(t, cli.ResponseCommand(ctx, cmd.RequestID, &mockCmdResponse{V3: "3", V4: 4}))
		case <-ctx.Done():
			require.FailNow(t, ctx.Err().Error())
		}

		// check received response
		select {
		case res := <-resCh:
			require.NotNil(t, res)
			require.Equal(t, recvRequestID, sendRequestID)
			require.Equal(t, "3", res.V3)
			require.Equal(t, 4, res.V4)
		case <-ctx.Done():
			require.FailNow(t, ctx.Err().Error())
		}

		// Take command again should return false, nil
		ok, err := cli.TakeCommand(ctx, recvRequestID)
		require.NoError(t, err)
		require.False(t, ok)

		// send command and expect an error
		go func() {
			var err error
			var res mockCmdResponse
			defer func() {
				resCh <- &res
			}()
			req := &mockCmdRequest{V1: "1", V2: 2}
			sendRequestID, err = cli.Command(ctx, "type1", req, &res)
			require.NotEmpty(t, sendRequestID)
			require.EqualError(t, err, "mockErr")
		}()

		// response an error
		watcher = cli.WatchCommand(ctx)
		select {
		case cmd, ok := <-watcher:
			require.True(t, ok)
			require.NotNil(t, cmd)
			_, err = cli.TakeCommand(ctx, cmd.RequestID)
			require.NoError(t, err)
			require.NoError(t, cli.ResponseCommand(ctx, cmd.RequestID, errors.New("mockErr")))
		case <-ctx.Done():
			require.FailNow(t, ctx.Err().Error())
		}

		// wait send goroutine exit
		select {
		case <-resCh:
		case <-ctx.Done():
			require.FailNow(t, ctx.Err().Error())
		}
	}
}

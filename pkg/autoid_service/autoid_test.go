// Copyright 2022 PingCAP, Inc.
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

package autoid

import (
	"context"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"go.etcd.io/etcd/tests/v3/integration"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type autoIDResp struct {
	*autoid.AutoIDResponse
	error
	*testing.T
}

func (resp autoIDResp) check(min, max int64) {
	require.NoError(resp.T, resp.error)
	require.Equal(resp.T, resp.AutoIDResponse, &autoid.AutoIDResponse{Min: min, Max: max})
}

func (resp autoIDResp) checkErrmsg() {
	require.NoError(resp.T, resp.error)
	require.True(resp.T, len(resp.GetErrmsg()) > 0)
}

type rebaseResp struct {
	*autoid.RebaseResponse
	error
	*testing.T
}

func (resp rebaseResp) check(msg string) {
	require.NoError(resp.T, resp.error)
	require.Equal(resp.T, string(resp.RebaseResponse.GetErrmsg()), msg)
}

func TestConcurrent(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	cli := MockForTest(store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (id int key auto_increment);")
	is := dom.InfoSchema()
	dbInfo, ok := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)

	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	tbInfo := tbl.Meta()

	to := dest{dbID: dbInfo.ID, tblID: tbInfo.ID}

	const concurrency = 30
	notify := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			<-notify
			autoIDRequest(t, cli, to, false, 1)
		}()
	}

	// Rebase to some value
	rebaseRequest(t, cli, to, true, 666).check("")
	checkCurrValue(t, cli, to, 666, 666)
	// And +1 concurrently for 30 times
	close(notify)
	wg.Wait()
	// Check the result is increased by 30
	checkCurrValue(t, cli, to, 666+concurrency, 666+concurrency)
}

type dest struct {
	dbID  int64
	tblID int64
}

func checkCurrValue(t *testing.T, cli autoid.AutoIDAllocClient, to dest, min, max int64) {
	req := &autoid.AutoIDRequest{DbID: to.dbID, TblID: to.tblID, N: 0, KeyspaceID: uint32(tikv.NullspaceID)}
	ctx := context.Background()
	resp, err := cli.AllocAutoID(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resp, &autoid.AutoIDResponse{Min: min, Max: max})
}

func autoIDRequest(t *testing.T, cli autoid.AutoIDAllocClient, to dest, unsigned bool, n uint64, more ...int64) autoIDResp {
	increment := int64(1)
	offset := int64(1)
	if len(more) >= 1 {
		increment = more[0]
	}
	if len(more) >= 2 {
		offset = more[1]
	}
	req := &autoid.AutoIDRequest{DbID: to.dbID, TblID: to.tblID, IsUnsigned: unsigned, N: n, Increment: increment, Offset: offset, KeyspaceID: uint32(tikv.NullspaceID)}
	resp, err := cli.AllocAutoID(context.Background(), req)
	return autoIDResp{resp, err, t}
}

func rebaseRequest(t *testing.T, cli autoid.AutoIDAllocClient, to dest, unsigned bool, n int64, force ...struct{}) rebaseResp {
	req := &autoid.RebaseRequest{
		DbID:       to.dbID,
		TblID:      to.tblID,
		Base:       n,
		IsUnsigned: unsigned,
		Force:      len(force) > 0,
	}
	resp, err := cli.Rebase(context.Background(), req)
	return rebaseResp{resp, err, t}
}

func TestAPI(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	cli := MockForTest(store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int key auto_increment);")
	is := dom.InfoSchema()
	dbInfo, ok := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, ok)

	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tbInfo := tbl.Meta()

	to := dest{dbID: dbInfo.ID, tblID: tbInfo.ID}
	var force = struct{}{}

	// basic auto id operation
	autoIDRequest(t, cli, to, false, 1).check(0, 1)
	autoIDRequest(t, cli, to, false, 10).check(1, 11)
	checkCurrValue(t, cli, to, 11, 11)
	autoIDRequest(t, cli, to, false, 128).check(11, 139)
	autoIDRequest(t, cli, to, false, 1, 10, 5).check(139, 145)

	// basic rebase operation
	rebaseRequest(t, cli, to, false, 666).check("")
	autoIDRequest(t, cli, to, false, 1).check(666, 667)

	rebaseRequest(t, cli, to, false, 6666).check("")
	autoIDRequest(t, cli, to, false, 1).check(6666, 6667)

	// rebase will not decrease the value without 'force'
	rebaseRequest(t, cli, to, false, 44).check("")
	checkCurrValue(t, cli, to, 6667, 6667)
	rebaseRequest(t, cli, to, false, 44, force).check("")
	checkCurrValue(t, cli, to, 44, 44)

	// max increase 1
	rebaseRequest(t, cli, to, false, math.MaxInt64, force).check("")
	checkCurrValue(t, cli, to, math.MaxInt64, math.MaxInt64)
	autoIDRequest(t, cli, to, false, 1).checkErrmsg()

	rebaseRequest(t, cli, to, true, 0, force).check("")
	checkCurrValue(t, cli, to, 0, 0)
	autoIDRequest(t, cli, to, true, 1).check(0, 1)
	autoIDRequest(t, cli, to, true, 10).check(1, 11)
	autoIDRequest(t, cli, to, true, 128).check(11, 139)
	autoIDRequest(t, cli, to, true, 1, 10, 5).check(139, 145)

	// max increase 1
	rebaseRequest(t, cli, to, true, math.MaxInt64).check("")
	checkCurrValue(t, cli, to, math.MaxInt64, math.MaxInt64)
	autoIDRequest(t, cli, to, true, 1).check(math.MaxInt64, math.MinInt64)
	autoIDRequest(t, cli, to, true, 1).check(math.MinInt64, math.MinInt64+1)

	rebaseRequest(t, cli, to, true, -1).check("")
	checkCurrValue(t, cli, to, -1, -1)
	autoIDRequest(t, cli, to, true, 1).check(-1, 0)
}

func TestGRPC(t *testing.T) {
	integration.BeforeTestExternal(t)
	store := testkit.CreateMockStore(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	etcdCli := cluster.RandClient()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	addr := listener.Addr().String()

	service := newWithCli(addr, etcdCli, store)
	defer service.Close()

	var i int
	for !service.leaderShip.IsOwner() {
		time.Sleep(100 * time.Millisecond)
		i++
		if i >= 20 {
			break
		}
	}
	require.Less(t, i, 20)

	grpcServer := grpc.NewServer()
	autoid.RegisterAutoIDAllocServer(grpcServer, service)
	go func() {
		grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	grpcConn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	cli := autoid.NewAutoIDAllocClient(grpcConn)
	_, err = cli.AllocAutoID(context.Background(), &autoid.AutoIDRequest{
		DbID:       0,
		TblID:      0,
		N:          1,
		Increment:  1,
		Offset:     1,
		IsUnsigned: false,
		KeyspaceID: uint32(tikv.NullspaceID),
	})
	require.NoError(t, err)
}

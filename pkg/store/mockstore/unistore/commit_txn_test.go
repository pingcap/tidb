// Copyright 2025 PingCAP, Inc.
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

package unistore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func ctxForKey(t *testing.T, cluster *Cluster, key []byte) *kvrpcpb.Context {
	t.Helper()

	encodedKey := codec.EncodeBytes(nil, key)
	region, peer, _, _ := cluster.GetRegionByKey(encodedKey)
	require.NotNil(t, region)
	require.NotNil(t, peer)

	return &kvrpcpb.Context{
		RegionId:    region.Id,
		RegionEpoch: region.RegionEpoch,
		Peer:        peer,
	}
}

func TestCommitTxnBasic(t *testing.T) {
	rpcClient, _, cluster, err := New("", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rpcClient.Close())
		cluster.Close()
	})

	storeID, _, _ := BootstrapWithMultiRegions(cluster, []byte("m"))
	addr := fmt.Sprintf("store%d", storeID)

	startTS := uint64(10)
	primaryKey := []byte("a")
	secondaryKey := []byte("z")

	primaryVal := []byte("va")
	secondaryVal := []byte("vz")

	prewritePrimary := &kvrpcpb.PrewriteRequest{
		Context:      ctxForKey(t, cluster, primaryKey),
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: primaryKey, Value: primaryVal}},
		PrimaryLock:  primaryKey,
		StartVersion: startTS,
		LockTtl:      3000,
	}
	prewriteSecondary := &kvrpcpb.PrewriteRequest{
		Context:      ctxForKey(t, cluster, secondaryKey),
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: secondaryKey, Value: secondaryVal}},
		PrimaryLock:  primaryKey,
		StartVersion: startTS,
		LockTtl:      3000,
	}

	commitTxnReq := &kvrpcpb.CommitTxnRequest{
		Context:      &kvrpcpb.Context{},
		StartVersion: startTS,
		PrewriteReqs: []*kvrpcpb.PrewriteRequest{prewritePrimary, prewriteSecondary},
	}
	rpcReq := tikvrpc.NewRequest(tikvrpc.CmdCommitTxn, commitTxnReq, kvrpcpb.Context{})
	rpcResp, err := rpcClient.SendRequest(context.Background(), addr, rpcReq, time.Second)
	require.NoError(t, err)

	commitTxnResp, ok := rpcResp.Resp.(*kvrpcpb.CommitTxnResponse)
	require.True(t, ok)
	require.NotNil(t, commitTxnResp)
	require.Nil(t, commitTxnResp.GetRegionError())
	require.Nil(t, commitTxnResp.GetError())
	require.True(t, commitTxnResp.GetPrewriteSuccess())
	require.Len(t, commitTxnResp.GetPrewriteResps(), 2)
	require.Greater(t, commitTxnResp.GetCommitTs(), uint64(0))

	for _, prewriteResp := range commitTxnResp.GetPrewriteResps() {
		require.NotNil(t, prewriteResp)
		require.Nil(t, prewriteResp.GetRegionError())
		require.Empty(t, prewriteResp.GetErrors())
	}

	commitTS := commitTxnResp.GetCommitTs()

	primaryCtx := ctxForKey(t, cluster, primaryKey)
	getPrimaryReq := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Context: primaryCtx,
		Key:     primaryKey,
		Version: commitTS,
	}, *primaryCtx)
	getPrimaryResp, err := rpcClient.SendRequest(context.Background(), addr, getPrimaryReq, time.Second)
	require.NoError(t, err)
	gotPrimary, ok := getPrimaryResp.Resp.(*kvrpcpb.GetResponse)
	require.True(t, ok)
	require.Nil(t, gotPrimary.GetRegionError())
	require.Nil(t, gotPrimary.GetError())
	require.Equal(t, primaryVal, gotPrimary.GetValue())

	secondaryCtx := ctxForKey(t, cluster, secondaryKey)
	getSecondaryReq := tikvrpc.NewRequest(tikvrpc.CmdGet, &kvrpcpb.GetRequest{
		Context: secondaryCtx,
		Key:     secondaryKey,
		Version: commitTS,
	}, *secondaryCtx)
	for i := 0; i < 10; i++ {
		getSecondaryResp, err := rpcClient.SendRequest(context.Background(), addr, getSecondaryReq, time.Second)
		require.NoError(t, err)
		gotSecondary, ok := getSecondaryResp.Resp.(*kvrpcpb.GetResponse)
		require.True(t, ok)
		require.Nil(t, gotSecondary.GetRegionError())
		keyErr := gotSecondary.GetError()
		if keyErr != nil {
			require.NotNil(t, keyErr.Locked)
			require.Equal(t, keyErr.Locked.LockVersion, startTS)
			time.Sleep(time.Millisecond * 10)
			continue
		}
		require.Nil(t, gotSecondary.GetError())
		require.Equal(t, secondaryVal, gotSecondary.GetValue())
		break
	}
}

func TestCommitTxnPrewriteError(t *testing.T) {
	rpcClient, _, cluster, err := New("", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rpcClient.Close())
		cluster.Close()
	})

	storeID, _, _ := BootstrapWithMultiRegions(cluster, []byte("m"))
	addr := fmt.Sprintf("store%d", storeID)

	primaryKey := []byte("a")
	secondaryKey := []byte("z")

	// Leave a lock on primaryKey.
	lockStartTS := uint64(5)
	lockCtx := ctxForKey(t, cluster, primaryKey)
	lockPrewriteReq := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		Context:      lockCtx,
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: primaryKey, Value: []byte("locked")}},
		PrimaryLock:  primaryKey,
		StartVersion: lockStartTS,
		LockTtl:      3000,
	}, *lockCtx)
	_, err = rpcClient.SendRequest(context.Background(), addr, lockPrewriteReq, time.Second)
	require.NoError(t, err)

	startTS := uint64(10)
	prewritePrimary := &kvrpcpb.PrewriteRequest{
		Context:      ctxForKey(t, cluster, primaryKey),
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: primaryKey, Value: []byte("va")}},
		PrimaryLock:  primaryKey,
		StartVersion: startTS,
		LockTtl:      3000,
	}
	prewriteSecondary := &kvrpcpb.PrewriteRequest{
		Context:      ctxForKey(t, cluster, secondaryKey),
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: secondaryKey, Value: []byte("vz")}},
		PrimaryLock:  primaryKey,
		StartVersion: startTS,
		LockTtl:      3000,
	}

	commitTxnReq := &kvrpcpb.CommitTxnRequest{
		Context:      &kvrpcpb.Context{},
		StartVersion: startTS,
		PrewriteReqs: []*kvrpcpb.PrewriteRequest{prewritePrimary, prewriteSecondary},
	}
	rpcReq := tikvrpc.NewRequest(tikvrpc.CmdCommitTxn, commitTxnReq, kvrpcpb.Context{})
	rpcResp, err := rpcClient.SendRequest(context.Background(), addr, rpcReq, time.Second)
	require.NoError(t, err)

	commitTxnResp, ok := rpcResp.Resp.(*kvrpcpb.CommitTxnResponse)
	require.True(t, ok)
	require.NotNil(t, commitTxnResp)
	require.Len(t, commitTxnResp.GetPrewriteResps(), 2)
	require.False(t, commitTxnResp.GetPrewriteSuccess())
	require.Equal(t, uint64(0), commitTxnResp.GetCommitTs())

	// At least one region should report a prewrite error (lock conflict).
	hasErr := false
	for _, prewriteResp := range commitTxnResp.GetPrewriteResps() {
		if prewriteResp.GetRegionError() != nil || len(prewriteResp.GetErrors()) != 0 {
			hasErr = true
			break
		}
	}
	require.True(t, hasErr)
}

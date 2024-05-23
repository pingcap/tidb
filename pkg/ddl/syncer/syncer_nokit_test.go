// Copyright 2024 PingCAP, Inc.
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

package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestNodeVersions(t *testing.T) {
	nv := newNodeVersions(1, nil)
	require.True(t, nv.emptyAndNotUsed())
	nv.add("a", 10)
	nv.add("b", 20)
	require.False(t, nv.emptyAndNotUsed())
	require.EqualValues(t, 2, nv.len())
	var waterMark int64 = 10
	fn := func(nodeVersions map[string]int64) bool {
		for _, v := range nodeVersions {
			if v < waterMark {
				return false
			}
		}
		return true
	}
	nv.matchOrSet(fn)
	require.Nil(t, nv.onceMatchFn)
	waterMark = 20
	nv.matchOrSet(fn)
	require.NotNil(t, nv.onceMatchFn)
	// matched and cleared
	nv.add("a", 20)
	require.Nil(t, nv.onceMatchFn)
	nv.del("a")
	require.EqualValues(t, 1, nv.len())
	nv.del("b")
	require.True(t, nv.emptyAndNotUsed())
	nv.matchOrSet(func(map[string]int64) bool { return false })
	require.False(t, nv.emptyAndNotUsed())
}

func TestDecodeJobVersionEvent(t *testing.T) {
	prefix := util.DDLAllSchemaVersionsByJob + "/"
	_, _, _, valid := decodeJobVersionEvent(&mvccpb.KeyValue{Key: []byte(prefix + "1")}, mvccpb.PUT, prefix)
	require.False(t, valid)
	_, _, _, valid = decodeJobVersionEvent(&mvccpb.KeyValue{Key: []byte(prefix + "a/aa")}, mvccpb.PUT, prefix)
	require.False(t, valid)
	_, _, _, valid = decodeJobVersionEvent(&mvccpb.KeyValue{
		Key: []byte(prefix + "1/aa"), Value: []byte("aa")}, mvccpb.PUT, prefix)
	require.False(t, valid)
	jobID, tidbID, schemaVer, valid := decodeJobVersionEvent(&mvccpb.KeyValue{
		Key: []byte(prefix + "1/aa"), Value: []byte("123")}, mvccpb.PUT, prefix)
	require.True(t, valid)
	require.EqualValues(t, 1, jobID)
	require.EqualValues(t, "aa", tidbID)
	require.EqualValues(t, 123, schemaVer)
	// value is not used on delete
	jobID, tidbID, schemaVer, valid = decodeJobVersionEvent(&mvccpb.KeyValue{
		Key: []byte(prefix + "1/aa"), Value: []byte("aaaa")}, mvccpb.DELETE, prefix)
	require.True(t, valid)
	require.EqualValues(t, 1, jobID)
	require.EqualValues(t, "aa", tidbID)
	require.EqualValues(t, 0, schemaVer)
}

func TestSyncJobSchemaVerLoop(t *testing.T) {
	integration.BeforeTestExternal(t)
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)

	ctx, cancel := context.WithCancel(context.Background())
	etcdCli := mockCluster.RandClient()
	_, err := etcdCli.Put(ctx, util.DDLAllSchemaVersionsByJob+"/1/aa", "123")
	require.NoError(t, err)
	s := NewSchemaSyncer(etcdCli, "1111").(*schemaVersionSyncer)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.SyncJobSchemaVerLoop(ctx)
	}()

	// job 1 is matched
	notifyCh := make(chan struct{})
	item := s.jobSchemaVerMatchOrSet(1, func(m map[string]int64) bool {
		for _, v := range m {
			if v < 123 {
				return false
			}
		}
		close(notifyCh)
		return true
	})
	<-notifyCh
	require.Nil(t, item.getMatchFn())
	_, err = etcdCli.Delete(ctx, util.DDLAllSchemaVersionsByJob+"/1/aa")
	require.NoError(t, err)

	// job 2 requires aa and bb
	notifyCh = make(chan struct{}, 1)
	item = s.jobSchemaVerMatchOrSet(2, func(m map[string]int64) bool {
		for _, nodeID := range []string{"aa", "bb"} {
			if v, ok := m[nodeID]; !ok || v < 123 {
				return false
			}
		}
		close(notifyCh)
		return true
	})
	require.NotNil(t, item.getMatchFn())
	require.Len(t, notifyCh, 0)
	_, err = etcdCli.Put(ctx, util.DDLAllSchemaVersionsByJob+"/2/aa", "123")
	require.NoError(t, err)
	_, err = etcdCli.Put(ctx, util.DDLAllSchemaVersionsByJob+"/2/bb", "124")
	require.NoError(t, err)
	<-notifyCh
	require.Nil(t, item.getMatchFn())
	jobNodeVersionCnt := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.jobNodeVersions)
	}
	require.EqualValues(t, 1, jobNodeVersionCnt())
	_, err = etcdCli.Delete(ctx, util.DDLAllSchemaVersionsByJob+"/2/aa")
	require.NoError(t, err)
	_, err = etcdCli.Delete(ctx, util.DDLAllSchemaVersionsByJob+"/2/bb")
	require.NoError(t, err)

	// job 3 is matched after restart from a compaction error
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/syncer/mockCompaction", `1*return(true)`))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/syncer/mockCompaction"))
	})
	notifyCh = make(chan struct{}, 1)
	item = s.jobSchemaVerMatchOrSet(3, func(m map[string]int64) bool {
		for _, nodeID := range []string{"aa"} {
			if v, ok := m[nodeID]; !ok || v < 123 {
				return false
			}
		}
		close(notifyCh)
		return true
	})
	require.NotNil(t, item.getMatchFn())
	require.Len(t, notifyCh, 0)
	_, err = etcdCli.Put(ctx, util.DDLAllSchemaVersionsByJob+"/3/aa", "123")
	require.NoError(t, err)
	<-notifyCh
	require.Nil(t, item.getMatchFn())
	_, err = etcdCli.Delete(ctx, util.DDLAllSchemaVersionsByJob+"/3/aa")
	require.NoError(t, err)

	// job 4 is matched using OwnerCheckAllVersions
	variable.EnableMDL.Store(true)
	serverInfos := map[string]*infosync.ServerInfo{"aa": {ID: "aa", IP: "test", Port: 4000}}
	bytes, err := json.Marshal(serverInfos)
	require.NoError(t, err)
	inTerms := fmt.Sprintf("return(`%s`)", string(bytes))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/domain/infosync/mockGetAllServerInfo", inTerms))
	_, err = etcdCli.Put(ctx, util.DDLAllSchemaVersionsByJob+"/4/aa", "333")
	require.NoError(t, err)
	require.NoError(t, s.OwnerCheckAllVersions(ctx, 4, 333))
	_, err = etcdCli.Delete(ctx, util.DDLAllSchemaVersionsByJob+"/4/aa")
	require.NoError(t, err)

	cancel()
	wg.Wait()
}

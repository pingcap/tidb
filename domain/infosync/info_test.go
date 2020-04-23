// Copyright 2020 PingCAP, Inc.
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

package infosync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/owner"
	"go.etcd.io/etcd/integration"
)

func (is *InfoSyncer) getTopologyFromEtcd(ctx context.Context) (*topologyInfo, error) {
	key := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, is.info.IP, is.info.Port)
	resp, err := is.etcdCli.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.New("not-exists")
	}
	if len(resp.Kvs) != 1 {
		return nil, errors.New("resp.Kvs error")
	}
	var ret topologyInfo
	err = json.Unmarshal(resp.Kvs[0].Value, &ret)
	if err != nil {
		return nil, err
	}
	return &ret, nil
}

func (is *InfoSyncer) ttlKeyExists(ctx context.Context) (bool, error) {
	key := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, is.info.IP, is.info.Port)
	resp, err := is.etcdCli.Get(ctx, key)
	if err != nil {
		return false, err
	}
	if len(resp.Kvs) >= 2 {
		return false, errors.New("too many arguments in resp.Kvs")
	}
	return len(resp.Kvs) == 1, nil
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func TestTopology(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	currentID := "test"

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	cli := clus.RandClient()

	failpoint.Enable("github.com/pingcap/tidb/domain/infosync/mockServerInfo", "return(true)")
	defer failpoint.Disable("github.com/pingcap/tidb/domain/infosync/mockServerInfo")

	info, err := GlobalInfoSyncerInit(ctx, currentID, cli)
	if err != nil {
		t.Fatal(err)
	}

	err = info.newTopologySessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)

	if err != nil {
		t.Fatal(err)
	}

	topo, err := info.getTopologyFromEtcd(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if topo.StartTimestamp != 1282967700000 {
		t.Fatal("start_timestamp of topology info does not match")
	}

	if !reflect.DeepEqual(*topo, info.getTopologyInfo()) {
		t.Fatal("the info in etcd is not match with info.")
	}

	nonTTLKey := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, info.info.IP, info.info.Port)
	ttlKey := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, info.info.IP, info.info.Port)

	err = util.DeleteKeyFromEtcd(nonTTLKey, cli, owner.NewSessionDefaultRetryCnt, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Refresh and re-test if the key exists
	err = info.RestartTopology(ctx)
	if err != nil {
		t.Fatal(err)
	}

	topo, err = info.getTopologyFromEtcd(ctx)
	if err != nil {
		t.Fatal(err)
	}

	s, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	dir := path.Dir(s)

	if topo.DeployPath != dir {
		t.Fatal("DeployPath not match expected path")
	}

	if topo.StartTimestamp != 1282967700000 {
		t.Fatal("start_timestamp of topology info does not match")
	}

	if !reflect.DeepEqual(*topo, info.getTopologyInfo()) {
		t.Fatal("the info in etcd is not match with info.")
	}

	// check ttl key
	ttlExists, err := info.ttlKeyExists(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ttlExists {
		t.Fatal("ttl non-exists")
	}

	err = util.DeleteKeyFromEtcd(ttlKey, cli, owner.NewSessionDefaultRetryCnt, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	err = info.updateTopologyAliveness(ctx)
	if err != nil {
		t.Fatal(err)
	}

	ttlExists, err = info.ttlKeyExists(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ttlExists {
		t.Fatal("ttl non-exists")
	}
}

package infosync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/owner"
	"go.etcd.io/etcd/integration"
)

func (is *InfoSyncer) getTopology(ctx context.Context) (topologyInfo, error) {
	key := fmt.Sprintf("%s/%s:%v/tidb", TopologyInformationPath, is.info.IP, is.info.Port)
	resp, err := is.etcdCli.Get(ctx, key)
	if err != nil {
		return topologyInfo{}, err
	}
	if len(resp.Kvs) == 0 {
		return topologyInfo{}, errors.New("not-exists")
	}
	if len(resp.Kvs) != 1 {
		return topologyInfo{}, errors.New("resp.Kvs error")
	}
	var ret topologyInfo
	err = json.Unmarshal(resp.Kvs[0].Value, &ret)
	if err != nil {
		return topologyInfo{}, err
	}
	return ret, nil
}

func TestTopology(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	currentId := "test"

	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)

	cli := clus.RandClient()

	info, err := GlobalInfoSyncerInit(ctx, currentId, cli)
	if err != nil {
		t.Fatal(err)
	}

	err = info.newTopologySessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)

	if err != nil {
		t.Fatal(err)
	}

	topo, err := info.getTopology(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if topo != info.getTopologyInfo() {
		t.Fatal("the info in etcd is not match with info.")
	}

	nonTTLKey := fmt.Sprintf("%s/%s:%v/tidb", TopologyInformationPath, info.info.IP, info.info.Port)
	err = util.DeleteKeyFromEtcd(nonTTLKey, cli, owner.NewSessionDefaultRetryCnt, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// check watchChan
	chanRecv := false
	select {
	case <-info.TopologyUpdateChan():
		chanRecv = true
	case <-time.After(time.Second):
	}
	if chanRecv == false {
		t.Fatal("Receive Error")
	}
}

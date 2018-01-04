package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	"github.com/pingcap/kvproto/pkg/metapb"
)

var (
	endpoints   = flag.String("endpoints", "http://127.0.0.1:2379", "endpoints urls")
	allocID     = flag.Uint64("alloc-id", 0, "please make sure alloced ID is safe")
	clusterID   = flag.Uint64("cluster-id", 0, "please make cluster ID match with tikv")
	maxReplicas = flag.Int("max-replicas", 3, "max replicas is the number of replicas for each region")
	caPath      = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs.")
	certPath    = flag.String("cert", "", "path of file that contains X509 certificate in PEM format..")
	keyPath     = flag.String("key", "", "path of file that contains X509 key in PEM format.")
)

const (
	requestTimeout = 10 * time.Second
	etcdTimeout    = 3 * time.Second

	pdRootPath      = "/pd"
	pdClusterIDPath = "/pd/cluster_id"
)

func exitErr(err error) {
	fmt.Println(err.Error())
	os.Exit(1)
}

func main() {
	flag.Parse()
	if *clusterID == 0 {
		fmt.Println("please specify safe cluster-id")
		return
	}
	if *allocID == 0 {
		fmt.Println("please specify safe alloc-id")
		return
	}

	rootPath := path.Join(pdRootPath, strconv.FormatUint(*clusterID, 10))
	clusterRootPath := path.Join(rootPath, "raft")
	raftBootstrapTimeKey := path.Join(clusterRootPath, "status", "raft_bootstrap_time")

	urls := strings.Split(*endpoints, ",")

	tlsInfo := transport.TLSInfo{
		CertFile:      *certPath,
		KeyFile:       *keyPath,
		TrustedCAFile: *caPath,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		fmt.Println("failed to connect: err")
		return
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   urls,
		DialTimeout: etcdTimeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		exitErr(err)
	}
	ctx, cancel := context.WithTimeout(client.Ctx(), requestTimeout)
	defer cancel()

	var ops []clientv3.Op
	// recover cluster_id
	ops = append(ops, clientv3.OpPut(pdClusterIDPath, string(uint64ToBytes(*clusterID))))
	// recover alloc_id
	allocIDPath := path.Join(rootPath, "alloc_id")
	ops = append(ops, clientv3.OpPut(allocIDPath, string(uint64ToBytes(*allocID))))

	// recover bootstrap
	// recover meta of cluster
	clusterMeta := metapb.Cluster{
		Id:           *clusterID,
		MaxPeerCount: uint32(*maxReplicas),
	}
	clusterValue, err := clusterMeta.Marshal()
	if err != nil {
		exitErr(err)
	}
	ops = append(ops, clientv3.OpPut(clusterRootPath, string(clusterValue)))

	// set raft bootstrap time
	nano := time.Now().UnixNano()
	timeData := uint64ToBytes(uint64(nano))
	ops = append(ops, clientv3.OpPut(raftBootstrapTimeKey, string(timeData)))

	// the new pd cluster should not bootstrapped by tikv
	bootstrapCmp := clientv3.Compare(clientv3.CreateRevision(clusterRootPath), "=", 0)
	resp, err := client.Txn(ctx).If(bootstrapCmp).Then(ops...).Commit()
	if err != nil {
		exitErr(err)
	}
	if !resp.Succeeded {
		fmt.Println("failed to recover: the cluster is already bootstrapped")
		return
	}
	fmt.Println("recover success! please restart the PD cluster")
}

func uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

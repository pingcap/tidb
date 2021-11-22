package stream

import "go.etcd.io/etcd/clientv3"

// MetaDataClient is the client for operations over metadata.
type MetaDataClient struct {
	clientv3.Client
}

package keyspace

import (
	"fmt"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/tikv/client-go/v2/tikv"
)

const (
	// tidbKeyspaceEtcdPathPrefix is the keyspace prefix for etcd namespace
	tidbKeyspaceEtcdPathPrefix = "/keyspaces/tidb/"
)

// CodecV1 represents api v1 codec.
var CodecV1 = tikv.NewCodecV1(tikv.ModeTxn)

// MakeKeyspaceEtcdNamespace return the keyspace prefix path for etcd namespace
func MakeKeyspaceEtcdNamespace(c tikv.Codec) string {
	if c.GetAPIVersion() == kvrpcpb.APIVersion_V1 {
		return ""
	}
	return fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d", c.GetKeyspaceID())
}

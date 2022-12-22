package keyspace

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/tikv/client-go/v2/tikv"
)

const (

	// EnvVarKeyspaceName is the system env name for keyspace name.
	EnvVarKeyspaceName = "KEYSPACE_NAME"

	// tidbKeyspaceEtcdPathPrefix is the keyspace prefix for etcd namespace
	tidbKeyspaceEtcdPathPrefix = "/keyspaces/tidb/"
)

// CodecV1 represents api v1 codec.
var CodecV1 = tikv.NewCodecV1(tikv.ModeTxn)

// GetKeyspaceNameBySettings is used to get Keyspace name setting.
func GetKeyspaceNameBySettings() (keyspaceName string) {

	// The cfg.keyspaceName get higher weights than KEYSPACE_NAME in system env.
	keyspaceName = config.GetGlobalConfig().KeyspaceName
	if !IsKeyspaceNameEmpty(keyspaceName) {
		return keyspaceName
	}

	keyspaceName = os.Getenv(EnvVarKeyspaceName)
	return keyspaceName
}

// IsKeyspaceNameEmpty is used to determine whether keyspaceName is set.
func IsKeyspaceNameEmpty(keyspaceName string) bool {
	return keyspaceName == ""
}

// GetKeyspacePathPrefix return the keyspace prefix path for etcd namespace
func GetKeyspacePathPrefix(keyspaceID uint32) string {
	path := fmt.Sprintf(tidbKeyspaceEtcdPathPrefix+"%d", keyspaceID)
	return path
}

// IsKvStorageKeyspaceSet return true if you get keyspace meta successes
func IsKvStorageKeyspaceSet(store kv.Storage) bool {
	return store.GetCodec().GetKeyspace() != nil
}

// toUint32 is used to convert byte array to uint32
func toUint32(b []byte) uint32 {
	c := make([]byte, 4)
	copy(c[1:4], b)
	return binary.BigEndian.Uint32(c)
}

// GetID is used to get keyspace id bytes from keyspace prefix
func GetID(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return toUint32(b[1:4])
}

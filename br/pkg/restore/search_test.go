package restore

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/pingcap/tidb/tablecodec"

	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/util/codec"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/br/pkg/storage"
)

func TestSearchKey(t *testing.T) {
	key, err := hex.DecodeString("7480000000000000BC5F72800000000000003A")
	require.NoError(t, err)
	backend, err := storage.ParseBackend("s3://stream/2022-05-20/1?access-key=XG0Z3WGDWX7J1PM3ORCM&secret-access-key=7OFdwBCBonZ8+WHF6QTBerEeq7diFXScAyS3rBlI&endpoint=http://172.16.102.96:9000&force-path-style=true", nil)
	require.NoError(t, err)

	ctx := context.Background()
	s, err := storage.New(ctx, backend, &storage.ExternalStorageOptions{SendCredentials: false})
	require.NoError(t, err)

	comparator := NewStartWithComparator()
	streamBackupSearch := NewStreamBackupSearch(s, comparator, key)
	kvs, err := streamBackupSearch.Search(ctx)
	require.NoError(t, err)

	for _, row := range kvs {
		t.Log(row)
		t.Log("key:", row.Key)
		t.Log("value:", row.Value)
	}
}

func TestEncodedKey(t *testing.T) {
	key, err := hex.DecodeString("74800000000000003B5F69800000000000000203800000000000000105BFF199999999999A013131310000000000FA")
	require.NoError(t, err)

	tableID, indexID, indexValues, err := tablecodec.DecodeIndexKey(key)
	require.NoError(t, err)
	t.Log("table id", tableID, "index id", indexID, "index values", indexValues)

	t.Log(string(key[:1]))
	key = key[1:]
	key, tableID, err = codec.DecodeInt(key)
	require.NoError(t, err)
	t.Log("table id", tableID)

	t.Log("split", string(key[:2]))
	key = key[2:]

	t.Log(key)

	key, intHandle, err := codec.DecodeInt(key)
	handle := kv.IntHandle(intHandle)

	t.Log(handle.Encoded())
	t.Log(handle.IntValue())

	t.Log(key)
}

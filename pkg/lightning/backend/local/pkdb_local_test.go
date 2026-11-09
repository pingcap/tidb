package local

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

type testKeyspacePDClient struct {
	pd.Client
	keyspaceMeta *keyspacepb.KeyspaceMeta
}

func (c *testKeyspacePDClient) LoadKeyspace(context.Context, string) (*keyspacepb.KeyspaceMeta, error) {
	return c.keyspaceMeta, nil
}

func TestConflictPrecheckWithAPIv2Keyspace(t *testing.T) {
	ctx := context.Background()
	keyspaceMeta := keyspacepb.KeyspaceMeta{
		Id:    123,
		Name:  "test_keyspace",
		State: keyspacepb.KeyspaceState_ENABLED,
	}
	rpcClient, pdClient, cluster, err := unistore.New("", nil)
	require.NoError(t, err)
	unistore.BootstrapWithSingleStore(cluster)
	keyspacePDClient := &testKeyspacePDClient{Client: pdClient, keyspaceMeta: &keyspaceMeta}
	tikvStore, err := tikv.NewTestKeyspaceTiKVStore(rpcClient, keyspacePDClient, nil, nil, 0, keyspaceMeta)
	require.NoError(t, err)

	apiV2Codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspaceMeta)
	require.NoError(t, err)
	cfg := BackendConfig{
		LocalStoreDir:               filepath.Join(t.TempDir(), "sorted-kv"),
		MaxOpenFiles:                1000,
		MemTableSize:                config.DefaultEngineMemCacheSize,
		LocalWriterMemCacheSize:     int64(config.DefaultLocalWriterMemCacheSize),
		DupeDetectEnabled:           true,
		DuplicateDetectOpt:          common.DupDetectOpt{ReportErrOnDup: true},
		PrecheckConcurrency:         1,
		DisableAutomaticCompactions: true,
	}
	local := &Backend{
		pdCli:         tikvStore.GetPDClient(),
		tikvCli:       tikvStore,
		tikvCodec:     apiV2Codec,
		BackendConfig: cfg,
	}
	local.engineMgr, err = newEngineManager(cfg, local, local.logger)
	require.NoError(t, err)
	t.Cleanup(func() {
		local.engineMgr.close()
		require.NoError(t, tikvStore.Close())
	})

	rawRecordKey := tablecodec.EncodeRowKeyWithHandle(1, tidbkv.IntHandle(1))
	txn, err := tikvStore.Begin()
	require.NoError(t, err)
	require.NoError(t, txn.Set(rawRecordKey, []byte("existing")))
	require.NoError(t, txn.Commit(ctx))

	engineUUID := uuid.New()
	engineCfg := &backend.EngineConfig{
		TableInfo: &checkpoints.TidbTableInfo{ID: 1, DB: "test", Name: "t"},
	}
	require.NoError(t, local.OpenEngine(ctx, engineCfg, engineUUID))
	writerCfg := &backend.LocalWriterConfig{}
	writerCfg.Local.IsKVSorted = true
	writer, err := local.LocalWriter(ctx, writerCfg, engineUUID)
	require.NoError(t, err)
	require.NoError(t, writer.AppendRows(ctx, nil, kv.MakeRowsFromKvPairs([]common.KvPair{{
		Key:   rawRecordKey,
		Val:   []byte("incoming"),
		RowID: common.MinRowID,
	}})))
	_, err = writer.Close(ctx)
	require.NoError(t, err)
	require.NoError(t, local.CloseEngine(ctx, engineCfg, engineUUID))

	localRecordKey := apiV2Codec.EncodeKey(rawRecordKey)
	err = local.PrecheckConflictForImportEngineRanges(ctx, engineUUID, []tidbkv.KeyRange{{
		StartKey: localRecordKey,
		EndKey:   nextKey(localRecordKey),
	}})
	require.True(t, common.ErrFoundDuplicateKeys.Equal(err), "unexpected error: %v", err)
}

func TestNextKeyWithAPIv2KeyspaceForConflictPrecheck(t *testing.T) {
	rawRecordKey := tablecodec.EncodeRowKeyWithHandle(1, tidbkv.IntHandle(1))
	apiV2Codec, err := tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
		Id:   123,
		Name: "test_keyspace",
	})
	require.NoError(t, err)

	// Conflict precheck uses local engine keys as the scan range for getTiKVScanner.
	// With API V2 enabled, those keys are already encoded with the keyspace prefix,
	// so nextKey must stay in the same encoded key space.
	encodedRecordKey := apiV2Codec.EncodeKey(rawRecordKey)
	nextEncodedKey := nextKey(encodedRecordKey)
	require.Equal(t, append(bytes.Clone(encodedRecordKey), 0), nextEncodedKey)
	require.Equal(t, apiV2Codec.GetKeyspace(), nextEncodedKey[:len(apiV2Codec.GetKeyspace())])

	decodedRecordKey, err := apiV2Codec.DecodeKey(encodedRecordKey)
	require.NoError(t, err)
	require.Equal(t, []byte(rawRecordKey), decodedRecordKey)
}

package server

import (
	"context"
	"path/filepath"

	"github.com/pingcap/badger"
	"github.com/pingcap/badger/options"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/mockstore/unistore/config"
	"github.com/pingcap/tidb/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/store/mockstore/unistore/pd"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
)

const (
	subPathRaft = "raft"
	subPathKV   = "kv"
)

// NewMock returns a new *tikv.Server, a new *tikv.MockRegionManager and a new *tikv.MockPD.
func NewMock(conf *config.Config, clusterID uint64) (*tikv.Server, *tikv.MockRegionManager, *tikv.MockPD, error) {
	physical, logical := tikv.GetTS()
	ts := uint64(physical)<<18 + uint64(logical)

	safePoint := &tikv.SafePoint{}
	db, err := createDB(subPathKV, safePoint, &conf.Engine)
	if err != nil {
		return nil, nil, nil, err
	}
	bundle := &mvcc.DBBundle{
		DB:        db,
		LockStore: lockstore.NewMemStore(8 << 20),
		StateTS:   ts,
	}

	rm, err := tikv.NewMockRegionManager(bundle, clusterID, tikv.RegionOptions{
		StoreAddr:  conf.Server.StoreAddr,
		PDAddr:     conf.Server.PDAddr,
		RegionSize: conf.Server.RegionSize,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	pdClient := tikv.NewMockPD(rm)
	svr, err := setupStandAlongInnerServer(bundle, safePoint, rm, pdClient, conf)
	if err != nil {
		return nil, nil, nil, err
	}
	return svr, rm, pdClient, nil
}

// New returns a new *tikv.Server.
func New(conf *config.Config, pdClient pd.Client) (*tikv.Server, error) {
	physical, logical, err := pdClient.GetTS(context.Background())
	if err != nil {
		return nil, err
	}
	ts := uint64(physical)<<18 + uint64(logical)

	safePoint := &tikv.SafePoint{}
	db, err := createDB(subPathKV, safePoint, &conf.Engine)
	if err != nil {
		return nil, err
	}
	bundle := &mvcc.DBBundle{
		DB:        db,
		LockStore: lockstore.NewMemStore(8 << 20),
		StateTS:   ts,
	}
	if conf.Server.Raft {
		return nil, errors.New("not support raftstore")
	}
	rm := tikv.NewStandAloneRegionManager(bundle, getRegionOptions(conf), pdClient)
	return setupStandAlongInnerServer(bundle, safePoint, rm, pdClient, conf)
}

func getRegionOptions(conf *config.Config) tikv.RegionOptions {
	return tikv.RegionOptions{
		StoreAddr:  conf.Server.StoreAddr,
		PDAddr:     conf.Server.PDAddr,
		RegionSize: conf.Server.RegionSize,
	}
}

func setupStandAlongInnerServer(bundle *mvcc.DBBundle, safePoint *tikv.SafePoint, rm tikv.RegionManager, pdClient pd.Client, conf *config.Config) (*tikv.Server, error) {
	innerServer := tikv.NewStandAlongInnerServer(bundle)
	innerServer.Setup(pdClient)
	store := tikv.NewMVCCStore(conf, bundle, conf.Engine.DBPath, safePoint, tikv.NewDBWriter(bundle), pdClient)
	store.DeadlockDetectSvr.ChangeRole(tikv.Leader)

	if err := innerServer.Start(pdClient); err != nil {
		return nil, err
	}

	store.StartDeadlockDetection(false)

	return tikv.NewServer(rm, store, innerServer), nil
}

func createDB(subPath string, safePoint *tikv.SafePoint, conf *config.Engine) (*badger.DB, error) {
	opts := badger.DefaultOptions
	opts.NumCompactors = conf.NumCompactors
	opts.ValueThreshold = conf.ValueThreshold
	if subPath == subPathRaft {
		// Do not need to write blob for raft engine because it will be deleted soon.
		return nil, errors.New("not support " + subPathRaft)
	}
	opts.ManagedTxns = true
	opts.ValueLogWriteOptions.WriteBufferSize = 4 * 1024 * 1024
	opts.Dir = filepath.Join(conf.DBPath, subPath)
	opts.ValueDir = opts.Dir
	opts.ValueLogFileSize = conf.VlogFileSize
	opts.ValueLogMaxNumFiles = 3
	opts.MaxMemTableSize = conf.MaxMemTableSize
	opts.TableBuilderOptions.MaxTableSize = conf.MaxTableSize
	opts.NumMemtables = conf.NumMemTables
	opts.NumLevelZeroTables = conf.NumL0Tables
	opts.NumLevelZeroTablesStall = conf.NumL0TablesStall
	opts.LevelOneSize = conf.L1Size
	opts.SyncWrites = conf.SyncWrite
	compressionPerLevel := make([]options.CompressionType, len(conf.Compression))
	for i := range opts.TableBuilderOptions.CompressionPerLevel {
		compressionPerLevel[i] = config.ParseCompression(conf.Compression[i])
	}
	opts.TableBuilderOptions.CompressionPerLevel = compressionPerLevel
	opts.MaxBlockCacheSize = conf.BlockCacheSize
	opts.MaxIndexCacheSize = conf.IndexCacheSize
	opts.TableBuilderOptions.SuRFStartLevel = conf.SurfStartLevel
	if safePoint != nil {
		opts.CompactionFilterFactory = safePoint.CreateCompactionFilter
	}
	opts.CompactL0WhenClose = conf.CompactL0WhenClose
	opts.VolatileMode = conf.VolatileMode
	return badger.Open(opts)
}

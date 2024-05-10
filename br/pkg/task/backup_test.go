// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"testing"
	"time"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParseTSString(t *testing.T) {
	var (
		ts  uint64
		err error
	)

	ts, err = ParseTSString("", false)
	require.NoError(t, err)
	require.Zero(t, ts)

	ts, err = ParseTSString("400036290571534337", false)
	require.NoError(t, err)
	require.Equal(t, uint64(400036290571534337), ts)

	ts, err = ParseTSString("2021-01-01 01:42:23", false)
	require.NoError(t, err)
	localTime := time.Date(2021, time.Month(1), 1, 1, 42, 23, 0, time.Local)
	localTimestamp := localTime.Unix()
	localTSO := uint64((localTimestamp << 18) * 1000)
	require.Equal(t, localTSO, ts)

	_, err = ParseTSString("2021-01-01 01:42:23", true)
	require.Error(t, err)
	require.Regexp(t, "must set timezone*", err.Error())

	ts, err = ParseTSString("2021-01-01 01:42:23+00:00", true)
	require.NoError(t, err)
	localTime = time.Date(2021, time.Month(1), 1, 1, 42, 23, 0, time.UTC)
	localTimestamp = localTime.Unix()
	localTSO = uint64((localTimestamp << 18) * 1000)
	require.Equal(t, localTSO, ts)

	ts, err = ParseTSString("2021-01-01 01:42:23+08:00", true)
	require.NoError(t, err)
	secondsEastOfUTC := int((8 * time.Hour).Seconds())
	beijing := time.FixedZone("Beijing Time", secondsEastOfUTC)
	localTime = time.Date(2021, time.Month(1), 1, 1, 42, 23, 0, beijing)
	localTimestamp = localTime.Unix()
	localTSO = uint64((localTimestamp << 18) * 1000)
	require.Equal(t, localTSO, ts)
}

func TestParseCompressionType(t *testing.T) {
	var (
		ct  backup.CompressionType
		err error
	)
	ct, err = parseCompressionType("lz4")
	require.NoError(t, err)
	require.Equal(t, 1, int(ct))

	ct, err = parseCompressionType("snappy")
	require.NoError(t, err)
	require.Equal(t, 2, int(ct))

	ct, err = parseCompressionType("zstd")
	require.NoError(t, err)
	require.Equal(t, 3, int(ct))

	ct, err = parseCompressionType("Other Compression (strings)")
	require.Error(t, err)
	require.Regexp(t, "invalid compression.*", err.Error())
	require.Zero(t, ct)
}

func hashCheck(t *testing.T, cfg *BackupConfig, originalHash []byte, check bool) {
	hash, err := cfg.Hash()
	require.NoError(t, err)
	if check {
		require.Equal(t, hash, originalHash)
	} else {
		require.NotEqual(t, hash, originalHash)
	}
}

func TestBackupConfigHash(t *testing.T) {
	cfg := &BackupConfig{
		Config: Config{
			BackendOptions: storage.BackendOptions{
				S3: storage.S3BackendOptions{
					Endpoint: "123",
				},
			},
			Storage:             "storage",
			PD:                  []string{"pd1", "pd2"},
			TLS:                 TLSConfig{},
			RateLimit:           123,
			ChecksumConcurrency: 123,
			Concurrency:         123,
			Checksum:            true,
			SendCreds:           true,
			LogProgress:         true,
			CaseSensitive:       true,
			NoCreds:             true,
			CheckRequirements:   true,
			EnableOpenTracing:   true,
			SkipCheckPath:       true,
			CipherInfo: backup.CipherInfo{
				CipherKey: []byte("123"),
			},
			FilterStr:            []string{"1", "2", "3"},
			SwitchModeInterval:   time.Second,
			GRPCKeepaliveTime:    time.Second,
			GRPCKeepaliveTimeout: time.Second,
			KeyspaceName:         "123",
		},
		TimeAgo:           time.Second,
		BackupTS:          10,
		LastBackupTS:      1,
		GCTTL:             123,
		RemoveSchedulers:  true,
		TableConcurrency:  123,
		IgnoreStats:       true,
		UseBackupMetaV2:   true,
		UseCheckpoint:     true,
		CompressionConfig: CompressionConfig{},
	}

	originalHash, err := cfg.Hash()
	require.NoError(t, err)

	{
		testCfg := *cfg
		testCfg.LastBackupTS = 0
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.UseCheckpoint = false
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.BackendOptions.GCS.Endpoint = "123"
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.Storage = ""
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.PD = testCfg.PD[:1]
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.SendCreds = false
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.NoCreds = false
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.FilterStr = []string{"3", "2", "1"}
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.CipherInfo.CipherKey = nil
		hashCheck(t, &testCfg, originalHash, false)
	}

	{
		testCfg := *cfg
		testCfg.KeyspaceName = "321"
		hashCheck(t, &testCfg, originalHash, false)
	}

	// modifing the configurations is allowed
	{
		testCfg := *cfg
		testCfg.TLS = TLSConfig{CA: "123"}
		testCfg.RateLimit = 321
		testCfg.ChecksumConcurrency = 321
		testCfg.TableConcurrency = 321
		testCfg.Concurrency = 321
		testCfg.Checksum = false
		testCfg.LogProgress = false
		testCfg.CaseSensitive = false
		testCfg.CheckRequirements = false
		testCfg.EnableOpenTracing = false
		testCfg.SkipCheckPath = false
		testCfg.CipherInfo = backup.CipherInfo{
			CipherKey: []byte("123"),
		}
		testCfg.SwitchModeInterval = time.Second * 2
		testCfg.GRPCKeepaliveTime = time.Second * 2
		testCfg.GRPCKeepaliveTimeout = time.Second * 2

		testCfg.TimeAgo = time.Second * 2
		testCfg.BackupTS = 100
		testCfg.GCTTL = 123
		testCfg.RemoveSchedulers = false
		testCfg.UseBackupMetaV2 = false
		testCfg.CompressionConfig = CompressionConfig{CompressionType: 1}
		hashCheck(t, &testCfg, originalHash, true)
	}
}

// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"encoding/hex"
	"fmt"
	"testing"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	kvconfig "github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/config"
	filter "github.com/pingcap/tidb/pkg/util/table-filter"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

type fakeValue string

func (f fakeValue) String() string {
	return string(f)
}

func (f fakeValue) Set(string) error {
	panic("implement me")
}

func (f fakeValue) Type() string {
	panic("implement me")
}

func TestUrlNoQuery(t *testing.T) {
	testCases := []struct {
		inputName     string
		expectedName  string
		inputValue    string
		expectedValue string
	}{
		{
			inputName:     flagSendCreds,
			expectedName:  "send-credentials-to-tikv",
			inputValue:    "true",
			expectedValue: "true",
		},
		{
			inputName:     flagStorage,
			expectedName:  "storage",
			inputValue:    "s3://some/what?secret=a123456789&key=987654321",
			expectedValue: "s3://some/what",
		},
		{
			inputName:     FlagStreamFullBackupStorage,
			expectedName:  "full-backup-storage",
			inputValue:    "s3://bucket/prefix/?access-key=1&secret-key=2",
			expectedValue: "s3://bucket/prefix/",
		},
		{
			inputName:     flagCipherKey,
			expectedName:  "crypter.key",
			inputValue:    "537570657253656372657456616C7565",
			expectedValue: "<redacted>",
		},
		{
			inputName:     "azblob.encryption-key",
			expectedName:  "azblob.encryption-key",
			inputValue:    "SUPERSECRET_AZURE_ENCRYPTION_KEY",
			expectedValue: "<redacted>",
		},
	}

	for _, tc := range testCases {
		flag := pflag.Flag{
			Name:  tc.inputName,
			Value: fakeValue(tc.inputValue),
		}
		field := flagToZapField(&flag)
		require.Equal(t, tc.expectedName, field.Key, `test-case [%s="%s"]`, tc.expectedName, tc.expectedValue)
		if stringer, ok := field.Interface.(fmt.Stringer); ok {
			field.String = stringer.String()
		}
		require.Equal(t, tc.expectedValue, field.String, `test-case [%s="%s"]`, tc.expectedName, tc.expectedValue)
	}
}

func TestTiDBConfigUnchanged(t *testing.T) {
	cfg := config.GetGlobalConfig()
	restoreConfig := tweakLocalConfForRestore()
	require.NotEqual(t, config.GetGlobalConfig(), cfg)
	restoreConfig()
	require.Equal(t, config.GetGlobalConfig(), cfg)
}

func TestStripingPDURL(t *testing.T) {
	nor1, err := normalizePDURL("https://pd:5432", true)
	require.NoError(t, err)
	require.Equal(t, "pd:5432", nor1)
	_, err = normalizePDURL("https://pd.pingcap.com", false)
	require.Error(t, err)
	require.Regexp(t, ".*pd url starts with https while TLS disabled.*", err.Error())
	_, err = normalizePDURL("http://127.0.0.1:2379", true)
	require.Error(t, err)
	require.Regexp(t, ".*pd url starts with http while TLS enabled.*", err.Error())
	nor, err := normalizePDURL("http://127.0.0.1", false)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", nor)
	noChange, err := normalizePDURL("127.0.0.1:2379", false)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:2379", noChange)
}

func TestCheckCipherKeyMatch(t *testing.T) {
	cases := []struct {
		CipherType encryptionpb.EncryptionMethod
		CipherKey  string
		ok         bool
	}{
		{
			CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_UNKNOWN,
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
			CipherKey:  "0123456789abcdef0123456789abcd",
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdefff",
			ok:         false,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			ok:         true,
		},
		{
			CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
			CipherKey:  "",
			ok:         false,
		},
	}

	for _, c := range cases {
		cipherKey, err := hex.DecodeString(c.CipherKey)
		require.NoError(t, err)
		require.Equal(t, c.ok, checkCipherKeyMatch(&backup.CipherInfo{
			CipherType: c.CipherType,
			CipherKey:  cipherKey,
		}))
	}
}

func TestCheckCipherKey(t *testing.T) {
	cases := []struct {
		cipherKey string
		keyFile   string
		ok        bool
	}{
		{
			cipherKey: "0123456789abcdef0123456789abcdef",
			keyFile:   "",
			ok:        true,
		},
		{
			cipherKey: "0123456789abcdef0123456789abcdef",
			keyFile:   "/tmp/abc",
			ok:        false,
		},
		{
			cipherKey: "",
			keyFile:   "/tmp/abc",
			ok:        true,
		},
		{
			cipherKey: "",
			keyFile:   "",
			ok:        false,
		},
	}

	for _, c := range cases {
		err := checkCipherKey(c.cipherKey, c.keyFile)
		if c.ok {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}
	return t
}

func expectedDefaultConfig() Config {
	return Config{
		BackendOptions:            storage.BackendOptions{S3: storage.S3BackendOptions{ForcePathStyle: true}},
		PD:                        []string{"127.0.0.1:2379"},
		ChecksumConcurrency:       4,
		Checksum:                  true,
		SendCreds:                 true,
		CheckRequirements:         true,
		FilterStr:                 []string(nil),
		TableFilter:               filter.CaseInsensitive(must(filter.Parse([]string{"*.*"}))),
		Schemas:                   map[string]struct{}{},
		Tables:                    map[string]struct{}{},
		SwitchModeInterval:        300000000000,
		GRPCKeepaliveTime:         10000000000,
		GRPCKeepaliveTimeout:      3000000000,
		CipherInfo:                backup.CipherInfo{CipherType: 1},
		MetadataDownloadBatchSize: 0x80,
	}
}

func expectedDefaultBackupConfig() BackupConfig {
	defaultConfig := expectedDefaultConfig()
	defaultConfig.Checksum = false
	return BackupConfig{
		Config: defaultConfig,
		GCTTL:  utils.DefaultBRGCSafePointTTL,
		CompressionConfig: CompressionConfig{
			CompressionType: backup.CompressionType_ZSTD,
		},
		IgnoreStats:     true,
		UseBackupMetaV2: true,
		UseCheckpoint:   true,
	}
}

func expectedDefaultRestoreConfig() RestoreConfig {
	defaultConfig := expectedDefaultConfig()
	defaultConfig.Concurrency = defaultRestoreConcurrency
	return RestoreConfig{
		Config: defaultConfig,
		RestoreCommonConfig: RestoreCommonConfig{Online: false,
			Granularity:               "fine-grained",
			MergeSmallRegionSizeBytes: kvconfig.ConfigTerm[uint64]{Value: 0x6000000},
			MergeSmallRegionKeyCount:  kvconfig.ConfigTerm[uint64]{Value: 0xea600},
			WithSysTable:              true,
			ResetSysUsers:             []string{"cloud_admin", "root"}},
		NoSchema:            false,
		LoadStats:           true,
		PDConcurrency:       0x1,
		StatsConcurrency:    0xc,
		BatchFlushInterval:  16000000000,
		DdlBatchSize:        0x80,
		WithPlacementPolicy: "STRICT",
		UseCheckpoint:       true,
	}
}

func TestDefault(t *testing.T) {
	def := DefaultConfig()
	defaultConfig := expectedDefaultConfig()
	require.Equal(t, defaultConfig, def)
}

func TestDefaultBackup(t *testing.T) {
	commonConfig := DefaultConfig()
	commonConfig.OverrideDefaultForBackup()
	def := DefaultBackupConfig(commonConfig)
	defaultConfig := expectedDefaultBackupConfig()
	require.Equal(t, defaultConfig, def)
}

func TestDefaultRestore(t *testing.T) {
	commonConfig := DefaultConfig()
	def := DefaultRestoreConfig(commonConfig)
	defaultConfig := expectedDefaultRestoreConfig()
	require.Equal(t, defaultConfig, def)
}

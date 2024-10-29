// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
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
	flag := &pflag.Flag{
		Name:  flagStorage,
		Value: fakeValue("s3://some/what?secret=a123456789&key=987654321"),
	}
	field := flagToZapField(flag)
	require.Equal(t, flagStorage, field.Key)
	require.Equal(t, "s3://some/what", field.Interface.(fmt.Stringer).String())
}

func TestTiDBConfigUnchanged(t *testing.T) {
	cfg := config.GetGlobalConfig()
	restoreConfig := enableTiDBConfig()
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
		name       string
		cipherInfo *backup.CipherInfo
		expectErr  bool
		errMsg     string
	}{
		{
			name: "PLAINTEXT",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
			},
			expectErr: false,
		},
		{
			name: "UNKNOWN",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_UNKNOWN,
			},
			expectErr: true,
			errMsg:    "Unknown encryption method: UNKNOWN",
		},
		{
			name: "AES128_CTR valid",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
				CipherKey:  make([]byte, crypterAES128KeyLen),
			},
			expectErr: false,
		},
		{
			name: "AES128_CTR invalid length",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES128_CTR,
				CipherKey:  make([]byte, crypterAES128KeyLen-1),
			},
			expectErr: true,
			errMsg:    fmt.Sprintf("AES-128 key length mismatch: expected %d, got %d", crypterAES128KeyLen, crypterAES128KeyLen-1),
		},
		{
			name: "AES192_CTR valid",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
				CipherKey:  make([]byte, crypterAES192KeyLen),
			},
			expectErr: false,
		},
		{
			name: "AES192_CTR invalid length",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES192_CTR,
				CipherKey:  make([]byte, crypterAES192KeyLen+1),
			},
			expectErr: true,
			errMsg:    fmt.Sprintf("AES-192 key length mismatch: expected %d, got %d", crypterAES192KeyLen, crypterAES192KeyLen+1),
		},
		{
			name: "AES256_CTR valid",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
				CipherKey:  make([]byte, crypterAES256KeyLen),
			},
			expectErr: false,
		},
		{
			name: "AES256_CTR invalid length",
			cipherInfo: &backup.CipherInfo{
				CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
				CipherKey:  make([]byte, 0),
			},
			expectErr: true,
			errMsg:    fmt.Sprintf("AES-256 key length mismatch: expected %d, got %d", crypterAES256KeyLen, 0),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := checkCipherKeyMatch(c.cipherInfo)
			if c.expectErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), c.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
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

func TestGetCipherKey(t *testing.T) {
	nonHexKey := "this is not a hex string"
	_, err := GetCipherKeyContent(nonHexKey, "")
	require.Error(t, err)
	require.Contains(t, err.Error(), cipherKeyNonHexErrorMsg)
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
		LogBackupCipherInfo:       backup.CipherInfo{CipherType: 1},
		MetadataDownloadBatchSize: 0x80,
	}
}

func expectedDefaultBackupConfig() BackupConfig {
	return BackupConfig{
		Config: expectedDefaultConfig(),
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
	def := DefaultBackupConfig()
	defaultConfig := expectedDefaultBackupConfig()
	require.Equal(t, defaultConfig, def)
}

func TestDefaultRestore(t *testing.T) {
	def := DefaultRestoreConfig()
	defaultConfig := expectedDefaultRestoreConfig()
	require.Equal(t, defaultConfig, def)
}

func TestParseAndValidateMasterKeyInfo(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		expectedKeys []*encryptionpb.MasterKey
		expectError  bool
	}{
		{
			name:         "Empty input",
			input:        "",
			expectedKeys: nil,
			expectError:  false,
		},
		{
			name:  "Single local config",
			input: "local:///path/to/key",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_File{
						File: &encryptionpb.MasterKeyFile{Path: "/path/to/key"},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "Single AWS config",
			input: "aws-kms:///key-id?AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE&AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY&REGION=us-west-2",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_Kms{
						Kms: &encryptionpb.MasterKeyKms{
							Vendor: "aws",
							KeyId:  "key-id",
							Region: "us-west-2",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "Single Azure config",
			input: "azure-kms:///key-name/key-version?AZURE_TENANT_ID=tenant-id&AZURE_CLIENT_ID=client-id&AZURE_CLIENT_SECRET=client-secret&AZURE_VAULT_NAME=vault-name",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_Kms{
						Kms: &encryptionpb.MasterKeyKms{
							Vendor: "azure",
							KeyId:  "key-name/key-version",
							AzureKms: &encryptionpb.AzureKms{
								TenantId:     "tenant-id",
								ClientId:     "client-id",
								ClientSecret: "client-secret",
								KeyVaultUrl:  "vault-name",
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:  "Single GCP config",
			input: "gcp-kms:///projects/project-id/locations/global/keyRings/ring-name/cryptoKeys/key-name?CREDENTIALS=credentials",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_Kms{
						Kms: &encryptionpb.MasterKeyKms{
							Vendor: "gcp",
							KeyId:  "projects/project-id/locations/global/keyRings/ring-name/cryptoKeys/key-name",
							GcpKms: &encryptionpb.GcpKms{
								Credential: "credentials",
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Multiple configs",
			input: "local:///path/to/key," +
				"aws-kms:///key-id?AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE&AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY&REGION=us-west-2",
			expectedKeys: []*encryptionpb.MasterKey{
				{
					Backend: &encryptionpb.MasterKey_File{
						File: &encryptionpb.MasterKeyFile{Path: "/path/to/key"},
					},
				},
				{
					Backend: &encryptionpb.MasterKey_Kms{
						Kms: &encryptionpb.MasterKeyKms{
							Vendor: "aws",
							KeyId:  "key-id",
							Region: "us-west-2",
						},
					},
				},
			},
			expectError: false,
		},
		{
			name:         "Invalid config",
			input:        "invalid:///config",
			expectedKeys: nil,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{}
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			flags.String(flagMasterKeyConfig, tt.input, "")
			flags.String(flagMasterKeyCipherType, "aes256-ctr", "")

			err := cfg.parseAndValidateMasterKeyInfo(false, flags)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedKeys, cfg.MasterKeyConfig.MasterKeys)
			}
		})
	}
}

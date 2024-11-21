// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package task

import (
	"encoding/hex"
	"fmt"
	"testing"

	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/config"
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
			inputName:     flagFullBackupCipherKey,
			expectedName:  "crypter.key",
			inputValue:    "537570657253656372657456616C7565",
			expectedValue: "<redacted>",
		},
		{
			inputName:     flagLogBackupCipherKey,
			expectedName:  "log.crypter.key",
			inputValue:    "537570657253656372657456616C7565",
			expectedValue: "<redacted>",
		},
		{
			inputName:     "azblob.encryption-key",
			expectedName:  "azblob.encryption-key",
			inputValue:    "SUPERSECRET_AZURE_ENCRYPTION_KEY",
			expectedValue: "<redacted>",
		},
		{
			inputName:     flagMasterKeyConfig,
			expectedName:  "master-key",
			inputValue:    "local:///path/abcd,aws-kms:///abcd?AWS_ACCESS_KEY_ID=SECRET1&AWS_SECRET_ACCESS_KEY=SECRET2&REGION=us-east-1,azure-kms:///abcd/v1?AZURE_TENANT_ID=tenant-id&AZURE_CLIENT_ID=client-id&AZURE_CLIENT_SECRET=client-secret&AZURE_VAULT_NAME=vault-name",
			expectedValue: "<redacted>",
			// expectedValue: "local:///path/abcd,aws-kms:///abcd,azure-kms:///abcd/v1"
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

// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redact

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type testStringer struct {
	str string
}

func (s *testStringer) String() string {
	return s.str
}

func TestRedact(t *testing.T) {
	for _, c := range []struct {
		mode   string
		input  string
		output string
	}{
		{"OFF", "fxcv", "fxcv"},
		{"OFF", "f‹xcv", "f‹xcv"},
		{"ON", "f‹xcv", ""},
		{"MARKER", "f‹xcv", "‹f‹‹xcv›"},
		{"MARKER", "f›xcv", "‹f››xcv›"},
	} {
		require.Equal(t, c.output, String(c.mode, c.input))
		require.Equal(t, c.output, Stringer(c.mode, &testStringer{c.input}).String())
	}
}

func TestDeRedact(t *testing.T) {
	for _, c := range []struct {
		remove bool
		input  string
		output string
	}{
		{true, "‹fxcv›ggg", "?ggg"},
		{false, "‹fxcv›ggg", "fxcvggg"},
		{true, "fxcv", "fxcv"},
		{false, "fxcv", "fxcv"},
		{true, "‹fxcv›ggg‹fxcv›eee", "?ggg?eee"},
		{false, "‹fxcv›ggg‹fxcv›eee", "fxcvgggfxcveee"},
		{true, "‹›", "?"},
		{false, "‹›", ""},
		{true, "gg‹ee", "gg‹ee"},
		{false, "gg‹ee", "gg‹ee"},
		{true, "gg›ee", "gg›ee"},
		{false, "gg›ee", "gg›ee"},
		{true, "gg‹ee‹ee", "gg‹ee‹ee"},
		{false, "gg‹ee‹gg", "gg‹ee‹gg"},
		{true, "gg›ee›gg", "gg›ee›gg"},
		{false, "gg›ee›ee", "gg›ee›ee"},
	} {
		w := bytes.NewBuffer(nil)
		require.NoError(t, DeRedact(c.remove, strings.NewReader(c.input), w, ""))
		require.Equal(t, c.output, w.String())
	}
}

func TestRedactInitAndValueAndKey(t *testing.T) {
	redacted, secret := "?", "secret"

	InitRedact(false)
	require.Equal(t, Value(secret), secret)
	require.Equal(t, Key([]byte(secret)), hex.EncodeToString([]byte(secret)))

	InitRedact(true)
	require.Equal(t, Value(secret), redacted)
	require.Equal(t, Key([]byte(secret)), redacted)
}

func TestTaskInfoS3CredentialsRedacted(t *testing.T) {
	for _, tc := range []struct{ name, endpoint, expected string }{
		{"signed", "https://FAKE_USER:FAKE_PASS@host/path?sig=FAKE_SIGNATURE#FAKE_FRAGMENT", "https://host/path"},
		{"malformed", "https://FAKE_USER:FAKE_PASS@host/path%zz?sig=FAKE_SIGNATURE", "(invalid URL)"},
	} {
		for _, backend := range []struct {
			name    string
			storage *backup.StorageBackend
		}{
			{"s3", &backup.StorageBackend{Backend: &backup.StorageBackend_S3{S3: &backup.S3{Endpoint: tc.endpoint}}}},
			{"gcs", &backup.StorageBackend{Backend: &backup.StorageBackend_Gcs{Gcs: &backup.GCS{Endpoint: tc.endpoint}}}},
			{"azure", &backup.StorageBackend{Backend: &backup.StorageBackend_AzureBlobStorage{AzureBlobStorage: &backup.AzureBlobStorage{Endpoint: tc.endpoint}}}},
		} {
			t.Run(backend.name+"/"+tc.name, func(t *testing.T) {
				info := &backup.StreamBackupTaskInfo{Storage: backend.storage}
				original := info.String()

				output := (TaskInfoRedacted{Info: info}).String()

				require.Equal(t, original, info.String(), "logging must not mutate task endpoints")
				for _, secret := range []string{"FAKE_USER", "FAKE_PASS", "FAKE_SIGNATURE", "FAKE_FRAGMENT"} {
					require.NotContains(t, output, secret)
				}
				require.Contains(t, output, `endpoint:"`+tc.expected+`"`)
			})
		}
	}
	s3 := &backup.S3{
		Bucket:                  "backup-bucket",
		SessionToken:            "session-token-value",
		SseKmsEncryptionContext: "eyJ0ZW5hbnQiOiJjb250ZXh0LXZhbHVlIn0=",
	}
	info := &backup.StreamBackupTaskInfo{
		Name:    "backup-task",
		Storage: &backup.StorageBackend{Backend: &backup.StorageBackend_S3{S3: s3}},
	}
	original := info.String()
	redacted := (TaskInfoRedacted{Info: info}).String()
	require.NotContains(t, redacted, s3.SessionToken)
	require.NotContains(t, redacted, s3.SseKmsEncryptionContext)
	require.Contains(t, redacted, s3.Bucket)
	require.Contains(t, redacted, info.Name)
	require.Equal(t, original, info.String())
}

func TestTaskInfoEncryptionCredentialsRedacted(t *testing.T) {
	previous := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(previous) })
	for _, mode := range []struct {
		name    string
		enabled bool
	}{{"off", false}, {"on", true}} {
		InitRedact(mode.enabled)
		for _, tc := range []struct {
			name   string
			config *backup.StreamBackupTaskSecurityConfig
		}{
			{name: "unencrypted"},
			{
				name: "plaintext-data-key",
				config: &backup.StreamBackupTaskSecurityConfig{
					Encryption: &backup.StreamBackupTaskSecurityConfig_PlaintextDataKey{
						PlaintextDataKey: &backup.CipherInfo{CipherKey: []byte("FAKE_DATA_KEY_01234567890123456789")},
					},
				},
			},
			{
				name: "master-keys",
				config: &backup.StreamBackupTaskSecurityConfig{
					Encryption: &backup.StreamBackupTaskSecurityConfig_MasterKeyConfig{
						MasterKeyConfig: &backup.MasterKeyConfig{MasterKeys: []*encryptionpb.MasterKey{
							{Backend: &encryptionpb.MasterKey_Kms{Kms: &encryptionpb.MasterKeyKms{
								Vendor: "aws", AwsKms: &encryptionpb.AwsKms{AccessKey: "FAKE_KMS_ACCESS", SecretAccessKey: "FAKE_KMS_SECRET"},
							}}},
							{Backend: &encryptionpb.MasterKey_Kms{Kms: &encryptionpb.MasterKeyKms{
								Vendor: "azure", AzureKms: &encryptionpb.AzureKms{
									ClientSecret: "FAKE_AZURE_SECRET", ClientCertificate: "FAKE_CERTIFICATE", ClientCertificatePassword: "FAKE_CERT_PASSWORD",
								},
							}}},
							{Backend: &encryptionpb.MasterKey_Kms{Kms: &encryptionpb.MasterKeyKms{
								Vendor: "gcp", GcpKms: &encryptionpb.GcpKms{Credential: "FAKE_GCP_CREDENTIAL"},
							}}},
						}},
					},
				},
			},
		} {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				// Given an encrypted task and the actual advancer INFO field.
				info := &backup.StreamBackupTaskInfo{
					Name: "backup-task", SecurityConfig: tc.config,
					Storage: &backup.StorageBackend{Backend: &backup.StorageBackend_S3{S3: &backup.S3{
						Bucket: "backup-bucket", SessionToken: "FAKE_SESSION_TOKEN",
					}}},
				}
				original, err := info.Marshal()
				require.NoError(t, err)
				var output bytes.Buffer
				logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&output), zap.InfoLevel))

				// When emitting the same stringer field as the added-event log.
				logger.Info("added event", zap.Stringer("task", TaskInfoRedacted{Info: info}))

				// Then diagnostic metadata survives, but security configuration does not.
				for _, secret := range []string{
					"FAKE_DATA_KEY", "FAKE_KMS_ACCESS", "FAKE_KMS_SECRET", "FAKE_AZURE_SECRET",
					"FAKE_CERTIFICATE", "FAKE_CERT_PASSWORD", "FAKE_GCP_CREDENTIAL", "FAKE_SESSION_TOKEN",
				} {
					require.NotContains(t, output.String(), secret)
				}
				require.NotContains(t, output.String(), "security_config")
				require.Contains(t, output.String(), info.Name)
				require.Contains(t, output.String(), "backup-bucket")
				after, err := info.Marshal()
				require.NoError(t, err)
				require.Equal(t, original, after, "logging must not mutate encryption configuration")
			})
		}
	}
}

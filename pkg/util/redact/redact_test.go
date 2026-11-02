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

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	backup "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
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

func TestRedactLockInfo(t *testing.T) {
	previousMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(previousMode) })
	lock := &kvrpcpb.LockInfo{
		PrimaryLock: []byte("secret-primary"),
		LockVersion: 42,
		Key:         []byte("secret-key"),
		LockTtl:     3000,
		Secondaries: [][]byte{[]byte("secret-secondary")},
		SharedLockInfos: []*kvrpcpb.LockInfo{
			{PrimaryLock: []byte("secret-shared-primary"), Key: []byte("secret-shared-key"), LockVersion: 43},
		},
	}

	InitRedact(false)
	require.Equal(t, lock.String(), LockInfo(lock).String())

	InitRedact(true)
	redacted := LockInfo(lock).String()
	require.NotContains(t, redacted, "secret")
	require.Contains(t, redacted, "lock_version:42")
	require.Contains(t, redacted, "lock_ttl:3000")
	require.Contains(t, redacted, "lock_version:43")
	// the original lock must not be modified.
	require.Equal(t, []byte("secret-primary"), lock.PrimaryLock)
	require.Equal(t, []byte("secret-key"), lock.Key)
	require.Equal(t, []byte("secret-secondary"), lock.Secondaries[0])
	require.Equal(t, []byte("secret-shared-key"), lock.SharedLockInfos[0].Key)

	var nilLock *kvrpcpb.LockInfo
	require.Equal(t, nilLock.String(), LockInfo(nilLock).String())

	for _, test := range []struct {
		globalMode string
		mode       string
		masked     bool
	}{
		{globalMode: "OFF", mode: "", masked: false},
		{globalMode: "OFF", mode: "OFF", masked: false},
		{globalMode: "OFF", mode: "ON", masked: true},
		{globalMode: "OFF", mode: "MARKER", masked: true},
		{globalMode: "ON", mode: "", masked: true},
		{globalMode: "ON", mode: "OFF", masked: true},
		{globalMode: "ON", mode: "ON", masked: true},
		{globalMode: "ON", mode: "MARKER", masked: true},
		{globalMode: "MARKER", mode: "", masked: true},
		{globalMode: "MARKER", mode: "OFF", masked: true},
		{globalMode: "MARKER", mode: "ON", masked: true},
		{globalMode: "MARKER", mode: "MARKER", masked: true},
	} {
		t.Run("global="+test.globalMode+"/session="+test.mode, func(t *testing.T) {
			errors.RedactLogEnabled.Store(test.globalMode)
			formatted := LockInfoWithMode(lock, test.mode).String()
			if test.masked {
				require.Equal(t, redacted, formatted)
				require.NotContains(t, formatted, "secret")
			} else {
				require.Equal(t, lock.String(), formatted)
			}
			require.Equal(t, nilLock.String(), LockInfoWithMode(nilLock, test.mode).String())
			require.Equal(t, []byte("secret-primary"), lock.PrimaryLock)
			require.Equal(t, []byte("secret-key"), lock.Key)
			require.Equal(t, []byte("secret-secondary"), lock.Secondaries[0])
			require.Equal(t, []byte("secret-shared-primary"), lock.SharedLockInfos[0].PrimaryLock)
			require.Equal(t, []byte("secret-shared-key"), lock.SharedLockInfos[0].Key)
		})
	}

	// Formatting remains lazy and checks the current process-global setting.
	InitRedact(false)
	formatted := LockInfoWithMode(lock, "OFF")
	require.Equal(t, lock.String(), formatted.String())
	InitRedact(true)
	require.Equal(t, redacted, formatted.String())
	InitRedact(false)
	require.Equal(t, lock.String(), formatted.String())
}

func TestRedactRegionError(t *testing.T) {
	previousMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(previousMode) })
	regionErr := &errorpb.Error{
		Message: "secret-message",
		NotLeader: &errorpb.NotLeader{
			RegionId: 1,
			Leader:   &metapb.Peer{Id: 11, StoreId: 21, Role: metapb.PeerRole_Learner},
		},
		EpochNotMatch: &errorpb.EpochNotMatch{
			CurrentRegions: []*metapb.Region{
				{
					Id:             2,
					StartKey:       []byte("secret-start"),
					EndKey:         []byte("secret-end"),
					RegionEpoch:    &metapb.RegionEpoch{ConfVer: 3, Version: 7},
					Peers:          []*metapb.Peer{{Id: 12, StoreId: 22, IsWitness: true}},
					EncryptionMeta: &encryptionpb.EncryptionMeta{KeyId: 31, Iv: []byte("secret-iv")},
				},
				{Id: 3},
			},
		},
		KeyNotInRegion: &errorpb.KeyNotInRegion{
			Key: []byte("secret-key"), RegionId: 4,
			StartKey: []byte("secret-key-start"), EndKey: []byte("secret-key-end"),
		},
		BucketVersionNotMatch: &errorpb.BucketVersionNotMatch{Version: 5, Keys: [][]byte{[]byte("secret-bucket"), {}}},
		ServerIsBusy:          &errorpb.ServerIsBusy{Reason: "raftstore is busy", BackoffMs: 10, EstimatedWaitMs: 20, AppliedIndex: 30},
		ReadIndexNotReady:     &errorpb.ReadIndexNotReady{Reason: "read index not ready", RegionId: 6},
		DiskFull:              &errorpb.DiskFull{StoreId: []uint64{23, 24}, Reason: "disk is full"},
		UndeterminedResult:    &errorpb.UndeterminedResult{Message: "result is undetermined"},
		IncompatibleRequest: &errorpb.IncompatibleRequest{
			Reason:  errorpb.IncompatibleRequestReason_IncompatibleRequestReasonTxnProtocolVersionOutOfRange,
			Message: "transaction protocol version is incompatible", ProvidedTxnProtocolVersion: 3,
			MinCompatibleTxnProtocolVersion: 1, MaxCompatibleTxnProtocolVersion: 2,
		},
		DataIsNotReady: &errorpb.DataIsNotReady{RegionId: 7, PeerId: 13, SafeTs: 42},
	}
	original := proto.Clone(regionErr)

	for _, mode := range []string{"OFF", "ON", "MARKER"} {
		t.Run(mode, func(t *testing.T) {
			errors.RedactLogEnabled.Store(mode)
			var nilErr *errorpb.Error
			require.Equal(t, nilErr.String(), RegionError(nilErr))
			formatted := RegionError(regionErr)
			if mode == "OFF" {
				require.Equal(t, regionErr.String(), formatted)
				require.Equal(t, "", RegionError(&errorpb.Error{}))
				return
			}

			require.NotContains(t, formatted, "secret")
			require.Equal(t, "?", RegionError(&errorpb.Error{}))
			require.True(t, strings.HasPrefix(formatted, `message:"?" `))
			region := regionErr.EpochNotMatch.CurrentRegions[0]
			for field, detail := range map[string]proto.Message{
				"not_leader": regionErr.NotLeader,
				"epoch_not_match": &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{
					{
						Id: region.Id, StartKey: []byte("?"), EndKey: []byte("?"),
						RegionEpoch: region.RegionEpoch, Peers: region.Peers,
						EncryptionMeta: &encryptionpb.EncryptionMeta{KeyId: 31, Iv: []byte("?")},
					},
					{Id: 3},
				}},
				"key_not_in_region": &errorpb.KeyNotInRegion{
					Key: []byte("?"), RegionId: 4, StartKey: []byte("?"), EndKey: []byte("?"),
				},
				"bucket_version_not_match": &errorpb.BucketVersionNotMatch{Version: 5, Keys: [][]byte{[]byte("?"), {}}},
				"server_is_busy":           regionErr.ServerIsBusy,
				"read_index_not_ready":     regionErr.ReadIndexNotReady,
				"disk_full":                regionErr.DiskFull,
				"undetermined_result":      regionErr.UndeterminedResult,
				"incompatible_request":     regionErr.IncompatibleRequest,
				"data_is_not_ready":        regionErr.DataIsNotReady,
			} {
				require.Contains(t, formatted, field+":<"+detail.String()+">", field)
			}
			require.Equal(t, original, regionErr)
			require.NotPanics(t, func() {
				RegionError(&errorpb.Error{EpochNotMatch: &errorpb.EpochNotMatch{CurrentRegions: []*metapb.Region{nil}}})
			})
		})
	}
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

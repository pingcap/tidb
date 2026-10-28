// Copyright 2026 PingCAP, Inc.
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

package task

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/gogo/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestCheckSnapshotRestoreMode verifies that --rename is only rejected for raw
// and incremental snapshot restores, and accepted for a full snapshot restore.
func TestCheckSnapshotRestoreMode(t *testing.T) {
	testCheck := func(t *testing.T, meta *backuppb.BackupMeta, rename []string) error {
		ctx := context.Background()
		store, err := storage.NewLocalStorage(t.TempDir())
		require.NoError(t, err)

		meta.BackupSchemaVersion = backuppb.BackupSchemaVersion
		data, err := proto.Marshal(meta)
		require.NoError(t, err)
		require.NoError(t, store.WriteFile(ctx, metautil.MetaFile, data))

		cfg := &RestoreConfig{
			Config: Config{
				Storage: "local://" + store.URI(),
				CipherInfo: backuppb.CipherInfo{
					CipherType: encryptionpb.EncryptionMethod_PLAINTEXT,
				},
			},
			Rename: rename,
		}
		return checkSnapshotRestoreMode(ctx, cfg)
	}

	t.Run("reject raw backup", func(t *testing.T) {
		err := testCheck(t, &backuppb.BackupMeta{IsRawKv: true}, nil)
		require.ErrorContains(t, err, "restore mode mismatch")
	})

	t.Run("allow full snapshot rename", func(t *testing.T) {
		err := testCheck(t, &backuppb.BackupMeta{StartVersion: 0, EndVersion: 2}, []string{"source:target"})
		require.NoError(t, err)
	})

	t.Run("reject incremental snapshot rename", func(t *testing.T) {
		err := testCheck(t, &backuppb.BackupMeta{StartVersion: 1, EndVersion: 2}, []string{"source:target"})
		require.ErrorContains(t, err, "--rename is not supported for incremental snapshot restore")
	})
}

func TestCheckpointStorageLogRedaction(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	restoreLogger := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{})
	defer restoreLogger()
	contextJSON := `{"tenant":"context-value"}`
	cfg := &RestoreConfig{
		// A missing bucket stops the test before any storage request.
		CheckpointStorage: "s3:///?sse-kms-context=" + url.QueryEscape(contextJSON),
	}
	require.Error(t, cfg.newStorageCheckpointMetaManagerPITR(context.Background(), 1, 2))
	entries := logs.FilterMessage("creating storage checkpoint meta managers for PiTR").All()
	require.Len(t, entries, 1)
	logged, err := json.Marshal(entries[0].ContextMap())
	require.NoError(t, err)
	require.NotContains(t, string(logged), "context-value")
	require.Contains(t, cfg.CheckpointStorage, "context-value")
}

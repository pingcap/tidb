// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestPhysicalRestoreSysTables(t *testing.T) {
	usePhysicalCfg := &SnapshotRestoreConfig{RestoreConfig: &RestoreConfig{
		FastLoadSysTables: true,
		RestoreCommonConfig: RestoreCommonConfig{
			WithSysTable: true,
		},
		LoadStats: true,
	}}
	loadSysTablePhysical, loadStatsPhysical := isRestoreSysTablesPhysically(usePhysicalCfg)
	// we only test how it works on next-gen, won't test other combinations
	if kerneltype.IsNextGen() {
		require.False(t, loadSysTablePhysical)
		require.False(t, loadStatsPhysical)
	} else {
		require.True(t, loadSysTablePhysical)
		require.True(t, loadStatsPhysical)
	}
}

func TestRestorePhaseRequiresCheckpoint(t *testing.T) {
	flags := pflag.NewFlagSet("restore", pflag.ContinueOnError)
	DefineRestoreFlags(flags)
	require.NoError(t, flags.Set(FlagRestorePhase, "1"))
	require.NoError(t, flags.Set(flagUseCheckpoint, "false"))

	cfg := &RestoreConfig{}
	err := cfg.ParseFromFlags(flags, true)
	require.Error(t, err)
	require.ErrorContains(t, err, FlagRestorePhase)
	require.ErrorContains(t, err, flagUseCheckpoint)
}

func TestSplitRegionIndexStepFlag(t *testing.T) {
	t.Run("coarse scatter", func(t *testing.T) {
		flags := pflag.NewFlagSet("restore", pflag.ContinueOnError)
		DefineRestoreFlags(flags)
		require.NoError(t, flags.Set("coarse-scatter", "true"))

		cfg := &RestoreConfig{}
		require.NoError(t, cfg.ParseFromFlags(flags, true))
		require.True(t, cfg.CoarseScatter)
	})

	t.Run("custom", func(t *testing.T) {
		flags := pflag.NewFlagSet("restore", pflag.ContinueOnError)
		DefineRestoreFlags(flags)
		require.NoError(t, flags.Set(FlagSplitRegionIndexStep, "64"))

		cfg := &RestoreConfig{}
		require.NoError(t, cfg.ParseFromFlags(flags, true))
		require.Equal(t, uint(64), cfg.SplitRegionIndexStep)
	})

	t.Run("zero", func(t *testing.T) {
		flags := pflag.NewFlagSet("restore", pflag.ContinueOnError)
		DefineRestoreFlags(flags)
		require.NoError(t, flags.Set(FlagSplitRegionIndexStep, "0"))

		cfg := &RestoreConfig{}
		err := cfg.ParseFromFlags(flags, true)
		require.Error(t, err)
		require.ErrorContains(t, err, FlagSplitRegionIndexStep)
		require.ErrorContains(t, err, "greater than 0")
	})
}

func TestCheckSnapshotRestoreModeRejectsRawBackup(t *testing.T) {
	ctx := context.Background()
	store, err := objstore.NewLocalStorage(t.TempDir())
	require.NoError(t, err)

	meta := &backuppb.BackupMeta{
		IsRawKv:             true,
		BackupSchemaVersion: backuppb.BackupSchemaVersion,
	}
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
	}

	err = checkSnapshotRestoreMode(ctx, cfg)
	require.Error(t, err)
	require.ErrorContains(t, err, "restore mode mismatch")
}

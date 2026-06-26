// Copyright 2022 PingCAP, Inc.
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
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestShiftTS(t *testing.T) {
	var startTS uint64 = 433155751280640000
	shiftTS := ShiftTS(startTS)
	require.Equal(t, true, shiftTS < startTS)

	delta := oracle.GetTimeFromTS(startTS).Sub(oracle.GetTimeFromTS(shiftTS))
	require.Equal(t, delta, streamShiftDuration)
}

func TestShouldOpenPiTRAddIndexSQLStorage(t *testing.T) {
	tests := []struct {
		name string
		cfg  RestoreConfig
		want bool
	}{
		{
			name: "empty storage",
			cfg:  RestoreConfig{},
			want: false,
		},
		{
			name: "full flow opens storage",
			cfg: RestoreConfig{
				PiTRAddIndexSQLStorage: "local:///tmp/pitr-add-index",
			},
			want: true,
		},
		{
			name: "phase 1 does not open storage",
			cfg: RestoreConfig{
				PiTRAddIndexSQLStorage: "local:///tmp/pitr-add-index",
				RestorePhase:           1,
			},
			want: false,
		},
		{
			name: "phase 2 opens storage",
			cfg: RestoreConfig{
				PiTRAddIndexSQLStorage: "local:///tmp/pitr-add-index",
				RestorePhase:           2,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldOpenPiTRAddIndexSQLStorage(&tt.cfg))
		})
	}
}

func TestCheckLogRange(t *testing.T) {
	cases := []struct {
		restoreFrom uint64
		restoreTo   uint64
		logMinTS    uint64
		logMaxTS    uint64
		result      bool
	}{
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   99,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 1,
			restoreTo:   99,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   10,
			logMaxTS:    100,
			result:      true,
		},
		{
			logMinTS:    11,
			restoreFrom: 10,
			restoreTo:   99,
			logMaxTS:    100,
			result:      false,
		},
		{
			logMinTS:    1,
			restoreFrom: 10,
			restoreTo:   9,
			logMaxTS:    100,
			result:      false,
		},
		{
			logMinTS:    1,
			restoreFrom: 9,
			restoreTo:   99,
			logMaxTS:    99,
			result:      true,
		},
		{
			logMinTS:    1,
			restoreFrom: 9,
			restoreTo:   99,
			logMaxTS:    98,
			result:      false,
		},
	}

	for _, c := range cases {
		err := checkLogRange(c.restoreFrom, c.restoreTo, c.logMinTS, c.logMaxTS)
		if c.result {
			require.Nil(t, err)
		} else {
			require.NotNil(t, err)
		}
	}
}

func fakeCheckpointFiles(
	ctx context.Context,
	tmpDir string,
	infos []fakeGlobalCheckPoint,
) error {
	cpDir := filepath.Join(tmpDir, stream.GetStreamBackupGlobalCheckpointPrefix())
	s, err := storage.NewLocalStorage(cpDir)
	if err != nil {
		return errors.Trace(err)
	}

	// create normal files belong to global-checkpoint files
	for _, info := range infos {
		filename := fmt.Sprintf("%v.ts", info.storeID)
		buff := make([]byte, 8)
		binary.LittleEndian.PutUint64(buff, info.globalCheckpoint)
		if _, err := s.Create(ctx, filename, nil); err != nil {
			return errors.Trace(err)
		}
		if err := s.WriteFile(ctx, filename, buff); err != nil {
			return errors.Trace(err)
		}
	}

	// create a file not belonging to global-checkpoint-ts files
	filename := fmt.Sprintf("%v.tst", 1)
	err = s.WriteFile(ctx, filename, []byte("ping"))
	return errors.AddStack(err)
}

type fakeGlobalCheckPoint struct {
	storeID          int64
	globalCheckpoint uint64
}

func TestGetGlobalCheckpointFromStorage(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpdir)
	require.Nil(t, err)

	infos := []fakeGlobalCheckPoint{
		{
			storeID:          1,
			globalCheckpoint: 98,
		},
		{
			storeID:          2,
			globalCheckpoint: 90,
		},
		{
			storeID:          2,
			globalCheckpoint: 99,
		},
	}

	err = fakeCheckpointFiles(ctx, tmpdir, infos)
	require.Nil(t, err)

	ts, err := getGlobalCheckpointFromStorage(ctx, s)
	require.Nil(t, err)
	require.Equal(t, ts, uint64(99))
}

func TestHasAnyWriteCFLogFile(t *testing.T) {
	makeLogFile := func(cf string) *logclient.LogDataFileInfo {
		return &logclient.LogDataFileInfo{
			DataFileInfo: &backuppb.DataFileInfo{
				Cf: cf,
			},
		}
	}
	defaultFile := makeLogFile(stream.DefaultCF)
	writeFile := makeLogFile(stream.WriteCF)

	cases := []struct {
		name  string
		files []*logclient.LogDataFileInfo
		want  *logclient.LogDataFileInfo
	}{
		{
			name: "empty",
		},
		{
			name:  "default only",
			files: []*logclient.LogDataFileInfo{defaultFile},
		},
		{
			name: "ignore default before write",
			files: []*logclient.LogDataFileInfo{
				defaultFile,
				writeFile,
			},
			want: writeFile,
		},
		{
			name: "write only",
			files: []*logclient.LogDataFileInfo{
				writeFile,
			},
			want: writeFile,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			file, err := hasAnyWriteCFLogFile(context.Background(), iter.FromSlice(c.files))
			require.NoError(t, err)
			require.Same(t, c.want, file)
		})
	}

	_, err := hasAnyWriteCFLogFile(context.Background(), iter.Fail[*logclient.LogDataFileInfo](errors.New("failed to read log file")))
	require.Error(t, err)
}

func TestGetMaxRecoverableCheckpointFromStoragePrefersResumeState(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpdir)
	require.Nil(t, err)

	err = fakeCheckpointFiles(ctx, tmpdir, []fakeGlobalCheckPoint{
		{
			storeID:          1,
			globalCheckpoint: 99,
		},
	})
	require.Nil(t, err)

	err = s.WriteFile(ctx, resumeStateFileName, []byte(`{"last_checkpoint":88}`))
	require.Nil(t, err)

	ts, err := getMaxRecoverableCheckpointFromStorage(ctx, s)
	require.Nil(t, err)
	require.Equal(t, uint64(88), ts)
}

func TestGetMaxRecoverableCheckpointFromStorageFallbackToGlobalCheckpoint(t *testing.T) {
	ctx := context.Background()
	tmpdir := t.TempDir()
	s, err := storage.NewLocalStorage(tmpdir)
	require.Nil(t, err)

	err = fakeCheckpointFiles(ctx, tmpdir, []fakeGlobalCheckPoint{
		{
			storeID:          1,
			globalCheckpoint: 98,
		},
		{
			storeID:          2,
			globalCheckpoint: 99,
		},
	})
	require.Nil(t, err)

	ts, err := getMaxRecoverableCheckpointFromStorage(ctx, s)
	require.Nil(t, err)
	require.Equal(t, uint64(99), ts)
}

func TestGetLogRangeWithFullBackupDir(t *testing.T) {
	var fullBackupTS uint64 = 123456
	testDir := t.TempDir()
	storage, err := storage.NewLocalStorage(testDir)
	require.Nil(t, err)

	m := backuppb.BackupMeta{
		EndVersion: fullBackupTS,
	}
	data, err := proto.Marshal(&m)
	require.Nil(t, err)

	err = storage.WriteFile(context.TODO(), metautil.MetaFile, data)
	require.Nil(t, err)

	cfg := Config{
		Storage: testDir,
	}
	_, err = getLogRange(context.TODO(), &cfg)
	require.Error(t, err, errors.Annotate(berrors.ErrStorageUnknown,
		"the storage has been used for full backup"))
}

func TestGetLogRangeWithLogBackupDir(t *testing.T) {
	var startLogBackupTS uint64 = 123456
	testDir := t.TempDir()
	storage, err := storage.NewLocalStorage(testDir)
	require.Nil(t, err)

	m := backuppb.BackupMeta{
		StartVersion: startLogBackupTS,
	}
	data, err := proto.Marshal(&m)
	require.Nil(t, err)

	err = storage.WriteFile(context.TODO(), metautil.MetaFile, data)
	require.Nil(t, err)

	cfg := Config{
		Storage: testDir,
	}
	logInfo, err := getLogRange(context.TODO(), &cfg)
	require.Nil(t, err)
	require.Equal(t, logInfo.logMinTS, startLogBackupTS)
}

func TestGetExternalStorageOptions(t *testing.T) {
	cfg := Config{}
	u, err := storage.ParseBackend("s3://bucket/path", nil)
	require.NoError(t, err)
	options := getExternalStorageOptions(&cfg, u)
	require.NotNil(t, options.HTTPClient)

	u, err = storage.ParseBackend("gs://bucket/path", nil)
	require.NoError(t, err)
	options = getExternalStorageOptions(&cfg, u)
	require.Nil(t, options.HTTPClient)
}

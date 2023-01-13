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

package checkpoint_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pingcap/tidb/br/pkg/checkpoint"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestCheckpointMeta(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)

	checkpointMeta := &checkpoint.CheckpointMetadata{
		ConfigHash: []byte("123456"),
		BackupTS:   123456,
	}

	err = checkpoint.SaveCheckpointMetadata(ctx, s, checkpointMeta)
	require.NoError(t, err)

	checkpointMeta2, err := checkpoint.LoadCheckpointMetadata(ctx, s)
	require.NoError(t, err)
	require.Equal(t, checkpointMeta.ConfigHash, checkpointMeta2.ConfigHash)
	require.Equal(t, checkpointMeta.BackupTS, checkpointMeta2.BackupTS)
}

type mockTimer struct {
	p int64
	l int64
}

func NewMockTimer(p, l int64) *mockTimer {
	return &mockTimer{p: p, l: l}
}

func (t *mockTimer) GetTS(ctx context.Context) (int64, int64, error) {
	return t.p, t.l, nil
}

func TestCheckpointRunner(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	os.MkdirAll(base+checkpoint.CheckpointDataDir, 0o755)
	os.MkdirAll(base+checkpoint.CheckpointChecksumDir, 0o755)

	cipher := &backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
		CipherKey:  []byte("01234567890123456789012345678901"),
	}
	checkpointRunner, err := checkpoint.StartCheckpointRunnerForTest(ctx, s, cipher, 5*time.Second, NewMockTimer(10, 10))
	require.NoError(t, err)

	data := map[string]struct {
		StartKey string
		EndKey   string
		Name     string
		Name2    string
	}{
		"a": {
			StartKey: "a",
			EndKey:   "b",
			Name:     "c",
			Name2:    "d",
		},
		"A": {
			StartKey: "A",
			EndKey:   "B",
			Name:     "C",
			Name2:    "D",
		},
		"1": {
			StartKey: "1",
			EndKey:   "2",
			Name:     "3",
			Name2:    "4",
		},
	}

	data2 := map[string]struct {
		StartKey string
		EndKey   string
		Name     string
		Name2    string
	}{
		"+": {
			StartKey: "+",
			EndKey:   "-",
			Name:     "*",
			Name2:    "/",
		},
	}

	for _, d := range data {
		err = checkpointRunner.Append(ctx, "a", []byte(d.StartKey), []byte(d.EndKey), []*backuppb.File{
			{Name: d.Name},
			{Name: d.Name2},
		})
		require.NoError(t, err)
	}

	checkpointRunner.FlushChecksum(ctx, 1, 1, 1, 1, checkpoint.MaxChecksumTotalCost-20.0)
	checkpointRunner.FlushChecksum(ctx, 2, 2, 2, 2, 40.0)
	// now the checksum is flushed, because the total time cost is larger than `MaxChecksumTotalCost`
	checkpointRunner.FlushChecksum(ctx, 3, 3, 3, 3, checkpoint.MaxChecksumTotalCost-20.0)
	time.Sleep(6 * time.Second)
	// the checksum has not been flushed even though after 6 seconds,
	// because the total time cost is less than `MaxChecksumTotalCost`
	checkpointRunner.FlushChecksum(ctx, 4, 4, 4, 4, 40.0)

	for _, d := range data2 {
		err = checkpointRunner.Append(ctx, "+", []byte(d.StartKey), []byte(d.EndKey), []*backuppb.File{
			{Name: d.Name},
			{Name: d.Name2},
		})
		require.NoError(t, err)
	}

	checkpointRunner.WaitForFinish(ctx)

	checker := func(groupKey string, resp *rtree.Range) {
		require.NotNil(t, resp)
		d, ok := data[string(resp.StartKey)]
		if !ok {
			d, ok = data2[string(resp.StartKey)]
			require.True(t, ok)
		}
		require.Equal(t, d.StartKey, string(resp.StartKey))
		require.Equal(t, d.EndKey, string(resp.EndKey))
		require.Equal(t, d.Name, resp.Files[0].Name)
		require.Equal(t, d.Name2, resp.Files[1].Name)
	}

	_, err = checkpoint.WalkCheckpointFile(ctx, s, cipher, checker)
	require.NoError(t, err)

	checkpointMeta := &checkpoint.CheckpointMetadata{
		ConfigHash: []byte("123456"),
		BackupTS:   123456,
	}

	err = checkpoint.SaveCheckpointMetadata(ctx, s, checkpointMeta)
	require.NoError(t, err)
	meta, err := checkpoint.LoadCheckpointMetadata(ctx, s)
	require.NoError(t, err)

	var i int64
	for i = 1; i <= 4; i++ {
		require.Equal(t, meta.CheckpointChecksum[i].Crc64xor, uint64(i))
	}

	// only 2 checksum files exists, they are t2_and__ and t4_and__
	count := 0
	err = s.WalkDir(ctx, &storage.WalkOption{SubDir: checkpoint.CheckpointChecksumDir}, func(s string, i int64) error {
		count += 1
		if !strings.Contains(s, "t2") {
			require.True(t, strings.Contains(s, "t4"))
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, count, 2)
}

func getLockData(p, l int64) ([]byte, error) {
	lock := checkpoint.CheckpointLock{
		LockId:   oracle.ComposeTS(p, l),
		ExpireAt: p + 10,
	}
	return json.Marshal(lock)
}

func TestCheckpointRunnerLock(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	s, err := storage.NewLocalStorage(base)
	require.NoError(t, err)
	os.MkdirAll(base+checkpoint.CheckpointDataDir, 0o755)
	os.MkdirAll(base+checkpoint.CheckpointChecksumDir, 0o755)

	cipher := &backuppb.CipherInfo{
		CipherType: encryptionpb.EncryptionMethod_AES256_CTR,
		CipherKey:  []byte("01234567890123456789012345678901"),
	}

	data, err := getLockData(10, 20)
	require.NoError(t, err)
	err = s.WriteFile(ctx, checkpoint.CheckpointLockPath, data)
	require.NoError(t, err)

	_, err = checkpoint.StartCheckpointRunnerForTest(ctx, s, cipher, 5*time.Second, NewMockTimer(10, 10))
	require.Error(t, err)

	runner, err := checkpoint.StartCheckpointRunnerForTest(ctx, s, cipher, 5*time.Second, NewMockTimer(30, 10))
	require.NoError(t, err)

	_, err = checkpoint.StartCheckpointRunnerForTest(ctx, s, cipher, 5*time.Second, NewMockTimer(40, 10))
	require.Error(t, err)

	runner.WaitForFinish(ctx)
}

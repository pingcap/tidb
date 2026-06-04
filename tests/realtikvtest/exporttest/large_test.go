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

package exporttest

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

// TestExportTableLargeDataset loads ~10GiB into one table and exports it,
// verifying global order and row count. Gated by EXPORT_LARGE_TEST since
// loading takes minutes.
func TestExportTableLargeDataset(t *testing.T) {
	if os.Getenv("EXPORT_LARGE_TEST") == "" {
		t.Skip("set EXPORT_LARGE_TEST=1 to run the ~10GiB export test")
	}
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists export_large")
	tk.MustExec("create database export_large")
	tk.MustExec("use export_large")
	tk.MustExec("create table t (id bigint primary key clustered, pad varchar(1100), v bigint)")
	tk.MustQuery("split table t between (0) and (10000000) regions 32")

	const (
		rowCnt  = 10_000_000 // ~1KiB/row => ~10GiB
		workers = 16
		batch   = 500
	)
	// one shared random blob, sliced per row at a pseudo-random offset, keeps
	// the data incompressible without per-row rand cost.
	blob := make([]byte, 4<<20)
	_, err := rand.New(rand.NewSource(1)).Read(blob)
	require.NoError(t, err)
	hexBlob := fmt.Sprintf("%x", blob) // 8MiB of hex chars

	loadStart := time.Now()
	var wg sync.WaitGroup
	rowsPerWorker := rowCnt / workers
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tkw := testkit.NewTestKit(t, store)
			tkw.MustExec("use export_large")
			var sb strings.Builder
			lo, hi := w*rowsPerWorker, (w+1)*rowsPerWorker
			for base := lo; base < hi; base += batch {
				sb.Reset()
				sb.WriteString("insert into t values ")
				for id := base; id < min(base+batch, hi); id++ {
					if id > base {
						sb.WriteByte(',')
					}
					off := (id * 977) % (len(hexBlob) - 1024)
					fmt.Fprintf(&sb, "(%d,'%s',%d)", id, hexBlob[off:off+1024], id*3)
				}
				tkw.MustExec(sb.String())
			}
		}()
	}
	wg.Wait()
	t.Logf("loaded %d rows in %s", rowCnt, time.Since(loadStart))

	dir := "/mnt/data/joechenrh/export_large_out"
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.MkdirAll(dir, 0755))

	exportStart := time.Now()
	rows := tk.MustQuery(fmt.Sprintf(
		"EXPORT TABLE export_large.t TO 'local://%s' WITH thread=8, file_size='256MiB'", dir)).Rows()
	exportDur := time.Since(exportStart)
	require.Equal(t, "succeed", rows[0][2])

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	var totalBytes int64
	for _, ent := range entries {
		info, err := ent.Info()
		require.NoError(t, err)
		totalBytes += info.Size()
		names = append(names, ent.Name())
	}
	sort.Strings(names)

	gotRows, lastID := 0, -1
	for _, name := range names {
		f, err := os.Open(filepath.Join(dir, name))
		require.NoError(t, err)
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 1<<20), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			id := 0
			for i := 0; i < len(line) && line[i] != ','; i++ {
				id = id*10 + int(line[i]-'0')
			}
			require.Greater(t, id, lastID, "ids must be strictly increasing across files in name order")
			lastID = id
			gotRows++
		}
		require.NoError(t, sc.Err())
		require.NoError(t, f.Close())
	}
	require.Equal(t, rowCnt, gotRows)
	t.Logf("exported %d rows, %.2f GiB in %d files, took %s (%.0f MiB/s)",
		gotRows, float64(totalBytes)/(1<<30), len(names), exportDur,
		float64(totalBytes)/(1<<20)/exportDur.Seconds())
}

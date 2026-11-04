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

package indexusage

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/diagnosticlog"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticSnapshotCumulativeAndEmpty(t *testing.T) {
	c := NewCollector()
	c.merge(indexUsage{GlobalIndexID{101, 1}: Sample{QueryTotal: math.MaxUint64, KvReqTotal: 9007199254740993, RowAccessTotal: 42, PercentageAccess: [7]uint64{1, 2, 3, 4, 5, 6, 7}}, GlobalIndexID{101, 2}: Sample{LastUsedAt: time.Date(2026, 10, 23, 1, 59, 58, 123000000, time.UTC), QueryTotal: 120}})
	at := time.Date(2026, 10, 23, 2, 0, 3, 123456789, time.UTC)
	var out bytes.Buffer
	require.NoError(t, c.writeSnapshot(&out, at, 1))
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 3)
	require.Contains(t, lines[0], `"query_total":18446744073709551615`)
	require.Contains(t, lines[0], `"kv_req_total":9007199254740993`)
	require.Contains(t, lines[0], `"last_used_at":null`)
	require.Contains(t, lines[0], `"percentage_access":[1,2,3,4,5,6,7]`)
	require.NotContains(t, lines[0], "record_type")
	require.JSONEq(t, `{"record_type":"footer","snapshot_time":"2026-10-23T02:00:03.123456Z","row_count":2}`, lines[2])
	require.Equal(t, uint64(math.MaxUint64), c.GetIndexUsage(101, 1).QueryTotal)
	var second bytes.Buffer
	require.NoError(t, c.WriteSnapshot(&second, at))
	require.Equal(t, out.String(), second.String())
	empty := NewCollector()
	var emptyOut bytes.Buffer
	require.NoError(t, empty.WriteSnapshot(&emptyOut, at))
	require.JSONEq(t, `{"record_type":"footer","snapshot_time":"2026-10-23T02:00:03.123456Z","row_count":0}`, emptyOut.String())
	// Optional artifact generation uses the production publisher and collector.
	if root := os.Getenv("DIAGNOSTIC_FIXTURE_DIR"); root != "" {
		_, err := diagnosticlog.NewPublisher(filepath.Join(root, "populated")).Publish(at, c.WriteSnapshot)
		require.NoError(t, err)
		_, err = diagnosticlog.NewPublisher(filepath.Join(root, "empty")).Publish(at, empty.WriteSnapshot)
		require.NoError(t, err)
	}
}

type snapshotWriteFunc func([]byte) (int, error)

func (f snapshotWriteFunc) Write(b []byte) (int, error) { return f(b) }

func TestDiagnosticSnapshotFailureAndConcurrentChanges(t *testing.T) {
	c := NewCollector()
	c.merge(indexUsage{GlobalIndexID{1, 1}: Sample{QueryTotal: 1}, GlobalIndexID{1, 2}: Sample{QueryTotal: 2}})
	var out bytes.Buffer
	once := false
	// Mutation from inside the writer proves encoding happens outside the lock.
	writer := snapshotWriteFunc(func(b []byte) (int, error) {
		if !once {
			once = true
			c.Lock()
			delete(c.indexUsage, GlobalIndexID{1, 2})
			c.indexUsage[GlobalIndexID{1, 3}] = Sample{QueryTotal: 3}
			c.Unlock()
		}
		return out.Write(b)
	})
	require.NoError(t, c.writeSnapshot(writer, time.Now(), 1))
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2)
	var footer struct {
		RowCount int `json:"row_count"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &footer))
	require.Equal(t, 1, footer.RowCount)
	written := 0
	err := c.WriteSnapshot(snapshotWriteFunc(func(b []byte) (int, error) { written++; return 0, errors.New("disk full") }), time.Now())
	require.ErrorContains(t, err, "disk full")
	require.Equal(t, 1, written)
	var _ io.Writer = writer
}

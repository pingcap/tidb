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

package importer

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/pkdbsummary"
	"github.com/pingcap/tidb/pkg/util/extsort"
	"github.com/stretchr/testify/require"
)

// An early parallel-import finisher has not checked remote duplicates. Its
// staged rows are not proven ordinary insertions even if no local dupe was seen.
func TestLocalSummaryWithoutRemoteDuplicateCheck(t *testing.T) {
	ctx := context.Background()
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	cfg.TikvImporter.ParallelImport = true
	cfg.Conflict.Strategy = config.ReplaceOnDup
	rc := &Controller{cfg: cfg, summary: pkdbsummary.Open(ctx, nil, false)}
	tr := &TableImporter{tableName: "table"}
	rc.summary.Ensure("chunk", tr.tableName, 0, 0)
	rc.summary.Observe("chunk", 0, pkdbsummary.Outcomes{})
	rc.summary.BeginWrite(ctx, "chunk", 0)
	rc.summary.FinishWrite(ctx, "chunk", 0, pkdbsummary.Outcomes{}, 1, 0)
	rc.confirmLocalSummary(ctx, tr, false, false)
	_, result := rc.summary.Snapshot()
	require.Zero(t, result.Imported)
	require.False(t, result.Complete)
	require.Equal(t, uint64(1), *result.Unresolved)
}

// Completing remote detection with no conflicts still confirms normal local
// writes. Detected conflicts and disabled detection must remain conservative.
func TestLocalSummaryWithRemoteDuplicateCheck(t *testing.T) {
	cases := []struct {
		name              string
		duplicates        bool
		detectionDisabled bool
		expectedImported  uint64
	}{
		{name: "no-conflicts", expectedImported: 1},
		{name: "conflicts", duplicates: true},
		{name: "detection-disabled", detectionDisabled: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := config.NewConfig()
			cfg.TikvImporter.Backend = config.BackendLocal
			cfg.TikvImporter.ParallelImport = true
			cfg.Conflict.Strategy = config.ReplaceOnDup
			if tc.detectionDisabled {
				cfg.Conflict.Strategy = config.NoneOnDup
			}
			rc := &Controller{cfg: cfg, summary: pkdbsummary.Open(ctx, nil, false)}
			tr := &TableImporter{tableName: "table"}
			rc.summary.Ensure("chunk", tr.tableName, 0, 0)
			rc.summary.Observe("chunk", 0, pkdbsummary.Outcomes{})
			rc.summary.BeginWrite(ctx, "chunk", 0)
			rc.summary.FinishWrite(ctx, "chunk", 0, pkdbsummary.Outcomes{}, 1, 0)
			rc.confirmLocalSummary(ctx, tr, tc.duplicates, true)
			_, result := rc.summary.Snapshot()
			require.Equal(t, tc.expectedImported, result.Imported)
			require.Equal(t, tc.expectedImported == 1, result.Complete)
			require.Equal(t, 1-tc.expectedImported, *result.Unresolved)
		})
	}
}

// Default delayed post-processing runs after importer cleanup. Confirmation
// depends on a successful duplicate pass, not on keeping its sorter open.
func TestLocalSummaryDuplicateCompletionSurvivesCleanup(t *testing.T) {
	for _, delayed := range []bool{true, false} {
		for _, failure := range []string{"none", "marker", "lookup"} {
			name := failure + "/immediate"
			if delayed {
				name = failure + "/delayed"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				cfg := config.NewConfig()
				cfg.TikvImporter.Backend = config.BackendLocal
				cfg.Conflict.Strategy = config.ReplaceOnDup
				cfg.PostRestore.PostProcessAtLast = delayed
				rc := &Controller{cfg: cfg, summary: pkdbsummary.Open(ctx, nil, false)}
				tr := &TableImporter{tableName: "table"}
				dir := t.TempDir()
				tr.prepareSummaryDuplicates(true, dir)
				defer tr.summaryDuplicates.close()
				require.False(t, tr.summaryDuplicates.complete())
				if failure == "marker" {
					require.NoError(t, os.Mkdir(filepath.Join(dir, "pkdb-statistics-complete"), 0700))
				}
				tr.finishSummaryDuplicates(ctx, dir)
				if failure == "lookup" {
					// Exercise the actual iterator error path after successful sorting.
					tr.summaryDuplicates.sorter = &summaryIteratorErrorSorter{ExternalSorter: tr.summaryDuplicates.sorter}
					lookup := tr.summaryWinnerLookup(ctx)
					lookup.close()
					require.True(t, tr.summaryDuplicates.failed.Load())
				}
				rc.summary.Ensure("chunk", tr.tableName, 0, 0)
				rc.summary.Observe("chunk", 0, pkdbsummary.Outcomes{Skipped: 1})
				rc.summary.Observe("chunk", 1, pkdbsummary.Outcomes{})
				rc.summary.BeginWrite(ctx, "chunk", 1)
				rc.summary.FinishWrite(ctx, "chunk", 1, pkdbsummary.Outcomes{}, 0, 1)
				if cfg.PostRestore.PostProcessAtLast {
					tr.summaryDuplicates.close()
					tr.summaryDuplicates.close()
					require.False(t, tr.summaryDuplicates.ready())
				}
				rc.confirmLocalSummary(ctx, tr, false, true)
				_, result := rc.summary.Snapshot()
				require.Equal(t, uint64(1), result.Skipped)
				require.Equal(t, failure == "none", result.Complete)
				if failure == "none" {
					require.Equal(t, uint64(1), result.Replaced)
					require.Zero(t, *result.Unresolved)
				} else {
					require.Zero(t, result.Replaced)
					require.Equal(t, uint64(1), *result.Unresolved)
				}
			})
		}
	}
}

// Keep lifecycle operations real while failing iterator creation.
type summaryIteratorErrorSorter struct{ extsort.ExternalSorter }

func (*summaryIteratorErrorSorter) NewIterator(context.Context) (extsort.Iterator, error) {
	return nil, os.ErrInvalid
}

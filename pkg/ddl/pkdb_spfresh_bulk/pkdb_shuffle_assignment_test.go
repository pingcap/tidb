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

package pkdbspfreshbulk

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestShuffleAssignmentGoGolden(t *testing.T) {
	ctx := context.Background()
	plan, _, _, _ := manifestSamples(t)
	planBytes, err := EncodeBuildPlanJSON(plan)
	require.NoError(t, err)
	store := storage.NewMemStorage()
	prefix := "s3://spfresh-mvp/build-demo/"
	_, err = WriteShuffleAssignment(ctx, store, "build-demo", NewManifestRef(prefix+"plan/build-plan.json", planBytes), 0,
		[]ManifestRef{{URI: prefix + "base/part-00000.spfmvp", SHA256: strings.Repeat("8", 64)}}, 4096)
	require.NoError(t, err)
	for _, item := range []struct{ key, name string }{
		{ShuffleAssignmentKey(0), "go_shuffle_assignment.json"},
		{"shuffle/assignments/worker-00000/page-00000000000000000000.json", "go_shuffle_page.json"},
	} {
		body, err := store.ReadFile(ctx, item.key)
		require.NoError(t, err)
		path := filepath.Join("testdata", item.name)
		if os.Getenv("SPFRESH_UPDATE_GOLDEN") == "1" {
			require.NoError(t, os.WriteFile(path, body, 0o644))
		} else {
			golden, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, golden, body)
		}
	}
}

func TestShuffleAssignmentPagesCoverLAIONScale(t *testing.T) {
	for _, count := range []int{49643, 496430} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			ctx := context.Background()
			prefix := "s3://spfresh-mvp/laion1b/"
			plan := ManifestRef{URI: prefix + "plan/build-plan.json", SHA256: strings.Repeat("1", 64)}
			refs := make([]ManifestRef, count)
			for i := range refs {
				refs[i] = ManifestRef{URI: fmt.Sprintf("%sbase/range-%020d/part-%020d.spfmvp", prefix, i/1000, i), SHA256: strings.Repeat("2", 64)}
			}
			for worker := range 5 {
				// Release each worker's test store before constructing the next one.
				store := storage.NewMemStorage()
				start, end := worker*count/5, (worker+1)*count/5
				rootRef, err := WriteShuffleAssignment(ctx, store, "laion1b", plan, uint32(worker), refs[start:end], uint64(end-start)*64<<20)
				require.NoError(t, err)
				rootBytes, err := store.ReadFile(ctx, ShuffleAssignmentKey(uint32(worker)))
				require.NoError(t, err)
				require.NoError(t, rootRef.VerifyBytes(rootBytes))
				require.Less(t, len(rootBytes), 1024)
				var root ShuffleAssignment
				require.NoError(t, json.Unmarshal(rootBytes, &root))
				require.Equal(t, uint64(end-start), root.FragmentCount)
				next, consumed, pages := &root.FirstPage, 0, 0
				for next != nil {
					body, err := store.ReadFile(ctx, strings.TrimPrefix(next.URI, prefix))
					require.NoError(t, err)
					require.LessOrEqual(t, len(body), ShufflePageBytes)
					require.NoError(t, next.VerifyBytes(body))
					var page ShuffleInputPage
					require.NoError(t, json.Unmarshal(body, &page))
					require.Equal(t, uint64(consumed), page.FirstOrdinal)
					require.NotEmpty(t, page.Fragments)
					require.Equal(t, refs[start+consumed:start+consumed+len(page.Fragments)], page.Fragments)
					consumed += len(page.Fragments)
					pages++
					next = page.Next
				}
				require.Equal(t, end-start, consumed)
				require.Greater(t, pages, 1)
			}
		})
	}
}

func TestShuffleAssignmentRejectsInvalidSources(t *testing.T) {
	ctx := context.Background()
	plan := ManifestRef{URI: "s3://spfresh-mvp/run/plan/build-plan.json", SHA256: strings.Repeat("1", 64)}
	ref := ManifestRef{URI: "s3://spfresh-mvp/run/base/part-00000.spfmvp", SHA256: strings.Repeat("2", 64)}
	for _, test := range []struct {
		name string
		refs []ManifestRef
	}{
		{"empty", nil},
		{"duplicate", []ManifestRef{ref, ref}},
		{"cross build", []ManifestRef{{URI: "s3://spfresh-mvp/other/base/part-00000.spfmvp", SHA256: ref.SHA256}}},
		{"wrong namespace", []ManifestRef{plan}},
		{"bad digest", []ManifestRef{{URI: ref.URI, SHA256: "bad"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := WriteShuffleAssignment(ctx, storage.NewMemStorage(), "run", plan, 0, test.refs, 1)
			require.Error(t, err)
		})
	}
	store := storage.NewMemStorage()
	_, err := WriteShuffleAssignment(ctx, store, "run", plan, 0, []ManifestRef{ref}, 1)
	require.NoError(t, err)
	_, err = WriteShuffleAssignment(ctx, store, "run", plan, 0, []ManifestRef{ref}, 1)
	require.ErrorContains(t, err, "already exists")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = WriteShuffleAssignment(canceled, storage.NewMemStorage(), "run", plan, 0, []ManifestRef{ref}, 1)
	require.ErrorIs(t, err, context.Canceled)
}

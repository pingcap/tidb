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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
)

// ShufflePageBytes bounds one authenticated input page, not the build size.
const ShufflePageBytes = 256 << 10

// ShuffleAssignment binds a complete ordered input stream to one worker and plan.
type ShuffleAssignment struct {
	Plan             ManifestRef `json:"plan"`
	WorkerID         uint32      `json:"worker_id"`
	FragmentCount    uint64      `json:"fragment_count"`
	InputObjectBytes uint64      `json:"input_object_bytes"`
	FirstPage        ManifestRef `json:"first_page"`
}

// ShuffleInputPage authenticates the next page as well as the current refs.
// Next is explicitly null only on the final page. Pages are written backwards
// from the already completed scan, so no growing page index is needed.
type ShuffleInputPage struct {
	FirstOrdinal uint64        `json:"first_ordinal"`
	Fragments    []ManifestRef `json:"fragments"`
	Next         *ManifestRef  `json:"next"`
}

// ShuffleAssignmentKey is the exclusive assignment namespace for a worker.
func ShuffleAssignmentKey(workerID uint32) string {
	return fmt.Sprintf("shuffle/assignments/worker-%05d/assignment.json", workerID)
}

// WriteShuffleAssignment publishes bounded pages before their immutable root.
// It never copies the complete source list or discovers objects through LIST.
func WriteShuffleAssignment(ctx context.Context, store storage.ExternalStorage, buildID string, plan ManifestRef, workerID uint32, fragments []ManifestRef, inputBytes uint64) (ManifestRef, error) {
	prefix := "s3://spfresh-mvp/" + buildID + "/"
	if err := plan.validate(buildID); err != nil {
		return ManifestRef{}, err
	}
	if plan.URI != prefix+"plan/build-plan.json" || len(fragments) == 0 || inputBytes == 0 {
		return ManifestRef{}, fmt.Errorf("shuffle assignment requires a frozen plan and nonempty source")
	}
	for i, ref := range fragments {
		if err := ref.validate(buildID); err != nil {
			return ManifestRef{}, err
		}
		if !strings.HasPrefix(ref.URI, prefix+"base/") ||
			i > 0 && fragments[i-1].URI >= ref.URI {
			return ManifestRef{}, fmt.Errorf("shuffle source refs must be ordered, unique base objects")
		}
	}
	var next *ManifestRef
	for end := len(fragments); end > 0; {
		if err := ctx.Err(); err != nil {
			return ManifestRef{}, err
		}
		// Reserve the largest ordinal's header before adding exact encoded refs.
		page := ShuffleInputPage{FirstOrdinal: uint64(end), Fragments: []ManifestRef{}, Next: next}
		header, err := marshalCanonicalJSON(page)
		if err != nil {
			return ManifestRef{}, err
		}
		size, start := len(header), end
		for start > 0 {
			encoded, err := marshalCanonicalJSON(fragments[start-1])
			if err != nil {
				return ManifestRef{}, err
			}
			if size+len(encoded)+1 > ShufflePageBytes {
				break
			}
			size += len(encoded) + 1
			start--
		}
		if start == end {
			return ManifestRef{}, fmt.Errorf("source reference exceeds shuffle page budget")
		}
		page.FirstOrdinal, page.Fragments = uint64(start), fragments[start:end]
		encoded, err := marshalCanonicalJSON(page)
		if err != nil {
			return ManifestRef{}, err
		}
		key := fmt.Sprintf("shuffle/assignments/worker-%05d/page-%020d.json", workerID, start)
		if err := WriteImmutableObject(ctx, store, key, encoded); err != nil {
			return ManifestRef{}, err
		}
		ref := NewManifestRef(prefix+key, encoded)
		next, end = &ref, start
	}
	assignment := ShuffleAssignment{Plan: plan, WorkerID: workerID, FragmentCount: uint64(len(fragments)), InputObjectBytes: inputBytes, FirstPage: *next}
	encoded, err := marshalCanonicalJSON(assignment)
	if err != nil {
		return ManifestRef{}, err
	}
	key := ShuffleAssignmentKey(workerID)
	if err := WriteImmutableObject(ctx, store, key, encoded); err != nil {
		return ManifestRef{}, err
	}
	return NewManifestRef(prefix+key, encoded), nil
}

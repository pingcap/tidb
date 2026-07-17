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

package autoembed

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
)

// SourceSnapshot preserves INSERT SELECT output provenance across logical
// optimization, which may replace the source tree with TableDual. Its cloned
// schema is immutable; lookup relies on corresponding SELECT outputs retaining
// their UniqueIDs and compatible types across optimization.
type SourceSnapshot struct {
	schema  *expression.Schema
	results []autoEmbedResolveResult
}

// Resolve selects exactly one provenance namespace. INSERT target columns take
// precedence only when the SELECT snapshot does not also claim the column;
// source/target ambiguity and an unprovable source both fail closed. Ordinary
// expressions resolve solely from the current logical root.
func Resolve(
	root base.LogicalPlan,
	insert *physicalop.Insert,
	insertSource *SourceSnapshot,
	vector *expression.Column,
	sidecar DualSourceLookup,
) (*expression.AutoEmbedInfo, bool) {
	if vector == nil || vector.RetType == nil || !vector.RetType.EvalType().IsVectorKind() {
		return nil, false
	}

	resolver := &autoEmbedResolver{
		state:   make(map[autoEmbedResolveKey]autoEmbedResolveState),
		memo:    make(map[autoEmbedResolveKey]autoEmbedResolveResult),
		sidecar: sidecar,
	}
	if insert != nil {
		if targetInfo, matched := resolveInsertTargetAutoEmbedInfo(insert, vector); matched {
			if insertSource != nil && insertSource.resolve(vector).found {
				return nil, false
			}
			if targetInfo == nil {
				return nil, false
			}
			return copyAutoEmbedInfo(targetInfo), true
		}
		if insertSource != nil {
			result := insertSource.resolve(vector)
			if result.found && result.info != nil {
				return copyAutoEmbedInfo(result.info), true
			}
			return nil, false
		}
	}

	result := resolver.resolve(root, vector)
	if !result.found || result.info == nil {
		return nil, false
	}
	return copyAutoEmbedInfo(result.info), true
}

// SnapshotSource records INSERT SELECT output proofs before logical
// optimization can discard their source plans. sidecar is builder-local and
// is consulted only for exact empty-Dual identities.
func SnapshotSource(root base.LogicalPlan, sidecar DualSourceLookup) *SourceSnapshot {
	if root == nil || root.Schema() == nil {
		return nil
	}
	resolver := &autoEmbedResolver{
		state:   make(map[autoEmbedResolveKey]autoEmbedResolveState),
		memo:    make(map[autoEmbedResolveKey]autoEmbedResolveResult),
		sidecar: sidecar,
	}
	snapshot := &SourceSnapshot{
		schema:  root.Schema().Clone(),
		results: make([]autoEmbedResolveResult, root.Schema().Len()),
	}
	for idx, col := range root.Schema().Columns {
		result := resolver.resolve(root, col)
		result.found = true
		snapshot.results[idx] = result
	}
	return snapshot
}

// resolve uses the same pointer-first, then UniqueID-and-type identity rules as
// logical traversal. Ambiguous snapshot matches claim the namespace without
// returning metadata, so INSERT cannot fall back to its target table by chance.
func (s *SourceSnapshot) resolve(target *expression.Column) autoEmbedResolveResult {
	if s == nil {
		return autoEmbedResolveResult{}
	}
	_, idx, found, ambiguous := findAutoEmbedColumn(s.schema, target)
	if ambiguous {
		return autoEmbedResolveResult{found: true}
	}
	if !found || idx >= len(s.results) {
		return autoEmbedResolveResult{}
	}
	return s.results[idx]
}

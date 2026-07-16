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
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
)

// DualSourceLookup exposes only the builder-local lineage needed by the
// resolver. Implementations must use exact Dual identity rather than schema or
// column identity.
type DualSourceLookup interface {
	SourceOfEmptyDual(*logicalop.LogicalTableDual) (base.LogicalPlan, bool)
}

// BuildState owns auto-embedding state for one outer PlanBuilder.Build call.
// The map is allocated lazily and never becomes part of a logical plan.
type BuildState struct {
	presence   resolve.AutoEmbedConsumerPresence
	dualSource map[*logicalop.LogicalTableDual]base.LogicalPlan
}

// NewBuildState creates state from the effective classification for a build.
func NewBuildState(p resolve.AutoEmbedConsumerPresence) *BuildState {
	return &BuildState{presence: p}
}

// ResetForBuild drops pointers from any previous build and initializes the
// classification for a new one. PlanBuilder uses this on embedded storage so
// ordinary statements do not allocate a separate BuildState object.
func (s *BuildState) ResetForBuild(p resolve.AutoEmbedConsumerPresence) {
	if s == nil {
		return
	}
	clear(s.dualSource)
	s.dualSource = nil
	s.presence = p
}

// Presence returns the current monotonically upgraded classification. A nil
// state is conservatively Unknown.
func (s *BuildState) Presence() resolve.AutoEmbedConsumerPresence {
	if s == nil {
		return resolve.AutoEmbedConsumerUnknown
	}
	return s.presence
}

// NeedsLineage reports whether folded inputs must be retained.
func (s *BuildState) NeedsLineage() bool {
	return s.Presence().NeedsLineage()
}

// UpgradePresence merges a dynamically observed AST classification without
// ever downgrading Present or turning uncertainty into Absent.
func (s *BuildState) UpgradePresence(p resolve.AutoEmbedConsumerPresence) {
	if s == nil {
		return
	}
	s.presence = resolve.MergeAutoEmbedConsumerPresence(s.presence, p)
}

// RecordEmptyDual preserves the input discarded by one build-time empty Dual.
// Invalid and non-empty Duals are ignored. The first exact-pointer record wins.
func (s *BuildState) RecordEmptyDual(dual *logicalop.LogicalTableDual, source base.LogicalPlan) {
	if s == nil || !s.NeedsLineage() || dual == nil || dual.RowCount != 0 || source == nil {
		return
	}
	if s.dualSource == nil {
		s.dualSource = make(map[*logicalop.LogicalTableDual]base.LogicalPlan)
	}
	if _, exists := s.dualSource[dual]; !exists {
		s.dualSource[dual] = source
	}
}

// SourceOfEmptyDual returns the discarded input for this exact empty Dual.
func (s *BuildState) SourceOfEmptyDual(dual *logicalop.LogicalTableDual) (base.LogicalPlan, bool) {
	if s == nil || dual == nil || s.dualSource == nil {
		return nil, false
	}
	source, ok := s.dualSource[dual]
	return source, ok
}

// ResetForReuse drops all logical-plan pointers and restores conservative
// zero-value semantics.
func (s *BuildState) ResetForReuse() {
	s.ResetForBuild(resolve.AutoEmbedConsumerUnknown)
}

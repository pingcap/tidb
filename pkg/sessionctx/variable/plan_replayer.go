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

package variable

// PlanReplayerInternalSQLType identifies the helper SQL currently executed by plan replayer.
type PlanReplayerInternalSQLType uint8

// PlanReplayerInternalSQLTypeNone disables the temporary privilege bypass.
// Other values identify the current internal helper SQL shape.
const (
	PlanReplayerInternalSQLTypeNone PlanReplayerInternalSQLType = iota
	PlanReplayerInternalSQLTypeExplain
	PlanReplayerInternalSQLTypeShowCreateTable
	PlanReplayerInternalSQLTypeShowCreateView
)

// GetPlanReplayerSQLPrivilegeType returns the current internal privilege mode.
func (s *SessionVars) GetPlanReplayerSQLPrivilegeType() PlanReplayerInternalSQLType {
	return s.planReplayerSQLPrivilegeType
}

// SetPlanReplayerSQLPrivilegeType swaps the current internal privilege mode.
func (s *SessionVars) SetPlanReplayerSQLPrivilegeType(tp PlanReplayerInternalSQLType) PlanReplayerInternalSQLType {
	restore := s.planReplayerSQLPrivilegeType
	s.planReplayerSQLPrivilegeType = tp
	return restore
}

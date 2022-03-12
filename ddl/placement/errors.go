// Copyright 2021 PingCAP, Inc.
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

package placement

import (
	"errors"
)

var (
	// ErrInvalidConstraintFormat is from constraint.go.
	ErrInvalidConstraintFormat = errors.New("label constraint should be in format '{+|-}key=value'")
	// ErrUnsupportedConstraint is from constraint.go.
	ErrUnsupportedConstraint = errors.New("unsupported label constraint")
	// ErrConflictingConstraints is from constraints.go.
	ErrConflictingConstraints = errors.New("conflicting label constraints")
	// ErrInvalidConstraintsMapcnt is from rule.go.
	ErrInvalidConstraintsMapcnt = errors.New("label constraints in map syntax have invalid replicas")
	// ErrInvalidConstraintsFormat is from rule.go.
	ErrInvalidConstraintsFormat = errors.New("invalid label constraints format")
	// ErrInvalidConstraintsRelicas is from rule.go.
	ErrInvalidConstraintsRelicas = errors.New("label constraints with invalid REPLICAS")
	// ErrInvalidBundleID is from bundle.go.
	ErrInvalidBundleID = errors.New("invalid bundle ID")
	// ErrInvalidBundleIDFormat is from bundle.go.
	ErrInvalidBundleIDFormat = errors.New("invalid bundle ID format")
	// ErrLeaderReplicasMustOne is from bundle.go.
	ErrLeaderReplicasMustOne = errors.New("REPLICAS must be 1 if ROLE=leader")
	// ErrMissingRoleField is from bundle.go.
	ErrMissingRoleField = errors.New("the ROLE field is not specified")
	// ErrNoRulesToDrop is from bundle.go.
	ErrNoRulesToDrop = errors.New("no rule of such role to drop")
	// ErrInvalidPlacementOptions is from bundle.go.
	ErrInvalidPlacementOptions = errors.New("invalid placement option")
	// ErrInvalidConstraintsMappingWrongSeparator is wrong separator in mapping.
	ErrInvalidConstraintsMappingWrongSeparator = errors.New("mappings use a colon and space (“: ”) to mark each key/value pair")
	// ErrInvalidConstraintsMappingNoColonFound is no colon found in mapping.
	ErrInvalidConstraintsMappingNoColonFound = errors.New("no colon found")
)

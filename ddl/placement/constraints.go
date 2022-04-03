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
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"
)

// Constraints is a slice of constraints.
type Constraints []Constraint

// NewConstraints will check each labels, and build the Constraints.
func NewConstraints(labels []string) (Constraints, error) {
	if len(labels) == 0 {
		return nil, nil
	}

	constraints := make(Constraints, 0, len(labels))
	for _, str := range labels {
		label, err := NewConstraint(strings.TrimSpace(str))
		if err != nil {
			return constraints, err
		}

		err = constraints.Add(label)
		if err != nil {
			return constraints, err
		}
	}
	return constraints, nil
}

// NewConstraintsFromYaml will transform parse the raw 'array' constraints and call NewConstraints.
// Refer to https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md.
func NewConstraintsFromYaml(c []byte) (Constraints, error) {
	constraints := []string{}
	err := yaml.UnmarshalStrict(c, &constraints)
	if err != nil {
		return nil, ErrInvalidConstraintsFormat
	}
	return NewConstraints(constraints)
}

// NewConstraintsDirect is a helper for creating new constraints from individual constraint.
func NewConstraintsDirect(c ...Constraint) Constraints {
	return c
}

// Restore converts label constraints to a string.
func (constraints *Constraints) Restore() (string, error) {
	var sb strings.Builder
	for i, constraint := range *constraints {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('"')
		conStr, err := constraint.Restore()
		if err != nil {
			return "", err
		}
		sb.WriteString(conStr)
		sb.WriteByte('"')
	}
	return sb.String(), nil
}

// Add will add a new label constraint, with validation of all constraints.
// Note that Add does not validate one single constraint.
func (constraints *Constraints) Add(label Constraint) error {
	pass := true

	for i := range *constraints {
		cnst := (*constraints)[i]
		res := label.CompatibleWith(&cnst)
		if res == ConstraintCompatible {
			continue
		}
		if res == ConstraintDuplicated {
			pass = false
			continue
		}
		s1, err := label.Restore()
		if err != nil {
			s1 = err.Error()
		}
		s2, err := cnst.Restore()
		if err != nil {
			s2 = err.Error()
		}
		return fmt.Errorf("%w: '%s' and '%s'", ErrConflictingConstraints, s1, s2)
	}

	if pass {
		*constraints = append(*constraints, label)
	}
	return nil
}

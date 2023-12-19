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
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"slices"
	"sort"
	"strings"

	pd "github.com/tikv/pd/client/http"
	"gopkg.in/yaml.v2"
)

// NewConstraints will check each labels, and build the Constraints.
func NewConstraints(labels []string) ([]pd.LabelConstraint, error) {
	if len(labels) == 0 {
		return nil, nil
	}

	constraints := make([]pd.LabelConstraint, 0, len(labels))
	for _, str := range labels {
		label, err := NewConstraint(strings.TrimSpace(str))
		if err != nil {
			return constraints, err
		}

		err = AddConstraint(&constraints, label)
		if err != nil {
			return constraints, err
		}
	}
	return constraints, nil
}

// preCheckDictConstraintStr will check the label string, and return the new labels and role.
// role maybe be override by the label string, eg `#evict-leader`.
func preCheckDictConstraintStr(labelStr string, role pd.PeerRoleType) ([]string, pd.PeerRoleType, error) {
	innerLabels := strings.Split(labelStr, ",")
	overrideRole := role
	newLabels := make([]string, 0, len(innerLabels))
	for _, str := range innerLabels {
		if strings.HasPrefix(str, attributePrefix) {
			switch str[1:] {
			case attributeEvictLeader:
				if role == pd.Voter {
					overrideRole = pd.Follower
				}
			default:
				return newLabels, overrideRole, fmt.Errorf("%w: unsupported attribute '%s'", ErrUnsupportedConstraint, str)
			}
			continue
		}
		newLabels = append(newLabels, str)
	}
	return newLabels, overrideRole, nil
}

// NewConstraintsFromYaml will transform parse the raw 'array' constraints and call NewConstraints.
// Refer to https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md.
func NewConstraintsFromYaml(c []byte) ([]pd.LabelConstraint, error) {
	constraints := []string{}
	err := yaml.UnmarshalStrict(c, &constraints)
	if err != nil {
		return nil, ErrInvalidConstraintsFormat
	}
	return NewConstraints(constraints)
}

// NewConstraintsDirect is a helper for creating new constraints from individual constraint.
func NewConstraintsDirect(c ...pd.LabelConstraint) []pd.LabelConstraint {
	return c
}

// RestoreConstraints converts label constraints to a string.
func RestoreConstraints(constraints *[]pd.LabelConstraint) (string, error) {
	var sb strings.Builder
	for i, constraint := range *constraints {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('"')
		conStr, err := RestoreConstraint(&constraint)
		if err != nil {
			return "", err
		}
		sb.WriteString(conStr)
		sb.WriteByte('"')
	}
	return sb.String(), nil
}

// AddConstraint will add a new label constraint, with validation of all constraints.
// Note that Add does not validate one single constraint.
func AddConstraint(constraints *[]pd.LabelConstraint, label pd.LabelConstraint) error {
	pass := true

	for i := range *constraints {
		cnst := (*constraints)[i]
		res := ConstraintCompatibleWith(&label, &cnst)
		if res == ConstraintCompatible {
			continue
		}
		if res == ConstraintDuplicated {
			pass = false
			continue
		}
		s1, err := RestoreConstraint(&label)
		if err != nil {
			s1 = err.Error()
		}
		s2, err := RestoreConstraint(&cnst)
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

// ConstraintsFingerPrint returns a unique string for the constraints.
func ConstraintsFingerPrint(constraints *[]pd.LabelConstraint) string {
	copied := make([]pd.LabelConstraint, len(*constraints))
	copy(copied, *constraints)
	slices.SortStableFunc(copied, func(i, j pd.LabelConstraint) int {
		a, b := constraintToString(&i), constraintToString(&j)
		return cmp.Compare(a, b)
	})
	var combinedConstraints string
	for _, constraint := range copied {
		combinedConstraints += constraintToString(&constraint)
	}

	// Calculate the SHA256 hash of the concatenated constraints
	hash := sha256.Sum256([]byte(combinedConstraints))

	// Encode the hash as a base64 string
	hashStr := base64.StdEncoding.EncodeToString(hash[:])

	return hashStr
}

func constraintToString(c *pd.LabelConstraint) string {
	// Sort the values in the constraint
	sortedValues := make([]string, len(c.Values))
	copy(sortedValues, c.Values)
	sort.Strings(sortedValues)
	sortedValuesStr := strings.Join(sortedValues, ",")
	return c.Key + "|" + string(c.Op) + "|" + sortedValuesStr
}

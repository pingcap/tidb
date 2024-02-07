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

	pd "github.com/tikv/pd/client/http"
)

// NewConstraint will create a Constraint from a string.
func NewConstraint(label string) (pd.LabelConstraint, error) {
	r := pd.LabelConstraint{}

	if len(label) < 4 {
		return r, fmt.Errorf("%w: %s", ErrInvalidConstraintFormat, label)
	}

	var op pd.LabelConstraintOp
	switch label[0] {
	case '+':
		op = pd.In
	case '-':
		op = pd.NotIn
	default:
		return r, fmt.Errorf("%w: %s", ErrInvalidConstraintFormat, label)
	}

	kv := strings.Split(label[1:], "=")
	if len(kv) != 2 {
		return r, fmt.Errorf("%w: %s", ErrInvalidConstraintFormat, label)
	}

	key := strings.TrimSpace(kv[0])
	if key == "" {
		return r, fmt.Errorf("%w: %s", ErrInvalidConstraintFormat, label)
	}

	val := strings.TrimSpace(kv[1])
	if val == "" {
		return r, fmt.Errorf("%w: %s", ErrInvalidConstraintFormat, label)
	}

	if op == pd.In && key == EngineLabelKey && strings.ToLower(val) == EngineLabelTiFlash {
		return r, fmt.Errorf("%w: %s", ErrUnsupportedConstraint, label)
	}

	r.Key = key
	r.Op = op
	r.Values = strings.Split(val, ",")
	return r, nil
}

// NewConstraintDirect will create a Constraint from argument directly.
func NewConstraintDirect(key string, op pd.LabelConstraintOp, val ...string) pd.LabelConstraint {
	return pd.LabelConstraint{
		Key:    key,
		Op:     op,
		Values: val,
	}
}

// RestoreConstraint converts a Constraint to a string.
func RestoreConstraint(c *pd.LabelConstraint) (string, error) {
	var sb strings.Builder
	if len(c.Values) != 1 {
		return "", fmt.Errorf("%w: constraint should have exactly one label value, got %v", ErrInvalidConstraintFormat, c.Values)
	}
	switch c.Op {
	case pd.In:
		sb.WriteString("+")
	case pd.NotIn:
		sb.WriteString("-")
	default:
		return "", fmt.Errorf("%w: disallowed operation '%s'", ErrInvalidConstraintFormat, c.Op)
	}
	sb.WriteString(c.Key)
	sb.WriteString("=")
	sb.WriteString(c.Values[0])
	return sb.String(), nil
}

// ConstraintCompatibility is the return type of CompatibleWith.
type ConstraintCompatibility byte

const (
	// ConstraintCompatible indicates two constraints are compatible.
	ConstraintCompatible ConstraintCompatibility = iota
	// ConstraintIncompatible indicates two constraints are incompatible.
	ConstraintIncompatible
	// ConstraintDuplicated indicates two constraints are duplicated.
	ConstraintDuplicated
)

// ConstraintCompatibleWith will check if two constraints are compatible.
// Return (compatible, duplicated).
func ConstraintCompatibleWith(c *pd.LabelConstraint, o *pd.LabelConstraint) ConstraintCompatibility {
	sameKey := c.Key == o.Key
	if !sameKey {
		return ConstraintCompatible
	}

	sameOp := c.Op == o.Op
	sameVal := true
	for i := range c.Values {
		if i < len(o.Values) && c.Values[i] != o.Values[i] {
			sameVal = false
			break
		}
	}
	// no following cases:
	// 1. duplicated constraint, skip it
	// 2. no instance can meet: +dc=sh, -dc=sh
	// 3. can not match multiple instances: +dc=sh, +dc=bj
	if sameOp && sameVal {
		return ConstraintDuplicated
	} else if (!sameOp && sameVal) || (sameOp && !sameVal && c.Op == pd.In) {
		return ConstraintIncompatible
	}

	return ConstraintCompatible
}

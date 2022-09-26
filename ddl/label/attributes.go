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

package label

import (
	"fmt"
	"strings"
)

const (
	dbKey        = "db"
	tableKey     = "table"
	partitionKey = "partition"
)

// AttributesCompatibility is the return type of CompatibleWith.
type AttributesCompatibility byte

const (
	// AttributesCompatible indicates two attributes are compatible.
	AttributesCompatible AttributesCompatibility = iota
	// AttributesIncompatible indicates two attributes are incompatible.
	AttributesIncompatible
	// AttributesDuplicated indicates two attributes are duplicated.
	AttributesDuplicated
)

// Label is used to describe attributes
type Label struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// NewLabel creates a new label for a given string.
func NewLabel(attr string) (Label, error) {
	l := Label{}
	kv := strings.Split(attr, "=")
	if len(kv) != 2 {
		return l, fmt.Errorf("%w: %s", ErrInvalidAttributesFormat, attr)
	}

	key := strings.TrimSpace(kv[0])
	if key == "" {
		return l, fmt.Errorf("%w: %s", ErrInvalidAttributesFormat, attr)
	}

	val := strings.TrimSpace(kv[1])
	if val == "" {
		return l, fmt.Errorf("%w: %s", ErrInvalidAttributesFormat, attr)
	}

	l.Key = key
	l.Value = val
	return l, nil
}

// Restore converts a Attribute to a string.
func (l *Label) Restore() string {
	return l.Key + "=" + l.Value
}

// CompatibleWith will check if two constraints are compatible.
// Return (compatible, duplicated).
func (l *Label) CompatibleWith(o *Label) AttributesCompatibility {
	if l.Key != o.Key {
		return AttributesCompatible
	}

	if l.Value == o.Value {
		return AttributesDuplicated
	}

	return AttributesIncompatible
}

// Labels is a slice of Label.
type Labels []Label

// NewLabels creates a slice of Label for given attributes.
func NewLabels(attrs []string) (Labels, error) {
	labels := make(Labels, 0, len(attrs))
	for _, attr := range attrs {
		label, err := NewLabel(attr)
		if err != nil {
			return nil, err
		}
		if err := labels.Add(label); err != nil {
			return nil, err
		}
	}
	return labels, nil
}

// Restore converts Attributes to a string.
func (labels *Labels) Restore() string {
	var sb strings.Builder
	for i, label := range *labels {
		switch label.Key {
		case dbKey, tableKey, partitionKey:
			continue
		default:
		}

		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('"')
		sb.WriteString(label.Restore())
		sb.WriteByte('"')
	}
	return sb.String()
}

// Add will add a new attribute, with validation of all attributes.
func (labels *Labels) Add(label Label) error {
	for i := range *labels {
		l := (*labels)[i]
		res := label.CompatibleWith(&l)
		if res == AttributesCompatible {
			continue
		}
		if res == AttributesDuplicated {
			return nil
		}
		s1 := label.Restore()
		s2 := l.Restore()
		return fmt.Errorf("'%s' and '%s' are conflicted", s1, s2)
	}

	*labels = append(*labels, label)
	return nil
}

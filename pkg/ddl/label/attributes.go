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

	pd "github.com/tikv/pd/client/http"
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

// NewLabel creates a new label for a given string.
func NewLabel(attr string) (pd.RegionLabel, error) {
	l := pd.RegionLabel{}
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

// RestoreRegionLabel converts a Attribute to a string.
func RestoreRegionLabel(l *pd.RegionLabel) string {
	return l.Key + "=" + l.Value
}

// CompatibleWith will check if two constraints are compatible.
// Return (compatible, duplicated).
func CompatibleWith(l *pd.RegionLabel, o *pd.RegionLabel) AttributesCompatibility {
	if l.Key != o.Key {
		return AttributesCompatible
	}

	if l.Value == o.Value {
		return AttributesDuplicated
	}

	return AttributesIncompatible
}

// NewLabels creates a slice of Label for given attributes.
func NewLabels(attrs []string) ([]pd.RegionLabel, error) {
	labels := make([]pd.RegionLabel, 0, len(attrs))
	for _, attr := range attrs {
		label, err := NewLabel(attr)
		if err != nil {
			return nil, err
		}
		if err := Add(&labels, label); err != nil {
			return nil, err
		}
	}
	return labels, nil
}

// RestoreRegionLabels converts Attributes to a string.
func RestoreRegionLabels(labels *[]pd.RegionLabel) string {
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
		sb.WriteString(RestoreRegionLabel(&label))
		sb.WriteByte('"')
	}
	return sb.String()
}

// Add will add a new attribute, with validation of all attributes.
func Add(labels *[]pd.RegionLabel, label pd.RegionLabel) error {
	for i := range *labels {
		l := (*labels)[i]
		res := CompatibleWith(&label, &l)
		if res == AttributesCompatible {
			continue
		}
		if res == AttributesDuplicated {
			return nil
		}
		s1 := RestoreRegionLabel(&label)
		s2 := RestoreRegionLabel(&l)
		return fmt.Errorf("'%s' and '%s' are conflicted", s1, s2)
	}

	*labels = append(*labels, label)
	return nil
}

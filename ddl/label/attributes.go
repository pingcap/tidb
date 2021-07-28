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
// See the License for the specific language governing permissions and
// limitations under the License.

package label

import (
	"strings"
)

const (
	dbKey    = "db"
	tableKey = "table"
)

// Label is used to describe attributes
type Label struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Labels is a slice of Label.
type Labels []Label

// NewLabels creates a slice of Label for given attributes.
func NewLabels(attrs []string) Labels {
	labels := make(Labels, 0, len(attrs))
	for _, attr := range attrs {
		label := NewLabel(attr)
		labels.Add(label)
	}
	return labels
}

// Restore converts Attributes to a string.
func (labels *Labels) Restore() string {
	var sb strings.Builder
	for i, label := range *labels {
		switch label.Key {
		case dbKey, tableKey:
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

// Add adds a new label to existed labels.
func (labels *Labels) Add(l Label) {
	for _, label := range *labels {
		if l.Key != label.Key {
			continue
		}
		return
	}

	*labels = append(*labels, l)
}

// NewLabel creates a new label for a given string.
func NewLabel(attr string) Label {
	return Label{Key: strings.TrimSpace(attr), Value: "true"}
}

// Restore converts a Attribute to a string.
func (a *Label) Restore() string {
	return a.Key
}

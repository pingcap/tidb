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

package pkdbspfreshbulk

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
)

// StoredColumn describes one column in the ordered persisted sidecar ABI.
type StoredColumn struct {
	ColumnID           int64      `json:"column_id"`
	FieldType          JSONBytes  `json:"field_type"`
	OriginDefaultValue *JSONBytes `json:"origin_default_value"`
}

// HandleColumn describes the integer primary key used by pushed filters.
type HandleColumn struct {
	ColumnID  int64     `json:"column_id"`
	FieldType JSONBytes `json:"field_type"`
}

// JSONBytes serializes protobuf and datum bytes as numeric JSON arrays.
type JSONBytes []byte

// MarshalJSON implements json.Marshaler.
func (b JSONBytes) MarshalJSON() ([]byte, error) {
	out := make([]byte, 0, len(b)*4+2)
	out = append(out, '[')
	for i, v := range b {
		if i > 0 {
			out = append(out, ',')
		}
		out = strconv.AppendUint(out, uint64(v), 10)
	}
	out = append(out, ']')
	return out, nil
}

// UnmarshalJSON accepts the same numeric byte array emitted by Rust.
func (b *JSONBytes) UnmarshalJSON(data []byte) error {
	var values []uint16
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	*b = make(JSONBytes, len(values))
	for i, value := range values {
		if value > 255 {
			return fmt.Errorf("byte value exceeds 255")
		}
		(*b)[i] = byte(value)
	}
	return nil
}

// EqualStoredColumns compares the complete ordered schema, including origin defaults.
func EqualStoredColumns(a, b []StoredColumn) bool {
	return slices.EqualFunc(a, b, func(a, b StoredColumn) bool {
		if a.ColumnID != b.ColumnID || !bytes.Equal(a.FieldType, b.FieldType) || (a.OriginDefaultValue == nil) != (b.OriginDefaultValue == nil) {
			return false
		}
		return a.OriginDefaultValue == nil || bytes.Equal(*a.OriginDefaultValue, *b.OriginDefaultValue)
	})
}

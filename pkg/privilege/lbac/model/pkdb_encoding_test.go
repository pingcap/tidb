// Copyright 2025 PingCAP, Inc.
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

package model

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

type unsupportedValue struct{}

func (unsupportedValue) componentType() ComponentType { return ComponentType(99) }

type rawComponent struct {
	id      byte
	compTyp ComponentType
	payload []byte
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	label := Label{
		Version: CurrentVersion,
		Components: []Component{
			{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 0}},
			{ID: 2, Type: ComponentTypeSet, Value: SetValue{Values: []uint64{3, 1}}},
			{ID: 3, Type: ComponentTypeTree, Value: TreeValue{In: 4, Out: 9}},
		},
	}
	data, err := EncodeLabel(label)
	require.NoError(t, err)
	decoded, err := DecodeLabel(data)
	require.NoError(t, err)

	expected := label
	expected.Components[1].Value = SetValue{Values: []uint64{1, 3}}
	require.Equal(t, expected, decoded)
}

func TestEncodeLabelErrors(t *testing.T) {
	id := uint8(1)
	otherID := uint8(2)

	tooMany := make([]Component, maxComponents+1)
	for i := range tooMany {
		tooMany[i] = Component{
			ID:    uint8(i + 1),
			Type:  ComponentTypeArray,
			Value: ArrayValue{Ordinal: uint64(i)},
		}
	}

	tests := []struct {
		name  string
		label Label
		want  error
	}{
		{
			name:  "invalid version",
			label: Label{Version: 0},
			want:  ErrInvalidVersion,
		},
		{
			name:  "too many components",
			label: Label{Version: CurrentVersion, Components: tooMany},
			want:  ErrTooManyComponents,
		},
		{
			name: "nil component value",
			label: Label{
				Version:    CurrentVersion,
				Components: []Component{{ID: id, Type: ComponentTypeArray}},
			},
			want: ErrInvalidComponentValue,
		},
		{
			name: "component type mismatch",
			label: Label{
				Version:    CurrentVersion,
				Components: []Component{{ID: id, Type: ComponentTypeSet, Value: ArrayValue{Ordinal: 1}}},
			},
			want: ErrInvalidComponentType,
		},
		{
			name: "empty set values",
			label: Label{
				Version:    CurrentVersion,
				Components: []Component{{ID: id, Type: ComponentTypeSet, Value: SetValue{}}},
			},
			want: ErrEmptySetValues,
		},
		{
			name: "duplicate set values",
			label: Label{
				Version:    CurrentVersion,
				Components: []Component{{ID: id, Type: ComponentTypeSet, Value: SetValue{Values: []uint64{1, 1}}}},
			},
			want: ErrDuplicateSetValue,
		},
		{
			name: "invalid tree range",
			label: Label{
				Version:    CurrentVersion,
				Components: []Component{{ID: id, Type: ComponentTypeTree, Value: TreeValue{In: 5, Out: 3}}},
			},
			want: ErrInvalidTreeRange,
		},
		{
			name: "duplicate component id",
			label: Label{
				Version: CurrentVersion,
				Components: []Component{
					{ID: id, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 1}},
					{ID: id, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 2}},
				},
			},
			want: ErrDuplicateComponentID,
		},
		{
			name: "unsupported component type",
			label: Label{
				Version:    CurrentVersion,
				Components: []Component{{ID: otherID, Type: ComponentType(99), Value: unsupportedValue{}}},
			},
			want: ErrUnsupportedComponent,
		},
		{
			name: "invalid component id",
			label: Label{
				Version:    CurrentVersion,
				Components: []Component{{ID: 0, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 1}}},
			},
			want: ErrInvalidComponentID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := EncodeLabel(tt.label)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestDecodeLabelErrors(t *testing.T) {
	id := byte(1)
	otherID := byte(2)

	tests := []struct {
		name string
		data []byte
		want error
	}{
		{
			name: "truncated header",
			data: []byte{CurrentVersion},
			want: ErrTruncatedLabel,
		},
		{
			name: "invalid version",
			data: []byte{0, 0},
			want: ErrInvalidVersion,
		},
		{
			name: "invalid component id",
			data: buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: 0, compTyp: ComponentTypeArray, payload: arrayPayload(t, 1)},
			}),
			want: ErrInvalidComponentID,
		},
		{
			name: "duplicate component id",
			data: buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: id, compTyp: ComponentTypeArray, payload: arrayPayload(t, 1)},
				{id: id, compTyp: ComponentTypeArray, payload: arrayPayload(t, 2)},
			}),
			want: ErrDuplicateComponentID,
		},
		{
			name: "trailing bytes",
			data: append(buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: id, compTyp: ComponentTypeArray, payload: arrayPayload(t, 1)},
			}), 0x00),
			want: ErrTrailingBytes,
		},
		{
			name: "unsupported component type",
			data: buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: id, compTyp: ComponentType(99), payload: nil},
			}),
			want: ErrUnsupportedComponent,
		},
		{
			name: "invalid array payload",
			data: buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: id, compTyp: ComponentTypeArray, payload: []byte{0x80}},
			}),
			want: ErrInvalidPayload,
		},
		{
			name: "set empty",
			data: buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: otherID, compTyp: ComponentTypeSet, payload: setPayload(t)},
			}),
			want: ErrEmptySetValues,
		},
		{
			name: "set duplicate value",
			data: buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: otherID, compTyp: ComponentTypeSet, payload: setPayload(t, 1, 1)},
			}),
			want: ErrDuplicateSetValue,
		},
		{
			name: "tree invalid range",
			data: buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: otherID, compTyp: ComponentTypeTree, payload: treePayload(t, 10, 5)},
			}),
			want: ErrInvalidTreeRange,
		},
		{
			name: "truncated payload",
			data: buildLabelBytes(t, CurrentVersion, []rawComponent{
				{id: id, compTyp: ComponentTypeTree, payload: arrayPayload(t, 1)},
			}),
			want: ErrTruncatedLabel,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeLabel(tt.data)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestLabelDominates(t *testing.T) {
	grant := Label{
		Version: CurrentVersion,
		Components: []Component{
			{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 0}},
			{ID: 2, Type: ComponentTypeSet, Value: SetValue{Values: []uint64{1, 2, 3}}},
			{ID: 3, Type: ComponentTypeTree, Value: TreeValue{In: 1, Out: 10}},
		},
	}
	row := Label{
		Version: CurrentVersion,
		Components: []Component{
			{ID: 3, Type: ComponentTypeTree, Value: TreeValue{In: 2, Out: 3}},
			{ID: 2, Type: ComponentTypeSet, Value: SetValue{Values: []uint64{1}}},
			{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 2}},
		},
	}
	ok, err := LabelDominates(grant, row)
	require.NoError(t, err)
	require.True(t, ok)

	rowExtra := Label{
		Version: CurrentVersion,
		Components: []Component{
			{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 2}},
			{ID: 4, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 1}},
		},
	}
	ok, err = LabelDominates(grant, rowExtra)
	require.NoError(t, err)
	require.False(t, ok)

	rowNoAccess := Label{
		Version: CurrentVersion,
		Components: []Component{
			{ID: 2, Type: ComponentTypeSet, Value: SetValue{Values: []uint64{1, 4}}},
		},
	}
	ok, err = LabelDominates(grant, rowNoAccess)
	require.NoError(t, err)
	require.False(t, ok)

	rowSubset := Label{
		Version: CurrentVersion,
		Components: []Component{
			{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 0}},
		},
	}
	ok, err = LabelDominates(grant, rowSubset)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestLabelDominatesErrors(t *testing.T) {
	grant := Label{
		Version:    CurrentVersion,
		Components: []Component{{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 0}}},
	}
	rowVersion := Label{
		Version:    CurrentVersion + 1,
		Components: []Component{{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 1}}},
	}
	ok, err := LabelDominates(grant, rowVersion)
	require.ErrorIs(t, err, ErrVersionMismatch)
	require.False(t, ok)

	rowType := Label{
		Version:    CurrentVersion,
		Components: []Component{{ID: 1, Type: ComponentTypeSet, Value: SetValue{Values: []uint64{1}}}},
	}
	ok, err = LabelDominates(grant, rowType)
	require.ErrorIs(t, err, ErrComponentTypeMismatch)
	require.False(t, ok)
}

func TestLabelBytesDominates(t *testing.T) {
	grant := Label{
		Version:    CurrentVersion,
		Components: []Component{{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 0}}},
	}
	row := Label{
		Version:    CurrentVersion,
		Components: []Component{{ID: 1, Type: ComponentTypeArray, Value: ArrayValue{Ordinal: 3}}},
	}

	grantBytes, err := EncodeLabel(grant)
	require.NoError(t, err)
	rowBytes, err := EncodeLabel(row)
	require.NoError(t, err)

	ok, err := LabelBytesDominates(grantBytes, rowBytes)
	require.NoError(t, err)
	require.True(t, ok)

	_, err = LabelBytesDominates([]byte{CurrentVersion}, rowBytes)
	require.ErrorIs(t, err, ErrTruncatedLabel)
}

func buildLabelBytes(t *testing.T, version uint8, comps []rawComponent) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteByte(version)
	buf.WriteByte(byte(len(comps)))
	for _, comp := range comps {
		buf.WriteByte(comp.id)
		buf.WriteByte(byte(comp.compTyp))
		buf.Write(comp.payload)
	}
	return buf.Bytes()
}

func arrayPayload(t *testing.T, ordinal uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, writeUvarint(&buf, ordinal))
	return buf.Bytes()
}

func setPayload(t *testing.T, values ...uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, writeUvarint(&buf, uint64(len(values))))
	for _, v := range values {
		require.NoError(t, writeUvarint(&buf, v))
	}
	return buf.Bytes()
}

func treePayload(t *testing.T, inVal, outVal uint64) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, writeUvarint(&buf, inVal))
	require.NoError(t, writeUvarint(&buf, outVal))
	return buf.Bytes()
}

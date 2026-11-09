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

package model

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
)

const (
	// CurrentVersion is the current label encoding version.
	CurrentVersion uint8 = 1
	maxComponents        = 255
)

// ComponentType identifies the encoded component value kind.
type ComponentType byte

const (
	// ComponentTypeArray represents a single ordinal value.
	ComponentTypeArray ComponentType = 1
	// ComponentTypeSet represents a set of ordinals.
	ComponentTypeSet ComponentType = 2
	// ComponentTypeTree represents a tree range value.
	ComponentTypeTree ComponentType = 3
)

// ErrInvalidVersion and related errors indicate label encoding failures.
var (
	ErrInvalidVersion        = errors.New("invalid label version")
	ErrTooManyComponents     = errors.New("too many components")
	ErrDuplicateComponentID  = errors.New("duplicate component id")
	ErrUnsupportedComponent  = errors.New("unsupported component type")
	ErrInvalidComponentValue = errors.New("invalid component value")
	ErrInvalidComponentType  = errors.New("invalid component type")
	ErrInvalidPayload        = errors.New("invalid component payload")
	ErrTruncatedLabel        = errors.New("truncated label")
	ErrTrailingBytes         = errors.New("trailing bytes in label")
	ErrVersionMismatch       = errors.New("label version mismatch")
	ErrComponentTypeMismatch = errors.New("component type mismatch")
	ErrEmptySetValues        = errors.New("empty set values")
	ErrDuplicateSetValue     = errors.New("duplicate set value")
	ErrInvalidTreeRange      = errors.New("invalid tree range")
	ErrInvalidComponentID    = errors.New("invalid component id")
)

// ComponentValue describes the decoded component payload.
type ComponentValue interface {
	componentType() ComponentType
}

// ArrayValue holds the ordinal for an array component.
type ArrayValue struct {
	Ordinal uint64
}

func (ArrayValue) componentType() ComponentType { return ComponentTypeArray }

// SetValue holds the ordinals for a set component.
type SetValue struct {
	Values []uint64
}

func (SetValue) componentType() ComponentType { return ComponentTypeSet }

// TreeValue holds the range for a tree component.
type TreeValue struct {
	In  uint64
	Out uint64
}

func (TreeValue) componentType() ComponentType { return ComponentTypeTree }

// Component describes a decoded component entry.
type Component struct {
	ID    uint8
	Type  ComponentType
	Value ComponentValue
}

// Label describes a decoded label payload.
type Label struct {
	Version    uint8
	Components []Component
}

// EncodeLabel encodes a label as: [version][component_count] then for each component:
// [component_id:1][type:1][payload]. Payload formats:
// ARRAY -> uvarint ordinal; SET -> uvarint count + uvarint value per entry (sorted, unique);
// TREE -> uvarint in + uvarint out.
func EncodeLabel(label Label) ([]byte, error) {
	if label.Version == 0 {
		return nil, ErrInvalidVersion
	}
	if len(label.Components) > maxComponents {
		return nil, ErrTooManyComponents
	}
	seen := make(map[uint8]struct{}, len(label.Components))
	var buf bytes.Buffer
	buf.WriteByte(label.Version)
	buf.WriteByte(byte(len(label.Components)))
	for _, comp := range label.Components {
		if comp.ID == 0 {
			return nil, ErrInvalidComponentID
		}
		if comp.Value == nil {
			return nil, ErrInvalidComponentValue
		}
		if comp.Value.componentType() != comp.Type {
			return nil, ErrInvalidComponentType
		}
		if _, ok := seen[comp.ID]; ok {
			return nil, ErrDuplicateComponentID
		}
		seen[comp.ID] = struct{}{}
		buf.WriteByte(comp.ID)
		buf.WriteByte(byte(comp.Type))
		if err := encodeComponentValue(&buf, comp); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// DecodeLabel decodes a self-describing label.
func DecodeLabel(data []byte) (Label, error) {
	if len(data) < 2 {
		return Label{}, ErrTruncatedLabel
	}
	version := data[0]
	if version == 0 {
		return Label{}, ErrInvalidVersion
	}
	compCnt := int(data[1])
	dec := decoder{data: data, offset: 2}
	components := make([]Component, 0, compCnt)
	seen := make(map[uint8]struct{}, compCnt)
	for i := 0; i < compCnt; i++ {
		componentID, err := dec.readByte()
		if err != nil {
			return Label{}, err
		}
		if componentID == 0 {
			return Label{}, ErrInvalidComponentID
		}
		if _, ok := seen[componentID]; ok {
			return Label{}, ErrDuplicateComponentID
		}
		seen[componentID] = struct{}{}
		compTypeRaw, err := dec.readByte()
		if err != nil {
			return Label{}, err
		}
		compType := ComponentType(compTypeRaw)
		value, err := decodeComponentValue(compType, &dec)
		if err != nil {
			return Label{}, err
		}
		components = append(components, Component{
			ID:    componentID,
			Type:  compType,
			Value: value,
		})
	}
	if dec.offset != len(data) {
		return Label{}, ErrTrailingBytes
	}
	return Label{
		Version:    version,
		Components: components,
	}, nil
}

// LabelDominates compares two decoded labels.
func LabelDominates(grant, row Label) (bool, error) {
	if grant.Version != row.Version {
		return false, ErrVersionMismatch
	}
	grantMap := make(map[uint8]Component, len(grant.Components))
	for _, comp := range grant.Components {
		grantMap[comp.ID] = comp
	}
	for _, rowComp := range row.Components {
		grantComp, ok := grantMap[rowComp.ID]
		if !ok {
			return false, nil
		}
		if grantComp.Type != rowComp.Type {
			return false, ErrComponentTypeMismatch
		}
		dominates, err := componentDominates(grantComp, rowComp)
		if err != nil {
			return false, err
		}
		if !dominates {
			return false, nil
		}
	}
	return true, nil
}

// LabelBytesDominates decodes and compares two encoded labels.
func LabelBytesDominates(grantBytes, rowBytes []byte) (bool, error) {
	grant, err := DecodeLabel(grantBytes)
	if err != nil {
		return false, err
	}
	row, err := DecodeLabel(rowBytes)
	if err != nil {
		return false, err
	}
	return LabelDominates(grant, row)
}

func componentDominates(grant, row Component) (bool, error) {
	switch grant.Type {
	case ComponentTypeArray:
		return grant.Value.(ArrayValue).Ordinal <= row.Value.(ArrayValue).Ordinal, nil
	case ComponentTypeSet:
		return setDominates(grant.Value.(SetValue), row.Value.(SetValue)), nil
	case ComponentTypeTree:
		grantVal := grant.Value.(TreeValue)
		rowVal := row.Value.(TreeValue)
		return grantVal.In <= rowVal.In && grantVal.Out >= rowVal.Out, nil
	default:
		return false, ErrUnsupportedComponent
	}
}

func setDominates(grant, row SetValue) bool {
	if len(row.Values) == 0 {
		return true
	}
	grantSet := make(map[uint64]struct{}, len(grant.Values))
	for _, v := range grant.Values {
		grantSet[v] = struct{}{}
	}
	for _, v := range row.Values {
		if _, ok := grantSet[v]; !ok {
			return false
		}
	}
	return true
}

func encodeComponentValue(buf *bytes.Buffer, comp Component) error {
	switch comp.Type {
	case ComponentTypeArray:
		val := comp.Value.(ArrayValue)
		return writeUvarint(buf, val.Ordinal)
	case ComponentTypeSet:
		val := comp.Value.(SetValue)
		values, err := canonicalizeSetValues(val.Values)
		if err != nil {
			return err
		}
		if err := writeUvarint(buf, uint64(len(values))); err != nil {
			return err
		}
		for _, v := range values {
			if err := writeUvarint(buf, v); err != nil {
				return err
			}
		}
		return nil
	case ComponentTypeTree:
		val := comp.Value.(TreeValue)
		if val.In > val.Out {
			return ErrInvalidTreeRange
		}
		if err := writeUvarint(buf, val.In); err != nil {
			return err
		}
		return writeUvarint(buf, val.Out)
	default:
		return ErrUnsupportedComponent
	}
}

func decodeComponentValue(compType ComponentType, dec *decoder) (ComponentValue, error) {
	switch compType {
	case ComponentTypeArray:
		ordinal, err := dec.readUvarint()
		if err != nil {
			return nil, err
		}
		return ArrayValue{Ordinal: ordinal}, nil
	case ComponentTypeSet:
		values, err := decodeSetValues(dec)
		if err != nil {
			return nil, err
		}
		return SetValue{Values: values}, nil
	case ComponentTypeTree:
		return decodeTreeValue(dec)
	default:
		return nil, ErrUnsupportedComponent
	}
}

func decodeSetValues(dec *decoder) ([]uint64, error) {
	count, err := dec.readUvarint()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, ErrEmptySetValues
	}
	values := make([]uint64, 0, count)
	seen := make(map[uint64]struct{}, count)
	for i := 0; i < int(count); i++ {
		value, err := dec.readUvarint()
		if err != nil {
			return nil, err
		}
		if _, ok := seen[value]; ok {
			return nil, ErrDuplicateSetValue
		}
		seen[value] = struct{}{}
		values = append(values, value)
	}
	return values, nil
}

func decodeTreeValue(dec *decoder) (ComponentValue, error) {
	inVal, err := dec.readUvarint()
	if err != nil {
		return nil, err
	}
	outVal, err := dec.readUvarint()
	if err != nil {
		return nil, err
	}
	if inVal > outVal {
		return nil, ErrInvalidTreeRange
	}
	return TreeValue{In: inVal, Out: outVal}, nil
}

func canonicalizeSetValues(values []uint64) ([]uint64, error) {
	if len(values) == 0 {
		return nil, ErrEmptySetValues
	}
	cpy := append([]uint64(nil), values...)
	sort.Slice(cpy, func(i, j int) bool {
		return cpy[i] < cpy[j]
	})
	for i := 1; i < len(cpy); i++ {
		if cpy[i] == cpy[i-1] {
			return nil, ErrDuplicateSetValue
		}
	}
	return cpy, nil
}

type decoder struct {
	data   []byte
	offset int
}

func (d *decoder) readByte() (byte, error) {
	if d.offset >= len(d.data) {
		return 0, ErrTruncatedLabel
	}
	b := d.data[d.offset]
	d.offset++
	return b, nil
}

func (d *decoder) readUvarint() (uint64, error) {
	if d.offset >= len(d.data) {
		return 0, ErrTruncatedLabel
	}
	val, n := binary.Uvarint(d.data[d.offset:])
	if n <= 0 {
		return 0, ErrInvalidPayload
	}
	d.offset += n
	return val, nil
}

func writeUvarint(buf *bytes.Buffer, value uint64) error {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], value)
	_, err := buf.Write(tmp[:n])
	return err
}

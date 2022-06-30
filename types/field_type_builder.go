// Copyright 2022 PingCAP, Inc.
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

package types

// FieldTypeBuilder constructor
type FieldTypeBuilder struct {
	ft FieldType
}

// NewFieldTypeBuilder will allocate the builder on the heap.
func NewFieldTypeBuilder() *FieldTypeBuilder {
	return &FieldTypeBuilder{}
}

// GetType returns type of the ft
func (b *FieldTypeBuilder) GetType() byte {
	return b.ft.GetType()
}

// GetFlag returns flag of the ft
func (b *FieldTypeBuilder) GetFlag() uint {
	return b.ft.GetFlag()
}

// GetFlen returns length of the ft
func (b *FieldTypeBuilder) GetFlen() int {
	return b.ft.GetFlen()
}

// GetDecimal returns decimals of the ft
func (b *FieldTypeBuilder) GetDecimal() int {
	return b.ft.GetDecimal()
}

// GetCharset returns charset of the ft
func (b *FieldTypeBuilder) GetCharset() string {
	return b.ft.GetCharset()
}

// GetCollate returns collation of the ft
func (b *FieldTypeBuilder) GetCollate() string {
	return b.ft.GetCollate()
}

// SetType sets type of the ft
func (b *FieldTypeBuilder) SetType(tp byte) *FieldTypeBuilder {
	b.ft.SetType(tp)
	return b
}

// SetFlag sets flag of the ft
func (b *FieldTypeBuilder) SetFlag(flag uint) *FieldTypeBuilder {
	b.ft.SetFlag(flag)
	return b
}

// AddFlag adds flag to the ft
func (b *FieldTypeBuilder) AddFlag(flag uint) *FieldTypeBuilder {
	b.ft.AddFlag(flag)
	return b
}

// ToggleFlag toggles flag of the ft
func (b *FieldTypeBuilder) ToggleFlag(flag uint) *FieldTypeBuilder {
	b.ft.ToggleFlag(flag)
	return b
}

// DelFlag deletes flag of the ft
func (b *FieldTypeBuilder) DelFlag(flag uint) *FieldTypeBuilder {
	b.ft.DelFlag(flag)
	return b
}

// SetFlen sets length of the ft
func (b *FieldTypeBuilder) SetFlen(flen int) *FieldTypeBuilder {
	b.ft.SetFlen(flen)
	return b
}

// SetDecimal sets decimal of the ft
func (b *FieldTypeBuilder) SetDecimal(decimal int) *FieldTypeBuilder {
	b.ft.SetDecimal(decimal)
	return b
}

// SetCharset sets charset of the ft
func (b *FieldTypeBuilder) SetCharset(charset string) *FieldTypeBuilder {
	b.ft.SetCharset(charset)
	return b
}

// SetCollate sets collation of the ft
func (b *FieldTypeBuilder) SetCollate(collate string) *FieldTypeBuilder {
	b.ft.SetCollate(collate)
	return b
}

// SetElems sets elements of the ft
func (b *FieldTypeBuilder) SetElems(elems []string) *FieldTypeBuilder {
	b.ft.SetElems(elems)
	return b
}

// Build returns the ft
func (b *FieldTypeBuilder) Build() FieldType {
	return b.ft
}

// BuildP returns pointer of the ft
func (b *FieldTypeBuilder) BuildP() *FieldType {
	return &b.ft
}

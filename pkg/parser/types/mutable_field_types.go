// Copyright 2024 PingCAP, Inc.
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

var _ MutableFieldType = &FieldType{}

// MutableFieldType refs to a mutable field type, with all SET interface integrated.
// SET interface.
// when calling MutableFieldType.SET(xxx), it resolve the func name successfully and change happens
// on deepest implemented core *FieldType structure
//
// MutableFieldType.SET(xxx) -------------------------> *FieldType (where the change happens)
//
// GET interface.
// when calling MutableFieldType.GET(xxx), itself can't resolve the function call of GET cause
// lack of definition, so it will resort to inner wrapped interface ImmutableFieldType to explain
// this and it works. Finally, the read happens to the deepest implemented core structure --- *FieldType.
//
// MutableFieldType.GET(xxx) (resolve func fail)
//
//	+-------------> ImmutableFieldType.GET(xxx) (resolve func success)
//	                         +---------------------------> *FieldType (where the read happens)
type MutableFieldType interface {
	ImmutableFieldType
	ImmutableRef() ImmutableFieldType

	SetType(tp byte)
	SetFlag(flag uint)
	AddFlag(flag uint)
	AndFlag(flag uint)
	ToggleFlag(flag uint)
	DelFlag(flag uint)
	SetFlen(flen int)
	SetFlenUnderLimit(flen int)
	SetDecimal(decimal int)
	SetDecimalUnderLimit(decimal int)
	UpdateFlenAndDecimalUnderLimit(old *FieldType, deltaDecimal int, deltaFlen int)
	SetCharset(charset string)
	SetCollate(collate string)
	SetElems(elems []string)
	SetElem(idx int, element string)
	SetArray(array bool)
	ArrayType() *FieldType
	SetElemWithIsBinaryLit(idx int, element string, isBinaryLit bool)
	CleanElemIsBinaryLit()
	Clone() *FieldType
	Init(tp byte)
	UnmarshalJSON(data []byte) error
}

func NewMutableFieldType(tp byte) MutableFieldType {
	return NewFieldType(tp)
}

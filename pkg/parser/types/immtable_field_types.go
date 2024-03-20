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

import (
	"io"

	"github.com/pingcap/tidb/pkg/parser/format"
)

var _ ImmutableFieldType = &FieldType{}

// ImmutableFieldType refs to an immutable field type, with all GET type interface integrated.
// SET interface.
// can't be resolved anywhere, lack of definition.
//
// GET interface.
// when calling the ImmutableFieldType.GET(xxx), it resolve the func name successfully itself. The read
// happens to the deepest internal implemented core structure --- *FieldType.
//
// ImmutableFieldType.GET(xxx) (resolve func success itself) ------------> *FieldType (where the read happens)
type ImmutableFieldType interface {
	// MutableCopy copy and return a new basic ft with COW.
	// MutableCopy is different from MutableRef, the inside immutable ref is not point
	// to original one. In otherwise, it's a brand-new one copied elements from the old.
	MutableCopy() MutableFieldType
	// MutableRef downcast the current interface pointer as son.
	MutableRef() MutableFieldType

	IsDecimalValid() bool
	IsVarLengthType() bool
	GetType() byte
	GetFlag() uint
	GetFlen() int
	GetDecimal() int
	GetCharset() string
	GetCollate() string
	GetElems() []string
	IsArray() bool
	GetElem(idx int) string
	GetElemIsBinaryLit(idx int) bool
	Equal(other *FieldType) bool
	PartialEqual(other *FieldType, unsafe bool) bool
	EvalType() EvalType
	Hybrid() bool
	CompactStr() string
	InfoSchemaStr() string
	String() string
	Restore(ctx *format.RestoreCtx) error
	RestoreAsCastType(ctx *format.RestoreCtx, explicitCharset bool)
	FormatAsCastType(w io.Writer, explicitCharset bool)
	StorageLength() int
	MarshalJSON() ([]byte, error)
	MemoryUsage() (sum int64)
}

// NewImmutableFieldType create an immutable field type.
func NewImmutableFieldType(tp byte) ImmutableFieldType {
	return NewFieldType(tp)
}

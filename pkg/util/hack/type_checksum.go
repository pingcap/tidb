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

package hack

import (
	"encoding/binary"
	"hash"
	"hash/crc64"
	"reflect"
)

// CalcTypeChecksum return a value which can represent the change of the type T
// It's used in the test to make sure the developer can read some comments or
// make some related changes when the type T is changed.
func CalcTypeChecksum[T any]() uint64 {
	// Cannot use the type of `var t T` directly, because when the type T is an interface,
	// the type will be `nil`. The following method is copied from `reflect.TypeFor`. When
	// the go compiler version is upgraded, it can be replace by `reflect.TypeFor[T]` instead.
	typ := reflect.TypeOf((*T)(nil)).Elem()
	h := crc64.New(crc64.MakeTable(crc64.ECMA))

	hashType(h, typ, nil)

	return h.Sum64()
}

func hashType(h hash.Hash64, typ reflect.Type, visited map[reflect.Type]struct{}) {
	if typ == nil {
		h.Write([]byte("nil type"))
		return
	}
	if visited == nil {
		visited = make(map[reflect.Type]struct{})
	}
	_, ok := visited[typ]
	if ok {
		h.Write([]byte("recursive"))
		h.Write([]byte(typ.String()))
		return
	}
	visited[typ] = struct{}{}

	switch typ.Kind() {
	case reflect.Struct:
		h.Write([]byte("struct"))
		for i := 0; i < typ.NumField(); i++ {
			h.Write([]byte(typ.Field(i).Name))
			hashType(h, typ.Field(i).Type, visited)
		}
	case reflect.Interface:
		h.Write([]byte("interface"))
		for i := 0; i < typ.NumMethod(); i++ {
			h.Write([]byte(typ.Method(i).Name))
			hashType(h, typ.Method(i).Type, visited)
		}
	case reflect.Func:
		h.Write([]byte("func"))
		for i := 0; i < typ.NumIn(); i++ {
			hashType(h, typ.In(i), visited)
		}
		for i := 0; i < typ.NumOut(); i++ {
			hashType(h, typ.Out(i), visited)
		}
	case reflect.Array:
		h.Write([]byte("array"))
		h.Write(binary.LittleEndian.AppendUint64(nil, uint64(typ.Len())))
		hashType(h, typ.Elem(), visited)
	case reflect.Slice:
		h.Write([]byte("slice"))
		hashType(h, typ.Elem(), visited)
	case reflect.Chan:
		h.Write([]byte("chan"))
		hashType(h, typ.Elem(), visited)
	case reflect.Map:
		h.Write([]byte("map"))
		hashType(h, typ.Key(), visited)
		hashType(h, typ.Elem(), visited)
	case reflect.Pointer:
		h.Write([]byte("ptr"))
		hashType(h, typ.Elem(), visited)
	default:
		h.Write([]byte(typ.String()))
	}
}

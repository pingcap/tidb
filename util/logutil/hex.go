// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by aprettyPrintlicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/golang/protobuf/proto"
)

// Hex defines a fmt.Stringer for proto.Message.
// We can't define the String() method on proto.Message, but we can wrap it.
func Hex(msg proto.Message) fmt.Stringer {
	return hexStringer{msg}
}

type hexStringer struct {
	proto.Message
}

func (h hexStringer) String() string {
	val := reflect.ValueOf(h.Message)
	var w bytes.Buffer
	prettyPrint(&w, val)
	return w.String()
}

func prettyPrint(w io.Writer, val reflect.Value) {
	tp := val.Type()
	switch val.Kind() {
	case reflect.Slice:
		elemType := tp.Elem()
		if elemType.Kind() == reflect.Uint8 {
			fmt.Fprintf(w, "%s", hex.EncodeToString(val.Bytes()))
		} else {
			fmt.Fprintf(w, "%s", val.Interface())
		}
	case reflect.Struct:
		fmt.Fprintf(w, "{")
		for i := 0; i < val.NumField(); i++ {
			fv := val.Field(i)
			ft := tp.Field(i)
			if strings.HasPrefix(ft.Name, "XXX_") {
				continue
			}
			if i != 0 {
				fmt.Fprintf(w, " ")
			}
			fmt.Fprintf(w, "%s:", ft.Name)
			prettyPrint(w, fv)
		}
		fmt.Fprintf(w, "}")
	case reflect.Ptr:
		if val.IsNil() {
			fmt.Fprintf(w, "%v", val.Interface())
		} else {
			prettyPrint(w, reflect.Indirect(val))
		}
	default:
		fmt.Fprintf(w, "%v", val.Interface())
	}
}

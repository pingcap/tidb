// Copyright 2019 PingCAP, Inc.
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

func ToHex(msg proto.Message) string {
	val := reflect.ValueOf(msg)
	var w bytes.Buffer
	pp(&w, val)
	return w.String()
}

func pp(w io.Writer, val reflect.Value) {
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
		w.Write([]byte{'{'})
		for i := 0; i < val.NumField(); i++ {
			fv := val.Field(i)
			ft := tp.Field(i)
			if strings.HasPrefix(ft.Name, "XXX_") {
				continue
			}
			if i != 0 {
				w.Write([]byte{' '})
			}
			fmt.Fprintf(w, "%s", ft.Name)
			w.Write([]byte{':'})
			pp(w, fv)
		}
		w.Write([]byte{'}'})
	case reflect.Ptr:
		if val.IsNil() {
			fmt.Fprintf(w, "%v", val.Interface())
		} else {
			pp(w, reflect.Indirect(val))
		}
	default:
		fmt.Fprintf(w, "%v", val.Interface())
	}
}

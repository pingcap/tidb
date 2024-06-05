// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package t

import (
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb/pkg/util/linter/nomarshal"
)

type noMarshalStruct struct {
	nomarshal.NoMarshal

	TestField int
}

type embedStruct struct {
	noMarshalStruct
}

// noMarshalInterface is a case which shows that an interface which doesn't implement `json.Marshaler` explicitly is not allowed
type noMarshalInterface any

// TestFunc is the function for test
func TestFunc() {
	s1 := noMarshalStruct{}
	_, err := json.Marshal(s1) // want `Invalid type t.noMarshalStruct: .*`
	fmt.Println(err)

	s2 := embedStruct{}
	_, err = json.Marshal(s2) // want `Invalid type t.embedStruct: .*`
	fmt.Println(err)

	var iface noMarshalInterface
	_, err = json.Marshal(iface) // want `Invalid type t.noMarshalInterface: .*`
	fmt.Println(err)
}

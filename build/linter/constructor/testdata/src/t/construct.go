// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/util/linter/constructor"
)

// StructWithSpecificConstructor is a struct with `Constructor`
type StructWithSpecificConstructor struct {
	_ constructor.Constructor `ctor:"NewStructWithSpecificConstructor,AnotherConstructor"`

	otherField string
}

// NewStructWithSpecificConstructor creates a new `StructWithSpecificConstructor`
// this function should work fine
func NewStructWithSpecificConstructor() *StructWithSpecificConstructor {
	_ = new(StructWithSpecificConstructor)
	var _ StructWithSpecificConstructor
	return &StructWithSpecificConstructor{}
}

// AnotherConstructor gives another constructor of StructWithSpecificConstructor
func AnotherConstructor() *StructWithSpecificConstructor {
	return &StructWithSpecificConstructor{}
}

func otherFunction() {
	_ = NewStructWithSpecificConstructor()
	_ = AnotherConstructor()

	_ = StructWithSpecificConstructor{}     // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`
	_ = &StructWithSpecificConstructor{}    // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`
	_ = []StructWithSpecificConstructor{{}} // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`
	_ = []*StructWithSpecificConstructor{
		{otherField: "a"}, // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`
		{},                // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`
	}

	// anonymous struct
	_ = struct { // want `struct can only be constructed in constructors NewStructWithSpecificConstructor`
		_ constructor.Constructor `ctor:"NewStructWithSpecificConstructor"`
	}{}

	// new
	_ = new(StructWithSpecificConstructor) // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`

	// var
	var _ StructWithSpecificConstructor  // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`
	var _ *StructWithSpecificConstructor // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`

	type compositeImplicitInitiate1 struct {
		StructWithSpecificConstructor
	}
	var _ compositeImplicitInitiate1    // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`
	_ = new(compositeImplicitInitiate1) // want `struct can only be constructed in constructors NewStructWithSpecificConstructor, AnotherConstructor`

	// pointer field is allowed
	type compositeImplicitInitiate2 struct {
		*StructWithSpecificConstructor
	}
	var _ compositeImplicitInitiate2
}

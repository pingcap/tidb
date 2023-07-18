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

import (
	"strings"

	"github.com/pingcap/tidb/parser/model"
)

// CIStr is new string
type CIStr string

// String is to return string
func (c CIStr) String() string {
	return string(c)
}

// ToModel converts to CIStr in model
func (c CIStr) ToModel() model.CIStr {
	return model.NewCIStr(string(c))
}

// EqString reports whether CIStr is equal to a string
func (c CIStr) EqString(s string) bool {
	return strings.EqualFold(string(c), s)
}

// EqModel reports whether CIStr is equal to another CIStr
func (c CIStr) EqModel(s model.CIStr) bool {
	return strings.EqualFold(string(c), s.L)
}

// Eq reports whether CIStr is equal to another CIStr
func (c CIStr) Eq(s CIStr) bool {
	return strings.EqualFold(string(c), string(s))
}

// HasPrefix implement strings.HasPrefix
func (c CIStr) HasPrefix(s string) bool {
	return strings.HasPrefix(strings.ToLower(string(c)), strings.ToLower(s))
}

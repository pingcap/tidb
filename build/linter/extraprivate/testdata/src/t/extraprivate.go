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

type structWithExtraPrivateField struct {
	field1 int `extraprivate:""`
	field2 int
}

func readField1(s structWithExtraPrivateField) int {
	return s.field1 // want `access to extraprivate field outside its struct's methods`
}

func readField1Ptr(s *structWithExtraPrivateField) int {
	return s.field1 // want `access to extraprivate field outside its struct's methods`
}

func readField2(s structWithExtraPrivateField) int {
	return s.field2
}
func readField2Ptr(s *structWithExtraPrivateField) int {
	return s.field2
}

func (s structWithExtraPrivateField) Field1() int {
	return s.field1
}

func (s *structWithExtraPrivateField) Field1Ptr() int {
	return s.field1
}

func (s structWithExtraPrivateField) Field2() int {
	return s.field2
}

func (s *structWithExtraPrivateField) Field2Ptr() int {
	return s.field2
}

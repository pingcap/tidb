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

package partialjson

import (
	"encoding/json"
)

// ExportedTopLevelJSONTokenIter is an exported version of topLevelJSONTokenIter for testing purposes.
type ExportedTopLevelJSONTokenIter struct {
	iter *topLevelJSONTokenIter
}

// ExportedNewTopLevelJSONTokenIter creates a new ExportedTopLevelJSONTokenIter for testing.
func ExportedNewTopLevelJSONTokenIter(content []byte) *ExportedTopLevelJSONTokenIter {
	return &ExportedTopLevelJSONTokenIter{
		iter: newTopLevelJSONTokenIter(content),
	}
}

// Next calls the internal next method.
func (e *ExportedTopLevelJSONTokenIter) Next(discard bool) ([]json.Token, error) {
	return e.iter.next(discard)
}

// ReadName calls the internal readName method.
func (e *ExportedTopLevelJSONTokenIter) ReadName() (string, error) {
	return e.iter.readName()
}

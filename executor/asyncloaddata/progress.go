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

package asyncloaddata

import "encoding/json"

// Progress is the progress of the LOAD DATA task.
type Progress struct {
	// SourceFileSize is the size of the source file in bytes. When we can't get
	// the size of the source file, it will be set to -1.
	SourceFileSize int64
	// LoadedFileSize is the size of the data that has been loaded in bytes.
	LoadedFileSize int64
	// LoadedRowCnt is the number of rows that has been loaded.
	LoadedRowCnt uint64
}

// String implements the fmt.Stringer interface.
func (p Progress) String() string {
	bs, _ := json.Marshal(p)
	return string(bs)
}

// ProgressFromJSON creates a Progress from a JSON string.
func ProgressFromJSON(bs []byte) (Progress, error) {
	var p Progress
	err := json.Unmarshal(bs, &p)
	return p, err
}

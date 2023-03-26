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

import (
	"encoding/json"

	"go.uber.org/atomic"
)

// Progress is the progress of the LOAD DATA task.
type Progress struct {
	// SourceFileSize is the size of the source file in bytes. When we can't get
	// the size of the source file, it will be set to -1.
	// Currently the value is read by seek(0, end), when LOAD DATA LOCAL we wrap
	// SimpleSeekerOnReadCloser on MySQL client connection which doesn't support
	// it.
	SourceFileSize int64
	// LoadedFileSizeSetter is used by multiple workers to record the progress,
	// its value is loaded into LoadedFileSize when String().
	LoadedFileSizeSetter atomic.Int64 `json:"-"`
	// LoadedRowCntSetter is used by multiple workers to record the progress,
	// its value is loaded into LoadedRowCnt when String().
	LoadedRowCntSetter atomic.Uint64 `json:"-"`
	// LoadedFileSize is the size of the data that will be loaded in bytes. It's
	// larger than the actual loaded data size, but due to the fact that reading
	// is once a block and a block may generate multiple tasks that are
	// concurrently executed, we can't know the actual loaded data size easily.
	LoadedFileSize int64
	// LoadedRowCnt is the number of rows that has been loaded.
	LoadedRowCnt uint64
}

// String implements the fmt.Stringer interface.
func (p *Progress) String() string {
	p.LoadedFileSize = p.LoadedFileSizeSetter.Load()
	p.LoadedRowCnt = p.LoadedRowCntSetter.Load()
	bs, _ := json.Marshal(p)
	return string(bs)
}

// ProgressFromJSON creates a Progress from a JSON string.
func ProgressFromJSON(bs []byte) (Progress, error) {
	var p Progress
	err := json.Unmarshal(bs, &p)
	return p, err
}

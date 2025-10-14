// Copyright 2025 PingCAP, Inc.
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

package txn

import "encoding/json"

type taskMeta struct {
	SQL string `json:"sql"`
}

func NewTxnTask(sql string) ([]byte, error) {
	meta := &taskMeta{
		SQL: sql,
	}
	bytes, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

type subtaskMeta struct {
	SQL string `json:"sql"`
}

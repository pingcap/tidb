// Copyright 2026 PingCAP, Inc.
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

package importinto

import (
	"encoding/json"

	"github.com/pingcap/errors"
)

// MViewBuildReadTS extracts the persisted snapshot from a DDL-owned import task.
func MViewBuildReadTS(data []byte) (uint64, error) {
	// Read only the fields used by DDL. Import planning and worker metadata can
	// evolve independently of task recovery in the DDL owner.
	var meta struct {
		DDLJobID int64
		Plan     struct {
			QuerySource *struct {
				ReadTS uint64
			}
		}
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		return 0, err
	}
	if meta.DDLJobID == 0 || meta.Plan.QuerySource == nil || meta.Plan.QuerySource.ReadTS == 0 {
		return 0, errors.New("MV build task has no read timestamp")
	}
	return meta.Plan.QuerySource.ReadTS, nil
}

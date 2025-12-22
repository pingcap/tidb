// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package external

import (
	"fmt"
	"strconv"
)

const (
	// DataKVGroup is the group name of the sorted kv for data.
	// index kv will be stored in a group named as index-id.
	DataKVGroup = "data"
)

// IndexID2KVGroup converts index id to kv group name.
// exported for test.
func IndexID2KVGroup(indexID int64) string {
	return fmt.Sprintf("%d", indexID)
}

// KVGroup2IndexID converts index kv group name to index id.
func KVGroup2IndexID(kvGroup string) (int64, error) {
	return strconv.ParseInt(kvGroup, 10, 64)
}

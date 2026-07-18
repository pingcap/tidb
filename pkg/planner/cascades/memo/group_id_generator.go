// Copyright 2024 PingCAP, Inc.
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

package memo

// GroupID is the unique id for a group.
type GroupID uint64

// GroupIDGenerator is used to generate group id.
type GroupIDGenerator struct {
	id uint64
}

// NextGroupID generates the next group id.
// It is not thread-safe, since memo optimizing is also in one thread.
func (gi *GroupIDGenerator) NextGroupID() GroupID {
	gi.id++
	return GroupID(gi.id)
}

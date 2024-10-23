// Copyright 2024 PingCAP, Inc.
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

package runaway

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordKey(t *testing.T) {
	// Initialize test data
	key1 := recordKey{
		ResourceGroupName: "group1",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
	}
	// key2 is identical to key1
	key2 := recordKey{
		ResourceGroupName: "group1",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
	}
	key3 := recordKey{
		ResourceGroupName: "group2",
	}
	// Test Hash method
	hash1 := key1.Hash()
	hash2 := key2.Hash()
	hash3 := key3.Hash()

	assert.Equal(t, hash1, hash2, "Hashes should be equal for identical keys")
	assert.NotEqual(t, hash1, hash3, "Hashes should not be equal for different keys")

	// Test MapKey method
	recordMap := make(map[recordKey]*Record)
	record1 := &Record{
		ResourceGroupName: "group1",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
	}
	// put key1 into recordMap
	recordMap[key1] = record1
	assert.Len(t, recordMap, 1, "recordMap should have 1 element")
	assert.Equal(t, "group1", recordMap[key1].ResourceGroupName, "Repeats should not be updated")
	assert.Equal(t, 0, recordMap[key1].Repeats, "Repeats should be incremented")
	// key2 is identical to key1, so we can use key2 to get the record
	assert.NotNil(t, recordMap[key1], "key1 should exist in recordMap")
	assert.NotNil(t, recordMap[key2], "key2 should exist in recordMap")
	assert.Nil(t, recordMap[key3], "key3 should not exist in recordMap")

	// put key2 into recordMap and update Repeats
	record2 := &Record{
		ResourceGroupName: "group1",
		Repeats:           1,
	}
	recordMap[key2] = record2
	assert.Len(t, recordMap, 1, "recordMap should have 1 element")
	assert.Equal(t, 1, recordMap[key1].Repeats, "Repeats should be updated")
	// change ResourceGroupName of key2 will not affect key1
	key2.ResourceGroupName = "group2"
	record3 := &Record{
		ResourceGroupName: "group2",
	}
	recordMap[key2] = record3
	assert.Len(t, recordMap, 2, "recordMap should have 1 element")
	assert.Equal(t, "group1", recordMap[key1].ResourceGroupName, "Repeats should not be updated")
	assert.Equal(t, "group2", recordMap[key2].ResourceGroupName, "ResourceGroupName should be updated")
}

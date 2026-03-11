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
	// Identical inputs produce the same key.
	key1 := newRecordKey("group1", "digest1", "plan1", "match1")
	key2 := newRecordKey("group1", "digest1", "plan1", "match1")
	assert.Equal(t, key1, key2, "identical inputs should produce the same key")

	// Different inputs produce different keys.
	key3 := newRecordKey("group2", "digest1", "plan1", "match1")
	assert.NotEqual(t, key1, key3, "different resource group should produce different key")

	key4 := newRecordKey("group1", "digest2", "plan1", "match1")
	assert.NotEqual(t, key1, key4, "different sql digest should produce different key")

	key5 := newRecordKey("group1", "digest1", "plan2", "match1")
	assert.NotEqual(t, key1, key5, "different plan digest should produce different key")

	key6 := newRecordKey("group1", "digest1", "plan1", "match2")
	assert.NotEqual(t, key1, key6, "different match type should produce different key")

	// Empty fields.
	key7 := newRecordKey("", "", "", "")
	key8 := newRecordKey("", "", "", "")
	assert.Equal(t, key7, key8, "empty inputs should produce the same key")
	assert.NotEqual(t, key1, key7, "empty vs non-empty should differ")

	// Separator prevents cross-field collisions: ("ab","c") vs ("a","bc").
	keyA := newRecordKey("ab", "c", "", "")
	keyB := newRecordKey("a", "bc", "", "")
	assert.NotEqual(t, keyA, keyB, "separator should prevent cross-field collision")

	// Map dedup behavior.
	recordMap := make(map[recordKey]*Record)
	record1 := &Record{
		ResourceGroupName: "group1",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
		Match:             "match1",
		Repeats:           1,
	}
	recordMap[key1] = record1
	assert.Len(t, recordMap, 1)
	assert.NotNil(t, recordMap[key2], "same key should find existing record")
	assert.Nil(t, recordMap[key3], "different key should not find record")

	// Overwrite with same key increments repeats (simulates mergeFn).
	record2 := &Record{
		ResourceGroupName: "group1",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
		Match:             "match1",
		Repeats:           2,
	}
	recordMap[key2] = record2
	assert.Len(t, recordMap, 1, "same key should not increase map size")
	assert.Equal(t, 2, recordMap[key1].Repeats, "value should be updated")

	// Different key adds new entry.
	record3 := &Record{
		ResourceGroupName: "group2",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
		Match:             "match1",
		Repeats:           1,
	}
	recordMap[key3] = record3
	assert.Len(t, recordMap, 2)
}

// Copyright 2017 The Kubernetes Authors.
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
// Modifications:
// 1. Use "github.com/stretchr/testify/require" to do assertions.
// 2. Test max heap instead of min heap.
// 3. Add a test for the peak API.
// 4. Add a test for the IsEmpty API.
// 5. Remove concurrency and thread-safety tests.
// 6. Add a test for the Len API.
// 7. Remove the BulkAdd related tests.

package priorityqueue

import (
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/stretchr/testify/require"
)

type testHeapObject struct {
	tableID int64
	val     float64
}

func (t testHeapObject) IsValidToAnalyze(sctx sessionctx.Context) (bool, string) {
	panic("implement me")
}
func (t testHeapObject) Analyze(statsHandle statstypes.StatsHandle, sysProcTracker sysproctrack.Tracker) error {
	panic("implement me")
}
func (t testHeapObject) SetWeight(weight float64) {
	panic("implement me")
}
func (t testHeapObject) GetWeight() float64 {
	return t.val
}
func (t testHeapObject) HasNewlyAddedIndex() bool {
	panic("implement me")
}
func (t testHeapObject) GetIndicators() Indicators {
	panic("implement me")
}
func (t testHeapObject) SetIndicators(indicators Indicators) {
	panic("implement me")
}
func (t testHeapObject) GetTableID() int64 {
	return t.tableID
}
func (t testHeapObject) RegisterSuccessHook(hook JobHook) {
	panic("implement me")
}
func (t testHeapObject) RegisterFailureHook(hook JobHook) {
	panic("implement me")
}
func (t testHeapObject) String() string {
	panic("implement me")
}
func mkHeapObj(
	tableID int64,
	val float64,
) testHeapObject {
	return testHeapObject{
		tableID: tableID,
		val:     val,
	}
}

func TestHeap_AddOrUpdate(t *testing.T) {
	h := newHeap()
	err := h.addOrUpdate(mkHeapObj(1, 10))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(2, 1))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(3, 11))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(4, 30))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(1, 13)) // This updates object with tableID 1.
	require.NoError(t, err)

	item, err := h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(4), item.GetTableID())

	item, err = h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(1), item.GetTableID())

	err = h.delete(mkHeapObj(3, 11)) // Deletes object with tableID 3.
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(1, 14)) // Updates object with tableID 1.
	require.NoError(t, err)

	item, err = h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(1), item.GetTableID())

	item, err = h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(2), item.GetTableID())
}

func TestHeapEmptyPop(t *testing.T) {
	h := newHeap()
	_, err := h.pop()
	require.EqualError(t, err, "heap is empty")
}

func TestHeap_Delete(t *testing.T) {
	h := newHeap()
	err := h.addOrUpdate(mkHeapObj(1, 10))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(2, 1))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(3, 31))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(4, 11))
	require.NoError(t, err)

	err = h.delete(mkHeapObj(3, 31))
	require.NoError(t, err)

	item, err := h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(4), item.GetTableID())

	err = h.addOrUpdate(mkHeapObj(5, 30))
	require.NoError(t, err)

	err = h.delete(mkHeapObj(2, 1))
	require.NoError(t, err)

	item, err = h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(5), item.GetTableID())

	item, err = h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(1), item.GetTableID())

	require.Equal(t, 0, h.len())
}

func TestHeap_Update(t *testing.T) {
	h := newHeap()
	err := h.addOrUpdate(mkHeapObj(1, 10))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(2, 1))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(3, 31))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(4, 11))
	require.NoError(t, err)

	err = h.update(mkHeapObj(4, 50))
	require.NoError(t, err)
	require.Equal(t, int64(4), h.data.queue[0])

	item, err := h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(4), item.GetTableID())

	err = h.update(mkHeapObj(2, 100))
	require.NoError(t, err)
	require.Equal(t, int64(2), h.data.queue[0])
}

func TestHeap_Get(t *testing.T) {
	h := newHeap()
	err := h.addOrUpdate(mkHeapObj(1, 10))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(2, 1))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(3, 31))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(4, 11))
	require.NoError(t, err)

	obj, exists, err := h.Get(mkHeapObj(4, 0))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(4), obj.GetTableID())

	_, exists, err = h.Get(mkHeapObj(5, 0))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestHeap_GetByKey(t *testing.T) {
	h := newHeap()
	err := h.addOrUpdate(mkHeapObj(1, 10))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(2, 1))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(3, 31))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(4, 11))
	require.NoError(t, err)

	obj, exists, err := h.getByKey(4)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(4), obj.GetTableID())

	_, exists, err = h.getByKey(5)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestHeap_List(t *testing.T) {
	h := newHeap()
	list := h.list()
	require.Empty(t, list)

	items := map[int64]float64{
		1: 10,
		2: 1,
		3: 30,
		4: 11,
		5: 30,
	}
	for k, v := range items {
		h.addOrUpdate(mkHeapObj(k, v))
	}
	list = h.list()
	require.Len(t, list, len(items))
	for _, obj := range list {
		require.Equal(t, items[obj.GetTableID()], obj.GetWeight())
	}
}

func TestHeap_ListKeys(t *testing.T) {
	h := newHeap()
	list := h.ListKeys()
	require.Empty(t, list)

	items := map[int64]float64{
		1: 10,
		2: 1,
		3: 30,
		4: 11,
		5: 30,
	}
	for k, v := range items {
		h.addOrUpdate(mkHeapObj(k, v))
	}
	list = h.ListKeys()
	require.Len(t, list, len(items))
	for _, key := range list {
		_, ok := items[key]
		require.True(t, ok)
	}
}

func TestHeap_Peek(t *testing.T) {
	h := newHeap()
	_, err := h.peek()
	require.EqualError(t, err, "heap is empty")

	err = h.addOrUpdate(mkHeapObj(1, 10))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(2, 1))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(3, 31))
	require.NoError(t, err)
	err = h.addOrUpdate(mkHeapObj(4, 11))
	require.NoError(t, err)

	item, err := h.peek()
	require.NoError(t, err)
	require.Equal(t, int64(3), item.GetTableID())

	item, err = h.pop()
	require.NoError(t, err)
	require.Equal(t, int64(3), item.GetTableID())
}

func TestHeap_IsEmpty(t *testing.T) {
	h := newHeap()
	require.True(t, h.isEmpty())

	err := h.addOrUpdate(mkHeapObj(1, 10))
	require.NoError(t, err)
	require.False(t, h.isEmpty())

	_, err = h.pop()
	require.NoError(t, err)
	require.True(t, h.isEmpty())
}

func TestHeap_Len(t *testing.T) {
	h := newHeap()
	require.Zero(t, h.len())

	err := h.addOrUpdate(mkHeapObj(1, 10))
	require.NoError(t, err)
	require.Equal(t, 1, h.len())

	_, err = h.pop()
	require.NoError(t, err)
	require.Zero(t, h.len())
}

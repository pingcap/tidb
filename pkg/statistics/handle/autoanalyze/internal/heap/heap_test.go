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

package heap

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testHeapObjectKeyFunc(obj testHeapObject) (string, error) {
	return obj.name, nil
}

type testHeapObject struct {
	name string
	val  int
}

func mkHeapObj(name string, val int) testHeapObject {
	return testHeapObject{name: name, val: val}
}

// max heap
func compareInts(val1 testHeapObject, val2 testHeapObject) bool {
	return val1.val > val2.val
}

func TestHeapBasic(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	var wg sync.WaitGroup
	wg.Add(2)
	const amount = 500
	var i, u int
	go func() {
		for i = 1; i <= amount; i++ {
			h.Add(mkHeapObj(string([]rune{'a', rune(i)}), i))
		}
		wg.Done()
	}()
	go func() {
		for u = amount; u > 0; u-- {
			h.Add(mkHeapObj(string([]rune{'b', rune(u)}), u))
		}
		wg.Done()
	}()
	wg.Wait()

	prevNum := 1000
	for i := 0; i < amount*2; i++ {
		obj, err := h.Pop()
		num := obj.val
		require.NoError(t, err)
		require.LessOrEqual(t, num, prevNum, "Items should be in descending order")
		prevNum = num
	}
}

func TestHeap_Add(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("baz", 11))
	h.Add(mkHeapObj("zab", 30))
	h.Add(mkHeapObj("foo", 13)) // This updates "foo".

	item, err := h.Pop()
	require.NoError(t, err)
	require.Equal(t, 30, item.val)

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 13, item.val)

	h.Delete(mkHeapObj("baz", 11)) // Nothing is deleted.
	h.Add(mkHeapObj("foo", 14))    // foo is updated.

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 14, item.val)

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 1, item.val)
}

func TestHeap_BulkAdd(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	const amount = 500
	go func() {
		var l []testHeapObject
		for i := 1; i <= amount; i++ {
			l = append(l, mkHeapObj(string([]rune{'a', rune(i)}), i))
		}
		h.BulkAdd(l)
	}()
	prevNum := 501
	for i := 0; i < amount; i++ {
		obj, err := h.Pop()
		require.NoError(t, err)
		num := obj.val
		require.Less(t, num, prevNum, "Items should be in descending order")
		prevNum = num
	}
}

func TestHeapEmptyPop(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	go func() {
		time.Sleep(1 * time.Second)
		h.Close()
	}()
	_, err := h.Pop()
	require.EqualError(t, err, closedMsg)
}

func TestHeap_AddIfNotPresent(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.AddIfNotPresent(mkHeapObj("foo", 10))
	h.AddIfNotPresent(mkHeapObj("bar", 1))
	h.AddIfNotPresent(mkHeapObj("baz", 11))
	h.AddIfNotPresent(mkHeapObj("zab", 30))
	h.AddIfNotPresent(mkHeapObj("foo", 13)) // This is not added.

	require.Len(t, h.data.items, 4)
	require.Equal(t, 10, h.data.items["foo"].obj.val)

	item, err := h.Pop()
	require.NoError(t, err)
	require.Equal(t, 30, item.val)

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 11, item.val)

	h.AddIfNotPresent(mkHeapObj("bar", 14))

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 10, item.val)

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 1, item.val)
}

func TestHeap_Delete(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	err := h.Delete(mkHeapObj("bal", 200))
	require.NoError(t, err)

	item, err := h.Pop()
	require.NoError(t, err)
	require.Equal(t, 11, item.val)

	h.Add(mkHeapObj("zab", 30))
	h.Add(mkHeapObj("faz", 30))
	l := h.data.Len()

	err = h.Delete(mkHeapObj("non-existent", 10))
	require.Error(t, err)
	require.Equal(t, l, h.data.Len())

	err = h.Delete(mkHeapObj("bar", 31))
	require.NoError(t, err)

	err = h.Delete(mkHeapObj("zab", 30))
	require.NoError(t, err)

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 30, item.val)

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 10, item.val)

	require.Equal(t, 0, h.data.Len())
}

func TestHeap_Update(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	h.Update(mkHeapObj("baz", 50))
	require.Equal(t, "baz", h.data.queue[0])
	require.Equal(t, 0, h.data.items["baz"].index)

	item, err := h.Pop()
	require.NoError(t, err)
	require.Equal(t, 50, item.val)

	h.Update(mkHeapObj("bar", 100))
	require.Equal(t, "bar", h.data.queue[0])
	require.Equal(t, 0, h.data.items["bar"].index)
}

func TestHeap_Get(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	obj, exists, err := h.Get(mkHeapObj("baz", 0))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 11, obj.val)

	_, exists, err = h.Get(mkHeapObj("non-existing", 0))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestHeap_GetByKey(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	obj, exists, err := h.GetByKey("baz")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, 11, obj.val)

	_, exists, err = h.GetByKey("non-existing")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestHeap_Close(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))

	require.False(t, h.IsClosed())
	h.Close()
	require.True(t, h.IsClosed())
}

func TestHeap_List(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	list := h.List()
	require.Empty(t, list)

	items := map[string]int{
		"foo": 10,
		"bar": 1,
		"bal": 30,
		"baz": 11,
		"faz": 30,
	}
	for k, v := range items {
		h.Add(mkHeapObj(k, v))
	}
	list = h.List()
	require.Len(t, list, len(items))
	for _, obj := range list {
		heapObj := obj
		v, ok := items[heapObj.name]
		require.True(t, ok)
		require.Equal(t, v, heapObj.val)
	}
}

func TestHeap_ListKeys(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	list := h.ListKeys()
	require.Empty(t, list)

	items := map[string]int{
		"foo": 10,
		"bar": 1,
		"bal": 30,
		"baz": 11,
		"faz": 30,
	}
	for k, v := range items {
		h.Add(mkHeapObj(k, v))
	}
	list = h.ListKeys()
	require.Len(t, list, len(items))
	for _, key := range list {
		_, ok := items[key]
		require.True(t, ok)
	}
}

func TestHeap_Peek(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	_, err := h.Peek()
	require.EqualError(t, err, "heap is empty")

	h.Add(mkHeapObj("foo", 10))
	h.Add(mkHeapObj("bar", 1))
	h.Add(mkHeapObj("bal", 31))
	h.Add(mkHeapObj("baz", 11))

	item, err := h.Peek()
	require.NoError(t, err)
	require.Equal(t, 31, item.val)

	item, err = h.Pop()
	require.NoError(t, err)
	require.Equal(t, 31, item.val)
}

func TestHeapAddAfterClose(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	h.Close()
	err := h.Add(mkHeapObj("test", 1))
	require.EqualError(t, err, closedMsg)

	err = h.AddIfNotPresent(mkHeapObj("test", 1))
	require.EqualError(t, err, closedMsg)

	err = h.BulkAdd([]testHeapObject{mkHeapObj("test", 1)})
	require.EqualError(t, err, closedMsg)
}

func TestHeap_IsEmpty(t *testing.T) {
	h := NewHeap(testHeapObjectKeyFunc, compareInts)
	require.True(t, h.IsEmpty())

	h.Add(mkHeapObj("foo", 10))
	require.False(t, h.IsEmpty())

	h.Pop()
	require.True(t, h.IsEmpty())
}

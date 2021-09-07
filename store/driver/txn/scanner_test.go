// Copyright 2021 PingCAP, Inc.
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

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/assert"
)

func r(key, value string) *kv.Entry {
	bKey := []byte(key)
	bValue := []byte(value)
	if value == "nil" {
		bValue = nil
	}

	return &kv.Entry{Key: bKey, Value: bValue}
}

type TestIter struct {
	kv.Iterator
	t                *testing.T
	nextErr          error
	closed           bool
	failOnMultiClose bool
}

func newDefaultTestIterFromRecords(t *testing.T, records []*kv.Entry) *TestIter {
	return &TestIter{
		t:                t,
		Iterator:         mock.NewSliceIter(records),
		failOnMultiClose: true,
	}
}

func (i *TestIter) InjectNextError(err error) {
	i.nextErr = err
}

func (i *TestIter) FailOnMultiClose(fail bool) {
	i.failOnMultiClose = fail
}

func (i *TestIter) Next() error {
	if i.nextErr != nil {
		return i.nextErr
	}
	return i.Iterator.Next()
}

func (i *TestIter) Close() {
	if i.closed && i.failOnMultiClose {
		assert.FailNow(i.t, "Multi close iter")
	}
	i.closed = true
	i.Iterator.Close()
}

func (i *TestIter) Closed() bool {
	return i.closed
}

func checkExpectedIterData(t *testing.T, expected []*kv.Entry, iter kv.Iterator) {
	for _, entry := range expected {
		assert.True(t, iter.Valid())
		assert.Equal(t, entry.Key, iter.Key())
		assert.Equal(t, entry.Value, iter.Value())
		err := iter.Next()
		assert.Nil(t, err)
	}

	assert.False(t, iter.Valid())
	err := iter.Next()
	assert.NotNil(t, err)
	checkCloseIter(t, iter)
}

func checkCloseIter(t *testing.T, iter kv.Iterator) {
	iter.Close()
	assert.False(t, iter.Valid())
	err := iter.Next()
	assert.NotNil(t, err)
}

func TestOneByOneIter(t *testing.T) {
	allocatedIters := make([]*TestIter, 0)

	makeCreateIterFunc := func(records []*kv.Entry, fail bool) createIterFunc {
		return func() (kv.Iterator, error) {
			if fail {
				return nil, errors.New("fail")
			}

			iter := newDefaultTestIterFromRecords(t, records)
			allocatedIters = append(allocatedIters, iter)
			return iter, nil
		}
	}

	checkCreatedIterClosed := func() {
		for _, iter := range allocatedIters {
			assert.True(t, iter.Closed())
		}
	}

	defer checkCreatedIterClosed()
	slices := [][]*kv.Entry{
		{
			r("k1", "v1"),
			r("k5", "v3"),
		},
		{
			r("k2", "v2"),
			r("k4", "v4"),
		},
		{
			// empty
		},
		{
			r("k3", "v3"),
			r("k6", "v6"),
		},
	}

	funcs := make([]createIterFunc, 0)
	for _, s := range slices {
		funcs = append(funcs, makeCreateIterFunc(s, false))
	}

	// test for normal iter
	oneByOne, err := newOneByOneIter(funcs)
	assert.Nil(t, err)
	expected := make([]*kv.Entry, 0)
	expected = append(expected, slices[0]...)
	expected = append(expected, slices[1]...)
	expected = append(expected, slices[2]...)
	expected = append(expected, slices[3]...)
	checkExpectedIterData(t, expected, oneByOne)

	// test for close
	oneByOne, err = newOneByOneIter(funcs)
	assert.Nil(t, err)
	checkCloseIter(t, oneByOne)

	// test for one inner iter
	oneByOne, err = newOneByOneIter(funcs[:1])
	assert.Nil(t, err)
	expected = make([]*kv.Entry, 0)
	expected = append(expected, slices[0]...)
	checkExpectedIterData(t, expected, oneByOne)

	// test for empty iter
	oneByOne, err = newOneByOneIter(funcs[2:3])
	assert.Nil(t, err)
	checkExpectedIterData(t, nil, oneByOne)

	// test for first func return error
	funcsWithFirstError := make([]createIterFunc, 0)
	for i, s := range slices {
		funcsWithFirstError = append(funcsWithFirstError, makeCreateIterFunc(s, i == 0))
	}
	oneByOne, err = newOneByOneIter(funcsWithFirstError)
	assert.Nil(t, oneByOne)
	assert.NotNil(t, err)

	// test for second func return error
	funcsWithSecondError := make([]createIterFunc, 0)
	for i, s := range slices {
		funcsWithSecondError = append(funcsWithSecondError, makeCreateIterFunc(s, i == 1))
	}
	oneByOne, err = newOneByOneIter(funcsWithSecondError)
	assert.Nil(t, err)
	for i, record := range slices[0] {
		assert.True(t, oneByOne.Valid())
		assert.Equal(t, record.Key, oneByOne.Key())
		assert.Equal(t, record.Value, oneByOne.Value())
		err = oneByOne.Next()
		if i == len(slices[0])-1 {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
		}
	}
	assert.False(t, oneByOne.Valid())
	// check error will close all inner iters
	checkCreatedIterClosed()
	// check close after error close only once
	oneByOne.Close()

	// test for error occurs when iter.Next()
	oneByOne, err = newOneByOneIter(funcs)
	assert.Nil(t, err)
	assert.True(t, oneByOne.Valid())
	injectedErr := errors.New("error")
	oneByOne.curIter.(*TestIter).InjectNextError(injectedErr)
	err = oneByOne.Next()
	assert.Equal(t, injectedErr, err)
	assert.False(t, oneByOne.Valid())
	checkCreatedIterClosed()
	oneByOne.Close()
}

func TestFilterEmptyValueIter(t *testing.T) {
	cases := []struct {
		data []*kv.Entry
	}{
		{data: nil},
		{data: []*kv.Entry{
			r("k1", "v1"),
		}},
		{data: []*kv.Entry{
			r("k1", ""),
		}},
		{data: []*kv.Entry{
			r("k1", "v1"),
			r("k2", "v2"),
			r("k3", ""),
		}},
		{data: []*kv.Entry{
			r("k1", "v1"),
			r("k2", "v2"),
			r("k3", ""),
			r("k4", "v4"),
		}},
		{data: []*kv.Entry{
			r("k1", "v1"),
			r("k2", "v2"),
			r("k3", ""),
			r("k4", ""),
			r("k5", "v5"),
		}},
		{data: []*kv.Entry{
			r("k1", "v1"),
			r("k2", "v2"),
			r("k3", ""),
			r("k4", ""),
			r("k5", ""),
		}},
	}

	for _, c := range cases {
		data := make([]*kv.Entry, 0)
		for _, entry := range c.data {
			if len(entry.Value) > 0 {
				data = append(data, entry)
			}
		}

		testIter := newDefaultTestIterFromRecords(t, c.data)
		iter, err := filterEmptyValue(testIter)
		assert.Nil(t, err)
		checkExpectedIterData(t, data, iter)
		assert.True(t, testIter.Closed())

		testIter = newDefaultTestIterFromRecords(t, c.data)
		iter, err = filterEmptyValue(testIter)
		assert.Nil(t, err)
		checkCloseIter(t, iter)
		assert.True(t, testIter.Closed())
	}

	// test error for create
	testIter := newDefaultTestIterFromRecords(t, []*kv.Entry{r("k1", ""), r("k2", "v2")})
	injectedErr := errors.New("error")
	testIter.InjectNextError(injectedErr)
	iter, err := filterEmptyValue(testIter)
	assert.Nil(t, iter)
	assert.Equal(t, injectedErr, err)
	// after error testIter.Close() should not cause multi close
	testIter.Close()

	// test error for next
	testIter = newDefaultTestIterFromRecords(t, []*kv.Entry{r("k1", "v1"), r("k2", "v2")})
	injectedErr = errors.New("error")
	testIter.InjectNextError(injectedErr)
	iter, err = filterEmptyValue(testIter)
	assert.Nil(t, err)
	assert.True(t, iter.Valid())
	err = iter.Next()
	assert.Equal(t, injectedErr, err)
	assert.False(t, iter.Valid())
	// inner iter.Close() should not be invoked after error
	assert.False(t, testIter.Closed())
	iter.Close()
	assert.True(t, testIter.Closed())
}

func TestLowerBoundReverseIter(t *testing.T) {
	cases := []struct {
		data       []*kv.Entry
		lowerBound kv.Key
		expected   []*kv.Entry
	}{
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: nil,
			expected: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
		},
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: kv.Key("k1"),
			expected: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
		},
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: kv.Key("k2"),
			expected: []*kv.Entry{
				r("k2", "v2"),
			},
		},
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: kv.Key("k3"),
			expected:   nil,
		},
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: kv.Key("k0"),
			expected: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
		},
		{
			data: []*kv.Entry{
				r("k2", "v2"),
				r("k1", "v1"),
			},
			lowerBound: kv.Key("k11"),
			expected: []*kv.Entry{
				r("k2", "v2"),
			},
		},
		{
			data:       nil,
			lowerBound: nil,
			expected:   nil,
		},
		{
			data:       nil,
			lowerBound: kv.Key("k1"),
			expected:   nil,
		},
	}

	for _, c := range cases {
		testIter := newDefaultTestIterFromRecords(t, c.data)
		iter := newLowerBoundReverseIter(testIter, c.lowerBound)
		checkExpectedIterData(t, c.expected, iter)
		assert.True(t, testIter.Closed())

		testIter = newDefaultTestIterFromRecords(t, c.data)
		iter = newLowerBoundReverseIter(testIter, c.lowerBound)
		checkCloseIter(t, iter)
		assert.True(t, testIter.Closed())
	}

	// test for iter.Next() error
	testIter := newDefaultTestIterFromRecords(t, []*kv.Entry{
		r("k2", "v2"),
		r("k1", "v1"),
	})
	injectedErr := errors.New("error")
	testIter.InjectNextError(injectedErr)
	iter := newLowerBoundReverseIter(testIter, kv.Key("k11"))
	assert.True(t, iter.Valid())
	err := iter.Next()
	assert.Equal(t, injectedErr, err)
	assert.False(t, iter.Valid())
	// inner iter.Close() should not be invoked after error
	assert.False(t, testIter.Closed())
	iter.Close()
	assert.True(t, testIter.Closed())
}

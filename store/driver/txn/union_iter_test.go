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

func TestUnionIter(t *testing.T) {
	// test iter normal cases, snap iter become invalid before dirty iter
	snapRecords := []*kv.Entry{
		r("k00", "v0"),
		r("k01", "v1"),
		r("k03", "v3"),
		r("k06", "v6"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
	}

	dirtyRecords := []*kv.Entry{
		r("k00", ""),
		r("k000", ""),
		r("k03", "x3"),
		r("k05", "x5"),
		r("k07", "x7"),
		r("k08", "x8"),
	}

	assertUnionIter(t, dirtyRecords, nil, []*kv.Entry{
		r("k03", "x3"),
		r("k05", "x5"),
		r("k07", "x7"),
		r("k08", "x8"),
	})

	assertUnionIter(t, nil, snapRecords, []*kv.Entry{
		r("k00", "v0"),
		r("k01", "v1"),
		r("k03", "v3"),
		r("k06", "v6"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
	})

	assertUnionIter(t, dirtyRecords, snapRecords, []*kv.Entry{
		r("k01", "v1"),
		r("k03", "x3"),
		r("k05", "x5"),
		r("k06", "v6"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
	})

	// test iter normal cases, dirty iter become invalid before snap iter
	dirtyRecords = []*kv.Entry{
		r("k03", "x3"),
		r("k05", "x5"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k17", "x17"),
		r("k18", "x18"),
	}

	assertUnionIter(t, dirtyRecords, snapRecords, []*kv.Entry{
		r("k00", "v0"),
		r("k01", "v1"),
		r("k03", "x3"),
		r("k05", "x5"),
		r("k06", "v6"),
		r("k07", "x7"),
		r("k08", "x8"),
		r("k10", "v10"),
		r("k12", "v12"),
		r("k15", "v15"),
		r("k16", "v16"),
		r("k17", "x17"),
		r("k18", "x18"),
	})
}

func TestUnionIterErrors(t *testing.T) {
	cases := []struct {
		dirty                     []*kv.Entry
		snap                      []*kv.Entry
		nextTimesWhenErrorHappens int
		injectDirtyError          error
		injectSnapError           error
	}{
		// dirtyIt.Next() for NewUnionIter
		{
			dirty:            []*kv.Entry{r("k0", ""), r("k1", "v1")},
			injectDirtyError: errors.New("error"),
		},
		{
			dirty:            []*kv.Entry{r("k0", ""), r("k1", "v1")},
			snap:             []*kv.Entry{r("k1", "x1")},
			injectDirtyError: errors.New("error"),
		},
		// snapIt.Next() for NewUnionIter
		{
			dirty:           []*kv.Entry{r("k0", "v1"), r("k1", "v1")},
			snap:            []*kv.Entry{r("k0", "x0"), r("k1", "x1")},
			injectSnapError: errors.New("error"),
		},
		// both dirtyIt.Next() and snapIt.Next() for NewUnionIter
		{
			dirty:            []*kv.Entry{r("k0", ""), r("k1", "v1")},
			snap:             []*kv.Entry{r("k0", "x0"), r("k1", "x1")},
			injectDirtyError: errors.New("error"),
		},
		{
			dirty:           []*kv.Entry{r("k0", ""), r("k1", "v1")},
			snap:            []*kv.Entry{r("k0", "x0"), r("k1", "x1")},
			injectSnapError: errors.New("error"),
		},
		// dirtyIt.Next() for iter.Next()
		{
			dirty:                     []*kv.Entry{r("k0", "v0"), r("k1", "v1")},
			snap:                      []*kv.Entry{r("k1", "x1")},
			nextTimesWhenErrorHappens: 1,
			injectDirtyError:          errors.New("error"),
		},
		// snapIt.Next() for iter.Next()
		{
			dirty:                     []*kv.Entry{r("k1", "v1")},
			snap:                      []*kv.Entry{r("k0", "x0"), r("k1", "x1")},
			nextTimesWhenErrorHappens: 1,
			injectSnapError:           errors.New("error"),
		},
		// both dirtyIt.Next() and snapIt.Next() for iter.Next()
		{
			dirty:                     []*kv.Entry{r("k0", "v0"), r("k1", "v1")},
			snap:                      []*kv.Entry{r("k1", "x1")},
			nextTimesWhenErrorHappens: 1,
			injectDirtyError:          errors.New("error"),
		},
		{
			dirty:                     []*kv.Entry{r("k1", "v1")},
			snap:                      []*kv.Entry{r("k0", "x0"), r("k1", "x1")},
			nextTimesWhenErrorHappens: 1,
			injectSnapError:           errors.New("error"),
		},
	}

	for _, ca := range cases {
		dirtyIter := mock.NewMockIterFromRecords(t, ca.dirty, true)
		snapIter := mock.NewMockIterFromRecords(t, ca.snap, true)
		var iter *UnionIter
		var err error

		if ca.nextTimesWhenErrorHappens > 0 {
			iter, err = NewUnionIter(dirtyIter, snapIter, false)
			assert.NotNil(t, iter)
			assert.Nil(t, err)
			for i := 0; i < ca.nextTimesWhenErrorHappens-1; i++ {
				err = iter.Next()
				assert.Nil(t, err)
			}
			dirtyIter.InjectNextError(ca.injectDirtyError)
			snapIter.InjectNextError(ca.injectSnapError)
			err = iter.Next()
			assert.NotNil(t, err)
		} else {
			dirtyIter.InjectNextError(ca.injectDirtyError)
			snapIter.InjectNextError(ca.injectSnapError)
			iter, err = NewUnionIter(dirtyIter, snapIter, false)
			assert.Nil(t, iter)
			assert.NotNil(t, err)
		}

		if ca.injectDirtyError != nil {
			assert.Equal(t, ca.injectDirtyError, err)
		}

		if ca.injectSnapError != nil {
			assert.Equal(t, ca.injectSnapError, err)
		}

		assert.False(t, dirtyIter.Closed())
		assert.False(t, snapIter.Closed())
		if iter != nil {
			assertClose(t, iter)
		}
	}
}

func assertUnionIter(t *testing.T, dirtyRecords, snapRecords, expected []*kv.Entry) {
	dirtyIter := mock.NewMockIterFromRecords(t, dirtyRecords, true)
	snapIter := mock.NewMockIterFromRecords(t, snapRecords, true)
	iter, err := NewUnionIter(dirtyIter, snapIter, false)
	assert.Nil(t, err)
	assertIter(t, iter, expected)
	assertClose(t, iter)

	// assert reverse is true
	dirtyIter = mock.NewMockIterFromRecords(t, reverseRecords(dirtyRecords), true)
	snapIter = mock.NewMockIterFromRecords(t, reverseRecords(snapRecords), true)
	iter, err = NewUnionIter(dirtyIter, snapIter, true)
	assert.Nil(t, err)
	assertIter(t, iter, reverseRecords(expected))
	assertClose(t, iter)
}

func assertIter(t *testing.T, iter *UnionIter, expected []*kv.Entry) {
	records := make([]*kv.Entry, 0, len(expected))
	for iter.Valid() {
		records = append(records, &kv.Entry{Key: iter.Key(), Value: iter.Value()})
		err := iter.Next()
		assert.Nil(t, err)
	}
	assert.Equal(t, len(expected), len(records))
	for idx, record := range records {
		assert.Equal(t, record.Key, expected[idx].Key)
		assert.Equal(t, record.Value, expected[idx].Value)
	}
}

func assertClose(t *testing.T, iter *UnionIter) {
	dirtyIt := iter.dirtyIt.(*mock.MockedIter)
	snapIt := iter.snapshotIt.(*mock.MockedIter)
	assert.False(t, dirtyIt.Closed())
	assert.False(t, snapIt.Closed())
	iter.Close()
	assert.True(t, dirtyIt.Closed())
	assert.True(t, snapIt.Closed())
	// multi close is safe
	iter.Close()
}

func reverseRecords(records []*kv.Entry) []*kv.Entry {
	reversed := make([]*kv.Entry, 0)
	for i := range records {
		reversed = append(reversed, records[len(records)-i-1])
	}
	return reversed
}

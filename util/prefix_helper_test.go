// Copyright 2015 PingCAP, Inc.
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

package util_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	startIndex = 0
	testCount  = 12
	testPow    = 10
)

func TestPrefix(t *testing.T) {
	s, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := s.Close()
		require.NoError(t, err)
	}()

	ctx := &mockContext{10000000, make(map[fmt.Stringer]interface{}), s, nil}
	err = ctx.fillTxn()
	require.NoError(t, err)
	txn, err := ctx.GetTxn()
	require.NoError(t, err)
	err = util.DelKeyWithPrefix(txn, encodeInt(ctx.prefix))
	require.NoError(t, err)
	err = ctx.CommitTxn()
	require.NoError(t, err)

	txn, err = s.Begin()
	require.NoError(t, err)
	k := []byte("key100jfowi878230")
	err = txn.Set(k, []byte(`val32dfaskli384757^*&%^`))
	require.NoError(t, err)
	err = util.ScanMetaWithPrefix(txn, k, func(kv.Key, []byte) bool {
		return true
	})
	require.NoError(t, err)
	err = util.ScanMetaWithPrefix(txn, k, func(kv.Key, []byte) bool {
		return false
	})
	require.NoError(t, err)
	err = util.DelKeyWithPrefix(txn, []byte("key"))
	require.NoError(t, err)
	_, err = txn.Get(context.TODO(), k)
	assert.True(t, terror.ErrorEqual(kv.ErrNotExist, err))

	err = txn.Commit(context.Background())
	require.NoError(t, err)
}

func TestPrefixFilter(t *testing.T) {
	rowKey := []byte(`test@#$%l(le[0]..prefix) 2uio`)
	rowKey[8] = 0x00
	rowKey[9] = 0x00
	f := util.RowKeyPrefixFilter(rowKey)
	assert.False(t, f(append(rowKey, []byte("akjdf3*(34")...)))
	assert.True(t, f([]byte("sjfkdlsaf")))
}

type mockContext struct {
	prefix int
	values map[fmt.Stringer]interface{}
	kv.Storage
	txn kv.Transaction
}

func (c *mockContext) SetValue(key fmt.Stringer, value interface{}) {
	c.values[key] = value
}

func (c *mockContext) Value(key fmt.Stringer) interface{} {
	value := c.values[key]
	return value
}

func (c *mockContext) ClearValue(key fmt.Stringer) {
	delete(c.values, key)
}

func (c *mockContext) GetTxn() (kv.Transaction, error) {
	var err error
	c.txn, err = c.Begin()
	if err != nil {
		return nil, err
	}

	return c.txn, nil
}

func (c *mockContext) fillTxn() error {
	if c.txn == nil {
		return nil
	}

	var err error
	for i := startIndex; i < testCount; i++ {
		val := encodeInt(i + (c.prefix * testPow))
		err = c.txn.Set(val, val)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *mockContext) CommitTxn() error {
	if c.txn == nil {
		return nil
	}
	return c.txn.Commit(context.Background())
}

func encodeInt(n int) []byte {
	return []byte(fmt.Sprintf("%d", n))
}

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

package kv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestBackOff(t *testing.T) {
	mustBackOff(t, 1, 2)
	mustBackOff(t, 2, 4)
	mustBackOff(t, 3, 8)
	mustBackOff(t, 100000, 100)
}

func mustBackOff(t *testing.T, cnt uint, sleep int) {
	assert.LessOrEqual(t, BackOff(cnt), sleep*int(time.Millisecond))
}

func TestRetryExceedCountError(t *testing.T) {
	defer func(cnt uint) {
		maxRetryCnt = cnt
	}(maxRetryCnt)

	maxRetryCnt = 5
	err := RunInNewTxn(context.Background(), &mockStorage{}, true, func(ctx context.Context, txn Transaction) error {
		return nil
	})
	assert.NotNil(t, err)

	err = RunInNewTxn(context.Background(), &mockStorage{}, true, func(ctx context.Context, txn Transaction) error {
		return ErrTxnRetryable
	})
	assert.NotNil(t, err)

	err = RunInNewTxn(context.Background(), &mockStorage{}, true, func(ctx context.Context, txn Transaction) error {
		return errors.New("do not retry")
	})
	assert.NotNil(t, err)

	var cfg InjectionConfig
	err1 := errors.New("foo")
	cfg.SetGetError(err1)
	cfg.SetCommitError(err1)
	storage := NewInjectedStore(newMockStorage(), &cfg)
	err = RunInNewTxn(context.Background(), storage, true, func(ctx context.Context, txn Transaction) error {
		return nil
	})
	assert.NotNil(t, err)
}

var wrapStoreInterTxnTSTest = wrapStoreInterTxnTS
var getGlobalInnerTxnTsBoxTest = getGlobalInnerTxnTsBox
var wrapDeleteInterTxnTSTest = wrapDeleteInterTxnTS

// fmt.Println("ts0:", ts0, " ts1:", ts1, " ts2:", ts2, " ts3:", ts3, " lowLimit:", lowLimit, " minStartTS:", minStartTS, " newMinStartTS:", newMinStartTS)
// fmt.Println("innerTxnStartTsMap:", ib.innerTxnStartTsMap)
func TestInnerTxnStartTsBox(t *testing.T) {
	InitInnerTxnStartTsBox()

	ib := getGlobalInnerTxnTsBoxTest()

	// case1: store and delete
	wrapStoreInterTxnTSTest(5)
	ts, ok := ib.innerTxnStartTsMap[5]
	assert.Equal(t, true, ok)
	assert.Equal(t, ts, (uint64)(5))

	wrapDeleteInterTxnTSTest(5)
	ts, ok = ib.innerTxnStartTsMap[5]
	assert.Equal(t, false, ok)
	assert.Equal(t, ts, (uint64)(0))

	// case2: test for WrapGetMinStartTs
	tm0 := time.Date(2022, time.March, 8, 12, 10, 01, 0, time.UTC)
	ts0 := oracle.GoTimeToTS(tm0)
	tm1 := time.Date(2022, time.March, 10, 12, 10, 01, 0, time.UTC)
	ts1 := oracle.GoTimeToTS(tm1)
	tm2 := time.Date(2022, time.March, 10, 12, 14, 03, 0, time.UTC)
	ts2 := oracle.GoTimeToTS(tm2)
	tm3 := time.Date(2022, time.March, 10, 12, 14, 05, 0, time.UTC)
	ts3 := oracle.GoTimeToTS(tm3)
	tm4 := time.Date(2022, time.March, 10, 12, 15, 0, 0, time.UTC)
	lowLimit := oracle.GoTimeToLowerLimitStartTS(tm4, 24*60*60*1000)
	minStartTS := oracle.GoTimeToTS(tm4)

	wrapStoreInterTxnTSTest(ts0)
	wrapStoreInterTxnTSTest(ts1)
	wrapStoreInterTxnTSTest(ts2)
	wrapStoreInterTxnTSTest(ts3)

	newMinStartTS := WrapGetMinStartTs(tm4, lowLimit, minStartTS)
	require.Equal(t, newMinStartTS, ts1)

	wrapDeleteInterTxnTSTest(ts0)
	wrapDeleteInterTxnTSTest(ts1)
	wrapDeleteInterTxnTSTest(ts2)
	wrapDeleteInterTxnTSTest(ts3)
}

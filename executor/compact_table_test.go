// Copyright 2022 PingCAP, Inc.
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

package executor_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestCompactTableNoTiFlashReplica(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(
		`Warning 1105 compact skipped: no tiflash replica in the table`,
	))
}

func TestCompactTableTooBusy(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		return &kvrpcpb.CompactResponse{
			Error: &kvrpcpb.CompactError{Error: &kvrpcpb.CompactError_ErrTooManyPendingTasks{}},
		}, nil
	})
	defer mocker.RequireAllHandlersHit()
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(1), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(
		`Warning 1105 compact on store tiflash0 failed: store is too busy`,
	))
}

func TestCompactTableInProgress(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		return &kvrpcpb.CompactResponse{
			Error: &kvrpcpb.CompactError{Error: &kvrpcpb.CompactError_ErrCompactInProgress{}},
		}, nil
	})
	defer mocker.RequireAllHandlersHit()
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(1), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(
		`Warning 1105 compact on store tiflash0 failed: table is compacting in progress`,
	))
}

func TestCompactTableInternalError(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		return &kvrpcpb.CompactResponse{
			Error: &kvrpcpb.CompactError{Error: &kvrpcpb.CompactError_ErrInvalidStartKey{}},
		}, nil
	})
	defer mocker.RequireAllHandlersHit()
	store, clean := testkit.CreateMockStore(t, withMockTiFlash(1), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(
		`Warning 1105 compact on store tiflash0 failed: internal error (check logs for details)`,
	))
}

// TestCompactTableNoRemaining: Returns NoRemaining for request #1.
func TestCompactTableNoRemaining(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	defer mocker.RequireAllHandlersHit()
	store, do, clean := testkit.CreateMockStoreAndDomain(t, withMockTiFlash(1), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "t")
		require.Empty(t, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, tableId)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows())
}

// TestCompactTableHasRemaining: Returns HasRemaining=true for request #1 and #2, returns HasRemaining=false for request #3.
func TestCompactTableHasRemaining(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	defer mocker.RequireAllHandlersHit()
	store, do, clean := testkit.CreateMockStoreAndDomain(t, withMockTiFlash(1), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "t")
		require.Empty(t, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, tableId)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#2`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "t")
		require.Equal(t, []byte{0xFF}, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, tableId)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF, 0x20},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#3`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "t")
		require.Equal(t, []byte{0xFF, 0x20}, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, tableId)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF, 0xA0},
		}, nil
	})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows())
}

// TestCompactTableErrorInHalfway: Returns error for request #2.
func TestCompactTableErrorInHalfway(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	defer mocker.RequireAllHandlersHit()
	store, _, clean := testkit.CreateMockStoreAndDomain(t, withMockTiFlash(1), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Empty(t, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#2`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		return &kvrpcpb.CompactResponse{
			HasRemaining: false,
			Error:        &kvrpcpb.CompactError{Error: &kvrpcpb.CompactError_ErrTooManyPendingTasks{}},
		}, nil
	})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows(
		`Warning 1105 compact on store tiflash0 failed: store is too busy`,
	))
}

// TestCompactTableNoRemainingMultipleTiFlash: 2 TiFlash stores, both returns NoRemaining for request #1.
func TestCompactTableNoRemainingMultipleTiFlash(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	defer mocker.RequireAllHandlersHit()
	store, do, clean := testkit.CreateMockStoreAndDomain(t, withMockTiFlash(2), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "t")
		require.Empty(t, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, tableId)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})
	mocker.MockFrom(`tiflash1/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "t")
		require.Empty(t, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, tableId)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows())
}

// TestCompactTableMultipleTiFlash: 2 TiFlash stores.
// Store0 - #1 (remaining=true), #2 (remaining=true), #3 (remaining=false)
// Store1 - #1 (remaining=true), #2 (remaining=false)
func TestCompactTableMultipleTiFlash(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	defer mocker.RequireAllHandlersHit()
	store, _, clean := testkit.CreateMockStoreAndDomain(t, withMockTiFlash(2), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Empty(t, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#2`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Equal(t, []byte{0xFF}, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF, 0x20},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#3`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Equal(t, []byte{0xFF, 0x20}, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF, 0xA0},
		}, nil
	})

	mocker.MockFrom(`tiflash1/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Empty(t, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xC0, 0xCC}, // Use a different end key as tiflash0
		}, nil
	})
	mocker.MockFrom(`tiflash1/#2`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Equal(t, []byte{0xC0, 0xCC}, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xDD},
		}, nil
	})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows())
}

// TestCompactTableMultipleTiFlashWithError: 3 TiFlash stores.
// Store0 - #1 (remaining=true), #2 (remaining=true), #3 (remaining=false)
// Store1 - #1 (remaining=true), #2 (error)
// Store2 - #1 (error)
func TestCompactTableMultipleTiFlashWithError(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	defer mocker.RequireAllHandlersHit()
	store, _, clean := testkit.CreateMockStoreAndDomain(t, withMockTiFlash(3), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Empty(t, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#2`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Equal(t, []byte{0xFF}, req.StartKey)
		time.Sleep(time.Second * 1)
		// This request must be returned after store1 and store2 return errors. We should still receive req #3.
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF, 0x20},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#3`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Equal(t, []byte{0xFF, 0x20}, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF, 0xA0},
		}, nil
	})

	mocker.MockFrom(`tiflash1/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Empty(t, req.StartKey)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xC0, 0xCC},
		}, nil
	})
	mocker.MockFrom(`tiflash1/#2`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Equal(t, []byte{0xC0, 0xCC}, req.StartKey)
		return &kvrpcpb.CompactResponse{
			Error: &kvrpcpb.CompactError{Error: &kvrpcpb.CompactError_ErrTooManyPendingTasks{}},
		}, nil
	})

	mocker.MockFrom(`tiflash2/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		require.Empty(t, req.StartKey)
		return &kvrpcpb.CompactResponse{
			Error: &kvrpcpb.CompactError{Error: &kvrpcpb.CompactError_ErrTooManyPendingTasks{}},
		}, nil
	})

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec(`alter table t set tiflash replica 1;`)
	tk.MustExec(`alter table t compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Sort().Check(testkit.Rows(
		"Warning 1105 compact on store tiflash1 failed: store is too busy",
		"Warning 1105 compact on store tiflash2 failed: store is too busy",
	))
}

// TestCompactTableWithPartition: 1 TiFlash, table has 4 partitions.
// Partition 0: 1 Partials
// Partition 1: 3 Partials
// Partition 2: 1 Partials
// Partition 3: 2 Partials
// There will be 7 requests sent in series.
func TestCompactTableWithPartition(t *testing.T) {
	mocker := newCompactRequestMocker(t)
	defer mocker.RequireAllHandlersHit()
	store, do, clean := testkit.CreateMockStoreAndDomain(t, withMockTiFlash(1), mocker.AsOpt())
	defer clean()
	tk := testkit.NewTestKit(t, store)

	mocker.MockFrom(`tiflash0/#1`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "employees")
		pid := do.MustGetPartitionAt(t, "test", "employees", 0)
		require.Empty(t, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, pid)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#2`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "employees")
		pid := do.MustGetPartitionAt(t, "test", "employees", 1)
		require.Empty(t, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, pid)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xCC},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#3`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "employees")
		pid := do.MustGetPartitionAt(t, "test", "employees", 1)
		require.Equal(t, []byte{0xCC}, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, pid)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#4`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "employees")
		pid := do.MustGetPartitionAt(t, "test", "employees", 1)
		require.Equal(t, []byte{0xFF}, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, pid)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xFF, 0xAA},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#5`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "employees")
		pid := do.MustGetPartitionAt(t, "test", "employees", 2)
		require.Empty(t, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, pid)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xAB},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#6`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "employees")
		pid := do.MustGetPartitionAt(t, "test", "employees", 3)
		require.Empty(t, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, pid)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      true,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xC0},
		}, nil
	})
	mocker.MockFrom(`tiflash0/#7`, func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error) {
		tableId := do.MustGetTableID(t, "test", "employees")
		pid := do.MustGetPartitionAt(t, "test", "employees", 3)
		require.Equal(t, []byte{0xC0}, req.StartKey)
		require.EqualValues(t, req.PhysicalTableId, pid)
		require.EqualValues(t, req.LogicalTableId, tableId)
		return &kvrpcpb.CompactResponse{
			HasRemaining:      false,
			CompactedStartKey: []byte{},
			CompactedEndKey:   []byte{0xC1, 0xFF, 0x00},
		}, nil
	})

	tk.MustExec("use test")
	tk.MustExec(`
	CREATE TABLE employees  (
		id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
		fname VARCHAR(25) NOT NULL,
		lname VARCHAR(25) NOT NULL,
		store_id INT NOT NULL,
		department_id INT NOT NULL
	)
	PARTITION BY RANGE(id)  (
		PARTITION p0 VALUES LESS THAN (5),
		PARTITION p1 VALUES LESS THAN (10),
		PARTITION p2 VALUES LESS THAN (15),
		PARTITION p3 VALUES LESS THAN MAXVALUE
	);
	`)
	tk.MustExec(`alter table employees set tiflash replica 1;`)
	tk.MustExec(`alter table employees compact tiflash replica;`)
	tk.MustQuery(`show warnings;`).Check(testkit.Rows())
}

// TODO: Test privilege, test partitions of different types, drop table while compacting, drop partition while compacting, store is not alive, partial network failure.

// Code below are helper utilities for the test cases.

type compactClientHandler struct {
	matcher       *regexp.Regexp
	matcherString string
	fn            func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error)
	hit           bool
}

type compactRequestMocker struct {
	tikv.Client
	handlers               []*compactClientHandler
	receivedRequestsOfAddr map[string]int
	t                      *testing.T
}

func (client *compactRequestMocker) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdCompact {
		client.receivedRequestsOfAddr[addr]++
		handlerKey := fmt.Sprintf("%s/#%d", addr, client.receivedRequestsOfAddr[addr])
		for _, handler := range client.handlers {
			if handler.matcher.MatchString(handlerKey) {
				handler.hit = true
				resp, err := handler.fn(req.Compact())
				if err != nil {
					return nil, err
				}
				return &tikvrpc.Response{Resp: resp}, nil
			}
		}
		// If we enter here, it means no handler is matching. We should fail!
		require.Fail(client.t, fmt.Sprintf("Received request %s but no matching handler, maybe caused by unexpected number of requests?", handlerKey))
	}
	return client.Client.SendRequest(ctx, addr, req, timeout)
}

func newCompactRequestMocker(t *testing.T) *compactRequestMocker {
	return &compactRequestMocker{
		handlers:               make([]*compactClientHandler, 0),
		receivedRequestsOfAddr: make(map[string]int),
		t:                      t,
	}
}

func (client *compactRequestMocker) MockFrom(matchPattern string, fn func(req *kvrpcpb.CompactRequest) (*kvrpcpb.CompactResponse, error)) *compactRequestMocker {
	m := regexp.MustCompile(matchPattern)
	client.handlers = append(client.handlers, &compactClientHandler{
		matcher:       m,
		matcherString: matchPattern,
		fn:            fn,
		hit:           false,
	})
	return client
}

func (client *compactRequestMocker) RequireAllHandlersHit() {
	for _, handler := range client.handlers {
		if !handler.hit {
			require.Fail(client.t, fmt.Sprintf("Request handler %s did not hit, maybe caused by request was missing?", handler.matcherString))
		}
	}
}

func (client *compactRequestMocker) AsOpt() mockstore.MockTiKVStoreOption {
	return mockstore.WithClientHijacker(func(kvClient tikv.Client) tikv.Client {
		client.Client = kvClient
		return client
	})
}

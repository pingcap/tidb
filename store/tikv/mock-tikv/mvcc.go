// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package mocktikv

import (
	"bytes"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/petar/GoLLRB/llrb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mvccpb"
)

var defaultColumn = [][]byte{nil}

type colKey struct {
	col string
	ts  uint64
}

type mvccEntry struct {
	rowKey []byte
	meta   *mvccpb.Meta
	values map[colKey][]byte
}

func newEntry(key []byte) *mvccEntry {
	return &mvccEntry{
		rowKey: key,
		meta:   &mvccpb.Meta{},
		values: make(map[colKey][]byte),
	}
}

func (e *mvccEntry) Less(than llrb.Item) bool {
	return bytes.Compare(e.rowKey, than.(*mvccEntry).rowKey) < 0
}

func (e *mvccEntry) putValue(col []byte, ts uint64, val []byte) {
	e.values[colKey{string(col), ts}] = val
}

func (e *mvccEntry) getValue(col []byte, ts uint64) []byte {
	return e.values[colKey{string(col), ts}]
}

func (e *mvccEntry) delValue(col []byte, ts uint64) {
	delete(e.values, colKey{string(col), ts})
}

func (e *mvccEntry) lockErr() error {
	return &ErrLocked{
		Key:     e.rowKey,
		Primary: e.meta.GetLock().GetPrimaryKey(),
		StartTS: e.meta.GetLock().GetStartTs(),
	}
}

func (e *mvccEntry) Get(col []byte, ts uint64) ([]byte, error) {
	if lock := e.meta.GetLock(); lock != nil {
		if lock.GetStartTs() <= ts {
			for _, c := range lock.GetColumns() {
				if bytes.Equal(c.Name, col) {
					return nil, e.lockErr()
				}
			}
		}
	}
	for i := len(e.meta.Items) - 1; i >= 0; i-- {
		item := e.meta.Items[i]
		if item.GetCommitTs() <= ts {
			for _, c := range item.GetColumns() {
				if bytes.Equal(c.Name, col) {
					switch c.GetOpType() {
					case mvccpb.MetaOpType_Put:
						return e.getValue(col, item.GetStartTs()), nil
					case mvccpb.MetaOpType_Delete:
						return nil, nil
					}
				}
			}
		}
	}
	return nil, nil
}

func (e *mvccEntry) GetColumns(cols [][]byte, ts uint64) ([][]byte, error) {
	// TODO: It can be faster without calling `Get`.
	vals := make([][]byte, len(cols))
	for i, col := range cols {
		val, err := e.Get(col, ts)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

func (e *mvccEntry) Prewrite(mut *kvrpcpb.Mutation, startTS uint64, primary []byte) error {
	if len(mut.Ops) != len(mut.Columns) || len(mut.Ops) != len(mut.Values) {
		panic("invalid rowMutation: fields len not match")
	}
	if l := len(e.meta.Items); l > 0 {
		if e.meta.Items[l-1].GetCommitTs() >= startTS {
			return ErrRetryable("write conflict")
		}
	}
	if lock := e.meta.GetLock(); lock != nil {
		if lock.GetStartTs() != startTS {
			return e.lockErr()
		}
		// If we have processed a RowMutation with the same ts before,
		// simply believe they are the same.
		// TODO: Be serious, check if they are equal or try to merge them.
		return nil
	}
	if e.meta.GetLock() == nil {
		e.meta.Lock = &mvccpb.MetaLock{
			StartTs:    proto.Uint64(startTS),
			PrimaryKey: primary,
		}
	}
	e.meta.Lock = &mvccpb.MetaLock{
		StartTs:    proto.Uint64(startTS),
		PrimaryKey: primary,
	}
	for i, op := range mut.GetOps() {
		col, val := mut.GetColumns()[i], mut.GetValues()[i]
		if op == kvrpcpb.Op_Put {
			e.putValue(col, startTS, val)
		}
		e.meta.Lock.Columns = append(e.meta.Lock.Columns, &mvccpb.MetaColumn{
			Name:   col,
			OpType: opMap[op].Enum(),
		})
	}
	return nil
}

var opMap map[kvrpcpb.Op]mvccpb.MetaOpType = map[kvrpcpb.Op]mvccpb.MetaOpType{
	kvrpcpb.Op_Put:  mvccpb.MetaOpType_Put,
	kvrpcpb.Op_Del:  mvccpb.MetaOpType_Delete,
	kvrpcpb.Op_Lock: mvccpb.MetaOpType_Lock,
}

func (e *mvccEntry) checkTxnCommitted(startTS uint64) (uint64, bool) {
	for i := len(e.meta.Items) - 1; i >= 0; i-- {
		item := e.meta.Items[i]
		if item.GetStartTs() == startTS {
			return item.GetCommitTs(), true
		}
		if item.GetStartTs() < startTS {
			break
		}
	}
	return 0, false
}

func (e *mvccEntry) Commit(startTS, commitTS uint64) error {
	if e.meta.Lock == nil || e.meta.GetLock().GetStartTs() != startTS {
		if _, ok := e.checkTxnCommitted(startTS); ok {
			return nil
		}
		return ErrRetryable("txn not found")
	}
	newItem := &mvccpb.MetaItem{
		StartTs:  proto.Uint64(startTS),
		CommitTs: proto.Uint64(commitTS),
	}
	for _, col := range e.meta.GetLock().GetColumns() {
		if col.GetOpType() != mvccpb.MetaOpType_Lock {
			newItem.Columns = append(newItem.Columns, col)
		}
	}
	e.meta.Items = append(e.meta.Items, newItem)
	e.meta.Lock = nil
	return nil
}

func (e *mvccEntry) Rollback(startTS uint64) error {
	if e.meta.GetLock() == nil || e.meta.GetLock().GetStartTs() != startTS {
		if commitTS, ok := e.checkTxnCommitted(startTS); ok {
			return ErrAlreadyCommitted(commitTS)
		}
		return nil
	}
	if lock := e.meta.GetLock(); lock != nil {
		for _, col := range lock.GetColumns() {
			if col.GetOpType() == mvccpb.MetaOpType_Put {
				e.delValue(col.GetName(), startTS)
			}
		}
		e.meta.Lock = nil
	}
	return nil
}

// MvccStore is an in-memory, multi-versioned, transaction-supported kv storage.
type MvccStore struct {
	mu   sync.RWMutex
	tree *llrb.LLRB
}

// NewMvccStore creates a MvccStore.
func NewMvccStore() *MvccStore {
	return &MvccStore{
		tree: llrb.New(),
	}
}

// Get reads a row by ts.
func (s *MvccStore) Get(rowKey []byte, cols [][]byte, startTS uint64) ([][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.get(rowKey, cols, startTS)
}

func (s *MvccStore) get(rowKey []byte, cols [][]byte, startTS uint64) ([][]byte, error) {
	entry := s.tree.Get(newEntry(rowKey))
	if entry == nil {
		return nil, nil
	}
	return entry.(*mvccEntry).GetColumns(cols, startTS)
}

func rowValsEmpty(vals [][]byte) bool {
	for _, b := range vals {
		if len(b) > 0 {
			return false
		}
	}
	return true
}

// A Row is a row read from MvccStore or an error if any occurs.
type Row struct {
	RowKey  []byte
	Columns [][]byte
	Values  [][]byte
	Err     error
}

// BatchGet gets values with keys and ts.
func (s *MvccStore) BatchGet(rowKeys [][]byte, cols [][][]byte, startTS uint64) []Row {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(rowKeys) != len(cols) {
		panic("invalid batchGet: row/cols len not match")
	}

	rows := make([]Row, len(rowKeys))
	for i, k := range rowKeys {
		vals, err := s.get(k, cols[i], startTS)
		if rowValsEmpty(vals) && err == nil {
			continue
		}
		rows[i] = Row{
			RowKey:  k,
			Columns: cols[i],
			Values:  vals,
			Err:     err,
		}
	}
	return rows
}

func regionContains(startRow []byte, endRow []byte, rowKey []byte) bool {
	return bytes.Compare(startRow, rowKey) <= 0 &&
		(bytes.Compare(rowKey, endRow) < 0 || len(endRow) == 0)
}

// Scan reads up to a limited number of Pairs that greater than or equal to startKey and less than endKey.
func (s *MvccStore) Scan(startRow, endRow []byte, cols [][]byte, limit int, startTS uint64) []Row {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var rows []Row
	iterator := func(item llrb.Item) bool {
		if len(rows) >= limit {
			return false
		}
		k := item.(*mvccEntry).rowKey
		if !regionContains(startRow, endRow, k) {
			return false
		}
		vals, err := s.get(k, cols, startTS)
		if !rowValsEmpty(vals) || err != nil {
			rows = append(rows, Row{
				RowKey:  k,
				Columns: cols,
				Values:  vals,
				Err:     err,
			})
		}
		return true
	}
	s.tree.AscendGreaterOrEqual(newEntry(startRow), iterator)
	return rows
}

// ReverseScan reads up to a limited number of Rows that greater than or equal to startRow and less than endRow
// in descending order.
func (s *MvccStore) ReverseScan(startRow, endRow []byte, cols [][]byte, limit int, startTS uint64) []Row {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var rows []Row
	iterator := func(item llrb.Item) bool {
		if len(rows) >= limit {
			return false
		}
		k := item.(*mvccEntry).rowKey
		if bytes.Equal(k, endRow) {
			return true
		}
		if bytes.Compare(k, startRow) < 0 {
			return false
		}
		vals, err := s.get(k, cols, startTS)
		if !rowValsEmpty(vals) || err != nil {
			rows = append(rows, Row{
				RowKey:  k,
				Columns: cols,
				Values:  vals,
				Err:     err,
			})
		}
		return true
	}
	s.tree.DescendLessOrEqual(newEntry(endRow), iterator)
	return rows
}

func (s *MvccStore) getOrNewEntry(key []byte) *mvccEntry {
	if item := s.tree.Get(newEntry(key)); item != nil {
		return item.(*mvccEntry)
	}
	return newEntry(key)
}

// submit writes entries into the rbtree.
func (s *MvccStore) submit(ents ...*mvccEntry) {
	for _, ent := range ents {
		s.tree.ReplaceOrInsert(ent)
	}
}

// Prewrite acquires a lock on a key. (1st phase of 2PC).
func (s *MvccStore) Prewrite(mutations []*kvrpcpb.Mutation, primary []byte, startTS uint64) []error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for _, m := range mutations {
		entry := s.getOrNewEntry(m.GetRowKey())
		err := entry.Prewrite(m, startTS, primary)
		s.submit(entry)
		errs = append(errs, err)
	}
	return errs
}

// Commit commits the lock on a row. (2nd phase of 2PC).
func (s *MvccStore) Commit(rows [][]byte, startTS, commitTS uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ents []*mvccEntry
	for _, k := range rows {
		entry := s.getOrNewEntry(k)
		err := entry.Commit(startTS, commitTS)
		if err != nil {
			return err
		}
		ents = append(ents, entry)
	}
	s.submit(ents...)
	return nil
}

// CommitThenGet is a shortcut for Commit+Get, often used when resolving lock.
func (s *MvccStore) CommitThenGet(key []byte, cols [][]byte, lockTS, commitTS, getTS uint64) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.getOrNewEntry(key)
	err := entry.Commit(lockTS, commitTS)
	if err != nil {
		return nil, err
	}
	s.submit(entry)
	return entry.GetColumns(cols, getTS)
}

// Cleanup cleanups a lock, often used when resolving a expired lock.
func (s *MvccStore) Cleanup(rowKey []byte, startTS uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.getOrNewEntry(rowKey)
	err := entry.Rollback(startTS)
	if err != nil {
		return err
	}
	s.submit(entry)
	return nil
}

// Rollback cleanups multiple locks, often used when rolling back a conflict txn.
func (s *MvccStore) Rollback(rows [][]byte, startTS uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var ents []*mvccEntry
	for _, k := range rows {
		entry := s.getOrNewEntry(k)
		err := entry.Rollback(startTS)
		if err != nil {
			return err
		}
		ents = append(ents, entry)
	}
	s.submit(ents...)
	return nil
}

// RollbackThenGet is a shortcut for Rollback+Get, often used when resolving lock.
func (s *MvccStore) RollbackThenGet(rowKey []byte, cols [][]byte, lockTS uint64) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.getOrNewEntry(rowKey)
	err := entry.Rollback(lockTS)
	if err != nil {
		return nil, err
	}
	s.submit(entry)
	return entry.GetColumns(cols, lockTS)
}

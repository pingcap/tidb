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
// See the License for the specific language governing permissions and
// limitations under the License.

package unionstore

import (
	"github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"go.uber.org/zap"
)

// UnionIter is the iterator on an UnionStore.
type UnionIter struct {
	dirtyIt    Iterator
	snapshotIt Iterator

	dirtyValid    bool
	snapshotValid bool

	curIsDirty bool
	isValid    bool
	reverse    bool
}

// NewUnionIter returns a union iterator for BufferStore.
func NewUnionIter(dirtyIt Iterator, snapshotIt Iterator, reverse bool) (*UnionIter, error) {
	it := &UnionIter{
		dirtyIt:       dirtyIt,
		snapshotIt:    snapshotIt,
		dirtyValid:    dirtyIt.Valid(),
		snapshotValid: snapshotIt.Valid(),
		reverse:       reverse,
	}
	err := it.updateCur()
	if err != nil {
		return nil, err
	}
	return it, nil
}

// dirtyNext makes iter.dirtyIt go and update valid status.
func (iter *UnionIter) dirtyNext() error {
	err := iter.dirtyIt.Next()
	iter.dirtyValid = iter.dirtyIt.Valid()
	return err
}

// snapshotNext makes iter.snapshotIt go and update valid status.
func (iter *UnionIter) snapshotNext() error {
	err := iter.snapshotIt.Next()
	iter.snapshotValid = iter.snapshotIt.Valid()
	return err
}

func (iter *UnionIter) updateCur() error {
	iter.isValid = true
	for {
		if !iter.dirtyValid && !iter.snapshotValid {
			iter.isValid = false
			break
		}

		if !iter.dirtyValid {
			iter.curIsDirty = false
			break
		}

		if !iter.snapshotValid {
			iter.curIsDirty = true
			// if delete it
			if len(iter.dirtyIt.Value()) == 0 {
				if err := iter.dirtyNext(); err != nil {
					return err
				}
				continue
			}
			break
		}

		// both valid
		if iter.snapshotValid && iter.dirtyValid {
			snapshotKey := iter.snapshotIt.Key()
			dirtyKey := iter.dirtyIt.Key()
			cmp := kv.CmpKey(dirtyKey, snapshotKey)
			if iter.reverse {
				cmp = -cmp
			}
			// if equal, means both have value
			if cmp == 0 {
				if len(iter.dirtyIt.Value()) == 0 {
					// snapshot has a record, but txn says we have deleted it
					// just go next
					if err := iter.dirtyNext(); err != nil {
						return err
					}
					if err := iter.snapshotNext(); err != nil {
						return err
					}
					continue
				}
				// both go next
				if err := iter.snapshotNext(); err != nil {
					return err
				}
				iter.curIsDirty = true
				break
			} else if cmp > 0 {
				// record from snapshot comes first
				iter.curIsDirty = false
				break
			} else {
				// record from dirty comes first
				if len(iter.dirtyIt.Value()) == 0 {
					logutil.BgLogger().Warn("delete a record not exists?",
						zap.String("key", kv.StrKey(iter.dirtyIt.Key())))
					// jump over this deletion
					if err := iter.dirtyNext(); err != nil {
						return err
					}
					continue
				}
				iter.curIsDirty = true
				break
			}
		}
	}
	return nil
}

// Next implements the Iterator Next interface.
func (iter *UnionIter) Next() error {
	var err error
	if !iter.curIsDirty {
		err = iter.snapshotNext()
	} else {
		err = iter.dirtyNext()
	}
	if err != nil {
		return err
	}
	err = iter.updateCur()
	return err
}

// Value implements the Iterator Value interface.
// Multi columns
func (iter *UnionIter) Value() []byte {
	if !iter.curIsDirty {
		return iter.snapshotIt.Value()
	}
	return iter.dirtyIt.Value()
}

// Key implements the Iterator Key interface.
func (iter *UnionIter) Key() []byte {
	if !iter.curIsDirty {
		return iter.snapshotIt.Key()
	}
	return iter.dirtyIt.Key()
}

// Valid implements the Iterator Valid interface.
func (iter *UnionIter) Valid() bool {
	return iter.isValid
}

// Close implements the Iterator Close interface.
func (iter *UnionIter) Close() {
	if iter.snapshotIt != nil {
		iter.snapshotIt.Close()
		iter.snapshotIt = nil
	}
	if iter.dirtyIt != nil {
		iter.dirtyIt.Close()
		iter.dirtyIt = nil
	}
}

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
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

type tikvScanner struct {
	*txnsnapshot.Scanner
}

// Next return next element.
func (s *tikvScanner) Next() error {
	err := s.Scanner.Next()
	return extractKeyErr(err)
}

func (s *tikvScanner) Key() kv.Key {
	return kv.Key(s.Scanner.Key())
}

type createIterFunc func() (kv.Iterator, error)

type oneByOneIter struct {
	createFuncs []createIterFunc
	curIter     kv.Iterator
	cur         int
}

func newOneByOneIter(createFuncs []createIterFunc) (*oneByOneIter, error) {
	iter := &oneByOneIter{
		createFuncs: createFuncs,
		cur:         0,
	}

	if err := iter.updateCur(); err != nil {
		return nil, err
	}

	return iter, nil
}

func (o *oneByOneIter) updateCur() error {
	for o.cur < len(o.createFuncs) {
		if o.curIter == nil {
			iter, err := o.createFuncs[o.cur]()
			if err != nil {
				return err
			}
			o.curIter = iter
		}

		if o.curIter.Valid() {
			break
		}

		o.curIter.Close()
		o.curIter = nil
		o.cur++
	}

	return nil
}

func (o *oneByOneIter) Valid() bool {
	return o.cur < len(o.createFuncs)
}

func (o *oneByOneIter) Next() error {
	if !o.Valid() {
		return errors.New("iterator is invalid")
	}

	if err := o.curIter.Next(); err != nil {
		o.Close()
		return err
	}

	if err := o.updateCur(); err != nil {
		o.Close()
		return err
	}

	return nil
}

func (o *oneByOneIter) Key() kv.Key {
	if !o.Valid() {
		return nil
	}
	return o.curIter.Key()
}

func (o *oneByOneIter) Value() []byte {
	if !o.Valid() {
		return nil
	}
	return o.curIter.Value()
}

func (o *oneByOneIter) Close() {
	if o.curIter != nil {
		o.curIter.Close()
		o.curIter = nil
	}
	o.cur = len(o.createFuncs)
}

type lowerBoundReverseIter struct {
	iter       kv.Iterator
	lowerBound kv.Key
	valid      bool
	closed     bool
}

func newLowerBoundReverseIter(iter kv.Iterator, lowerBound kv.Key) kv.Iterator {
	i := &lowerBoundReverseIter{
		iter:       iter,
		lowerBound: lowerBound,
		valid:      iter.Valid(),
		closed:     false,
	}
	i.update()
	return i
}

func (i *lowerBoundReverseIter) update() {
	if !i.valid {
		return
	}

	if !i.iter.Valid() || bytes.Compare(i.Key(), i.lowerBound) < 0 {
		i.valid = false
	}
}

func (i *lowerBoundReverseIter) Valid() bool {
	return i.valid
}

func (i *lowerBoundReverseIter) Key() kv.Key {
	if !i.valid {
		return nil
	}
	return i.iter.Key()
}

func (i *lowerBoundReverseIter) Value() []byte {
	if !i.valid {
		return nil
	}
	return i.iter.Value()
}

func (i *lowerBoundReverseIter) Next() error {
	if !i.valid {
		return errors.New("iterator is invalid")
	}

	if err := i.iter.Next(); err != nil {
		i.valid = false
		return err
	}
	i.update()
	return nil
}

func (i *lowerBoundReverseIter) Close() {
	i.valid = false
	if !i.closed {
		i.iter.Close()
		i.closed = true
	}
}

type filterEmptyValueIter struct {
	iter   kv.Iterator
	valid  bool
	closed bool
}

func (i *filterEmptyValueIter) update() error {
	if !i.valid {
		return nil
	}

	for i.iter.Valid() && len(i.iter.Value()) == 0 {
		if err := i.iter.Next(); err != nil {
			i.valid = false
			return err
		}
	}

	i.valid = i.iter.Valid()
	return nil
}

func (i *filterEmptyValueIter) Key() kv.Key {
	if !i.valid {
		return nil
	}
	return i.iter.Key()
}

func (i *filterEmptyValueIter) Value() []byte {
	if !i.valid {
		return nil
	}
	return i.iter.Value()
}

func (i *filterEmptyValueIter) Valid() bool {
	return i.valid
}

func (i *filterEmptyValueIter) Next() error {
	if !i.valid {
		return errors.New("iterator is invalid")
	}

	if err := i.iter.Next(); err != nil {
		i.valid = false
		return err
	}

	return i.update()
}

func (i *filterEmptyValueIter) Close() {
	i.valid = false
	if !i.closed {
		i.iter.Close()
		i.closed = true
	}
}

func filterEmptyValue(iter kv.Iterator) (kv.Iterator, error) {
	i := &filterEmptyValueIter{iter: iter, valid: iter.Valid()}
	if err := i.update(); err != nil {
		return nil, err
	}
	return i, nil
}

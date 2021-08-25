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

type oneByOneIter struct {
	iters []kv.Iterator
	cur   int
}

func newOneByOneIter(iters []kv.Iterator) *oneByOneIter {
	iter := &oneByOneIter{
		iters: iters,
		cur:   0,
	}

	iter.updateCur()
	return iter
}

func (o *oneByOneIter) updateCur() {
	for o.cur >= 0 && o.cur < len(o.iters) {
		if o.iters[o.cur].Valid() {
			break
		}
		o.cur++
	}
}

func (o *oneByOneIter) Valid() bool {
	return o.cur >= 0 && o.cur < len(o.iters)
}

func (o *oneByOneIter) Next() error {
	if !o.Valid() {
		return errors.New("iterator is invalid")
	}

	err := o.iters[o.cur].Next()
	if err != nil {
		o.Close()
		return err
	}

	o.updateCur()
	return nil
}

func (o *oneByOneIter) Key() kv.Key {
	if !o.Valid() {
		return nil
	}
	return o.iters[o.cur].Key()
}

func (o *oneByOneIter) Value() []byte {
	if !o.Valid() {
		return nil
	}
	return o.iters[o.cur].Value()
}

func (o *oneByOneIter) Close() {
	for _, iter := range o.iters {
		iter.Close()
	}
	o.cur = -1
}

type lowerBoundReverseIter struct {
	iter       kv.Iterator
	lowerBound kv.Key
	valid      bool
}

func newLowerBoundReverseIter(iter kv.Iterator, lowerBound kv.Key) kv.Iterator {
	i := &lowerBoundReverseIter{
		iter:       iter,
		lowerBound: lowerBound,
		valid:      iter.Valid(),
	}
	i.update()
	return i
}

func (i *lowerBoundReverseIter) update() {
	if !i.valid {
		return
	}

	if !i.iter.Valid() || bytes.Compare(i.Key(), i.lowerBound) < 0 {
		i.Close()
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
		i.Close()
		return err
	}
	i.update()
	return nil
}

func (i *lowerBoundReverseIter) Close() {
	i.valid = false
	i.iter.Close()
}

type filterEmptyValueIter struct {
	kv.Iterator
}

func (i *filterEmptyValueIter) update() error {
	for i.Iterator.Valid() && len(i.Iterator.Value()) == 0 {
		if err := i.Iterator.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (i *filterEmptyValueIter) Next() error {
	if err := i.Iterator.Next(); err != nil {
		return err
	}
	return i.update()
}

func filterEmptyValue(iter kv.Iterator) (kv.Iterator, error) {
	i := &filterEmptyValueIter{iter}
	if err := i.update(); err != nil {
		return nil, err
	}
	return i, nil
}

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
	"context"

	tikverr "github.com/pingcap/tidb/store/tikv/error"
)

type mockSnapshot struct {
	store *MemDB
}

func (s *mockSnapshot) Get(_ context.Context, k []byte) ([]byte, error) {
	return s.store.Get(k)
}

func (s *mockSnapshot) SetPriority(priority int) {

}

func (s *mockSnapshot) BatchGet(_ context.Context, keys [][]byte) (map[string][]byte, error) {
	m := make(map[string][]byte, len(keys))
	for _, k := range keys {
		v, err := s.store.Get(k)
		if tikverr.IsErrNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		m[string(k)] = v
	}
	return m, nil
}

func (s *mockSnapshot) Iter(k []byte, upperBound []byte) (Iterator, error) {
	return s.store.Iter(k, upperBound)
}

func (s *mockSnapshot) IterReverse(k []byte) (Iterator, error) {
	return s.store.IterReverse(k)
}

func (s *mockSnapshot) SetOption(opt int, val interface{}) {}

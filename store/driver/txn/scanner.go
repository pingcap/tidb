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
// See the License for the specific language governing permissions and
// limitations under the License.

package txn

import (
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

type tikvScanner struct {
	*tikv.Scanner
}

// Next return next element.
func (s *tikvScanner) Next() error {
	err := s.Scanner.Next()
	return extractKeyErr(err)
}

func (s *tikvScanner) Key() kv.Key {
	return kv.Key(s.Scanner.Key())
}

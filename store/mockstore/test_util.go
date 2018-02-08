// Copyright 2018 PingCAP, Inc.
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

package mockstore

import (
	"flag"
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/store/tikv"
)

// NewTestTiKVStorage creates a Storage for test.
func NewTestTiKVStorage(withTiKV bool, pdAddrs string) (tikv.Storage, error) {
	if !flag.Parsed() {
		flag.Parse()
	}

	if withTiKV {
		var d tikv.Driver
		store, err := d.Open(fmt.Sprintf("tikv://%s", pdAddrs))
		if err != nil {
			return nil, errors.Trace(err)
		}
		return store.(tikv.Storage), nil
	}
	store, err := NewMockTikvStore()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return store.(tikv.Storage), nil
}

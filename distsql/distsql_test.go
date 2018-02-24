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

package distsql

import (
	"github.com/juju/errors"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

type mockResponse struct {
	count int
}

func (resp *mockResponse) Next(ctx context.Context) ([]byte, error) {
	resp.count++
	if resp.count == 100 {
		return nil, errors.New("error happened")
	}
	return mockSubresult(), nil
}

func (resp *mockResponse) Close() error {
	return nil
}

func mockSubresult() []byte {
	resp := new(tipb.SelectResponse)
	b, err := resp.Marshal()
	if err != nil {
		panic(err)
	}
	return b
}

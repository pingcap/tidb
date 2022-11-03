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

package autoid

import (
	"context"

	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/kv"
	"google.golang.org/grpc"
)

type mockClient struct {
}

func (m *mockClient) AllocAutoID(ctx context.Context, in *autoid.AutoIDRequest, opts ...grpc.CallOption) (*autoid.AutoIDResponse, error) {
	panic("TODO")
}

func (m *mockClient) Rebase(ctx context.Context, in *autoid.RebaseRequest, opts ...grpc.CallOption) (*autoid.RebaseResponse, error) {
	panic("TODO")
}

var global = make(map[string]*mockClient)

// MockForTest is used for testing, the UT test and unistore use this.
func MockForTest(store kv.Storage) *mockClient {
	uuid := store.UUID()
	ret, ok := global[uuid]
	if !ok {
		ret = &mockClient{}
		global[uuid] = ret
	}
	return ret
}

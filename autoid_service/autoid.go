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

func MockForTest(store kv.Storage) *mockClient {
	uuid := store.UUID()
	ret, ok := global[uuid]
	if !ok {
		ret = &mockClient{}
		global[uuid] = ret
	}
	return ret
}

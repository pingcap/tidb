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

package resolver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/pingcap/tidb/br/pkg/restore"
	grpcresolver "google.golang.org/grpc/resolver"
)

const (
	schemePrefix           = "tikvstore-"
	defaultGetStoreTimeout = time.Second * 30
)

type Builder struct {
	scheme      string
	splitClient restore.SplitClient
}

func NewBuilder(splitClient restore.SplitClient) *Builder {
	return &Builder{
		scheme:      generateScheme(),
		splitClient: splitClient,
	}
}

func generateScheme() string {
	var buf [8]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic(err)
	}
	return schemePrefix + hex.EncodeToString(buf[:])
}

func (b *Builder) Target(storeID uint64) string {
	return fmt.Sprintf("%s://%d", b.scheme, storeID)
}

func (b *Builder) Build(target grpcresolver.Target, conn grpcresolver.ClientConn, opts grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	storeID, err := strconv.ParseUint(target.URL.Host, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid tikv store target %s: %w", target.URL.String(), err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &storeResolver{
		ctx:         ctx,
		cancel:      cancel,
		storeID:     storeID,
		splitClient: b.splitClient,
		conn:        conn,
	}
	r.resolveNow()
	return r, nil
}

func (b *Builder) Scheme() string {
	return b.scheme
}

var _ grpcresolver.Builder = &Builder{}

type storeResolver struct {
	ctx         context.Context
	cancel      context.CancelFunc
	storeID     uint64
	splitClient restore.SplitClient
	conn        grpcresolver.ClientConn
}

func (s *storeResolver) resolveNow() {
	ctx, cancel := context.WithTimeout(s.ctx, defaultGetStoreTimeout)
	defer cancel()
	store, err := s.splitClient.GetStore(ctx, s.storeID)
	if err != nil {
		s.conn.ReportError(err)
	} else {
		// We should use peer address for tiflash. For tikv, peer address is empty.
		addr := store.GetPeerAddress()
		if addr == "" {
			addr = store.GetAddress()
		}
		state := grpcresolver.State{Addresses: []grpcresolver.Address{{Addr: addr}}}
		s.conn.UpdateState(state)
	}
}

func (s *storeResolver) ResolveNow(grpcresolver.ResolveNowOptions) {
	s.splitClient.InvalidateStoreCache(s.storeID)
	s.resolveNow()
}

func (s *storeResolver) Close() {
	s.cancel()
}

var _ grpcresolver.Resolver = &storeResolver{}

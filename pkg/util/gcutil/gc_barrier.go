// Copyright 2026 PingCAP, Inc.
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

package gcutil

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

// SetGCBarrier keeps GC of the store's keyspace from advancing past ts for ttl,
// until DeleteGCBarrier is called with the same id. Setting an existing id
// replaces its ts and ttl. On a keyspace without keyspace-level GC, PD applies
// the barrier to the unified GC state.
func SetGCBarrier(ctx context.Context, store kv.Storage, id string, ts uint64, ttl time.Duration) error {
	cli, err := gcStatesClient(store)
	if err != nil {
		return err
	}
	_, err = cli.SetGCBarrier(ctx, id, ts, ttl)
	return errors.Trace(err)
}

// DeleteGCBarrier removes the barrier set by SetGCBarrier. Deleting a missing
// barrier is not an error.
func DeleteGCBarrier(ctx context.Context, store kv.Storage, id string) error {
	cli, err := gcStatesClient(store)
	if err != nil {
		return err
	}
	_, err = cli.DeleteGCBarrier(ctx, id)
	return errors.Trace(err)
}

func gcStatesClient(store kv.Storage) (pdgc.GCStatesClient, error) {
	s, ok := store.(kv.StorageWithPD)
	if !ok {
		return nil, errors.New("GC barrier requires a storage with PD")
	}
	return s.GetPDClient().GetGCStatesClient(uint32(store.GetCodec().GetKeyspaceID())), nil
}

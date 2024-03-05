// package storewatch provides a `Watcher` type which allows
// the user to listen the events of lifetime of stores.
package storewatch

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/conn"
	"github.com/pingcap/tidb/br/pkg/conn/util"
)

// Callback will be called the supported event triggered.
type Callback interface {
	OnNewStoreRegistered(store *metapb.Store)
	OnDisconnect(store *metapb.Store)
	OnReboot(store *metapb.Store)
}

// DynCallback is a function based callback set.
type DynCallback struct {
	onNewStoreRegistered func(*metapb.Store)
	onDisconnect         func(*metapb.Store)
	onReboot             func(*metapb.Store)
}

// OnNewStoreRegistered will be called once new region added to be watched.
func (cb *DynCallback) OnNewStoreRegistered(store *metapb.Store) {
	if cb.onNewStoreRegistered != nil {
		cb.onNewStoreRegistered(store)
	}
}

// OnDisconnect will be called once the store is disconnected.
func (cb *DynCallback) OnDisconnect(store *metapb.Store) {
	if cb.onDisconnect != nil {
		cb.onDisconnect(store)
	}
}

// OnReboot will be called once the store is rebooted.
func (cb *DynCallback) OnReboot(store *metapb.Store) {
	if cb.onReboot != nil {
		cb.onReboot(store)
	}
}

// DynCallbackOpt is the option for DynCallback.
type DynCallbackOpt func(*DynCallback)

// WithOnNewStoreRegistered adds a hook to the callback.
func WithOnNewStoreRegistered(f func(*metapb.Store)) DynCallbackOpt {
	return func(cb *DynCallback) {
		cb.onNewStoreRegistered = f
	}
}

// WithOnDisconnect adds a hook to the callback.
func WithOnDisconnect(f func(*metapb.Store)) DynCallbackOpt {
	return func(cb *DynCallback) {
		cb.onDisconnect = f
	}
}

// WithOnReboot adds a hook to the callback.
func WithOnReboot(f func(*metapb.Store)) DynCallbackOpt {
	return func(cb *DynCallback) {
		cb.onReboot = f
	}
}

// MakeCallback creates a callback with the given options.
// Allowed options: WithOnNewStoreRegistered, WithOnDisconnect, WithOnReboot.
func MakeCallback(opts ...DynCallbackOpt) Callback {
	cb := &DynCallback{}
	for _, opt := range opts {
		opt(cb)
	}
	return cb
}

// Watcher watches the lifetime of stores.
// generally it should be advanced by calling the `Step` call.
type Watcher struct {
	cli util.StoreMeta
	cb  Callback

	lastStores map[uint64]*metapb.Store
}

func New(cli util.StoreMeta, cb Callback) *Watcher {
	return &Watcher{
		cli:        cli,
		cb:         cb,
		lastStores: make(map[uint64]*metapb.Store),
	}
}

func (w *Watcher) Step(ctx context.Context) error {
	liveStores, err := conn.GetAllTiKVStoresWithRetry(ctx, w.cli, util.SkipTiFlash)
	if err != nil {
		return errors.Annotate(err, "failed to update store list")
	}
	recorded := map[uint64]struct{}{}
	for _, store := range liveStores {
		w.updateStore(store)
		recorded[store.GetId()] = struct{}{}
	}
	w.retain(recorded)
	return nil
}

// updateStore updates the current store. and call the hooks needed.
func (w *Watcher) updateStore(newStore *metapb.Store) {
	lastStore, ok := w.lastStores[newStore.GetId()]
	w.lastStores[newStore.GetId()] = newStore
	if !ok {
		w.cb.OnNewStoreRegistered(newStore)
		return
	}
	if lastStore.GetState() == metapb.StoreState_Up && newStore.GetState() == metapb.StoreState_Offline {
		w.cb.OnDisconnect(newStore)
	}
	if lastStore.StartTimestamp != newStore.StartTimestamp {
		w.cb.OnReboot(newStore)
	}
}

func (w *Watcher) retain(storeSet map[uint64]struct{}) {
	for id := range w.lastStores {
		if _, ok := storeSet[id]; !ok {
			delete(w.lastStores, id)
		}
	}
}

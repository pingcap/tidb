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

package tikvhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/server/handler"
	tikvcfg "github.com/tikv/client-go/v2/config"
	"golang.org/x/sync/semaphore"
)

// Enforce preparations are serialized with cancellable admission. Off bypasses it.
var kvServerSecurityUpdateSem = semaphore.NewWeighted(1)

// Protect publication and invalidate older updates when off succeeds. Never hold
// this lock across an enforce preflight, so off can cancel it immediately.
var kvServerSecurityState struct {
	sync.Mutex
	offEpoch uint64
	cancel   context.CancelFunc
}

func (h SettingsHandler) updateKVServerSecurity(w http.ResponseWriter, req *http.Request) {
	if !req.Form.Has("kv_server_verify_cn_mode") || !req.Form.Has("kv_server_verify_cn") || len(req.Form) != 2 {
		handler.WriteError(w, errors.New("provide kv_server_verify_cn_mode and kv_server_verify_cn together, without other settings"))
		return
	}
	mode := req.Form.Get("kv_server_verify_cn_mode")
	if mode != "off" && mode != "enforce" {
		handler.WriteError(w, errors.New("kv_server_verify_cn_mode must be off or enforce"))
		return
	}
	var allowed []string
	if err := json.Unmarshal([]byte(req.Form.Get("kv_server_verify_cn")), &allowed); err != nil {
		handler.WriteError(w, errors.Annotate(err, "kv_server_verify_cn must be a JSON string array"))
		return
	}
	updater, ok := h.Store.(interface {
		UpdateKVServerSecurity(context.Context, tikvcfg.Security) error
	})
	if !ok {
		handler.WriteError(w, errors.New("KV server security hot update is unavailable for this store"))
		return
	}
	security := config.GetGlobalConfig().Security.ClusterSecurity()
	security.KVServerVerifyCNMode = mode
	security.KVServerVerifyCN = slices.Clone(allowed)
	if err := security.ValidateKVServerCN(); err != nil {
		handler.WriteError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), requestDefaultTimeout)
	defer cancel()
	if err := applyKVServerSecurity(ctx, security, updater.UpdateKVServerSecurity); err != nil {
		handler.WriteError(w, errors.Annotate(err, "KV server security update rejected"))
		return
	}
	handler.WriteData(w, map[string]any{"kv_server_verify_cn_mode": mode, "kv_server_verify_cn": allowed})
}

func applyKVServerSecurity(ctx context.Context, security tikvcfg.Security, update func(context.Context, tikvcfg.Security) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	kvServerSecurityState.Lock()
	epoch := kvServerSecurityState.offEpoch
	if security.KVServerVerifyCNMode == "enforce" {
		kvServerSecurityState.Unlock()
		if err := kvServerSecurityUpdateSem.Acquire(ctx, 1); err != nil {
			return err
		}
		defer kvServerSecurityUpdateSem.Release(1)
		kvServerSecurityState.Lock()
	}
	defer kvServerSecurityState.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if epoch != kvServerSecurityState.offEpoch {
		return errors.New("KV server security update superseded by off")
	}
	var err error
	if security.KVServerVerifyCNMode == "off" {
		if kvServerSecurityState.cancel != nil {
			kvServerSecurityState.cancel()
		}
		// The client off path does no network or TLS-file I/O. Keep publication
		// serialized with this switch, without waiting for old preflight cleanup.
		err = update(ctx, security)
		if err == nil {
			kvServerSecurityState.offEpoch++
		}
	} else {
		kvServerSecurityState.cancel = cancel
		kvServerSecurityState.Unlock()
		err = update(ctx, security)
		kvServerSecurityState.Lock()
		kvServerSecurityState.cancel = nil
		if epoch != kvServerSecurityState.offEpoch {
			err = errors.New("KV server security update superseded by off")
		}
	}
	if err != nil {
		return err
	}
	config.UpdateGlobal(func(c *config.Config) {
		c.Security.KVServerVerifyCNMode = security.KVServerVerifyCNMode
		c.Security.KVServerVerifyCN = slices.Clone(security.KVServerVerifyCN)
	})
	return nil
}

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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"net/url"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var stores = make(map[string]kv.Driver)
var storesLock sync.RWMutex

// Register registers a kv storage with unique name and its associated Driver.
func Register(name string, driver kv.Driver) error {
	storesLock.Lock()
	defer storesLock.Unlock()

	name = strings.ToLower(name)

	if _, ok := stores[name]; ok {
		return errors.Errorf("%s is already registered", name)
	}

	stores[name] = driver
	return nil
}

// New creates a kv Storage with path.
//
// The path must be a URL format 'engine://path?params' like the one for
// session.Open() but with the dbname cut off.
// Examples:
//
//	goleveldb://relative/path
//	boltdb:///absolute/path
//
// The engine should be registered before creating storage.
func New(path string) (kv.Storage, error) {
	return newStoreWithRetry(path, util.DefaultMaxRetries)
}

func newStoreWithRetry(path string, maxRetries int) (kv.Storage, error) {
	storeURL, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	name := strings.ToLower(storeURL.Scheme)
	d, ok := loadDriver(name)
	if !ok {
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	var s kv.Storage
	err = util.RunWithRetry(maxRetries, util.RetryInterval, func() (bool, error) {
		logutil.BgLogger().Info("new store", zap.String("path", path))
		s, err = d.Open(path)
		return isNewStoreRetryableError(err), err
	})

	if err == nil {
		logutil.BgLogger().Info("new store with retry success")
	} else {
		logutil.BgLogger().Warn("new store with retry failed", zap.Error(err))
	}
	return s, errors.Trace(err)
}

func loadDriver(name string) (kv.Driver, bool) {
	storesLock.RLock()
	defer storesLock.RUnlock()
	d, ok := stores[name]
	return d, ok
}

// isOpenRetryableError check if the new store operation should be retried under given error
// currently, it should be retried if:
//
//	Transaction conflict and is retryable (kv.IsTxnRetryableError)
//	PD is not bootstrapped at the time of request
//	Keyspace requested does not exist (request prior to PD keyspace pre-split)
func isNewStoreRetryableError(err error) bool {
	if err == nil {
		return false
	}
	return kv.IsTxnRetryableError(err) || IsNotBootstrappedError(err) || IsKeyspaceNotExistError(err)
}

// IsNotBootstrappedError returns true if the error is pd not bootstrapped error.
func IsNotBootstrappedError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_NOT_BOOTSTRAPPED.String())
}

// IsKeyspaceNotExistError returns true the error is caused by keyspace not exists.
func IsKeyspaceNotExistError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pdpb.ErrorType_ENTRY_NOT_FOUND.String())
}

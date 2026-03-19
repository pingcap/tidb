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
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	storeDrivers    = make(map[config.StoreType]kv.Driver)
	storeDriverLock sync.RWMutex

	// systemStore is the kv.Storage for the SYSTEM keyspace.
	// which is only initialized and used in nextgen kernel.
	// for description of SYSTEM keyspace, see keyspace.System.
	systemStore kv.Storage
)

// Register registers a kv storage with unique name and its associated Driver.
// TODO: remove this function and use driver directly, TiDB is not a SDK.
func Register(tp config.StoreType, driver kv.Driver) error {
	storeDriverLock.Lock()
	defer storeDriverLock.Unlock()

	if !tp.Valid() {
		return errors.Errorf("invalid storage type %s", tp)
	}

	if _, ok := storeDrivers[tp]; ok {
		return errors.Errorf("%s is already registered", tp)
	}

	storeDrivers[tp] = driver
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
	d, ok := loadDriver(config.StoreType(name))
	if !ok {
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	var s kv.Storage
	samLogger := logutil.SampleLoggerFactory(30*time.Second, 1, zap.String("path", path))()
	err = util.RunWithRetry(maxRetries, util.RetryInterval, func() (bool, error) {
		logutil.BgLogger().Info("new store", zap.String("path", path))
		s, err = d.Open(path)
		if err != nil {
			samLogger.Info("open store failed, retrying", zap.Error(err))
		}
		return isNewStoreRetryableError(err), err
	})

	if err == nil {
		logutil.BgLogger().Info("new store with retry success")
	} else {
		logutil.BgLogger().Warn("new store with retry failed", zap.Error(err))
	}
	return s, errors.Trace(err)
}

func loadDriver(tp config.StoreType) (kv.Driver, bool) {
	storeDriverLock.RLock()
	defer storeDriverLock.RUnlock()
	d, ok := storeDrivers[tp]
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

// MustInitStorage initializes the kv.Storage for this instance.
func MustInitStorage(keyspaceName string) kv.Storage {
	defaultStore := mustInitStorage(keyspaceName)
	if kerneltype.IsNextGen() {
		if kv.IsUserKS(defaultStore) {
			systemStore = mustInitStorage(keyspace.System)
		} else {
			systemStore = defaultStore
		}
	}
	return defaultStore
}

// GetSystemStorage returns the kv.Storage for the SYSTEM keyspace.
func GetSystemStorage() kv.Storage {
	return systemStore
}

// SetSystemStorage returns the kv.Storage for the SYSTEM keyspace.
// it's only used in test.
func SetSystemStorage(s kv.Storage) {
	if s != nil {
		intest.Assert(s.GetKeyspace() == keyspace.System, "systemStore should be set with SYSTEM keyspace")
	}
	systemStore = s
}

func mustInitStorage(keyspaceName string) kv.Storage {
	storage, err := InitStorage(keyspaceName)
	terror.MustNil(err)
	return storage
}

// InitStorage initializes the kv.Storage for the given keyspace name.
func InitStorage(keyspaceName string) (kv.Storage, error) {
	cfg := config.GetGlobalConfig()
	var fullPath string
	if keyspaceName == "" {
		fullPath = fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	} else {
		fullPath = fmt.Sprintf("%s://%s?keyspaceName=%s", cfg.Store, cfg.Path, keyspaceName)
	}
	return New(fullPath)
}

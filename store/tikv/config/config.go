// Copyright 2021 PingCAP, Inc.
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

package config

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"go.uber.org/zap"
)

var (
	globalConf atomic.Value
)

const (
	// DefStoresRefreshInterval is the default value of StoresRefreshInterval
	DefStoresRefreshInterval = 60
)

func init() {
	conf := DefaultConfig()
	StoreGlobalConfig(&conf)
}

// Config contains configuration options.
type Config struct {
	CommitterConcurrency int
	MaxTxnTTL            uint64
	TiKVClient           TiKVClient
	Security             Security
	PDClient             PDClient
	PessimisticTxn       PessimisticTxn
	TxnLocalLatches      TxnLocalLatches
	// StoresRefreshInterval indicates the interval of refreshing stores info, the unit is second.
	StoresRefreshInterval uint64
	OpenTracingEnable     bool
	Path                  string
	EnableForwarding      bool
	TxnScope              string
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	return Config{
		CommitterConcurrency:  128,
		MaxTxnTTL:             60 * 60 * 1000, // 1hour
		TiKVClient:            DefaultTiKVClient(),
		PDClient:              DefaultPDClient(),
		TxnLocalLatches:       DefaultTxnLocalLatches(),
		StoresRefreshInterval: DefStoresRefreshInterval,
		OpenTracingEnable:     false,
		Path:                  "",
		EnableForwarding:      false,
		TxnScope:              "",
	}
}

// PDClient is the config for PD client.
type PDClient struct {
	// PDServerTimeout is the max time which PD client will wait for the PD server in seconds.
	PDServerTimeout uint `toml:"pd-server-timeout" json:"pd-server-timeout"`
}

// DefaultPDClient returns the default configuration for PDClient
func DefaultPDClient() PDClient {
	return PDClient{
		PDServerTimeout: 3,
	}
}

// TxnLocalLatches is the TxnLocalLatches section of the config.
type TxnLocalLatches struct {
	Enabled  bool `toml:"-" json:"-"`
	Capacity uint `toml:"-" json:"-"`
}

// DefaultTxnLocalLatches returns the default configuration for TxnLocalLatches
func DefaultTxnLocalLatches() TxnLocalLatches {
	return TxnLocalLatches{
		Enabled:  false,
		Capacity: 0,
	}
}

// Valid returns true if the configuration is valid.
func (c *TxnLocalLatches) Valid() error {
	if c.Enabled && c.Capacity == 0 {
		return fmt.Errorf("txn-local-latches.capacity can not be 0")
	}
	return nil
}

// PessimisticTxn is the config for pessimistic transaction.
type PessimisticTxn struct {
	// The max count of retry for a single statement in a pessimistic transaction.
	MaxRetryCount uint `toml:"max-retry-count" json:"max-retry-count"`
}

// GetGlobalConfig returns the global configuration for this server.
// It should store configuration from command line and configuration file.
// Other parts of the system can read the global configuration use this function.
func GetGlobalConfig() *Config {
	return globalConf.Load().(*Config)
}

// StoreGlobalConfig stores a new config to the globalConf. It mostly uses in the test to avoid some data races.
func StoreGlobalConfig(config *Config) {
	globalConf.Store(config)
}

// UpdateGlobal updates the global config, and provide a restore function that can be used to restore to the original.
func UpdateGlobal(f func(conf *Config)) func() {
	g := GetGlobalConfig()
	restore := func() {
		StoreGlobalConfig(g)
	}
	newConf := *g
	f(&newConf)
	StoreGlobalConfig(&newConf)
	return restore
}

const (
	globalTxnScope = "global"
)

// GetTxnScopeFromConfig extracts @@txn_scope value from config
func GetTxnScopeFromConfig() (bool, string) {
	failpoint.Inject("injectTxnScope", func(val failpoint.Value) {
		v := val.(string)
		if len(v) > 0 {
			failpoint.Return(false, v)
		}
		failpoint.Return(true, globalTxnScope)
	})

	if kvcfg := GetGlobalConfig(); kvcfg != nil && len(kvcfg.TxnScope) > 0 {
		return false, kvcfg.TxnScope
	}
	return true, globalTxnScope
}

// ParsePath parses this path.
// Path example: tikv://etcd-node1:port,etcd-node2:port?cluster=1&disableGC=false
func ParsePath(path string) (etcdAddrs []string, disableGC bool, err error) {
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if strings.ToLower(u.Scheme) != "tikv" {
		err = errors.Errorf("Uri scheme expected [tikv] but found [%s]", u.Scheme)
		logutil.BgLogger().Error("parsePath error", zap.Error(err))
		return
	}
	switch strings.ToLower(u.Query().Get("disableGC")) {
	case "true":
		disableGC = true
	case "false", "":
	default:
		err = errors.New("disableGC flag should be true/false")
		return
	}
	etcdAddrs = strings.Split(u.Host, ",")
	return
}

var (
	internalClientInit sync.Once
	internalHTTPClient *http.Client
	internalHTTPSchema string
)

// InternalHTTPClient is used by TiDB-Server to request other components.
func InternalHTTPClient() *http.Client {
	internalClientInit.Do(initInternalClient)
	return internalHTTPClient
}

// InternalHTTPSchema specifies use http or https to request other components.
func InternalHTTPSchema() string {
	internalClientInit.Do(initInternalClient)
	return internalHTTPSchema
}

func initInternalClient() {
	clusterSecurity := GetGlobalConfig().Security
	tlsCfg, err := clusterSecurity.ToTLSConfig()
	if err != nil {
		logutil.BgLogger().Fatal("could not load cluster ssl", zap.Error(err))
	}
	if tlsCfg == nil {
		internalHTTPSchema = "http"
		internalHTTPClient = http.DefaultClient
		return
	}
	internalHTTPSchema = "https"
	internalHTTPClient = &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
	}
}

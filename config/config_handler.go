// Copyright 2020 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/v4/util/logutil"
	"go.uber.org/zap"
)

// ConfHandler is used to load and update config online.
// See https://github.com/pingcap/tidb/v4/pull/13660 for more details.
type ConfHandler interface {
	Start()
	Close()
	GetConfig() *Config // read only
	SetConfig(conf *Config) error
}

// ConfReloadFunc is used to reload the config to make it work.
type ConfReloadFunc func(oldConf, newConf *Config)

// NewConfHandler creates a new ConfHandler according to the local config.
func NewConfHandler(confPath string, localConf *Config, reloadFunc ConfReloadFunc,
	newPDCliFunc func([]string, pd.SecurityOption) (pd.ConfigClient, error), // for test
) (ConfHandler, error) {
	if strings.ToLower(localConf.Store) == "tikv" && localConf.EnableDynamicConfig {
		return newPDConfHandler(confPath, localConf, reloadFunc, newPDCliFunc)
	}
	cch := new(constantConfHandler)
	cch.curConf.Store(localConf)
	return cch, nil
}

// constantConfHandler is used when EnableDynamicConfig is false.
// The conf in it will always be the configuration that initialized when TiDB is started.
type constantConfHandler struct {
	curConf atomic.Value
}

func (cch *constantConfHandler) Start() {}

func (cch *constantConfHandler) Close() {}

func (cch *constantConfHandler) GetConfig() *Config { return cch.curConf.Load().(*Config) }

func (cch *constantConfHandler) SetConfig(conf *Config) error {
	cch.curConf.Store(conf)
	return nil
}

var (
	unspecifiedVersion = new(configpb.Version)
)

const (
	pdConfHandlerRefreshInterval = 30 * time.Second
	tidbComponentName            = "tidb"
)

type pdConfHandler struct {
	id         string // ip:port
	version    *configpb.Version
	curConf    atomic.Value
	interval   time.Duration
	wg         sync.WaitGroup
	exit       chan struct{}
	pdConfCli  pd.ConfigClient
	reloadFunc func(oldConf, newConf *Config)
	registered bool
	confPath   string

	// attributes for test
	timeAfter func(d time.Duration) <-chan time.Time
}

func newPDConfHandler(confPath string, localConf *Config, reloadFunc ConfReloadFunc,
	newPDCliFunc func([]string, pd.SecurityOption) (pd.ConfigClient, error), // for test
) (*pdConfHandler, error) {
	fullPath := fmt.Sprintf("%s://%s", localConf.Store, localConf.Path)
	addresses, _, err := ParsePath(fullPath)
	if err != nil {
		return nil, err
	}
	host := localConf.Host
	if localConf.AdvertiseAddress != "" {
		host = localConf.AdvertiseAddress
	}
	id := fmt.Sprintf("%v:%v", host, localConf.Port)
	security := localConf.Security
	if newPDCliFunc == nil {
		newPDCliFunc = pd.NewConfigClient
	}
	pdCli, err := newPDCliFunc(addresses, pd.SecurityOption{
		CAPath:   security.ClusterSSLCA,
		CertPath: security.ClusterSSLCert,
		KeyPath:  security.ClusterSSLKey,
	})
	if err != nil {
		return nil, err
	}
	ch := &pdConfHandler{
		id:         id,
		version:    unspecifiedVersion,
		interval:   pdConfHandlerRefreshInterval,
		exit:       make(chan struct{}),
		pdConfCli:  pdCli,
		reloadFunc: reloadFunc,
		registered: false,
		confPath:   confPath,
	}
	ch.curConf.Store(localConf) // use the local config at first
	return ch, nil
}

func (ch *pdConfHandler) Start() {
	ch.wg.Add(1)
	go ch.run()
}

func (ch *pdConfHandler) Close() {
	close(ch.exit)
	ch.wg.Wait()
	ch.pdConfCli.Close()
}

func (ch *pdConfHandler) GetConfig() *Config {
	return ch.curConf.Load().(*Config)
}

func (ch *pdConfHandler) SetConfig(conf *Config) error {
	return errors.New("PDConfHandler only support to update the config from PD whereas forbid to modify it locally")
}

func (ch *pdConfHandler) register() {
	// register to PD and get the new default config.
	// see https://github.com/pingcap/tidb/v4/pull/13660 for more details.
	// suppose port and security config items cannot be change online.
	confContent, err := encodeConfig(ch.curConf.Load().(*Config))
	if err != nil {
		logutil.Logger(context.Background()).Warn("encode config error when registering", zap.Error(err))
		return
	}

	status, version, conf, err := ch.pdConfCli.Create(context.Background(), new(configpb.Version), tidbComponentName, ch.id, confContent)
	if err != nil {
		logutil.Logger(context.Background()).Warn("RPC to PD error when registering", zap.Error(err))
		return
	} else if status.Code != configpb.StatusCode_OK && status.Code != configpb.StatusCode_WRONG_VERSION {
		logutil.Logger(context.Background()).Warn("invalid status when registering", zap.String("code", status.Code.String()), zap.String("errmsg", status.Message))
		return
	}

	if ch.updateConfig(conf, version) {
		ch.registered = true
		logutil.Logger(context.Background()).Info("PDConfHandler register successfully")
		ch.writeConfig()
	}
}

func (ch *pdConfHandler) writeConfig() {
	if ch.confPath == "" { // for test
		return
	}
	conf := ch.curConf.Load().(*Config)
	if err := atomicWriteConfig(conf, ch.confPath); err != nil {
		logutil.Logger(context.Background()).Warn("write config to disk error", zap.Error(err))
	}
}

func (ch *pdConfHandler) updateConfig(newConfContent string, newVersion *configpb.Version) (updated bool) {
	newConf, err := decodeConfig(newConfContent)
	if err != nil {
		logutil.Logger(context.Background()).Warn("decode config error", zap.Error(err))
		return false
	} else if err := newConf.Valid(); err != nil {
		logutil.Logger(context.Background()).Warn("invalid remote config", zap.Error(err))
		return false
	}
	oldConf := ch.curConf.Load().(*Config)
	mergedConf, err := CloneConf(oldConf)
	if err != nil {
		logutil.Logger(context.Background()).Warn("clone config error", zap.Error(err))
		return false
	}
	as, rs := MergeConfigItems(mergedConf, newConf)
	ch.reloadFunc(oldConf, mergedConf)
	ch.curConf.Store(mergedConf)
	ch.version = newVersion
	logutil.Logger(context.Background()).Info("PDConfHandler updates config successfully",
		zap.String("new_version", newVersion.String()),
		zap.Any("accepted_conf_items", as), zap.Any("rejected_conf_items", rs))
	return len(as) > 0
}

func (ch *pdConfHandler) run() {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(context.Background()).Error("panic in the recoverable goroutine",
				zap.Reflect("r", r),
				zap.Stack("stack trace"))
		}
		ch.wg.Done()
	}()

	ch.register() // the first time to register
	if ch.timeAfter == nil {
		ch.timeAfter = time.After
	}
	for {
		select {
		case <-ch.timeAfter(ch.interval):
			if !ch.registered {
				ch.register()
				continue
			}

			// fetch new config from PD
			status, version, newConfContent, err := ch.pdConfCli.Get(context.Background(), ch.version, tidbComponentName, ch.id)
			if err != nil {
				logutil.Logger(context.Background()).Error("PDConfHandler fetch new config error", zap.Error(err))
				continue
			}
			if status.Code == configpb.StatusCode_OK { // StatusCode_OK represents the request is successful and there is no change.
				continue
			}
			if status.Code != configpb.StatusCode_WRONG_VERSION {
				// StatusCode_WRONG_VERSION represents the request is successful and the config has been updated.
				logutil.Logger(context.Background()).Error("PDConfHandler fetch new config PD error",
					zap.Int("code", int(status.Code)), zap.String("message", status.Message))
				continue
			}

			if ch.updateConfig(newConfContent, version) {
				ch.writeConfig()
			}
		case <-ch.exit:
			return
		}
	}
}

func encodeConfig(conf *Config) (string, error) {
	confBuf := bytes.NewBuffer(nil)
	te := toml.NewEncoder(confBuf)
	if err := te.Encode(conf); err != nil {
		return "", errors.New("encode config error=" + err.Error())
	}
	return confBuf.String(), nil
}

func decodeConfig(content string) (*Config, error) {
	c := new(Config)
	_, err := toml.Decode(content, c)
	return c, err
}

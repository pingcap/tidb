// Copyright 2019 PingCAP, Inc.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/configpb"
	"github.com/pingcap/pd/client"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ConfHandler is used to load and update config online.
// See https://github.com/pingcap/tidb/pull/13660 for more details.
type ConfHandler interface {
	Start()
	Close()
	GetConfig() *Config // read only
}

// ConfReloadFunc is used to reload the config to make it work.
type ConfReloadFunc func(oldConf, newConf *Config)

// NewConfHandler creates a new ConfHandler according to the local config.
func NewConfHandler(localConf *Config, reloadFunc ConfReloadFunc) (ConfHandler, error) {
	switch defaultConf.Store {
	case "tikv":
		return newPDConfHandler(localConf, reloadFunc, nil)
	default:
		return &constantConfHandler{localConf}, nil
	}
}

type constantConfHandler struct {
	conf *Config
}

func (cch *constantConfHandler) Start() {}

func (cch *constantConfHandler) Close() {}

func (cch *constantConfHandler) GetConfig() *Config { return cch.conf }

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
}

func newPDConfHandler(localConf *Config, reloadFunc ConfReloadFunc,
	newPDCliFunc func([]string, pd.SecurityOption) (pd.ConfigClient, error), // for test
) (*pdConfHandler, error) {
	addresses, _, err := ParsePath(localConf.Path)
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
		return nil, errors.Trace(err)
	}
	confContent, err := encodeConfig(localConf)
	if err != nil {
		return nil, err
	}

	// register to PD and get the new default config.
	// see https://github.com/pingcap/tidb/pull/13660 for more details.
	// suppose port and security config items cannot be change online.
	status, version, conf, err := pdCli.Create(context.Background(), new(configpb.Version), tidbComponentName, id, confContent)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if status.Code != configpb.StatusCode_OK {
		return nil, errors.New(fmt.Sprintf("fail to register config to PD, errmsg=%v", status.Message))
	}

	newConf, err := decodeConfig(conf)
	if err != nil {
		return nil, err
	}
	if err := newConf.Valid(); err != nil {
		return nil, errors.Trace(err)
	}

	ch := &pdConfHandler{
		id:         id,
		version:    version,
		interval:   pdConfHandlerRefreshInterval,
		exit:       make(chan struct{}),
		pdConfCli:  pdCli,
		reloadFunc: reloadFunc,
	}
	ch.curConf.Store(newConf)
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

func (ch *pdConfHandler) run() {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(context.Background()).Error("[PDConfHandler] panic: " + fmt.Sprintf("%v", r))
		}
		ch.wg.Done()
	}()
	for {
		select {
		case <-time.After(ch.interval):
			// fetch new config from PD
			status, version, newConfContent, err := ch.pdConfCli.Get(context.Background(), ch.version, tidbComponentName, ch.id)
			if err != nil {
				logutil.Logger(context.Background()).Error("[PDConfHandler] fetch new config error", zap.Error(err))
				continue
			}
			if status.Code == configpb.StatusCode_NOT_CHANGE {
				continue
			}
			if status.Code != configpb.StatusCode_OK {
				logutil.Logger(context.Background()).Error("[PDConfHandler] fetch new config PD error",
					zap.Int("code", int(status.Code)), zap.String("message", status.Message))
				continue
			}
			newConf, err := decodeConfig(newConfContent)
			if err != nil {
				logutil.Logger(context.Background()).Error("[PDConfHandler] decode config error", zap.Error(err))
				continue
			}
			if err := newConf.Valid(); err != nil {
				logutil.Logger(context.Background()).Error("[PDConfHandler] invalid config", zap.Error(err))
				continue
			}

			if ch.reloadFunc != nil {
				ch.reloadFunc(ch.curConf.Load().(*Config), newConf)
			}
			ch.curConf.Store(newConf)
			logutil.Logger(context.Background()).Info("[PDConfHandler] update config successfully",
				zap.String("fromVersion", ch.version.String()), zap.String("toVersion", version.String()))
			ch.version = version
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
	return c, errors.Trace(err)
}

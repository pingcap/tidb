// Copyright 2024 PingCAP, Inc.
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

package cdcutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"regexp"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// CDCPrefix is the prefix of CDC information in etcd.
	CDCPrefix = "/tidb/cdc/"
	// ChangefeedPath is the path of changefeed information in etcd.
	ChangefeedPath = "/changefeed/info/"
	// CDCPrefixV61 is the prefix of CDC information in etcd.
	CDCPrefixV61 = "/tidb/cdc/changefeed/info/"
)

type keyVersion int

const (
	legacy     keyVersion = 1
	namespaced keyVersion = 2

	invalidTs uint64 = math.MaxUint64
)

var (
	// cluster id should be valid in
	// https://github.com/pingcap/tiflow/blob/ca69c33948bea082aff9f4c0a357ace735b494ed/pkg/config/server_config.go#L218
	clusterNameRe = regexp.MustCompile("^[a-zA-Z0-9]+(-[a-zA-Z0-9]+)*$")
)

type changefeed struct {
	ID         string
	Cluster    string
	Namespace  string
	KeyVersion keyVersion
}

func (c *changefeed) infoKey() string {
	switch c.KeyVersion {
	case legacy:
		return path.Join(CDCPrefix, "changefeed", "info", c.ID)
	case namespaced:
		return path.Join(CDCPrefix, c.Cluster, c.Namespace, "changefeed", "info", c.ID)
	}
	log.Panic("Invalid changefeed version type.", zap.Any("this", c))
	return ""
}

func (c *changefeed) statusKey() string {
	switch c.KeyVersion {
	case legacy:
		return path.Join(CDCPrefix, "changefeed", "status", c.ID)
	case namespaced:
		return path.Join(CDCPrefix, c.Cluster, c.Namespace, "changefeed", "status", c.ID)
	}
	log.Panic("Invalid changefeed version type.", zap.Any("this", c))
	return ""
}

type checkCDCClient struct {
	cli *clientv3.Client
}

func (c checkCDCClient) loadChangefeeds(ctx context.Context, out *[]changefeed) error {
	resp, err := c.cli.Get(ctx, CDCPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return errors.Trace(err)
	}
	for _, kv := range resp.Kvs {
		// example: /tidb/cdc/<clusterID>/<namespace>/changefeed/info/<changefeedID>
		k := kv.Key[len(CDCPrefix)-1:]
		// example of k( >6.1): /<clusterID>/<namespace>/changefeed/info/<changefeedID>
		// example of k(<=6.1): /changefeed/info/<changefeedID>
		clusterAndNamespace, changefeedID, found := bytes.Cut(k, []byte(ChangefeedPath))
		if !found {
			continue
		}

		if len(clusterAndNamespace) == 0 {
			// They should be keys with format /tidb/cdc/changefeed/info.
			*out = append(*out, changefeed{
				ID:         string(changefeedID),
				KeyVersion: legacy,
			})
			continue
		}

		// example: clusterAndNamespace[1:] normally is <clusterID>/<namespace>
		// but in migration scenario it become __backup__. we need handle it
		// see https://github.com/pingcap/tiflow/issues/9807
		clusterID, namespace, found := bytes.Cut(clusterAndNamespace[1:], []byte(`/`))
		if !found {
			// ignore __backup__ or other formats
			continue
		}

		matched := clusterNameRe.Match(clusterID)
		if !matched {
			continue
		}

		*out = append(*out, changefeed{
			ID:         string(changefeedID),
			Cluster:    string(clusterID),
			Namespace:  string(namespace),
			KeyVersion: namespaced,
		})
	}
	return nil
}

type changefeedInfoView struct {
	State string `json:"state"`
	Start uint64 `json:"start-ts"`
}

type changefeedStatusView struct {
	Checkpoint uint64 `json:"checkpoint-ts"`
}

func (c checkCDCClient) fetchCheckpointTSFromStatus(ctx context.Context, cf changefeed) (uint64, error) {
	statusResp, err := c.cli.KV.Get(ctx, cf.statusKey())
	if err != nil {
		return 0, errors.Trace(err)
	}
	if statusResp.Count == 0 || len(statusResp.Kvs[0].Value) == 0 {
		// The changefeed might was created recently, or just a phantom in test cases...
		return 0, nil
	}
	var status changefeedStatusView
	if err := json.Unmarshal(statusResp.Kvs[0].Value, &status); err != nil {
		return 0, errors.Trace(err)
	}
	return status.Checkpoint, nil
}

func (c checkCDCClient) checkpointTSFor(ctx context.Context, cf changefeed) (uint64, error) {
	infoResp, err := c.cli.KV.Get(ctx, cf.infoKey())
	if err != nil {
		return 0, errors.Trace(err)
	}
	if infoResp.Count == 0 {
		// The changefeed have been removed.
		return invalidTs, nil
	}
	var info changefeedInfoView
	if err := json.Unmarshal(infoResp.Kvs[0].Value, &info); err != nil {
		return 0, errors.Trace(err)
	}
	switch info.State {
	// https://docs.pingcap.com/zh/tidb/stable/ticdc-changefeed-overview
	case "finished":
		return invalidTs, nil
	case "failed", "running", "warning", "normal", "stopped", "error":
		cts, err := c.fetchCheckpointTSFromStatus(ctx, cf)
		if err != nil {
			return 0, err
		}
		return mathutil.Max(cts, info.Start), nil
	default:
		// This changefeed may be noise, ignore it.
		log.Warn("Ignoring invalid changefeed.", zap.Any("changefeed", cf), zap.String("state", info.State))
		return invalidTs, nil
	}
}

func (c checkCDCClient) getIncompatible(ctx context.Context, safeTS uint64) (*CDCNameSet, error) {
	changefeeds := make([]changefeed, 0)
	if err := c.loadChangefeeds(ctx, &changefeeds); err != nil {
		return nil, err
	}

	nameset := new(CDCNameSet)
	for _, cf := range changefeeds {
		cts, err := c.checkpointTSFor(ctx, cf)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to check changefeed %v", cf)
		}
		if cts < safeTS {
			log.Info("Found incompatible changefeed.", zap.Any("changefeed", cf), zap.Uint64("checkpoint-ts", cts), zap.Uint64("safe-ts", safeTS))
			nameset.save(cf)
		}
	}

	return nameset, nil
}

// CDCNameSet saves CDC changefeed's information.
// nameSet maps `cluster/namespace` to `changefeed`s
type CDCNameSet struct {
	changefeeds map[string][]string
}

func (s *CDCNameSet) save(cf changefeed) {
	if s.changefeeds == nil {
		s.changefeeds = map[string][]string{}
	}

	switch cf.KeyVersion {
	case legacy:
		s.changefeeds["<nil>"] = append(s.changefeeds["<nil>"], cf.ID)
	case namespaced:
		k := path.Join(cf.Cluster, cf.Namespace)
		s.changefeeds[k] = append(s.changefeeds[k], cf.ID)
	}
}

// Empty that the nameSet is empty means no changefeed exists.
func (s *CDCNameSet) Empty() bool {
	return len(s.changefeeds) == 0
}

// MessageToUser convert the map `nameSet` to a readable message to user.
func (s *CDCNameSet) MessageToUser() string {
	var changefeedMsgBuf strings.Builder
	changefeedMsgBuf.WriteString("found CDC changefeed(s): ")
	for clusterID, changefeedID := range s.changefeeds {
		changefeedMsgBuf.WriteString("cluster/namespace: ")
		changefeedMsgBuf.WriteString(clusterID)
		changefeedMsgBuf.WriteString(" changefeed(s): ")
		changefeedMsgBuf.WriteString(fmt.Sprintf("%v", changefeedID))
		changefeedMsgBuf.WriteString(", ")
	}
	return changefeedMsgBuf.String()
}

// GetRunningChangefeeds gets CDC changefeed information and wraps them to a map
// for CDC >= v6.2, the etcd key format is /tidb/cdc/<clusterID>/<namespace>/changefeed/info/<changefeedID>
// for CDC <= v6.1, the etcd key format is /tidb/cdc/changefeed/info/<changefeedID>
func GetRunningChangefeeds(ctx context.Context, cli *clientv3.Client) (*CDCNameSet, error) {
	checkCli := checkCDCClient{cli: cli}
	return checkCli.getIncompatible(ctx, invalidTs)
}

// GetIncompatibleChangefeedsWithSafeTS gets CDC changefeed that may not compatible with the safe ts and wraps them to a map.
func GetIncompatibleChangefeedsWithSafeTS(ctx context.Context, cli *clientv3.Client, safeTS uint64) (*CDCNameSet, error) {
	checkCli := checkCDCClient{cli: cli}
	return checkCli.getIncompatible(ctx, safeTS)
}

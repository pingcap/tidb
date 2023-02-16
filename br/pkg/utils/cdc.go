package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	CDCPrefix      = "/tidb/cdc/"
	ChangefeedPath = "/changefeed/info/"
	CDCPrefixV61   = "/tidb/cdc/changefeed/info/"
)

type CDCNameSet struct {
	nameSet map[string][]string
}

func (s *CDCNameSet) Empty() bool {
	return len(s.nameSet) == 0
}

func (s *CDCNameSet) String() string {
	var changefeedMsgBuf strings.Builder
	changefeedMsgBuf.WriteString("found CDC changefeed(s): ")
	isFirst := true
	for clusterID, captureIDs := range s.nameSet {
		if !isFirst {
			changefeedMsgBuf.WriteString(", ")
		}
		isFirst = false
		changefeedMsgBuf.WriteString("cluster/namespace: ")
		changefeedMsgBuf.WriteString(clusterID)
		changefeedMsgBuf.WriteString(" changefeed(s): ")
		changefeedMsgBuf.WriteString(fmt.Sprintf("%v", captureIDs))
	}
	changefeedMsgBuf.WriteString(",")
	return changefeedMsgBuf.String()
}

func GetCDCNameSet(ctx context.Context, cli *clientv3.Client) (*CDCNameSet, error) {
	nameSet := make(map[string][]string, 1)
	// check etcd KV of CDC >= v6.2
	resp, err := cli.Get(ctx, CDCPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, kv := range resp.Kvs {
		// example: /tidb/cdc/<clusterID>/<namespace>/changefeed/info/<changefeedID>
		k := kv.Key[len(CDCPrefix):]
		clusterAndNamespace, changefeedID, found := bytes.Cut(k, []byte(ChangefeedPath))
		if !found {
			continue
		}
		if !isActiveCDCChangefeed(kv.Value) {
			continue
		}

		nameSet[string(clusterAndNamespace)] = append(nameSet[string(clusterAndNamespace)], string(changefeedID))
	}
	if len(nameSet) == 0 {
		// check etcd KV of CDC <= v6.1
		resp, err = cli.Get(ctx, CDCPrefixV61, clientv3.WithPrefix())
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, kv := range resp.Kvs {
			// example: /tidb/cdc/changefeed/info/<changefeedID>
			k := kv.Key[len(CDCPrefixV61):]
			if len(k) == 0 {
				continue
			}
			if !isActiveCDCChangefeed(kv.Value) {
				continue
			}

			nameSet["<nil>"] = append(nameSet["<nil>"], string(k))
		}
	}

	return &CDCNameSet{nameSet}, nil
}

type onlyState struct {
	State string `json:"state"`
}

func isActiveCDCChangefeed(jsonBytes []byte) bool {
	s := onlyState{}
	err := json.Unmarshal(jsonBytes, &s)
	if err != nil {
		// maybe a compatible issue, skip this key
		log.L().Error("unmarshal etcd value failed when check CDC changefeed, will skip this key",
			zap.ByteString("value", jsonBytes),
			zap.Error(err))
		return false
	}
	switch s.State {
	case "normal", "stopped", "error":
		return true
	default:
		return false
	}
}

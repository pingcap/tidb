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

package serverinfo

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// serverStatusAddressPath stores active advertised status endpoint claims.
const serverStatusAddressPath = "/tidb/server/status_addr"

type statusEndpointClaimState int

const (
	statusEndpointClaimSkipped statusEndpointClaimState = iota
	statusEndpointClaimAcquired
	statusEndpointClaimConflict
	statusEndpointClaimCheckFailed
)

type statusEndpointClaimResult struct {
	state         statusEndpointClaimState
	endpoint      string
	claimKey      string
	localID       string
	existingID    string
	existingLease clientv3.LeaseID
	err           error
}

type observedStatusEndpointClaim struct {
	id          string
	lease       clientv3.LeaseID
	modRevision int64
}

// statusEndpointClaim owns the etcd representation and operations for one advertised status endpoint.
// Syncer still owns the server-info session lifecycle and passes the current lease to each operation.
type statusEndpointClaim struct {
	etcdClient *clientv3.Client
	endpoint   string
	key        string
	localID    string
	keyspace   string
	report     func(statusEndpointClaimResult)
}

func newStatusEndpointClaim(etcdClient *clientv3.Client, info *ServerInfo, enabled bool) *statusEndpointClaim {
	endpoint, key := buildStatusEndpointClaim(info, enabled)
	claim := &statusEndpointClaim{
		etcdClient: etcdClient,
		endpoint:   endpoint,
		key:        key,
		localID:    info.ID,
		keyspace:   info.Keyspace,
	}
	claim.report = claim.reportResult
	return claim
}

func buildStatusEndpointClaim(info *ServerInfo, reportStatus bool) (endpoint, claimKey string) {
	if !reportStatus || info.IsAssumed() || info.StatusPort == 0 {
		return "", ""
	}

	host := strings.TrimSpace(info.IP)
	if host == "" {
		return "", ""
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		host = addr.String()
	} else {
		host = strings.TrimSuffix(strings.ToLower(host), ".")
	}
	if host == "" {
		return "", ""
	}

	endpoint = net.JoinHostPort(host, strconv.Itoa(int(info.StatusPort)))
	segment := base64.RawURLEncoding.EncodeToString([]byte(endpoint))
	claimKey = fmt.Sprintf("%s/%s", serverStatusAddressPath, segment)
	return endpoint, claimKey
}

func (c *statusEndpointClaim) check(ctx context.Context, lease clientv3.LeaseID) {
	result := c.acquire(ctx, lease)
	if ctx.Err() == nil && c.report != nil {
		c.report(result)
	}
}

func (c *statusEndpointClaim) acquire(ctx context.Context, lease clientv3.LeaseID) statusEndpointClaimResult {
	result := statusEndpointClaimResult{
		state:    statusEndpointClaimSkipped,
		endpoint: c.endpoint,
		claimKey: c.key,
		localID:  c.localID,
	}
	if c.etcdClient == nil || c.key == "" {
		return result
	}

	claimCtx, cancel := context.WithTimeout(ctx, KeyOpDefaultTimeout)
	defer cancel()

	created, observed, err := c.tryCreate(claimCtx, lease)
	if err != nil {
		result.state = statusEndpointClaimCheckFailed
		result.err = err
		return result
	}
	if created {
		result.state = statusEndpointClaimAcquired
		return result
	}
	result.existingID = observed.id
	result.existingLease = observed.lease
	if observed.id != result.localID {
		result.state = statusEndpointClaimConflict
		return result
	}

	reattached, err := c.reattach(claimCtx, observed, lease)
	if err != nil {
		result.state = statusEndpointClaimCheckFailed
		result.err = err
		return result
	}
	if reattached {
		result.state = statusEndpointClaimAcquired
		return result
	}

	created, observed, err = c.tryCreate(claimCtx, lease)
	if err != nil {
		result.state = statusEndpointClaimCheckFailed
		result.err = err
		return result
	}
	if created {
		result.state = statusEndpointClaimAcquired
		return result
	}
	result.existingID = observed.id
	result.existingLease = observed.lease
	if observed.id != result.localID {
		result.state = statusEndpointClaimConflict
		return result
	}

	result.state = statusEndpointClaimCheckFailed
	result.err = errors.New("advertised status endpoint claim changed while reattaching the same server info ID")
	return result
}

func (c *statusEndpointClaim) tryCreate(
	ctx context.Context,
	lease clientv3.LeaseID,
) (bool, observedStatusEndpointClaim, error) {
	resp, err := c.etcdClient.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(c.key), "=", 0)).
		Then(clientv3.OpPut(c.key, c.localID, clientv3.WithLease(lease))).
		Else(clientv3.OpGet(c.key)).
		Commit()
	if err != nil {
		return false, observedStatusEndpointClaim{}, errors.Trace(err)
	}
	if resp.Succeeded {
		return true, observedStatusEndpointClaim{}, nil
	}
	observed, err := observedStatusEndpointClaimFromTxn(resp)
	return false, observed, err
}

func (c *statusEndpointClaim) reattach(
	ctx context.Context,
	observed observedStatusEndpointClaim,
	lease clientv3.LeaseID,
) (bool, error) {
	resp, err := c.etcdClient.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(c.key), "=", observed.id),
			clientv3.Compare(clientv3.ModRevision(c.key), "=", observed.modRevision),
		).
		Then(clientv3.OpPut(c.key, observed.id, clientv3.WithLease(lease))).
		Else(clientv3.OpGet(c.key)).
		Commit()
	if err != nil {
		return false, errors.Trace(err)
	}
	return resp.Succeeded, nil
}

func observedStatusEndpointClaimFromTxn(resp *clientv3.TxnResponse) (observedStatusEndpointClaim, error) {
	if len(resp.Responses) != 1 {
		return observedStatusEndpointClaim{}, errors.Errorf("unexpected advertised status endpoint claim response count %d", len(resp.Responses))
	}
	rangeResp := resp.Responses[0].GetResponseRange()
	if rangeResp == nil || len(rangeResp.Kvs) != 1 {
		return observedStatusEndpointClaim{}, errors.New("advertised status endpoint claim disappeared while reading its owner")
	}
	kv := rangeResp.Kvs[0]
	return observedStatusEndpointClaim{
		id:          string(kv.Value),
		lease:       clientv3.LeaseID(kv.Lease),
		modRevision: kv.ModRevision,
	}, nil
}

func (c *statusEndpointClaim) reportResult(result statusEndpointClaimResult) {
	fields := []zap.Field{
		zap.String("advertised-status-endpoint", result.endpoint),
		zap.String("claim-key", result.claimKey),
		zap.String("local-server-info-id", result.localID),
	}
	if c.keyspace != "" {
		fields = append(fields, zap.String("keyspace", c.keyspace))
	}

	switch result.state {
	case statusEndpointClaimConflict:
		fields = append(fields,
			zap.String("existing-server-info-id", result.existingID),
			zap.String("existing-lease-id", tidbutil.FormatLeaseID(result.existingLease)),
			zap.String("action", "check for duplicate advertise-address and status-port settings, copied startup configuration, or a TiDB instance outside the intended topology"),
		)
		logutil.BgLogger().Warn("advertised status endpoint already has an active claim", fields...)
	case statusEndpointClaimCheckFailed:
		fields = append(fields,
			zap.String("action", "check etcd connectivity and whether the advertised status endpoint claim can be read or updated"),
			zap.Error(result.err),
		)
		logutil.BgLogger().Warn("failed to check advertised status endpoint claim", fields...)
	}
}

func (c *statusEndpointClaim) cleanupFields(lease clientv3.LeaseID, action string) []zap.Field {
	return []zap.Field{
		zap.String("advertised-status-endpoint", c.endpoint),
		zap.String("claim-key", c.key),
		zap.String("local-server-info-id", c.localID),
		zap.String("lease-id", tidbutil.FormatLeaseID(lease)),
		zap.String("action", action),
	}
}

func (c *statusEndpointClaim) remove(ctx context.Context, lease clientv3.LeaseID) error {
	if c.key == "" {
		return nil
	}
	_, err := c.etcdClient.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(c.key), "=", c.localID),
			clientv3.Compare(clientv3.LeaseValue(c.key), "=", lease),
		).
		Then(clientv3.OpDelete(c.key)).
		Commit()
	return errors.Trace(err)
}

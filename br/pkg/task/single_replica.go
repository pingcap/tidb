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

package task

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/conn"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	pdhttp "github.com/tikv/pd/client/http"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

const (
	// FlagSingleReplica is the flag of the single replica restore.
	FlagSingleReplica = "single-replica"

	// singleReplicaMarkKey holds the replica settings of the cluster from
	// before a single replica restore lowers them until it has put them back.
	singleReplicaMarkKey = "/tidb/br/single-replica-restore"

	singleReplicaMarkTimeout = 10 * time.Second

	singleReplicaNoRedundancy = "PD is adding the other replicas in the background. Until it finishes the cluster " +
		"has no redundancy: losing a TiKV loses the regions it holds. `pd-ctl region check miss-peer` lists the " +
		"regions still waiting."
)

var (
	singleReplicaResetTimeout       = 3 * time.Minute
	singleReplicaResetRetryInterval = 3 * time.Second
	singleReplicaShrinkTimeout      = 5 * time.Minute
	singleReplicaShrinkPollInterval = 2 * time.Second
)

// replicaConfigClient is the part of the PD client the single replica restore
// changes the cluster with.
type replicaConfigClient interface {
	GetReplicateConfig(context.Context) (map[string]any, error)
	SetConfig(context.Context, map[string]any, ...float64) error
	GetPlacementRule(context.Context, string, string) (*pdhttp.Rule, error)
	SetPlacementRule(context.Context, *pdhttp.Rule) error
	GetRegions(context.Context) (*pdhttp.RegionsInfo, error)
}

// replicaSettings is where PD keeps the replica count of the cluster: the
// default placement rule when placement rules are on, max-replicas otherwise.
type replicaSettings struct {
	MaxReplicas  int          `json:"max-replicas"`
	RulesEnabled bool         `json:"placement-rules"`
	DefaultRule  *pdhttp.Rule `json:"default-rule,omitempty"`
}

func (r *replicaSettings) count() int {
	if r.RulesEnabled {
		return r.DefaultRule.Count
	}
	return r.MaxReplicas
}

// where names the setting that holds the replica count.
func (r *replicaSettings) where() string {
	if r.RulesEnabled {
		return "the count of the default placement rule"
	}
	return "max-replicas"
}

func readReplicaSettings(ctx context.Context, pd replicaConfigClient) (*replicaSettings, error) {
	cfg, err := pd.GetReplicateConfig(ctx)
	if err != nil {
		return nil, errors.Annotate(err, "single replica: read the replication config")
	}
	replicas, ok := cfg["max-replicas"].(float64)
	if !ok {
		return nil, errors.Errorf("single replica: unexpected max-replicas %v", cfg["max-replicas"])
	}
	r := &replicaSettings{
		MaxReplicas:  int(replicas),
		RulesEnabled: fmt.Sprint(cfg["enable-placement-rules"]) == "true",
	}
	if !r.RulesEnabled {
		return r, nil
	}
	if r.DefaultRule, err = pd.GetPlacementRule(ctx, "pd", "default"); err != nil {
		return nil, errors.Annotate(err, "single replica: read the default placement rule")
	}
	if r.DefaultRule == nil {
		return nil, errors.New("single replica: PD has no default placement rule")
	}
	return r, nil
}

// setReplicaCount sets the replica count where PD keeps it. With placement
// rules on PD follows the default rule and refuses a new max-replicas that
// differs from it, so only the rule is set.
func setReplicaCount(ctx context.Context, pd replicaConfigClient, r *replicaSettings, count int) error {
	if r.RulesEnabled {
		rule := *r.DefaultRule
		rule.Count = count
		return pd.SetPlacementRule(ctx, &rule)
	}
	return pd.SetConfig(ctx, map[string]any{"max-replicas": count})
}

// putBackReplicaCount sets the replica count back to the one of r. It retries
// while PD does not take it, for example during a leader change, and goes on
// after ctx is cancelled.
func putBackReplicaCount(ctx context.Context, pd replicaConfigClient, r *replicaSettings) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), singleReplicaResetTimeout)
	defer cancel()
	var lastErr error
	for {
		err := setReplicaCount(ctx, pd, r, r.count())
		if err == nil {
			return nil
		}
		// An attempt cut short by the timeout does not hide what PD said before.
		if lastErr == nil || ctx.Err() == nil {
			lastErr = err
		}
		log.Warn("single replica restore: failed to put the replica count back, retrying", zap.Error(err))
		select {
		case <-ctx.Done():
			return errors.Annotatef(lastErr, "single replica: set %s back to %d", r.where(), r.count())
		case <-time.After(singleReplicaResetRetryInterval):
		}
	}
}

// singleReplicaMark keeps the replica settings of the cluster outside of BR
// while a single replica restore has them lowered. A restore that dies before
// it puts the replica count back leaves the mark, and the next restore or
// abort puts the count back from it.
type singleReplicaMark interface {
	// load returns nil when there is no mark.
	load(context.Context) (*replicaSettings, error)
	save(context.Context, *replicaSettings) error
	remove(context.Context) error
}

type etcdSingleReplicaMark struct {
	cli *clientv3.Client
}

func (m etcdSingleReplicaMark) load(ctx context.Context) (*replicaSettings, error) {
	ctx, cancel := context.WithTimeout(ctx, singleReplicaMarkTimeout)
	defer cancel()
	resp, err := m.cli.Get(ctx, singleReplicaMarkKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	r := &replicaSettings{}
	if err := json.Unmarshal(resp.Kvs[0].Value, r); err != nil {
		return nil, errors.Annotatef(err, "unexpected content of %s", singleReplicaMarkKey)
	}
	if r.RulesEnabled && r.DefaultRule == nil {
		return nil, errors.Errorf("no placement rule in %s", singleReplicaMarkKey)
	}
	return r, nil
}

func (m etcdSingleReplicaMark) save(ctx context.Context, r *replicaSettings) error {
	data, err := json.Marshal(r)
	if err != nil {
		return errors.Trace(err)
	}
	ctx, cancel := context.WithTimeout(ctx, singleReplicaMarkTimeout)
	defer cancel()
	_, err = m.cli.Put(ctx, singleReplicaMarkKey, string(data))
	return errors.Trace(err)
}

func (m etcdSingleReplicaMark) remove(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, singleReplicaMarkTimeout)
	defer cancel()
	_, err := m.cli.Delete(ctx, singleReplicaMarkKey)
	return errors.Trace(err)
}

// removeSingleReplicaMark drops the mark once the replica count is back. A
// mark that stays is dropped by the next run, which sees the count is back.
func removeSingleReplicaMark(ctx context.Context, mark singleReplicaMark) {
	if err := mark.remove(context.WithoutCancel(ctx)); err != nil {
		log.Warn("single replica restore: failed to remove the mark", zap.Error(err))
	}
}

// checkSingleReplica rejects --single-replica where it cannot work. The replica
// count belongs to the whole cluster, which its keyspaces share.
func (cfg *RestoreConfig) checkSingleReplica() error {
	if !cfg.SingleReplica {
		return nil
	}
	if kerneltype.IsNextGen() {
		return errors.Annotatef(berrors.ErrInvalidArgument, "--%s is not supported by the next-gen kernel", FlagSingleReplica)
	}
	if len(cfg.KeyspaceName) > 0 {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"--%s is not supported with a keyspace: it changes the replica count of every keyspace", FlagSingleReplica)
	}
	return nil
}

// singleReplicaRestore lowers the cluster to one replica for the restore and
// puts the replica count back afterwards. PD then adds the other replicas in
// the background at its own pace.
type singleReplicaRestore struct {
	pd      replicaConfigClient
	mark    singleReplicaMark
	console io.Writer

	// before is the replica settings the restore found.
	before *replicaSettings
	// lowered tells leave has a replica count to put back.
	lowered bool
}

// startSingleReplicaRestore puts the replica count back when an earlier single
// replica restore died before it could, then lowers it when this restore asks
// for it. It returns nil when the restore does not ask for it.
func startSingleReplicaRestore(
	ctx context.Context, cfg *RestoreConfig, mgr *conn.Mgr, etcdCLI *clientv3.Client, console io.Writer,
) (*singleReplicaRestore, error) {
	pd, mark := mgr.GetPDHTTPClient(), etcdSingleReplicaMark{cli: etcdCLI}
	if err := putBackInterruptedSingleReplicaRestore(ctx, pd, mark, console); err != nil {
		if cfg.SingleReplica {
			return nil, err
		}
		log.Warn("single replica restore: failed to put back what an interrupted restore left", zap.Error(err))
	}
	if !cfg.SingleReplica {
		return nil, nil
	}
	// Lowering the replicas also drops them from data that is already there.
	if err := restore.AssertUserDBsEmpty(mgr.GetDomain()); err != nil {
		return nil, errors.Annotate(err, "single replica restore needs a cluster without user tables")
	}
	s := &singleReplicaRestore{pd: pd, mark: mark, console: console}
	if err := s.enter(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// putBackSingleReplicaOnAbort puts the replica count back when the restore to
// abort was a single replica restore that died.
func putBackSingleReplicaOnAbort(ctx context.Context, cfg *RestoreConfig, mgr *conn.Mgr, console io.Writer) {
	etcdCLI, err := dialEtcdWithCfg(ctx, cfg.Config)
	if err != nil {
		log.Warn("single replica restore: failed to check for an interrupted restore", zap.Error(err))
		return
	}
	defer func() {
		if err := etcdCLI.Close(); err != nil {
			log.Warn("failed to close the etcd client", zap.Error(err))
		}
	}()
	mark := etcdSingleReplicaMark{cli: etcdCLI}
	if err := putBackInterruptedSingleReplicaRestore(ctx, mgr.GetPDHTTPClient(), mark, console); err != nil {
		log.Warn("single replica restore: failed to put back what an interrupted restore left", zap.Error(err))
	}
}

// putBackInterruptedSingleReplicaRestore puts the replica count back when a
// single replica restore died before it could, which its mark tells. It cannot
// tell a dead restore from a running one: a restore started next to a running
// single replica restore ends the single replica part of that one early.
func putBackInterruptedSingleReplicaRestore(
	ctx context.Context, pd replicaConfigClient, mark singleReplicaMark, console io.Writer,
) error {
	before, err := mark.load(ctx)
	if err != nil {
		return errors.Annotate(err, "single replica: read the mark of an interrupted restore")
	}
	if before == nil {
		return nil
	}
	now, err := readReplicaSettings(ctx, pd)
	if err != nil {
		return err
	}
	if now.RulesEnabled != before.RulesEnabled || now.count() > 1 {
		log.Info("single replica restore: the replica count was set after an interrupted restore, keeping it",
			zap.Int("replicas", now.count()))
	} else {
		if err := putBackReplicaCount(ctx, pd, before); err != nil {
			sayResetFailed(console, before, err)
			return err
		}
		log.Warn("single replica restore: put back the replica count an interrupted restore left at one",
			zap.Int("replicas", before.count()))
		say(console, "An interrupted restore with --%s left one replica of each region. The replica count is back to %d. %s",
			FlagSingleReplica, before.count(), singleReplicaNoRedundancy)
	}
	removeSingleReplicaMark(ctx, mark)
	return nil
}

// enter keeps one replica of each region. When it returns no error, leave must
// run however the restore ends.
func (s *singleReplicaRestore) enter(ctx context.Context) (err error) {
	if s.before, err = readReplicaSettings(ctx, s.pd); err != nil {
		return err
	}
	if s.before.count() <= 1 {
		log.Info("single replica restore: the cluster already keeps one replica of each region")
		return nil
	}
	if rule := s.before.DefaultRule; rule != nil && (len(rule.StartKeyHex) > 0 || len(rule.EndKeyHex) > 0) {
		return errors.Annotatef(berrors.ErrInvalidArgument,
			"--%s needs the default placement rule to cover the whole key space, it covers [%s, %s)",
			FlagSingleReplica, rule.StartKeyHex, rule.EndKeyHex)
	}
	// The mark goes first, so the replica count is never lowered without it.
	if err := s.mark.save(ctx, s.before); err != nil {
		return errors.Annotate(err, "single replica: save the replica count to put back")
	}
	log.Info("single replica restore: keeping one replica of each region during the restore",
		zap.Int("replicas", s.before.count()), zap.Bool("placement-rules", s.before.RulesEnabled))
	if err := setReplicaCount(ctx, s.pd, s.before, 1); err != nil {
		// PD may have applied it and only the response was lost.
		return s.undo(ctx, errors.Annotatef(err, "single replica: set %s to 1", s.before.where()))
	}
	if err := s.waitForOnePeer(ctx); err != nil {
		return s.undo(ctx, err)
	}
	s.lowered = true
	return nil
}

// waitForOnePeer waits until PD has removed the other peers of the regions
// that are there. The regions the restore splits from them then start with one
// peer, also in the ranges the restore denies scheduling for.
func (s *singleReplicaRestore) waitForOnePeer(ctx context.Context) error {
	deadline := time.Now().Add(singleReplicaShrinkTimeout)
	for {
		waiting, err := s.regionsWithOtherPeers(ctx)
		if err == nil && waiting == 0 {
			return nil
		}
		if ctx.Err() != nil {
			return errors.Trace(ctx.Err())
		}
		if time.Now().After(deadline) {
			log.Warn("single replica restore: PD has not removed the other replicas yet, restoring anyway",
				zap.Int("regions", waiting), zap.Error(err))
			say(s.console, "PD still keeps more than one replica of some regions. The restore goes on, "+
				"those regions get the data once per replica.")
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-time.After(singleReplicaShrinkPollInterval):
		}
	}
}

func (s *singleReplicaRestore) regionsWithOtherPeers(ctx context.Context) (int, error) {
	regions, err := s.pd.GetRegions(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	waiting := 0
	for i := range regions.Regions {
		voters := 0
		for _, peer := range regions.Regions[i].Peers {
			if !peer.IsLearner {
				voters++
			}
		}
		if voters > 1 {
			waiting++
		}
	}
	return waiting, nil
}

// undo puts back what enter already changed when enter fails half way.
func (s *singleReplicaRestore) undo(ctx context.Context, enterErr error) error {
	if err := s.putBack(ctx); err != nil {
		return multierr.Append(enterErr, err)
	}
	return enterErr
}

// putBack sets the replica count back and drops the mark. The mark stays when
// the count is not back, for the next run to try again.
func (s *singleReplicaRestore) putBack(ctx context.Context) error {
	if err := putBackReplicaCount(ctx, s.pd, s.before); err != nil {
		sayResetFailed(s.console, s.before, err)
		return err
	}
	removeSingleReplicaMark(ctx, s.mark)
	return nil
}

// replicasAfterRestore is the replica count the restored data will have once
// PD has added the other replicas.
func (s *singleReplicaRestore) replicasAfterRestore() uint64 {
	return uint64(s.before.count())
}

// leave puts the replica count back, also after a failed or cancelled restore.
func (s *singleReplicaRestore) leave(ctx context.Context, restoreErr error) error {
	if !s.lowered {
		say(s.console, "The cluster keeps one replica of each region, --%s changed nothing.", FlagSingleReplica)
		return restoreErr
	}
	if err := s.putBack(ctx); err != nil {
		return multierr.Append(restoreErr, err)
	}
	if restoreErr != nil {
		log.Warn("single replica restore: the restore failed, the replica count is back",
			zap.Int("replicas", s.before.count()))
		say(s.console, "The restore failed. The replica count is back to %d. %s",
			s.before.count(), singleReplicaNoRedundancy)
		return restoreErr
	}
	log.Warn("single replica restore: restored with one replica, PD is adding the other replicas",
		zap.Int("replicas", s.before.count()))
	say(s.console, "Restored with one replica of each region. The replica count is back to %d. %s",
		s.before.count(), singleReplicaNoRedundancy)
	return nil
}

func sayResetFailed(console io.Writer, before *replicaSettings, err error) {
	log.Error("single replica restore: failed to put the replica count back",
		zap.Int("replicas", before.count()), zap.Error(err))
	say(console, "The cluster may still keep one replica of each region. Set %s to %d in PD by hand, "+
		"or run br restore again: it puts the replica count back when it starts.", before.where(), before.count())
}

func say(console io.Writer, format string, args ...any) {
	if console != nil {
		_, _ = fmt.Fprintf(console, format+"\n", args...)
	}
}

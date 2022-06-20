// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/glue"
	"go.uber.org/zap"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	StateSuccess = "success"
)

// EBSVolume is passed by TiDB deployment tools: TiDB Operator and TiUP(in future)
// we should do snapshot inside BR, because we need some logic to determine the order of snapshot starts.
// TODO finish the info with TiDB Operator developer.
type EBSVolume struct {
	ID         string `json:"id" toml:"id"`
	Type       string `json:"type" toml:"type"`
	SnapshotID string `json:"snapshot_id" toml:"snapshot_id"`
	Status     string `json:"status" toml:"status"`
}

type EBSStore struct {
	StoreID uint64       `json:"store_id" toml:"store_id"`
	Volumes []*EBSVolume `json:"volumes" toml:"volumes"`
}

// ClusterInfo represents the tidb cluster level meta infos. such as
// pd cluster id/alloc id, cluster resolved ts and tikv configuration.
type ClusterInfo struct {
	ID         uint64            `json:"cluster_id" toml:"cluster_id"`
	Version    string            `json:"cluster_version" toml:"cluster_version"`
	MaxAllocID uint64            `json:"max_alloc_id" toml:"max_alloc_id"`
	ResolvedTS uint64            `json:"resolved_ts" toml:"resolved_ts"`
	Replicas   map[string]uint64 `json:"replicas" toml:"replicas"`
}

type Kubernetes struct {
	PVs     []interface{}          `json:"pvs" toml:"pvs"`
	PVCs    []interface{}          `json:"pvcs" toml:"pvcs"`
	CRD     interface{}            `json:"crd_tidb_cluster" toml:"crd_tidb_cluster"`
	Options map[string]interface{} `json:"options" toml:"options""`
}

type TiKVComponent struct {
	Replicas int         `json:"replicas"`
	Stores   []*EBSStore `json:"stores"`
}

type PDComponent struct {
	Replicas int `json:"replicas"`
}

type TiDBComponent struct {
	Replicas int `json:"replicas"`
}

type EBSBackupInfo struct {
	ClusterInfo    *ClusterInfo           `json:"cluster_info" toml:"cluster_info"`
	TiKVComponent  *TiKVComponent         `json:"tikv" toml:"tikv"`
	TiDBComponent  *TiDBComponent         `json:"tidb" toml:"tidb"`
	PDComponent    *PDComponent           `json:"pd" toml:"pd"`
	KubernetesMeta *Kubernetes            `json:"kubernetes" toml:"kubernetes"`
	Options        map[string]interface{} `json:"options" toml:"options"`
	Region         string                 `json:"region" toml:"region"`
}

func (c *EBSBackupInfo) GetSnapshotCount() uint64 {
	if c.TiKVComponent == nil {
		return 0
	}
	return uint64(len(c.TiKVComponent.Stores))
}

func (c *EBSBackupInfo) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

// ConfigFromFile loads config from file.
func (c *EBSBackupInfo) ConfigFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return errors.Trace(err)
	}
	err = json.Unmarshal(data, c)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *EBSBackupInfo) SetClusterID(id uint64) {
	c.ClusterInfo.ID = id
}

func (c *EBSBackupInfo) SetAllocID(id uint64) {
	c.ClusterInfo.MaxAllocID = id
}

type EC2Session struct {
	*ec2.EC2
}

func NewEC2Session() (*EC2Session, error) {
	awsConfig := aws.NewConfig()
	// NOTE: we do not need credential. TiDB Operator need make sure we have the correct permission to access
	// ec2 snapshot. we may change this behaviour in the future.
	sessionOptions := session.Options{Config: *awsConfig}
	sess, err := session.NewSessionWithOptions(sessionOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ec2Session := ec2.New(sess)
	return &EC2Session{ec2Session}, nil
}

// StartsEBSSnapshot is the mainly steps to control the data volume snapshots.
// It will do the following works.
// 1. determine the order of volume snapshot.
// 2. send snapshot requests to aws.
func (e *EC2Session) StartsEBSSnapshot(backupInfo *EBSBackupInfo) ([]*EBSVolume, error) {
	allVolumes := make([]*EBSVolume, 0)

	for _, store := range backupInfo.TiKVComponent.Stores {
		volumes := store.Volumes
		if len(volumes) > 1 {
			// if one store has multiple volume, we should respect the order
			// raft log/engine first, then kv db. then wal
			sort.SliceStable(volumes, func(i, j int) bool {
				if strings.Contains(volumes[i].Type, "raft") {
					return true
				}
				if strings.Contains(volumes[j].Type, "raft") {
					return false
				}
				if strings.Contains(volumes[i].Type, "storage") {
					return true
				}
				if strings.Contains(volumes[j].Type, "storage") {
					return true
				}
				return true
			})

		}
		for _, volume := range volumes {
			// TODO: build concurrent requests here.
			log.Debug("starts snapshot", zap.Any("volume", volume))
			resp, err := e.CreateSnapshot(&ec2.CreateSnapshotInput{
				VolumeId: &volume.ID,
				TagSpecifications: []*ec2.TagSpecification{
					{
						ResourceType: aws.String(ec2.ResourceTypeSnapshot),
					},
				},
			})
			if err != nil {
				// TODO: build an retry mechanism for EBS backup
				// consider remove the exists starts snapshots outside.
				return allVolumes, errors.Trace(err)
			}
			// record snapshot id here has two mainly reasons:
			// 1. check the snapshot whether finished or not.
			// 2. recover volume from snapshotID in EBS restoration.
			volume.SnapshotID = *resp.SnapshotId
		}
		// write back sorted volumes to EBSBackupInfo
		allVolumes = append(allVolumes, volumes...)
		store.Volumes = volumes
	}
	return allVolumes, nil
}

// WaitEBSSnapshotFinished waits all snapshots finished.
// according to EBS snapshot will do real snapshot background.
// so we'll check whether all snapshots finished.
func (e *EC2Session) WaitEBSSnapshotFinished(allVolumes []*EBSVolume, progress glue.Progress) error {
	pendingSnapshots := make([]*string, 0, len(allVolumes))
	// snapshot id -> status
	pendingMap := make(map[string]bool)
	for _, v := range allVolumes {
		pendingSnapshots = append(pendingSnapshots, &v.SnapshotID)
		pendingMap[v.SnapshotID] = false
	}

	log.Info("starts check pending snapshots", zap.Any("snapshots", pendingSnapshots))
	for {
		if len(pendingSnapshots) == 0 {
			log.Info("all pending volume snapshots are finished.")
			return nil
		}

		select {
		// check pending snapshots every 5 seconds
		case <-time.After(5 * time.Second):
			log.Info("check pending snapshots", zap.Int("count", len(pendingSnapshots)))
			resp, err := e.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
				SnapshotIds: pendingSnapshots,
			})
			if err != nil {
				// TODO build retry mechanism
				return errors.Trace(err)
			}
			for _, s := range resp.Snapshots {
				if *s.State == StateSuccess {
					// this snapshot has finished.
					pendingMap[*s.SnapshotId] = true
					progress.Inc()
				}
			}
			pendingSnapshots = nil
			for snap, ok := range pendingMap {
				if !ok {
					pendingSnapshots = append(pendingSnapshots, &snap)
				}
			}
		}
	}
}

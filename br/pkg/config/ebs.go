// Copyright 2022 PingCAP, Inc.
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
	"context"
	"encoding/json"
	"os"

	"github.com/Masterminds/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
	corev1 "k8s.io/api/core/v1"
)

type EBSVolumeType string

const (
	GP3Volume EBSVolumeType = "gp3"
	IO1Volume               = "io1"
	IO2Volume               = "io2"
)

func (t EBSVolumeType) Valid() bool {
	return t == GP3Volume || t == IO1Volume || t == IO2Volume
}

// EBSVolume is passed by TiDB deployment tools: TiDB Operator and TiUP(in future)
// we should do snapshot inside BR, because we need some logic to determine the order of snapshot starts.
// TODO finish the info with TiDB Operator developer.
type EBSVolume struct {
	ID              string `json:"volume_id" toml:"volume_id"`
	Type            string `json:"type" toml:"type"`
	SnapshotID      string `json:"snapshot_id" toml:"snapshot_id"`
	RestoreVolumeId string `json:"restore_volume_id" toml:"restore_volume_id"`
	VolumeAZ        string `json:"volume_az" toml:"volume_az"`
	Status          string `json:"status" toml:"status"`
}

type EBSStore struct {
	StoreID uint64       `json:"store_id" toml:"store_id"`
	Volumes []*EBSVolume `json:"volumes" toml:"volumes"`
}

// ClusterInfo represents the tidb cluster level meta infos. such as
// pd cluster id/alloc id, cluster resolved ts and tikv configuration.
type ClusterInfo struct {
	Version        string            `json:"cluster_version" toml:"cluster_version"`
	FullBackupType string            `json:"full_backup_type" toml:"full_backup_type"`
	ResolvedTS     uint64            `json:"resolved_ts" toml:"resolved_ts"`
	Replicas       map[string]uint64 `json:"replicas" toml:"replicas"`
}

type Kubernetes struct {
	PVs     []*corev1.PersistentVolume      `json:"pvs" toml:"pvs"`
	PVCs    []*corev1.PersistentVolumeClaim `json:"pvcs" toml:"pvcs"`
	CRD     any                             `json:"crd_tidb_cluster" toml:"crd_tidb_cluster"`
	Options map[string]any                  `json:"options" toml:"options"`
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

type EBSBasedBRMeta struct {
	ClusterInfo    *ClusterInfo   `json:"cluster_info" toml:"cluster_info"`
	TiKVComponent  *TiKVComponent `json:"tikv" toml:"tikv"`
	TiDBComponent  *TiDBComponent `json:"tidb" toml:"tidb"`
	PDComponent    *PDComponent   `json:"pd" toml:"pd"`
	KubernetesMeta *Kubernetes    `json:"kubernetes" toml:"kubernetes"`
	Options        map[string]any `json:"options" toml:"options"`
	Region         string         `json:"region" toml:"region"`
}

func (c *EBSBasedBRMeta) GetStoreCount() uint64 {
	if c.TiKVComponent == nil {
		return 0
	}
	return uint64(len(c.TiKVComponent.Stores))
}

func (c *EBSBasedBRMeta) GetTiKVVolumeCount() uint64 {
	if c.TiKVComponent == nil || len(c.TiKVComponent.Stores) == 0 {
		return 0
	}
	// Assume TiKV nodes are symmetric
	return uint64(len(c.TiKVComponent.Stores[0].Volumes))
}

func (c *EBSBasedBRMeta) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

// ConfigFromFile loads config from file.
func (c *EBSBasedBRMeta) ConfigFromFile(path string) error {
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

func NewMetaFromStorage(ctx context.Context, s storage.ExternalStorage) (*EBSBasedBRMeta, error) {
	metaInfo := &EBSBasedBRMeta{}
	metaBytes, err := s.ReadFile(ctx, metautil.MetaFile)
	if err != nil {
		return metaInfo, errors.Trace(err)
	}
	err = json.Unmarshal(metaBytes, metaInfo)
	if err != nil {
		return metaInfo, errors.Trace(err)
	}
	if err = metaInfo.checkEBSBRMeta(); err != nil {
		return metaInfo, errors.Trace(err)
	}

	return metaInfo, nil
}

func (c *EBSBasedBRMeta) CheckClusterInfo() {
	if c.ClusterInfo == nil {
		c.ClusterInfo = &ClusterInfo{}
	}
}

func (c *EBSBasedBRMeta) checkEBSBRMeta() error {
	if c.ClusterInfo == nil {
		return errors.New("no cluster info")
	}
	if _, err := semver.NewVersion(c.ClusterInfo.Version); err != nil {
		return errors.Annotatef(err, "invalid cluster version")
	}
	if c.ClusterInfo.ResolvedTS == 0 {
		return errors.New("invalid resolved ts")
	}
	if c.GetStoreCount() == 0 {
		return errors.New("tikv info is empty")
	}
	return nil
}

func (c *EBSBasedBRMeta) SetResolvedTS(id uint64) {
	c.CheckClusterInfo()
	c.ClusterInfo.ResolvedTS = id
}

func (c *EBSBasedBRMeta) GetResolvedTS() uint64 {
	return c.ClusterInfo.ResolvedTS
}

func (c *EBSBasedBRMeta) SetFullBackupType(t string) {
	c.CheckClusterInfo()
	c.ClusterInfo.FullBackupType = t
}

func (c *EBSBasedBRMeta) GetFullBackupType() string {
	return c.ClusterInfo.FullBackupType
}

func (c *EBSBasedBRMeta) SetClusterVersion(version string) {
	c.CheckClusterInfo()
	c.ClusterInfo.Version = version
}

func (c *EBSBasedBRMeta) SetSnapshotIDs(idMap map[string]string) {
	for _, store := range c.TiKVComponent.Stores {
		for _, volume := range store.Volumes {
			volume.SnapshotID = idMap[volume.ID]
		}
	}
}

func (c *EBSBasedBRMeta) SetRestoreVolumeIDs(idMap map[string]string) {
	for _, store := range c.TiKVComponent.Stores {
		for _, volume := range store.Volumes {
			volume.RestoreVolumeId = idMap[volume.ID]
		}
	}
}

func (c *EBSBasedBRMeta) SetVolumeAZs(idMap map[string]string) {
	for _, store := range c.TiKVComponent.Stores {
		for _, volume := range store.Volumes {
			volume.VolumeAZ = idMap[volume.ID]
		}
	}
}

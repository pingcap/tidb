// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pingcap/errors"
	"os"
)

// EBSVolume is passed by TiDB deployment tools: TiDB Operator and TiUP(in future)
// we should do snapshot inside BR, because we need some logic to determine the order of snapshot starts.
// TODO finish the info with TiDB Operator developer.
type EBSVolume struct {
	ID         string `json:"id" toml:"id"`
	Type       string `json:"type" toml:"type"`
	SnapshotID uint64 `json:"snapshot-id" toml:"snapshot-id"`
}

type EBSStore struct {
	StoreID uint64       `json:"store-id" toml:"store-id"`
	Volumes []*EBSVolume `json:"volumes" toml:"volumes"`
}

// ClusterMeta represents the tidb cluster level meta infos. such as
// pd cluster id/alloc id, cluster resolved ts and tikv configuration.
type ClusterMeta struct {
	ClusterID      uint64            `json:"cluster-id" toml:"cluster-id"`
	ClusterVersion string            `json:"cluster-version" toml:"cluster-version"`
	MaxAllocID     uint64            `json:"max-alloc-id" toml:"max-alloc-id"`
	ResolvedTS     uint64            `json:"resolved-ts" toml:"resolved-ts"`
	Stores         []*EBSStore       `json:"stores" toml:"stores"`
	Replicas       map[string]uint64 `json:"replicas" toml:"replicas"`
}

type KubernetesMeta struct {
	PVs     []string               `json:"pvs" toml:"pvs"`
	PVCs    []string               `json:"pvcs" toml:"pvcs"`
	Options map[string]interface{} `json:"options" toml:"options""`
}

type EBSBackupInfo struct {
	ClusterMeta    ClusterMeta            `json:"cluster-meta" toml:"cluster-meta"`
	KubernetesMeta KubernetesMeta         `json:"kubernetes-meta" toml:"kubernetes-meta"`
	Options        map[string]interface{} `json:"options" toml:"options"`
	Region         string                 `json:"region" toml:"region"`
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
	c.ClusterMeta.ClusterID = id
}

func (c *EBSBackupInfo) SetAllocID(id uint64) {
	c.ClusterMeta.MaxAllocID = id
}

// StartsEBSSnapshot is the mainly steps to control the data volume snapshots.
// It will do the following works.
// 1. determine the order of volume snapshot.
// 2. send snapshot requests to aws.
// 3. wait all snapshot finished.
func StartsEBSSnapshot(backupInfo *EBSBackupInfo) error {
	// TODO get region from ebsConfig
	awsConfig := aws.NewConfig().WithRegion(backupInfo.Region)
	// NOTE: we do not need credential. TiDB Operator need make sure we have the correct permission to access
	// ec2 snapshot. we may change this behaviour in the future.
	sessionOptions := session.Options{Config: *awsConfig}
	sess, err := session.NewSessionWithOptions(sessionOptions)
	if err != nil {
		return errors.Trace(err)
	}
	ec2Session := ec2.New(sess)

	for _, cfg := range backupInfo.ClusterMeta.Stores {
		for _, volume := range cfg.Volumes {
			// TODO sort by type
			_, err = ec2Session.CreateSnapshot(&ec2.CreateSnapshotInput{
				VolumeId: &volume.ID,
				TagSpecifications: []*ec2.TagSpecification{
					{
						ResourceType: aws.String(ec2.ResourceTypeSnapshot),
					},
				},
			})
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"encoding/json"
	"github.com/BurntSushi/toml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pingcap/errors"
)

// EBSVolume is passed by TiDB deployment tools: TiDB Operator and TiUP(in future)
// we should do snapshot inside BR, because we need some logic to determine the order of snapshot starts.
// TODO finish the info with TiDB Operator developer.
type EBSVolume struct {
	ID   string `json:"id" toml:"id"`
	Type string `json:"type" toml:"type"`
}

type EBSStore struct {
	Volumes []*EBSVolume `json:"volumes" toml:"volumes"`
}

type EBSBackupConfig struct {
	Region string               `json:"region" toml:"region"`
	Stores map[string]*EBSStore `json:"stores" toml:"stores"`
}

func (c *EBSBackupConfig) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}

// ConfigFromFile loads config from file.
func (c *EBSBackupConfig) ConfigFromFile(path string) error {
	meta, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	if len(meta.Undecoded()) > 0 {
		return errors.Errorf("unknown keys in config file %s: %v", path, meta.Undecoded())
	}
	return nil
}

// StartsEBSSnapshot is the mainly steps to control the data volume snapshots.
// It will do the following works.
// 1. determine the order of volume snapshot.
// 2. send snapshot requests to aws.
// 3. wait all snapshot finished.
func StartsEBSSnapshot(ebsCfg *EBSBackupConfig) error {
	// TODO get region from ebsConfig
	awsConfig := aws.NewConfig().WithRegion(ebsCfg.Region)
	// NOTE: we do not need credential. TiDB Operator need make sure we have the correct permission to access
	// ec2 snapshot. we may change this behaviour in the future.
	sessionOptions := session.Options{Config: *awsConfig}
	sess, err := session.NewSessionWithOptions(sessionOptions)
	if err != nil {
		return errors.Trace(err)
	}
	ec2Session := ec2.New(sess)

	for _, cfg := range ebsCfg.Stores {
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

// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package aws

import (
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/glue"
	"go.uber.org/zap"
)

type EC2Session struct {
	ec2 ec2iface.EC2API
}

func NewEC2Session() (*EC2Session, error) {
	// aws-sdk has builtin exponential backoff retry mechanism, see:
	// https://github.com/aws/aws-sdk-go/blob/db4388e8b9b19d34dcde76c492b17607cd5651e2/aws/client/default_retryer.go#L12-L16
	// with default retryer & max-retry=9, we will wait for at least 30s in total
	awsConfig := aws.NewConfig().WithMaxRetries(9)
	// TiDB Operator need make sure we have the correct permission to call aws api(through aws env variables)
	// we may change this behaviour in the future.
	sessionOptions := session.Options{Config: *awsConfig}
	sess, err := session.NewSessionWithOptions(sessionOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ec2Session := ec2.New(sess)
	return &EC2Session{ec2: ec2Session}, nil
}

// StartsEBSSnapshot is the mainly steps to control the data volume snapshots.
// It will do the following works.
// 1. determine the order of volume snapshot.
// 2. send snapshot requests to aws.
func (e *EC2Session) StartsEBSSnapshot(backupInfo *config.EBSBasedBRMeta) (map[string]string, error) {
	snapIDMap := make(map[string]string)
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
			resp, err := e.ec2.CreateSnapshot(&ec2.CreateSnapshotInput{
				VolumeId: &volume.ID,
				TagSpecifications: []*ec2.TagSpecification{
					{
						ResourceType: aws.String(ec2.ResourceTypeSnapshot),
					},
				},
			})
			if err != nil {
				// todo: consider remove the exists starts snapshots outside.
				return snapIDMap, errors.Trace(err)
			}
			log.Info("snapshot creating", zap.Stringer("snap", resp))
			snapIDMap[volume.ID] = *resp.SnapshotId
		}
	}
	return snapIDMap, nil
}

// WaitSnapshotFinished waits all snapshots finished.
// according to EBS snapshot will do real snapshot background.
// so we'll check whether all snapshots finished.
func (e *EC2Session) WaitSnapshotFinished(snapIDMap map[string]string, progress glue.Progress) (int64, error) {
	pendingSnapshots := make([]*string, 0, len(snapIDMap))
	for volID := range snapIDMap {
		snapID := snapIDMap[volID]
		pendingSnapshots = append(pendingSnapshots, &snapID)
	}
	totalVolumeSize := int64(0)

	log.Info("starts check pending snapshots", zap.Any("snapshots", pendingSnapshots))
	for {
		if len(pendingSnapshots) == 0 {
			log.Info("all pending volume snapshots are finished.")
			return totalVolumeSize, nil
		}

		// check pending snapshots every 5 seconds
		time.Sleep(5 * time.Second)
		log.Info("check pending snapshots", zap.Int("count", len(pendingSnapshots)))
		resp, err := e.ec2.DescribeSnapshots(&ec2.DescribeSnapshotsInput{
			SnapshotIds: pendingSnapshots,
		})
		if err != nil {
			return 0, errors.Trace(err)
		}

		var uncompletedSnapshots []*string
		for _, s := range resp.Snapshots {
			if *s.State == ec2.SnapshotStateCompleted {
				log.Info("snapshot completed", zap.String("id", *s.SnapshotId))
				totalVolumeSize += *s.VolumeSize
				progress.Inc()
			} else {
				log.Debug("snapshot creating...", zap.Stringer("snap", s))
				uncompletedSnapshots = append(uncompletedSnapshots, s.SnapshotId)
			}
		}
		pendingSnapshots = uncompletedSnapshots
	}
}

func (e *EC2Session) DeleteSnapshots(snapIDMap map[string]string) {
	pendingSnaps := make([]*string, 0, len(snapIDMap))
	for volID := range snapIDMap {
		snapID := snapIDMap[volID]
		pendingSnaps = append(pendingSnaps, &snapID)
	}

	for _, snapID := range pendingSnaps {
		// todo: concurrent delete
		_, err2 := e.ec2.DeleteSnapshot(&ec2.DeleteSnapshotInput{
			SnapshotId: snapID,
		})
		if err2 != nil {
			log.Error("failed to delete snapshot", zap.Error(err2))
			// todo: we can only retry for a few times, might fail still, need to handle error from outside.
		}
	}
}

// CreateVolumes create volumes from snapshots
// if err happens in the middle, return half-done result
// returned map: store id -> old volume id -> new volume id
func (e *EC2Session) CreateVolumes(meta *config.EBSBasedBRMeta, volumeType string, iops, throughput int64) (map[string]string, error) {
	template := ec2.CreateVolumeInput{VolumeType: &volumeType}
	if iops > 0 {
		template.SetIops(iops)
	}
	if throughput > 0 {
		template.SetThroughput(throughput)
	}

	newVolumeIDMap := make(map[string]string)
	for _, store := range meta.TiKVComponent.Stores {
		for _, oldVol := range store.Volumes {
			// TODO: build concurrent requests here.
			log.Debug("create volume from snapshot", zap.Any("volume", oldVol))
			req := template
			req.SetSnapshotId(oldVol.SnapshotID)
			resp, err := e.ec2.CreateVolume(&req)
			if err != nil {
				return newVolumeIDMap, errors.Trace(err)
			}
			log.Info("new volume creating", zap.Stringer("snap", resp))
			newVolumeIDMap[oldVol.ID] = *resp.VolumeId
		}
	}
	return newVolumeIDMap, nil
}

func (e *EC2Session) WaitVolumesCreated(volumeIDMap map[string]string, progress glue.Progress) (int64, error) {
	pendingVolumes := make([]*string, 0, len(volumeIDMap))
	for oldVolID := range volumeIDMap {
		newVolumeID := volumeIDMap[oldVolID]
		pendingVolumes = append(pendingVolumes, &newVolumeID)
	}
	totalVolumeSize := int64(0)

	log.Info("starts check pending volumes", zap.Any("volumes", pendingVolumes))
	for len(pendingVolumes) > 0 {
		// check every 5 seconds
		time.Sleep(5 * time.Second)
		log.Info("check pending snapshots", zap.Int("count", len(pendingVolumes)))
		resp, err := e.ec2.DescribeVolumes(&ec2.DescribeVolumesInput{
			VolumeIds: pendingVolumes,
		})
		if err != nil {
			return 0, errors.Trace(err)
		}

		var unfinishedVolumes []*string
		for _, volume := range resp.Volumes {
			if *volume.State == ec2.VolumeStateAvailable {
				log.Info("volume is available", zap.String("id", *volume.SnapshotId))
				totalVolumeSize += *volume.Size
				progress.Inc()
			} else {
				log.Debug("volume creating...", zap.Stringer("volume", volume))
				unfinishedVolumes = append(unfinishedVolumes, volume.SnapshotId)
			}
		}
		pendingVolumes = unfinishedVolumes
	}
	log.Info("all pending volume are created.")
	return totalVolumeSize, nil
}

func (e *EC2Session) DeleteVolumes(volumeIDMap map[string]string) {
	pendingVolumes := make([]*string, 0, len(volumeIDMap))
	for oldVolID := range volumeIDMap {
		volumeID := volumeIDMap[oldVolID]
		pendingVolumes = append(pendingVolumes, &volumeID)
	}

	for _, volID := range pendingVolumes {
		// todo: concurrent delete
		_, err2 := e.ec2.DeleteVolume(&ec2.DeleteVolumeInput{
			VolumeId: volID,
		})
		if err2 != nil {
			log.Error("failed to delete volume", zap.Error(err2))
			// todo: we can only retry for a few times, might fail still, need to handle error from outside.
		}
	}
}

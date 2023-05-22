// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package aws

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/utils"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	AnnPodNameKey        string = "tidb.pingcap.com/pod-name"
	NamespaceLabelKey    string = "app.kubernetes.io/namespace"
	NameLabelKey         string = "app.kubernetes.io/name"
	AnnTemporaryVolumeID string = "temporary/volume-id"
	EC2K8SClusterNameKey string = "eksctl.cluster.k8s.io/v1alpha1/cluster-name"

	SourcePvcNameKey   string = "source/pvcName"
	SourceVolumeIdKey  string = "source/VolumeId"
	SourceTikvNameKey  string = "source/TikvName"
	SourceNamespaceKey string = "source/Namespace"
	SourceContextKey   string = "source/context"
)

type EC2Session struct {
	ec2 ec2iface.EC2API
	// aws operation concurrency
	concurrency uint
}

type VolumeAZs map[string]string

type SnapshotTags struct {
	sourcePVCName   string
	sourceTiKVName  string
	sourceNameSpace string
}

type VolumeSnapshotTags map[string]SnapshotTags

func NewEC2Session(concurrency uint, region string) (*EC2Session, error) {
	// aws-sdk has builtin exponential backoff retry mechanism, see:
	// https://github.com/aws/aws-sdk-go/blob/db4388e8b9b19d34dcde76c492b17607cd5651e2/aws/client/default_retryer.go#L12-L16
	// with default retryer & max-retry=9, we will wait for at least 30s in total
	awsConfig := aws.NewConfig().WithMaxRetries(9).WithRegion(region)
	// TiDB Operator need make sure we have the correct permission to call aws api(through aws env variables)
	// we may change this behaviour in the future.
	sessionOptions := session.Options{Config: *awsConfig}
	sess, err := session.NewSessionWithOptions(sessionOptions)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ec2Session := ec2.New(sess)
	return &EC2Session{ec2: ec2Session, concurrency: concurrency}, nil
}

func GenerateVolumeSnapshotTags(backupInfo *config.EBSBasedBRMeta) VolumeSnapshotTags {
	vst := make(VolumeSnapshotTags)
	for j := range backupInfo.KubernetesMeta.PVs {
		pv := backupInfo.KubernetesMeta.PVs[j]
		vst[pv.GetAnnotations()[AnnTemporaryVolumeID]] = SnapshotTags{
			pv.GetName(),
			pv.GetAnnotations()[AnnPodNameKey],
			pv.GetLabels()[NamespaceLabelKey],
		}
	}
	return vst
}

// CreateSnapshots is the mainly steps to control the data volume snapshots.
func (e *EC2Session) CreateSnapshots(backupInfo *config.EBSBasedBRMeta) (map[string]string, VolumeAZs, error) {
	snapIDMap := make(map[string]string)
	var volumeIDs []*string

	var mutex sync.Mutex
	eg, _ := errgroup.WithContext(context.Background())

	vst := GenerateVolumeSnapshotTags(backupInfo)
	taggingAndFillResult := func(createOutput *ec2.CreateSnapshotsOutput, vst VolumeSnapshotTags, k8sClusterName string) error {
		mutex.Lock()
		defer mutex.Unlock()
		for j := range createOutput.Snapshots {
			snapshot := createOutput.Snapshots[j]
			snapIDMap[aws.StringValue(snapshot.VolumeId)] = aws.StringValue(snapshot.SnapshotId)

			createTagInput := &ec2.CreateTagsInput{
				Resources: []*string{
					snapshot.SnapshotId,
				},
				Tags: []*ec2.Tag{
					ec2Tag(SourcePvcNameKey, vst[aws.StringValue(snapshot.VolumeId)].sourcePVCName),
					ec2Tag(SourceVolumeIdKey, aws.StringValue(snapshot.VolumeId)),
					ec2Tag(SourceTikvNameKey, vst[aws.StringValue(snapshot.VolumeId)].sourceTiKVName),
					ec2Tag(SourceNamespaceKey, vst[aws.StringValue(snapshot.VolumeId)].sourceNameSpace),
					ec2Tag(SourceContextKey, k8sClusterName),
				},
			}
			_, err := e.ec2.CreateTags(createTagInput)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}

	workerPool := utils.NewWorkerPool(e.concurrency, "create snapshots")
	for i := range backupInfo.TiKVComponent.Stores {
		store := backupInfo.TiKVComponent.Stores[i]
		volumes := store.Volumes
		if len(volumes) >= 1 {
			log.Info("fetch EC2 instance id using first volume")
			var targetVolumeIDs []*string
			for j := range volumes {
				volume := volumes[j]
				targetVolumeIDs = append(targetVolumeIDs, &volume.ID)
				volumeIDs = append(volumeIDs, &volume.ID)
			}

			// determine the ec2 instance id
			resp, err := e.ec2.DescribeVolumes(&ec2.DescribeVolumesInput{VolumeIds: targetVolumeIDs[0:1]})
			if err != nil {
				return snapIDMap, nil, errors.Trace(err)
			}
			if resp.Volumes[0].Attachments[0] == nil || resp.Volumes[0].Attachments[0].InstanceId == nil {
				return snapIDMap, nil, errors.Errorf("specified volume %s is not attached", volumes[0].ID)
			}
			ec2InstanceId := resp.Volumes[0].Attachments[0].InstanceId
			log.Info("EC2 instance id is", zap.Stringp("id", ec2InstanceId))

			// determine the exclude volume list
			var excludedVolumeIDs []*string
			resp1, err := e.ec2.DescribeInstances(&ec2.DescribeInstancesInput{InstanceIds: []*string{ec2InstanceId}})
			if err != nil {
				return snapIDMap, nil, errors.Trace(err)
			}

			// retrieve the k8s cluster name from EC2 instance tags
			var k8sClusterName = ""

			for j := range resp1.Reservations[0].Instances[0].Tags {
				tag := resp1.Reservations[0].Instances[0].Tags[j]
				if aws.StringValue(tag.Key) == EC2K8SClusterNameKey {
					k8sClusterName = aws.StringValue(tag.Value)
				}
			}

			for j := range resp1.Reservations[0].Instances[0].BlockDeviceMappings {
				device := resp1.Reservations[0].Instances[0].BlockDeviceMappings[j]
				// skip root volume
				if aws.StringValue(device.DeviceName) == aws.StringValue(resp1.Reservations[0].Instances[0].RootDeviceName) {
					continue
				}
				toInclude := false
				for k := range targetVolumeIDs {
					targetVolumeID := targetVolumeIDs[k]
					if aws.StringValue(targetVolumeID) == aws.StringValue(device.Ebs.VolumeId) {
						toInclude = true
						break
					}
				}
				if !toInclude {
					excludedVolumeIDs = append(excludedVolumeIDs, device.Ebs.VolumeId)
				}
			}

			log.Info("exclude volume list", zap.Stringp("ec2", ec2InstanceId), zap.Any("exclude volume list", excludedVolumeIDs))

			// create snapshots for volumes on this ec2 instance
			workerPool.ApplyOnErrorGroup(eg, func() error {
				// Prepare for aws requests

				instanceSpecification := ec2.InstanceSpecification{}
				instanceSpecification.SetInstanceId(aws.StringValue(ec2InstanceId)).SetExcludeBootVolume(true).SetExcludeDataVolumeIds(excludedVolumeIDs)

				createSnapshotInput := ec2.CreateSnapshotsInput{}
				createSnapshotInput.SetInstanceSpecification(&instanceSpecification)

				resp, err := e.ec2.CreateSnapshots(&createSnapshotInput)
				if err != nil {
					return errors.Trace(err)
				}
				err = taggingAndFillResult(resp, vst, k8sClusterName)
				if err != nil {
					return errors.Trace(err)
				}

				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return snapIDMap, nil, err
	}

	volAZs := make(map[string]string)
	resp, err := e.ec2.DescribeVolumes(&ec2.DescribeVolumesInput{VolumeIds: volumeIDs})
	if err != nil {
		return snapIDMap, volAZs, errors.Trace(err)
	}
	for _, vol := range resp.Volumes {
		log.Info("volume information", zap.Stringer("vol", vol))
		volAZs[*vol.VolumeId] = *vol.AvailabilityZone
	}

	return snapIDMap, volAZs, nil
}

func (e *EC2Session) extractSnapProgress(str *string) int64 {
	if str == nil {
		return 0
	}
	var val float64
	// example output from: https://docs.aws.amazon.com/cli/latest/reference/ec2/describe-snapshots.html
	// {
	//   ...
	//   "Progress": "100%",
	//   ...
	// }
	// not sure whether it's always an integer or can be float point, so we scan it as float
	*str = strings.Trim(*str, " ")
	n, err := fmt.Sscanf(*str, "%f%%", &val)
	if err != nil || n != 1 {
		log.Warn("failed to extract aws progress", zap.Stringp("progress-str", str))
		return 0
	}
	if val > 100 {
		// may not happen
		val = 100
	}
	return int64(val)
}

// WaitSnapshotsCreated waits all snapshots finished.
// according to EBS snapshot will do real snapshot background.
// so we'll check whether all snapshots finished.
func (e *EC2Session) WaitSnapshotsCreated(snapIDMap map[string]string, progress glue.Progress) (int64, error) {
	pendingSnapshots := make([]*string, 0, len(snapIDMap))
	for volID := range snapIDMap {
		snapID := snapIDMap[volID]
		pendingSnapshots = append(pendingSnapshots, &snapID)
	}
	totalVolumeSize := int64(0)
	snapProgressMap := make(map[string]int64, len(snapIDMap))

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
			} else {
				log.Debug("snapshot creating...", zap.Stringer("snap", s))
				uncompletedSnapshots = append(uncompletedSnapshots, s.SnapshotId)
			}
			currSnapProgress := e.extractSnapProgress(s.Progress)
			if currSnapProgress > snapProgressMap[*s.SnapshotId] {
				progress.IncBy(currSnapProgress - snapProgressMap[*s.SnapshotId])
				snapProgressMap[*s.SnapshotId] = currSnapProgress
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

	var deletedCnt atomic.Int32
	eg, _ := errgroup.WithContext(context.Background())
	workerPool := utils.NewWorkerPool(e.concurrency, "delete snapshot")
	for i := range pendingSnaps {
		snapID := pendingSnaps[i]
		workerPool.ApplyOnErrorGroup(eg, func() error {
			_, err2 := e.ec2.DeleteSnapshot(&ec2.DeleteSnapshotInput{
				SnapshotId: snapID,
			})
			if err2 != nil {
				log.Error("failed to delete snapshot", zap.Error(err2), zap.Stringp("snap-id", snapID))
				// todo: we can only retry for a few times, might fail still, need to handle error from outside.
				// we don't return error if it fails to make sure all snapshot got chance to delete.
			} else {
				deletedCnt.Add(1)
			}
			return nil
		})
	}
	_ = eg.Wait()
	log.Info("delete snapshot end", zap.Int("need-to-del", len(snapIDMap)), zap.Int32("deleted", deletedCnt.Load()))
}

// CreateVolumes create volumes from snapshots
// if err happens in the middle, return half-done result
// returned map: store id -> old volume id -> new volume id
func (e *EC2Session) CreateVolumes(meta *config.EBSBasedBRMeta, volumeType string, iops, throughput int64, targetAZ string) (map[string]string, error) {
	template := ec2.CreateVolumeInput{
		VolumeType: &volumeType,
	}
	if iops > 0 {
		template.SetIops(iops)
	}
	if throughput > 0 {
		template.SetThroughput(throughput)
	}

	newVolumeIDMap := make(map[string]string)
	var mutex sync.Mutex
	eg, _ := errgroup.WithContext(context.Background())
	fillResult := func(newVol *ec2.Volume, oldVol *config.EBSVolume) {
		mutex.Lock()
		defer mutex.Unlock()
		newVolumeIDMap[oldVol.ID] = *newVol.VolumeId
	}

	fetchTagValue := func(tags []*ec2.Tag, key string) string {
		for i := range tags {
			tag := tags[i]
			if aws.StringValue(tag.Key) == key {
				return aws.StringValue(tag.Value)
			}
		}
		return ""
	}
	workerPool := utils.NewWorkerPool(e.concurrency, "create volume")
	for i := range meta.TiKVComponent.Stores {
		store := meta.TiKVComponent.Stores[i]
		for j := range store.Volumes {
			oldVol := store.Volumes[j]
			workerPool.ApplyOnErrorGroup(eg, func() error {
				log.Debug("create volume from snapshot", zap.Any("volume", oldVol))
				req := template
				req.SetSnapshotId(oldVol.SnapshotID)

				snapshotIds := make([]*string, 0)
				snapshotIds = append(snapshotIds, &oldVol.SnapshotID)
				resp, err := e.ec2.DescribeSnapshots(&ec2.DescribeSnapshotsInput{SnapshotIds: snapshotIds})
				if err != nil {
					return errors.Trace(err)
				}

				if len(resp.Snapshots) <= 0 {
					return errors.Errorf("specified snapshot [%s] is not found", oldVol.SnapshotID)
				}

				// Copy interested tags of snapshots to the restored volume
				tags := []*ec2.Tag{
					ec2Tag("TiDBCluster-BR", "new"),
					ec2Tag("ebs.csi.aws.com/cluster", "true"),
				}

				snapshotTags := resp.Snapshots[0].Tags
				tags = append(tags, ec2Tag("snapshot/createdFromSnapshotId", oldVol.SnapshotID),
					ec2Tag("snapshot/"+SourcePvcNameKey, fetchTagValue(snapshotTags, "source/pvcName")),
					ec2Tag("snapshot/"+SourceVolumeIdKey, fetchTagValue(snapshotTags, "source/VolumeId")),
					ec2Tag("snapshot/"+SourceTikvNameKey, fetchTagValue(snapshotTags, "source/TikvName")),
					ec2Tag("snapshot/"+SourceNamespaceKey, fetchTagValue(snapshotTags, "source/Namespace")),
					ec2Tag("snapshot/"+SourceContextKey, fetchTagValue(snapshotTags, "source/context")))

				req.SetTagSpecifications([]*ec2.TagSpecification{
					{
						ResourceType: aws.String(ec2.ResourceTypeVolume),
						Tags:         tags,
					},
				})
				if targetAZ == "" {
					req.SetAvailabilityZone(oldVol.VolumeAZ)
				} else {
					req.SetAvailabilityZone(targetAZ)
				}
				newVol, err := e.ec2.CreateVolume(&req)
				if err != nil {
					return errors.Trace(err)
				}
				log.Info("new volume creating", zap.Stringer("vol", newVol))
				fillResult(newVol, oldVol)
				return nil
			})
		}
	}
	return newVolumeIDMap, eg.Wait()
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

		createdVolumeSize, unfinishedVolumes := e.HandleDescribeVolumesResponse(resp)
		progress.IncBy(int64(len(pendingVolumes) - len(unfinishedVolumes)))
		totalVolumeSize += createdVolumeSize
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

	var deletedCnt atomic.Int32
	eg, _ := errgroup.WithContext(context.Background())
	workerPool := utils.NewWorkerPool(e.concurrency, "delete volume")
	for i := range pendingVolumes {
		volID := pendingVolumes[i]
		workerPool.ApplyOnErrorGroup(eg, func() error {
			_, err2 := e.ec2.DeleteVolume(&ec2.DeleteVolumeInput{
				VolumeId: volID,
			})
			if err2 != nil {
				log.Error("failed to delete volume", zap.Error(err2), zap.Stringp("volume-id", volID))
				// todo: we can only retry for a few times, might fail still, need to handle error from outside.
				// we don't return error if it fails to make sure all volume got chance to delete.
			} else {
				deletedCnt.Add(1)
			}
			return nil
		})
	}
	_ = eg.Wait()
	log.Info("delete volume end", zap.Int("need-to-del", len(volumeIDMap)), zap.Int32("deleted", deletedCnt.Load()))
}

func ec2Tag(key, val string) *ec2.Tag {
	return &ec2.Tag{Key: &key, Value: &val}
}

func (e *EC2Session) HandleDescribeVolumesResponse(resp *ec2.DescribeVolumesOutput) (int64, []*string) {
	totalVolumeSize := int64(0)

	var unfinishedVolumes []*string
	for _, volume := range resp.Volumes {
		if *volume.State == ec2.VolumeStateAvailable {
			log.Info("volume is available", zap.String("id", *volume.VolumeId))
			totalVolumeSize += *volume.Size
		} else {
			log.Debug("volume creating...", zap.Stringer("volume", volume))
			unfinishedVolumes = append(unfinishedVolumes, volume.VolumeId)
		}
	}

	return totalVolumeSize, unfinishedVolumes
}

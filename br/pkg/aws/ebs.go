// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package aws

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/config"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/util"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	pollingPendingSnapshotInterval = 30 * time.Second
	errCodeTooManyPendingSnapshots = "PendingSnapshotLimitExceeded"
	FsrApiSnapshotsThreshold       = 10
)

type EC2Session struct {
	ec2              ec2iface.EC2API
	cloudwatchClient *cloudwatch.CloudWatch
	// aws operation concurrency
	concurrency uint
}

type VolumeAZs map[string]string

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
	cloudwatchClient := cloudwatch.New(sess)
	return &EC2Session{ec2: ec2Session, cloudwatchClient: cloudwatchClient, concurrency: concurrency}, nil
}

// CreateSnapshots is the mainly steps to control the data volume snapshots.
func (e *EC2Session) CreateSnapshots(backupInfo *config.EBSBasedBRMeta) (map[string]string, VolumeAZs, error) {
	snapIDMap := make(map[string]string)
	var volumeIDs []*string

	var mutex sync.Mutex
	eg, _ := errgroup.WithContext(context.Background())
	fillResult := func(createOutput *ec2.CreateSnapshotsOutput) {
		mutex.Lock()
		defer mutex.Unlock()
		for j := range createOutput.Snapshots {
			snapshot := createOutput.Snapshots[j]
			snapIDMap[aws.StringValue(snapshot.VolumeId)] = aws.StringValue(snapshot.SnapshotId)
		}
	}

	tags := []*ec2.Tag{
		ec2Tag("TiDBCluster-BR-Snapshot", "new"),
	}

	workerPool := util.NewWorkerPool(e.concurrency, "create snapshots")
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
				createSnapshotInput := ec2.CreateSnapshotsInput{}

				instanceSpecification.SetInstanceId(aws.StringValue(ec2InstanceId)).SetExcludeBootVolume(true).SetExcludeDataVolumeIds(excludedVolumeIDs)

				createSnapshotInput.SetInstanceSpecification(&instanceSpecification)
				// Copy tags from source volume
				createSnapshotInput.SetCopyTagsFromSource("volume")
				createSnapshotInput.SetTagSpecifications([]*ec2.TagSpecification{
					{
						ResourceType: aws.String(ec2.ResourceTypeSnapshot),
						Tags:         tags,
					},
				})
				resp, err := e.createSnapshotsWithRetry(context.TODO(), &createSnapshotInput)

				if err != nil {
					return errors.Trace(err)
				}
				fillResult(resp)
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

func (e *EC2Session) createSnapshotsWithRetry(ctx context.Context, input *ec2.CreateSnapshotsInput) (*ec2.CreateSnapshotsOutput, error) {
	for {
		res, err := e.ec2.CreateSnapshotsWithContext(ctx, input)
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == errCodeTooManyPendingSnapshots {
			log.Warn("the pending snapshots exceeds the limit. waiting...",
				zap.String("instance", aws.StringValue(input.InstanceSpecification.InstanceId)),
				zap.Strings("volumns", aws.StringValueSlice(input.InstanceSpecification.ExcludeDataVolumeIds)),
			)
			time.Sleep(pollingPendingSnapshotInterval)
			continue
		}
		if err != nil {
			return nil, errors.Annotatef(err, "failed to create snapshot for request %s", input)
		}
		return res, nil
	}
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
			} else if *s.State == ec2.SnapshotStateError {
				log.Error("snapshot failed", zap.String("id", *s.SnapshotId), zap.String("error", (*s.StateMessage)))
				return 0, errors.Errorf("snapshot %s failed", *s.SnapshotId)
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
	workerPool := util.NewWorkerPool(e.concurrency, "delete snapshot")
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

// EnableDataFSR enables FSR for data volume snapshots
func (e *EC2Session) EnableDataFSR(meta *config.EBSBasedBRMeta, targetAZ string) (map[string][]*string, error) {
	snapshotsIDsMap := fetchTargetSnapshots(meta, targetAZ)

	if len(snapshotsIDsMap) == 0 {
		return snapshotsIDsMap, errors.Errorf("empty backup meta")
	}

	eg, _ := errgroup.WithContext(context.Background())

	for availableZone := range snapshotsIDsMap {
		targetAZ := availableZone
		// We have to control the batch size to avoid the error of "parameter SourceSnapshotIds must be less than or equal to 10"
		for i := 0; i < len(snapshotsIDsMap[targetAZ]); i += FsrApiSnapshotsThreshold {
			start := i
			end := i + FsrApiSnapshotsThreshold
			if end > len(snapshotsIDsMap[targetAZ]) {
				end = len(snapshotsIDsMap[targetAZ])
			}
			eg.Go(func() error {
				log.Info("enable fsr for snapshots", zap.String("available zone", targetAZ), zap.Any("snapshots", snapshotsIDsMap[targetAZ][start:end]))
				resp, err := e.ec2.EnableFastSnapshotRestores(&ec2.EnableFastSnapshotRestoresInput{
					AvailabilityZones: []*string{&targetAZ},
					SourceSnapshotIds: snapshotsIDsMap[targetAZ][start:end],
				})

				if err != nil {
					return errors.Trace(err)
				}

				if len(resp.Unsuccessful) > 0 {
					log.Warn("not all snapshots enabled FSR")
					return errors.Errorf("Some snapshot fails to enable FSR for available zone %s, such as %s, error code is %v", targetAZ, *resp.Unsuccessful[0].SnapshotId, resp.Unsuccessful[0].FastSnapshotRestoreStateErrors)
				}

				return e.waitDataFSREnabled(snapshotsIDsMap[targetAZ][start:end], targetAZ)
			})
		}
	}
	return snapshotsIDsMap, eg.Wait()
}

// waitDataFSREnabled waits FSR for data volume snapshots are all enabled and also have enough credit balance
func (e *EC2Session) waitDataFSREnabled(snapShotIDs []*string, targetAZ string) error {
	resp, err := e.ec2.DescribeSnapshots(&ec2.DescribeSnapshotsInput{SnapshotIds: snapShotIDs})
	if err != nil {
		return errors.Trace(err)
	}
	if len(resp.Snapshots) <= 0 {
		return errors.Errorf("specified snapshot [%s] is not found", *snapShotIDs[0])
	}

	// Wait that all snapshot has enough fsr credit balance
	log.Info("Start check and wait all snapshots have enough fsr credit balance")

	startIdx := 0
	retryCount := 0
	for startIdx < len(snapShotIDs) {
		creditBalance, _ := e.getFSRCreditBalance(snapShotIDs[startIdx], targetAZ)
		if creditBalance != nil && *creditBalance >= 1.0 {
			startIdx++
			retryCount = 0
		} else {
			if creditBalance == nil {
				// For invalid calling, retry 3 times
				if retryCount >= 3 {
					return errors.Errorf("cloudwatch metrics for %s operation failed after retrying", *snapShotIDs[startIdx])
				}
				retryCount++
			}
			// Retry for both invalid calling and not enough fsr credit at 3 minute intervals
			time.Sleep(3 * time.Minute)
		}
	}

	// Create a map to store the strings as keys
	pendingSnapshots := make(map[string]struct{})

	// Populate the map with the strings from the array
	for _, str := range snapShotIDs {
		pendingSnapshots[*str] = struct{}{}
	}

	log.Info("starts check fsr pending snapshots", zap.Any("snapshots", pendingSnapshots), zap.String("available zone", targetAZ))
	for {
		if len(pendingSnapshots) == 0 {
			log.Info("all snapshots in current batch fsr enablement is finished", zap.String("available zone", targetAZ), zap.Any("snapshots", snapShotIDs))
			return nil
		}

		// check pending snapshots every 1 minute
		time.Sleep(1 * time.Minute)
		log.Info("check snapshots not fsr enabled", zap.Int("count", len(pendingSnapshots)))
		input := &ec2.DescribeFastSnapshotRestoresInput{
			Filters: []*ec2.Filter{
				{
					Name:   aws.String("state"),
					Values: []*string{aws.String("disabled"), aws.String("disabling"), aws.String("enabling"), aws.String("optimizing")},
				},
				{
					Name:   aws.String("availability-zone"),
					Values: []*string{aws.String(targetAZ)},
				},
			},
		}

		result, err := e.ec2.DescribeFastSnapshotRestores(input)
		if err != nil {
			return errors.Trace(err)
		}

		uncompletedSnapshots := make(map[string]struct{})
		for _, fastRestore := range result.FastSnapshotRestores {
			_, found := pendingSnapshots[*fastRestore.SnapshotId]
			if found {
				// Detect some conflict states
				if strings.EqualFold(*fastRestore.State, "disabled") || strings.EqualFold(*fastRestore.State, "disabling") {
					log.Error("detect conflict status", zap.String("snapshot", *fastRestore.SnapshotId), zap.String("status", *fastRestore.State))
					return errors.Errorf("status of snapshot %s is %s ", *fastRestore.SnapshotId, *fastRestore.State)
				}
				uncompletedSnapshots[*fastRestore.SnapshotId] = struct{}{}
			}
		}
		pendingSnapshots = uncompletedSnapshots
	}
}

// getFSRCreditBalance is used to get maximum fsr credit balance of snapshot for last 5 minutes
func (e *EC2Session) getFSRCreditBalance(snapshotID *string, targetAZ string) (*float64, error) {
	// Set the time range to query for metrics
	startTime := time.Now().Add(-5 * time.Minute)
	endTime := time.Now()

	// Prepare the input for the GetMetricStatisticsWithContext API call
	input := &cloudwatch.GetMetricStatisticsInput{
		StartTime:  aws.Time(startTime),
		EndTime:    aws.Time(endTime),
		Namespace:  aws.String("AWS/EBS"),
		MetricName: aws.String("FastSnapshotRestoreCreditsBalance"),
		Dimensions: []*cloudwatch.Dimension{
			{
				Name:  aws.String("SnapshotId"),
				Value: snapshotID,
			},
			{
				Name:  aws.String("AvailabilityZone"),
				Value: aws.String(targetAZ),
			},
		},
		Period:     aws.Int64(300),
		Statistics: []*string{aws.String("Maximum")},
	}

	log.Info("metrics input", zap.Any("input", input))

	// Call cloudwatchClient API to retrieve the FastSnapshotRestoreCreditsBalance metric data
	resp, err := e.cloudwatchClient.GetMetricStatisticsWithContext(context.Background(), input)
	if err != nil {
		log.Error("GetMetricStatisticsWithContext failed", zap.Error(err))
		return nil, errors.Trace(err)
	}

	// parse the response
	if len(resp.Datapoints) == 0 {
		log.Warn("No result for metric FastSnapshotRestoreCreditsBalance returned", zap.Stringp("snapshot", snapshotID))
		return nil, nil
	}
	result := resp.Datapoints[0]
	log.Info("credit balance", zap.Stringp("snapshot", snapshotID), zap.Float64p("credit", result.Maximum))
	return result.Maximum, nil
}

// DisableDataFSR disables FSR for data volume snapshots
func (e *EC2Session) DisableDataFSR(snapshotsIDsMap map[string][]*string) error {
	if len(snapshotsIDsMap) == 0 {
		return nil
	}

	eg, _ := errgroup.WithContext(context.Background())

	for availableZone := range snapshotsIDsMap {
		targetAZ := availableZone
		// We have to control the batch size to avoid the error of "parameter SourceSnapshotIds must be less than or equal to 10"
		for i := 0; i < len(snapshotsIDsMap[targetAZ]); i += FsrApiSnapshotsThreshold {
			start := i
			end := i + FsrApiSnapshotsThreshold
			if end > len(snapshotsIDsMap[targetAZ]) {
				end = len(snapshotsIDsMap[targetAZ])
			}
			eg.Go(func() error {
				resp, err := e.ec2.DisableFastSnapshotRestores(&ec2.DisableFastSnapshotRestoresInput{
					AvailabilityZones: []*string{&targetAZ},
					SourceSnapshotIds: snapshotsIDsMap[targetAZ][start:end],
				})

				if err != nil {
					return errors.Trace(err)
				}

				if len(resp.Unsuccessful) > 0 {
					log.Warn("not all snapshots disabled FSR", zap.String("available zone", targetAZ))
					return errors.Errorf("Some snapshot fails to disable FSR for available zone %s, such as %s, error code is %v", targetAZ, *resp.Unsuccessful[0].SnapshotId, resp.Unsuccessful[0].FastSnapshotRestoreStateErrors)
				}

				log.Info("Disable FSR issued", zap.String("available zone", targetAZ), zap.Any("snapshots", snapshotsIDsMap[targetAZ][start:end]))

				return nil
			})
		}
	}
	return eg.Wait()
}

func fetchTargetSnapshots(meta *config.EBSBasedBRMeta, specifiedAZ string) map[string][]*string {
	var sourceSnapshotIDs = make(map[string][]*string)

	if len(meta.TiKVComponent.Stores) == 0 {
		return sourceSnapshotIDs
	}

	for i := range meta.TiKVComponent.Stores {
		store := meta.TiKVComponent.Stores[i]
		for j := range store.Volumes {
			oldVol := store.Volumes[j]
			// Handle data volume snapshots only
			if strings.Compare(oldVol.Type, "storage.data-dir") == 0 {
				if specifiedAZ != "" {
					sourceSnapshotIDs[specifiedAZ] = append(sourceSnapshotIDs[specifiedAZ], &oldVol.SnapshotID)
				} else {
					sourceSnapshotIDs[oldVol.VolumeAZ] = append(sourceSnapshotIDs[oldVol.VolumeAZ], &oldVol.SnapshotID)
				}
			}
		}
	}

	return sourceSnapshotIDs
}

// CreateVolumes create volumes from snapshots
// if err happens in the middle, return half-done result
// returned map: store id -> old volume id -> new volume id
func (e *EC2Session) CreateVolumes(meta *config.EBSBasedBRMeta, volumeType string, iops, throughput int64, encrypted bool, targetAZ string) (map[string]string, error) {
	template := ec2.CreateVolumeInput{
		VolumeType: &volumeType,
	}
	if iops > 0 {
		template.SetIops(iops)
	}
	if throughput > 0 {
		template.SetThroughput(throughput)
	}
	template.Encrypted = &encrypted

	newVolumeIDMap := make(map[string]string)
	var mutex sync.Mutex
	eg, _ := errgroup.WithContext(context.Background())
	fillResult := func(newVol *ec2.Volume, oldVol *config.EBSVolume) {
		mutex.Lock()
		defer mutex.Unlock()
		newVolumeIDMap[oldVol.ID] = *newVol.VolumeId
	}

	workerPool := util.NewWorkerPool(e.concurrency, "create volume")
	for i := range meta.TiKVComponent.Stores {
		store := meta.TiKVComponent.Stores[i]
		for j := range store.Volumes {
			oldVol := store.Volumes[j]
			workerPool.ApplyOnErrorGroup(eg, func() error {
				log.Debug("create volume from snapshot", zap.Any("volume", oldVol))
				req := template

				req.SetSnapshotId(oldVol.SnapshotID)

				// set target AZ
				if targetAZ == "" {
					req.SetAvailabilityZone(oldVol.VolumeAZ)
				} else {
					req.SetAvailabilityZone(targetAZ)
				}

				// Copy interested tags of snapshots to the restored volume
				tags := []*ec2.Tag{
					ec2Tag("TiDBCluster-BR", "new"),
					ec2Tag("ebs.csi.aws.com/cluster", "true"),
					ec2Tag("snapshot/createdFromSnapshotId", oldVol.SnapshotID),
				}
				snapshotIds := make([]*string, 0)

				snapshotIds = append(snapshotIds, &oldVol.SnapshotID)
				resp, err := e.ec2.DescribeSnapshots(&ec2.DescribeSnapshotsInput{SnapshotIds: snapshotIds})
				if err != nil {
					return errors.Trace(err)
				}
				if len(resp.Snapshots) <= 0 {
					return errors.Errorf("specified snapshot [%s] is not found", oldVol.SnapshotID)
				}

				// Copy tags from source snapshots, but avoid recursive tagging
				for j := range resp.Snapshots[0].Tags {
					if !strings.HasPrefix(aws.StringValue(resp.Snapshots[0].Tags[j].Key), "snapshot/") {
						tags = append(tags,
							ec2Tag("snapshot/"+aws.StringValue(resp.Snapshots[0].Tags[j].Key), aws.StringValue(resp.Snapshots[0].Tags[j].Value)))
					}
				}

				req.SetTagSpecifications([]*ec2.TagSpecification{
					{
						ResourceType: aws.String(ec2.ResourceTypeVolume),
						Tags:         tags,
					},
				})

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

func (e *EC2Session) WaitVolumesCreated(volumeIDMap map[string]string, progress glue.Progress, fsrEnabledRequired bool) (int64, error) {
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
		log.Info("check pending volumes", zap.Int("count", len(pendingVolumes)))
		resp, err := e.ec2.DescribeVolumes(&ec2.DescribeVolumesInput{
			VolumeIds: pendingVolumes,
		})
		if err != nil {
			return 0, errors.Trace(err)
		}

		createdVolumeSize, unfinishedVolumes, err := e.HandleDescribeVolumesResponse(resp, fsrEnabledRequired)
		if err != nil {
			return 0, errors.Trace(err)
		}

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
	workerPool := util.NewWorkerPool(e.concurrency, "delete volume")
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

func (e *EC2Session) HandleDescribeVolumesResponse(resp *ec2.DescribeVolumesOutput, fsrEnabledRequired bool) (int64, []*string, error) {
	totalVolumeSize := int64(0)

	var unfinishedVolumes []*string
	for _, volume := range resp.Volumes {
		if *volume.State == ec2.VolumeStateAvailable {
			if fsrEnabledRequired && volume.FastRestored != nil && !*volume.FastRestored {
				log.Error("snapshot fsr is not enabled for the volume", zap.String("volume", *volume.SnapshotId))
				return 0, nil, errors.Errorf("Snapshot [%s] of volume [%s] is not fsr enabled", *volume.SnapshotId, *volume.VolumeId)
			}
			log.Info("volume is available", zap.String("id", *volume.VolumeId))
			totalVolumeSize += *volume.Size
		} else {
			log.Debug("volume creating...", zap.Stringer("volume", volume))
			unfinishedVolumes = append(unfinishedVolumes, volume.VolumeId)
		}
	}

	return totalVolumeSize, unfinishedVolumes, nil
}

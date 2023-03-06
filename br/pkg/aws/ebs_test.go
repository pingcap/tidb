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

package aws

import (
	"testing"

	awsapi "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/stretchr/testify/require"
)

func TestEC2SessionExtractSnapProgress(t *testing.T) {
	tests := []struct {
		str  *string
		want int64
	}{
		{nil, 0},
		{awsapi.String("12.12%"), 12},
		{awsapi.String("44.99%"), 44},
		{awsapi.String("  89.89%  "), 89},
		{awsapi.String("100%"), 100},
		{awsapi.String("111111%"), 100},
	}
	e := &EC2Session{}
	for _, tt := range tests {
		require.Equal(t, tt.want, e.extractSnapProgress(tt.str))
	}
}

func createVolume(snapshotId string, volumeId string, state string) *ec2.Volume {
	return &ec2.Volume{
		Attachments:        nil,
		AvailabilityZone:   awsapi.String("us-west-2"),
		CreateTime:         nil,
		Encrypted:          awsapi.Bool(true),
		FastRestored:       awsapi.Bool(true),
		Iops:               awsapi.Int64(3000),
		KmsKeyId:           nil,
		MultiAttachEnabled: awsapi.Bool(true),
		OutpostArn:         awsapi.String("arn:12342"),
		Size:               awsapi.Int64(1),
		SnapshotId:         awsapi.String(snapshotId),
		State:              awsapi.String(state),
		Tags:               nil,
		Throughput:         nil,
		VolumeId:           awsapi.String(volumeId),
		VolumeType:         awsapi.String("gp3"),
	}
}
func TestHandleDescribeVolumesResponse(t *testing.T) {
	curentVolumesStates := &ec2.DescribeVolumesOutput{
		NextToken: awsapi.String("fake token"),
		Volumes: []*ec2.Volume{
			createVolume("snap-0873674883", "vol-98768979", "available"),
			createVolume("snap-0873674883", "vol-98768979", "creating"),
			createVolume("snap-0873674883", "vol-98768979", "available"),
			createVolume("snap-0873674883", "vol-98768979", "available"),
			createVolume("snap-0873674883", "vol-98768979", "available"),
		},
	}

	e := &EC2Session{}
	createdVolumeSize, unfinishedVolumes := e.HandleDescribeVolumesResponse(curentVolumesStates)
	require.Equal(t, int64(4), createdVolumeSize)
	require.Equal(t, 1, len(unfinishedVolumes))
}

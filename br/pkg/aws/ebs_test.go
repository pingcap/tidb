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
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/pingcap/tidb/br/pkg/gluetidb"
	"github.com/stretchr/testify/require"
)

func TestEC2SessionExtractSnapProgress(t *testing.T) {
	tests := []struct {
		str  *string
		want int64
	}{
		{nil, 0},
		{aws.String("12.12%"), 12},
		{aws.String("44.99%"), 44},
		{aws.String("  89.89%  "), 89},
		{aws.String("100%"), 100},
		{aws.String("111111%"), 100},
	}
	e := &EC2Session{}
	for _, tt := range tests {
		require.Equal(t, tt.want, e.extractSnapProgress(tt.str))
	}
}

func createVolume(snapshotId string, volumeId string, state string) *ec2.Volume {
	return &ec2.Volume{
		nil, nil, nil, nil, nil, nil, nil, nil, nil, aws.Int64(1), aws.String(snapshotId), aws.String(state), nil, nil, aws.String(volumeId), nil,
	}
}
func TestHandleDescribeVolumesResponse(t *testing.T) {

	curentVolumesStates := &ec2.DescribeVolumesOutput{
		NextToken: aws.String("fake token"),
		Volumes: []*ec2.Volume{
			createVolume("snap-0873674883", "vol-98768979", "available"),
			createVolume("snap-0873674883", "vol-98768979", "creating"),
			createVolume("snap-0873674883", "vol-98768979", "available"),
			createVolume("snap-0873674883", "vol-98768979", "available"),
			createVolume("snap-0873674883", "vol-98768979", "available"),
		},
	}

	mockGlue := &gluetidb.MockGlue{}
	ctx, _ := context.WithCancel(context.Background())
	fakeProgress := mockGlue.StartProgress(ctx, "Restore Data", int64(5), false)

	e := &EC2Session{}
	createdVolumeSize, unfinishedVolumes := e.HandleDescribeVolumesResponse(curentVolumesStates, fakeProgress)
	require.Equal(t, 4, createdVolumeSize)
	require.Equal(t, 1, len(unfinishedVolumes))
}

// Copyright 2024 PingCAP, Inc.
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

package cdcutil_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/util/cdcutil"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestGetCDCChangefeedNameSet(t *testing.T) {
	integration.BeforeTestExternal(t)
	testEtcdCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer testEtcdCluster.Terminate(t)

	ctx := context.Background()
	cli := testEtcdCluster.RandClient()
	checkEtcdPut := func(key string, vals ...string) {
		val := ""
		if len(vals) == 1 {
			val = vals[0]
		}
		_, err := cli.Put(ctx, key, val)
		require.NoError(t, err)
	}

	nameSet, err := cdcutil.GetCDCChangefeedNameSet(ctx, cli)
	require.NoError(t, err)
	require.True(t, nameSet.Empty())

	// TiCDC >= v6.2
	checkEtcdPut("/tidb/cdc/default/__cdc_meta__/capture/3ecd5c98-0148-4086-adfd-17641995e71f")
	checkEtcdPut("/tidb/cdc/default/__cdc_meta__/meta/meta-version")
	checkEtcdPut("/tidb/cdc/default/__cdc_meta__/meta/ticdc-delete-etcd-key-count")
	checkEtcdPut("/tidb/cdc/default/__cdc_meta__/owner/22318498f4dd6639")
	checkEtcdPut(
		"/tidb/cdc/default/default/changefeed/info/test",
		`{"upstream-id":7195826648407968958,"namespace":"default","changefeed-id":"test-1","sink-uri":"mysql://root@127.0.0.1:3306?time-zone=","create-time":"2023-02-03T15:23:34.773768+08:00","start-ts":439198420741652483,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","config":{"memory-quota":1073741824,"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"enable-sync-point":false,"bdr-mode":false,"sync-point-interval":600000000000,"sync-point-retention":86400000000000,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"event-filters":null},"mounter":{"worker-num":16},"sink":{"transaction-atomicity":"","protocol":"","dispatchers":null,"csv":{"delimiter":",","quote":"\"","null":"\\N","include-commit-ts":false},"column-selectors":null,"schema-registry":"","encoder-concurrency":16,"terminator":"\r\n","date-separator":"none","enable-partition-separator":false},"consistent":{"level":"none","max-log-size":64,"flush-interval":2000,"storage":""},"scheduler":{"region-per-span":0}},"state":"normal","error":null,"creator-version":"v6.5.0-master-dirty"}`,
	)
	checkEtcdPut(
		"/tidb/cdc/default/default/changefeed/info/test-1",
		`{"upstream-id":7195826648407968958,"namespace":"default","changefeed-id":"test-1","sink-uri":"mysql://root@127.0.0.1:3306?time-zone=","create-time":"2023-02-03T15:23:34.773768+08:00","start-ts":439198420741652483,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","config":{"memory-quota":1073741824,"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"enable-sync-point":false,"bdr-mode":false,"sync-point-interval":600000000000,"sync-point-retention":86400000000000,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"event-filters":null},"mounter":{"worker-num":16},"sink":{"transaction-atomicity":"","protocol":"","dispatchers":null,"csv":{"delimiter":",","quote":"\"","null":"\\N","include-commit-ts":false},"column-selectors":null,"schema-registry":"","encoder-concurrency":16,"terminator":"\r\n","date-separator":"none","enable-partition-separator":false},"consistent":{"level":"none","max-log-size":64,"flush-interval":2000,"storage":""},"scheduler":{"region-per-span":0}},"state":"failed","error":null,"creator-version":"v6.5.0-master-dirty"}`,
	)
	checkEtcdPut("/tidb/cdc/default/default/changefeed/status/test")
	checkEtcdPut("/tidb/cdc/default/default/changefeed/status/test-1")
	checkEtcdPut("/tidb/cdc/default/default/task/position/3ecd5c98-0148-4086-adfd-17641995e71f/test-1")
	checkEtcdPut("/tidb/cdc/default/default/upstream/7168358383033671922")

	nameSet, err = cdcutil.GetCDCChangefeedNameSet(ctx, cli)
	require.NoError(t, err)
	require.False(t, nameSet.Empty())
	require.Equal(t, "found CDC changefeed(s): cluster/namespace: default/default changefeed(s): [test], ",
		nameSet.MessageToUser())

	_, err = cli.Delete(ctx, "/tidb/cdc/", clientv3.WithPrefix())
	require.NoError(t, err)

	// TiCDC <= v6.1
	checkEtcdPut("/tidb/cdc/capture/f14cb04d-5ba1-410e-a59b-ccd796920e9d")
	checkEtcdPut(
		"/tidb/cdc/changefeed/info/test",
		`{"upstream-id":7195826648407968958,"namespace":"default","changefeed-id":"test-1","sink-uri":"mysql://root@127.0.0.1:3306?time-zone=","create-time":"2023-02-03T15:23:34.773768+08:00","start-ts":439198420741652483,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","config":{"memory-quota":1073741824,"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"enable-sync-point":false,"bdr-mode":false,"sync-point-interval":600000000000,"sync-point-retention":86400000000000,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"event-filters":null},"mounter":{"worker-num":16},"sink":{"transaction-atomicity":"","protocol":"","dispatchers":null,"csv":{"delimiter":",","quote":"\"","null":"\\N","include-commit-ts":false},"column-selectors":null,"schema-registry":"","encoder-concurrency":16,"terminator":"\r\n","date-separator":"none","enable-partition-separator":false},"consistent":{"level":"none","max-log-size":64,"flush-interval":2000,"storage":""},"scheduler":{"region-per-span":0}},"state":"stopped","error":null,"creator-version":"v6.5.0-master-dirty"}`,
	)
	checkEtcdPut("/tidb/cdc/job/test")
	checkEtcdPut("/tidb/cdc/owner/223184ad80a88b0b")
	checkEtcdPut("/tidb/cdc/task/position/f14cb04d-5ba1-410e-a59b-ccd796920e9d/test")

	nameSet, err = cdcutil.GetCDCChangefeedNameSet(ctx, cli)
	require.NoError(t, err)
	require.False(t, nameSet.Empty())
	require.Equal(t, "found CDC changefeed(s): cluster/namespace: <nil> changefeed(s): [test], ",
		nameSet.MessageToUser())

	_, err = cli.Delete(ctx, "/tidb/cdc/", clientv3.WithPrefix())
	require.NoError(t, err)

	// ignore __backup__ changefeed
	checkEtcdPut(
		"/tidb/cdc/__backup__/changefeed/info/test",
		`{"upstream-id":7195826648407968958,"namespace":"default","changefeed-id":"test-1","sink-uri":"mysql://root@127.0.0.1:3306?time-zone=","create-time":"2023-02-03T15:23:34.773768+08:00","start-ts":439198420741652483,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","config":{"memory-quota":1073741824,"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"enable-sync-point":false,"bdr-mode":false,"sync-point-interval":600000000000,"sync-point-retention":86400000000000,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"event-filters":null},"mounter":{"worker-num":16},"sink":{"transaction-atomicity":"","protocol":"","dispatchers":null,"csv":{"delimiter":",","quote":"\"","null":"\\N","include-commit-ts":false},"column-selectors":null,"schema-registry":"","encoder-concurrency":16,"terminator":"\r\n","date-separator":"none","enable-partition-separator":false},"consistent":{"level":"none","max-log-size":64,"flush-interval":2000,"storage":""},"scheduler":{"region-per-span":0}},"state":"normal","error":null,"creator-version":"v6.5.0-master-dirty"}`,
	)
	// ignore cluster id only changefeed
	checkEtcdPut(
		"/tidb/cdc/5402613591834624000/changefeed/info/test",
		`{"upstream-id":7195826648407968958,"namespace":"default","changefeed-id":"test-1","sink-uri":"mysql://root@127.0.0.1:3306?time-zone=","create-time":"2023-02-03T15:23:34.773768+08:00","start-ts":439198420741652483,"target-ts":0,"admin-job-type":0,"sort-engine":"unified","sort-dir":"","config":{"memory-quota":1073741824,"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"enable-sync-point":false,"bdr-mode":false,"sync-point-interval":600000000000,"sync-point-retention":86400000000000,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null,"event-filters":null},"mounter":{"worker-num":16},"sink":{"transaction-atomicity":"","protocol":"","dispatchers":null,"csv":{"delimiter":",","quote":"\"","null":"\\N","include-commit-ts":false},"column-selectors":null,"schema-registry":"","encoder-concurrency":16,"terminator":"\r\n","date-separator":"none","enable-partition-separator":false},"consistent":{"level":"none","max-log-size":64,"flush-interval":2000,"storage":""},"scheduler":{"region-per-span":0}},"state":"normal","error":null,"creator-version":"v6.5.0-master-dirty"}`,
	)
	nameSet, err = cdcutil.GetCDCChangefeedNameSet(ctx, cli)
	require.NoError(t, err)
	require.True(t, nameSet.Empty())
}

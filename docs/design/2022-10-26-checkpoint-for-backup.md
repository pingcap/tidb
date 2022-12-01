# Proposal: Checkpoint For Backup
- Author(s):   [Leavrth](https://github.com/Leavrth)
- Tracking Issues: https://github.com/pingcap/tidb/issues/38647

## Abstract

This proposal aims to support `Checkpoint For Backup` and describe what `Checkpoint For Backup` should look like and how to implement it.

## Background

Snapshot backup might be interrupted due to recoverable errors, such as disk exhaustion, node crash and network timeout. After these errors are addressed, users can try the next backup. However, data that is backed up before the interruption would be invalidated, and everything needs to start from scratch. For large-scale clusters, this incurs considerable extra cost.

## Detailed Design

### Implementation Overview

At the begining of a snapshot backup, BR encodes the table by table's ID into a `range`. All the data's keys of the table are involved of the `range`.

Then BR sends a backup request with the `range` to all the TiKV nodes. Once each TiKV node receives the request, it begins to back up the regions with leader status involved of the `range`. And once each region finishes being backed up, the TiKV node, which the region belongs to, sends a response with the overlapping range of `region` and `range`. When BR receives the response, the data of the overlapping range is regarded as having backed up.

The response contains not only the overlappping range, but also the paths of the backup files. BR persist these `meta-information` to the external storage. 

After BR exits due to an error and the error is addressed, users try to run BR again. At the begining of this snapshot backup, BR encodes the table by table's ID into a `range`, which is called `the origin range` now, like before. Then BR loads the backed up data's `meta-informationt`, which contains these backed up data's ranges called `sub ranges` now. And finally the difference ranges between `the origin range` and `sub ranges` are the data that needs to be backed up this time.

Instead of sending the request with `the origin range`, BR sends the request with the difference ranges to all the TiKV nodes. To avoid an increase in the number of requests, we add a new field for the backup request. The protobuf related change shown below: 

```protobuf
message BackupRequest {
    uint64 cluster_id = 1;

    bytes start_key = 2;
    bytes end_key = 3;

    // ...

    // with checkpoint, some subintervals of the range have been backed up and recorded.
    // only the remaining sub ranges of the range need to be backed up this time.
    repeated kvrpcpb.KeyRange sub_ranges = 16;
}
```

### Limitations and future Work

Once BR runs snapshot backup, it tries to write a lock file to the external storage at first. This prevents different BRs from performing the same task.

However, BR skips checking the lock file when checkpoint metadata exists in the external storage. So there may be different BRs performing the same task at the same time.

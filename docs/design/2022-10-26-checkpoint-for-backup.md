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

The response contains not only the overlappping range, but also the paths of the backup files. BR persist these meta-information to the external storage. 

After BR exits due to an error and the error is addressed, users try to run BR again. At the begining of this snapshot backup, 




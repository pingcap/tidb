# PD Change Log

## v2.1.0-rc2
### Features
* Support the `GetAllStores` interface
* Add the statistics of scheduling estimation in Simulator
### Improvements
* Optimize the handling process of down stores to make up replicas as soon as possible
* Optimize the start of Coordinator to reduce the unnecessary scheduling caused by restarting PD
* Optimize the memory usage to reduce the overhead caused by heartbeats
* Optimize error handling and improve the log information
* Support querying the Region information of a specific store in pd-ctl
* Support querying the topN Region information based on version
* Support more accurate TSO decoding in pd-ctl
### Bug fix
* Fix the issue that pd-ctl uses the `hot store` command to exit wrongly

## v2.1.0-rc1
### Features
* Introduce the version control mechanism and support rolling update of the cluster with compatibility
* Enable the `region merge` feature
* Support the `GetPrevRegion` interface
* Support splitting Regions in batch
* Support storing the GC safepoint
### Improvements
* Optimize the issue that TSO allocation is affected by the system clock going backwards
* Optimize the performance of handling Region hearbeats
* Optimize the Region tree performance
* Optimize the performance of computing hotspot statistics
* Optimize returning the error code of API interface
* Add options of controlling scheduling strategies
* Prohibit using special characters in `label`
* Improve the scheduling simulator
* Support splitting Regions using statistics in pd-ctl
* Support formatting JSON output by calling `jq` in pd-ctl
* Add metrics about etcd Raft state machine
### Bug fixes
* Fix the issue that the namespace is not reloaded after switching Leader
* Fix the issue that namespace scheduling exceeds the schedule limit
* Fix the issue that hotspot scheduling exceeds the schedule limit
* Fix the issue that wrong logs are output when the PD client closes
* Fix the wrong statistics of Region heartbeat latency

## v2.1.0-beta
### Improvements
* Enable Raft PreVote between PD nodes to avoid leader reelection when network recovers after network isolation
* Optimize the issue that Balance Scheduler schedules small Regions frequently
* Optimize the hotspot scheduler to improve its adaptability in traffic statistics information jitters
* Skip the Regions with a large number of rows when scheduling `region merge` 
* Enable `raft learner` by default to lower the risk of unavailable data caused by machine failure during scheduling
* Remove `max-replica` from `pd-recover`
* Add `Filter` metrics
### Bug Fixes
* Fix the issue that Region information is not updated after tikv-ctl unsafe recovery
* Fix the issue that TiKV disk space is used up caused by replica migration in some scenarios
### Compatibility notes
* Do not support rolling back to v2.0.x or earlier due to update of the new version storage engine
* Enable `raft learner` by default in the new version of PD. If the cluster is upgraded from 1.x to 2.1, the machine should be stopped before upgrade or a rolling update should be first applied to TiKV and then PD 

## v2.0.4
### Improvement
* Improve the behavior of the unset scheduling argument `max-pending-peer-count` by changing it to no limit for the maximum number of `PendingPeer`s

## v2.0.3
### Bug Fixes
* Fix the issue about scheduling of the obsolete Regions
* Fix the panic issue when collecting the hot-cache metrics in specific conditions

## v2.0.2
### Improvements
* Make the balance leader scheduler filter the disconnected nodes
* Make the tick interval of patrol Regions configurable
* Modify the timeout of the transfer leader operator to 10s
### Bug Fixes
* Fix the issue that the label scheduler does not schedule when the cluster Regions are in an unhealthy state
* Fix the improper scheduling issue of `evict leader scheduler`

## v2.0.1
### New Feature
* Add the `Scatter Range` scheduler to balance Regions with the specified key range
### Improvements
* Optimize the scheduling of Merge Region to prevent the newly split Region from being merged
* Add Learner related metrics
### Bug Fixes
* Fix the issue that the scheduler is mistakenly deleted after restart
* Fix the error that occurs when parsing the configuration file
* Fix the issue that the etcd leader and the PD leader are not synchronized
* Fix the issue that Learner still appears after it is closed
* Fix the issue that Regions fail to load because the packet size is too large

## v2.0.0-GA
### New Feature
* Support using pd-ctl to scatter specified Regions for manually adjusting hotspot Regions in some cases
### Improvements
* Improve configuration check rules to prevent unreasonable scheduling configuration
* Optimize the scheduling strategy when a TiKV node has insufficient space so as to prevent the disk from being fully occupied
* Optimize hot-region scheduler execution efficiency and add more metrics
* Optimize Region health check logic to avoid generating redundant schedule operators

## v2.0.0-rc.5
### New Feature
* Support adding the learner node
### Improvements
* Optimize the Balance Region Scheduler to reduce scheduling overhead
* Adjust the default value of `schedule-limit` configuration
* Fix the compatibility issue when adding a new scheduler
### Bug Fix
* Fix the issue of allocating IDs frequently

## v2.0.0-rc.4
### New Feature
* Support splitting Region manually to handle the hot spot in a single Region
### Improvement
* Optimize metrics
### Bug Fix
* Fix the issue that the label property is not displayed when `pdctl` runs `config show all`

## v2.0.0-rc3
### New Feature
* Support Region Merge, to merge empty Regions or small Regions after deleting data
### Improvements
* Ignore the nodes that have a lot of pending peers during adding replicas, to improve the speed of restoring replicas or making nodes offline
* Optimize the scheduling speed of leader balance in scenarios of unbalanced resources within different labels
* Add more statistics about abnormal Regions
### Bug Fix
* Fix the frequent scheduling issue caused by a large number of empty Regions

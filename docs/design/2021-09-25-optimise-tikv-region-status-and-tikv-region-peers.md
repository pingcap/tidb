# **Push down optimisation of  TIKV_REGION_STATUS and TiKV_REGION_PEERS**

- Author(s): @qidi1, @IcePigZDB
- Discussion PR: [https://github.com/pingcap/tidb/pull/28331](https://github.com/pingcap/tidb/pull/28331)
- Tracking Issue: [https://github.com/pingcap/tidb/issues/28330](https://github.com/pingcap/tidb/issues/28330)

## **Table of Contents**

  * [Introduction](#introduction)
  * [Motivation or Background](#motivation-or-background)
  * [Detailed Design](#detailed-design)
    + [TIKV_REGION_STATUS](#TIKV_REGION_STATUS)
    + [TIKV_REGION_PEERS](TIKV_REGION_PEERS)
  * [Test Design](#test-design)
    + [Functional Tests](#functional-tests)
    + [Scenario Tests](#scenario-tests)
    + [Compatibility Tests](#compatibility-tests)
    + [Benchmark Tests](#benchmark-tests)
  * [Impacts & Risks](#impacts--risks)
  * [Investigation & Alternatives](#investigation--alternatives)

## **Introduction**

Push down optimization of table `INFORMATION_SCHEMA.TIKV_REGION_STATUS` and table `INFORMATION_SCHEMA.TIKV_REGION_PEERS` to reduce execution time.

## **Motivation or Background**

The existing implementation of both tables fetches all tables' regions infomation, which results in the same overhead  of fetching a small table's regions infomation as a large one. To avoid this, some of the predicates can be pushed down to reduce unnecessary data transfer overhead.

## **Detailed Design**

### TIKV_REGION_STATUS

The following table describes all fields of table `TIKV_REGION_STATUS`:

```go
+---------------------------+-------------+------+------+---------+-------+
| Field                     | Type        | Null | Key  | Default | Extra |
+---------------------------+-------------+------+------+---------+-------+
| REGION_ID                 | bigint(21)  | YES  |      | NULL    |       |
| START_KEY                 | text        | YES  |      | NULL    |       |
| END_KEY                   | text        | YES  |      | NULL    |       |
| TABLE_ID                  | bigint(21)  | YES  |      | NULL    |       |
| DB_NAME                   | varchar(64) | YES  |      | NULL    |       |
| TABLE_NAME                | varchar(64) | YES  |      | NULL    |       |
| IS_INDEX                  | tinyint(1)  | NO   |      | 0       |       |
| INDEX_ID                  | bigint(21)  | YES  |      | NULL    |       |
| INDEX_NAME                | varchar(64) | YES  |      | NULL    |       |
| EPOCH_CONF_VER            | bigint(21)  | YES  |      | NULL    |       |
| EPOCH_VERSION             | bigint(21)  | YES  |      | NULL    |       |
| WRITTEN_BYTES             | bigint(21)  | YES  |      | NULL    |       |
| READ_BYTES                | bigint(21)  | YES  |      | NULL    |       |
| APPROXIMATE_SIZE          | bigint(21)  | YES  |      | NULL    |       |
| APPROXIMATE_KEYS          | bigint(21)  | YES  |      | NULL    |       |
| REPLICATIONSTATUS_STATE   | varchar(64) | YES  |      | NULL    |       |
| REPLICATIONSTATUS_STATEID | bigint(21)  | YES  |      | NULL    |       |
+---------------------------+-------------+------+------+---------+-------+
```

The alteration steps are as follows：

TiDB

1.  remove current function `setDataForTiKVRegionStatus` in executor/infoschema_reader.go.
2. add  `TikvRegionStatusExtractor` to push down `DB_NAME` , `TABLE_NAME`, `INDEX_NAME`, `TABLE_ID`, `INDEX_ID`.
3. add `tikvRegionStatusRetriever` to fetch regions information by region range keys, which construct start key and end key pairs from pushed down predicates.

PD 

1. add http interface `ScanRegionsByKeys` to scan regions in a given key range. 
2. Add support of this api in pd-ctl tool.

### TIKV_REGION_PEERS

The following table describes all fields of table `TIKV_REGION_PEERS` :

```go
+--------------+-------------+------+------+---------+-------+
| Field        | Type        | Null | Key  | Default | Extra |
+--------------+-------------+------+------+---------+-------+
| REGION_ID    | bigint(21)  | YES  |      | NULL    |       |
| PEER_ID      | bigint(21)  | YES  |      | NULL    |       |
| STORE_ID     | bigint(21)  | YES  |      | NULL    |       |
| IS_LEARNER   | tinyint(1)  | NO   |      | 0       |       |
| IS_LEADER    | tinyint(1)  | NO   |      | 0       |       |
| STATUS       | varchar(10) | YES  |      | 0       |       |
| DOWN_SECONDS | bigint(21)  | YES  |      | 0       |       |
+--------------+-------------+------+------+---------+-------+
```

To avoid adding additional PD interfaces, only the store_ID and peer_ID fields are pushed down to PD.

The alteration steps are as follows：

1. remove current func `setDataForTikVRegionPeers`  in executor/infoschema_reader.go.
2. add `TikvRegionPeersExtractor`  to push down `STORE_ID` and `REGION_ID`.
3. add `tikvRegionPeersRetriever`  to fetch regions information from PD. The following pseudo code describes the retrieve process:

    ```go
    func (e *tikvRegionPeersRetriever) retrieve(){
    	// case 1: no specified STORE_ID and REGION_ID. 
    	if(...){ 
    		// Get all RegionsInfo.
    		regionsInfo, err := tikvHelper.GetRegionsInfo()
    		return e.packTiKVRegionPeersRows(regionsInfo)
    		}
    var regionsInfoByStoreID, regionsInfoByRegionID , regionsInfo []helper.RegionInfo
    	regionMap := make(map[int64]bool)
    	// specified STORE_ID
    	if(...){
    		for _,storeID:= range e.extractor.StoreIDS{
    		regionsInfo, err := tikvHelper.GetStoreRegionsInfo(storeID)
    		...
    		for _,region: range regionsInfo.Regions{
    			// remove dup reigon by region id
    			if !regionMap[region.ID]{
    					regionMap[region.ID] = true
    					regionsInfoByStoreID = append(regionsInfoByStoreID,region)
    				}	
    			}
    		}
    		// no specified REGION_ID
    		if (...){
    			return packTiKVRegionPeersRows(regionsInfoByStoreID)
    		}
    	}
    	// specified REGION_ID
    	if(...){
    		for _,regionID:= range e.extractor.RegionIDs{
    		regionInfo, err := tikvHelper.GetRegionInfoByID(regionID)
    		...
    		regionsInfoByRegionID = append(regionsInfoByRegionID, *regionInfo)
    		}
    		// no specified STORE_ID
    		if (...){ 
    		return e.packTiKVRegionPeersRows(regionsInfoByRegionID)
    		}
    	}
    	
    	// intersect
    	for _, region := range regionsInfoByRegionID {
    			if regionMap[region.ID] {
    				regionsInfo = append(regionsInfo, region)
    			}
    		}
    		return e.packTiKVRegionPeersRows(regionsInfo)
    }
    
    ```

## **Test Design**

### **Functional Tests**

- /tidb/TikvRegionStatusExtractor & TikvRegionPeersExtractor
- /tidb/tikvRegionStatusRetriever & tikvRegionPeersRetriever
- /pd/ScanRegionsByKeys
- /pd/pd-ctl/ScanRegionsByKeys

### **Scenario Tests**

Run a workload in a cluster and compare the results of prvious version's  `setDataForTiKVRegionStatus` and `setDataForTikVRegionPeers`  to ensure this feature works as expected.

### **Compatibility Tests**

Both table are virtual table, scenario tests can cover compatibility tests**.**

### **Benchmark Tests**

Test whether the optimisztion make effect with different workloads.

## **Impacts & Risks**

Take a common query sql as example, table `TIKV_REGION_PEERS` does have `TABLE_NAME` field, it still fetch all regions infomation and become the bottleneck of this join sql.

```sql
# Query the leader distribution of a table
SELECT p.STORE_ID, COUNT(s.REGION_ID) PEER_COUNT FROM INFORMATION_SCHEMA.TIKV_REGION_STATUS s JOIN INFORMATION_SCHEMA.TIKV_REGION_PEERS p on s.REGION_ID = p.REGION_ID WHERE TABLE_NAME = 'table_name' and p.is_leader = 1 GROUP BY p.STORE_ID ORDER BY PEER_COUNT DESC;
```

## **Investigation & Alternatives**

Create a new table to concatenate two table toghter.

- Advantage
    - avoid join in comom use cases.
    - share pushed down predicates and avoid bottleneck mentioned in Impacts & Risks.
- Disadvantage
    - the  concatenated table has too many fields.

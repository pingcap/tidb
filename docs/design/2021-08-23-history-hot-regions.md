# Record history hotspot information in TiDB_HOT_REGIONS_HISTORY

- Author(s): [@qidi1](https://github.com/qidi1), [@icepigzdb](http://github.com/icepigzdb)
- Discussion PR: https://github.com/pingcap/tidb/pull/27487
- Tracking Issue: https://github.com/pingcap/tidb/issues/25281

## Table of Contents

  * [Introduction](#introduction)
  * [Motivation or Background](#motivation-or-background)
  * [Detailed Design](#detailed-design)
    + [Existing TIDB HOT REGIONS](#existing-tidb-hot-regions)
    + [Table Header Design](#table-header-design)
    + [PD](#pd)
    + [TiDB](#tidb)
  * [Test Design](#test-design)
    + [Functional Tests](#functional-tests)
    + [Scenario Tests](#scenario-tests)
    + [Compatibility Tests](#compatibility-tests)
    + [Benchmark Tests](#benchmark-tests)
  * [Impacts & Risks](#impacts--risks)
  * [Investigation & Alternatives](#investigation--alternatives)

## Introduction

Create a new table `TIDB_HOT_REGIONS_HISTORY` in the `INFORMATION_SCHEMAT` schema to retrieve history hotspot regions stored by PD periodically.  

## Motivation or Background

TiDB has a memory table `TIDB_HOT_REGIONS` that provides information about hotspot regions. But it only shows the current hotspot information. This leads to the fact that when DBAs want to query historical hotspot details, they have no way to find the corresponding hotspot information.

According to the [documentation](https://docs.pingcap.com/tidb/stable/information-schema-tidb-hot-regions) for the current `TIDB_HOT_REGIONS` table,  it can only provides information about recent hotspot regions calculated by PD according to the heartbeat from tikv.  It is inconvenient to obtain hotspot regions of past time, and locate which store the region is. For ease of use, we can store extended hotspot region information in PD. The following list some common use cases:

```SQL
# Query hotspot regions within a specified period of time
SELECT * FROM INFORMATION_SCHEMA.TIDB_HOT_REGIONS_HISTORY WHERE update_time >'2021-08-18 21:40:00' and update_time <'2021-09-19 00:00:00';

# Query hotspot regions of a table within a specified period of time
SELECT * FROM INFORMATION_SCHEMA.TIDB_HOT_REGIONS_HISTORY WHERE update_time >'2021-08-18 21:40:00' and update_time <'2021-09-19 00:00:00' and TABLE_NAME = 'table_name';

# Query the distribution of hotspot regions within a specified period of time
SELECT count(region_id) cnt, store_id FROM INFORMATION_SCHEMA.TIDB_HOT_REGIONS_HISTORY WHERE update_time >'2021-08-18 21:40:00' and update_time <'2021-09-19 00:00:00'  and table_name = 'table_name' GROUP BY STORE_ID ORDER BY cnt DESC;

# Query the distribution of hotspot leader regions within a specified period of time
SELECT count(region_id) cnt, store_id FROM INFORMATION_SCHEMA.TIDB_HOT_REGIONS_HISTORY WHERE update_time >'2021-08-18 21:40:00' and update_time <'2021-09-19 00:00:00'  and table_name = 'table_name' and is_leader=1 GROUP BY STORE_ID ORDER BY cnt DESC;

# Query the distribution of hotspot index regions within a specified period of time
SELECT count(region_id) cnt, index_name, store_id FROM INFORMATION_SCHEMA.TIDB_HOT_REGIONS_HISTORY WHERE update_time >'2021-08-18 21:40:00' and update_time <'2021-09-19 00:00:00' and table_name = 'table_name' group by index_name, store_id order by index_name,cnt desc;

# Query the distribution of hotspot index leader regions within a specified period of time
SELECT count(region_id) cnt, index_name, store_id FROM INFORMATION_SCHEMA.TIDB_HOT_REGIONS_HISTORY WHERE update_time >'2021-08-18 21:40:00' and update_time <'2022-09-19 00:00:00' and table_name = 'table_name' and is_leader=1 group by index_name, store_id order by index_name,cnt desc;
```

## Detailed Design

### Existing TIDB HOT REGIONS

Before we introduce `TIDB_HOT_REGIONS_HISTORY`, let's see how `TIDB_HOT_REGIONS` works.

```SQL
+----------------+-------------+------+------+---------+-------+
| Field          | Type        | Null | Key  | Default | Extra |
+----------------+-------------+------+------+---------+-------+
| TABLE_ID       | bigint(21)  | YES  |      | NULL    |       |
| INDEX_ID       | bigint(21)  | YES  |      | NULL    |       |
| DB_NAME        | varchar(64) | YES  |      | NULL    |       |
| TABLE_NAME     | varchar(64) | YES  |      | NULL    |       |
| INDEX_NAME     | varchar(64) | YES  |      | NULL    |       |
| REGION_ID      | bigint(21)  | YES  |      | NULL    |       |
| TYPE           | varchar(64) | YES  |      | NULL    |       |
| MAX_HOT_DEGREE | bigint(21)  | YES  |      | NULL    |       |
| REGION_COUNT   | bigint(21)  | YES  |      | NULL    |       |
| FLOW_BYTES     | bigint(21)  | YES  |      | NULL    |       |
+----------------+-------------+------+------+---------+-------+
10 rows in set (0.00 sec)
```
There are two types of hotspot regions: `read` and `write`. The memory table retriever processes the following steps to fetch current hotspot regions from PD server:

1. TiDB send an HTTP request to the PD to obtain current hotspot regions. 
   
2. PD returns the following fields：

    ```go
    // HotPeerStatShow records the hot region statistics for output
    type HotPeerStatShow struct {
      StoreID        uint64    `json:"store_id"`
      RegionID       uint64    `json:"region_id"`
      HotDegree      int       `json:"hot_degree"`
      ByteRate       float64   `json:"flow_bytes"`
      KeyRate        float64   `json:"flow_keys"`
      QueryRate      float64   `json:"flow_query"`
      AntiCount      int       `json:"anti_count"`
      LastUpdateTime time.Time `json:"last_update_time"`
    }
    ```

3. After TiDB catch the response, it  fetch the `START_KEY` and `END_KEY` of the hot region from region cache or PD by `REGION_ID` to decode the corresponding schema information like：`DB_NAME`, `TABLE_NAME`,  `TABLE_ID`,  `INDEX_NAME`, `INDEX_ID`.

4. TiDB return the hotspot region row to the upper caller.

In addition, hot regions can also be obtained directly through [pd-ctl](https://docs.pingcap.com/zh/tidb/stable/pd-control#health).  

### Table Header Design
1. New table header
  ```SQL
  > USE information_schema;
  > DESC tidb_hot_regions_history;
  +-------------+-------------+------+------+---------+-------+
  | Field       | Type        | Null | Key  | Default | Extra |
  +-------------+-------------+------+------+---------+-------+
  | UPDATE_TIME | timestamp(6)| YES  |      | NULL    |       | // new
  | DB_NAME     | varchar(64) | YES  |      | NULL    |       |
  | TABLE_NAME  | varchar(64) | YES  |      | NULL    |       |
  | TABLE_ID    | bigint(21)  | YES  |      | NULL    |       |
  | INDEX_NAME  | varchar(64) | YES  |      | NULL    |       |
  | INDEX_ID    | bigint(21)  | YES  |      | NULL    |       |
  | REGION_ID   | bigint(21)  | YES  |      | NULL    |       | 
  | STORE_ID    | bigint(21)  | YES  |      | NULL    |       | // new
  | PEER_ID     | bigint(21)  | YES  |      | NULL    |       | // new
  | IS_LEARNER  | tinyint(1)  | NO   |      | 0       |       | // new
  | IS_LEADER   | tinyint(1)  | NO   |      | 0       |       | // new
  | TYPE        | varchar(64) | YES  |      | NULL    |       |
  | HOT_DEGREE  | bigint(21)  | YES  |      | NULL    |       | // rename max_hot_degree to HOT_DEGREE
  | FLOW_BYTES  | double      | YES  |      | NULL    |       |
  | KEY_RATE    | double      | YES  |      | NULL    |       | // new
  | QUERY_RATE  | double      | YES  |      | NULL    |       | // new
  +-------------+-------------+------+------+---------+-------+
  | REGION_COUNT| bigint(21)  | YES  |      | NULL    |       | // deleted fields
  ```
  * Add `UPDATE_TIME` to support history.
  * Add `STORE_ID` ,  `PEER_ID`,  `IS_LAEDER`to track the machine of region.
  * Rename `MAX_HOT_DEGREE`  to `HOT_DEGREE` for precise meaning in history scenario.
  * Add `IS_LEARNER` and `IS_LEADER` to show role of regions.
  * Add `KEY_RATE` and `QUERY_RATE` for future expansion in hotspot determination dimensions.
  * Remove `REGION_COUNT` for disuse and repeat with `STORE_ID`.

### PD
1. Timing write：

     The leader of PD will periodically encrypt  `START_KEY` and `END_KEY`  in data,and write hotspot region data into `LevelDB`.The write interval can be configured. The write fieldes are: `REGION_ID`, `TYPE`,  `HOT_DEGREE`, `FLOW_BYTES`, `KEY_RATE`, `QUERY_RATE`, `STORE_ID`, `PEER_ID`, `UPDATE_TIME`, `START_KEY`,  `END_KEY`.

2. Timing delete

   PD runs the delete check task periodically, and deletes the hotspot region data that exceeds the configured TTL time.

3. Pull interface

   PD query the data in `LevleDB` with filters pushed down, decodes the data and returns to TiDB.

4. New options

   There are two config options need to be add in PD’s `config.go`:

   * `HisHotRegionSaveInterval`:  time interval for pd to record hotspot region information, default: 10 minutes.
   * `HisHotRegionTTL`: maximum hold day for his hot region, default: 7 days. 0 means close.
   
5. GC

     ```go
     type HistoryHotRegion struct {
     	UpdateTime    int64   `json:"update_time,omitempty"`
     	RegionID      uint64  `json:"region_id,omitempty"`
     	PeerID        uint64  `json:"peer_id,omitempty"`
     	StoreID       uint64  `json:"store_id,omitempty"`
     	IsLeader      bool    `json:"is_leader,omitempty"`
     	IsLearner     bool    `json:"is_learner,omitempty"`
     	HotRegionType string  `json:"hot_region_type,omitempty"`
     	HotDegree     int64   `json:"hot_degree,omitempty"`
     	FlowBytes     float64 `json:"flow_bytes,omitempty"`
     	KeyRate       float64 `json:"key_rate,omitempty"`
     	QueryRate     float64 `json:"query_rate,omitempty"`
     	StartKey      string  `json:"start_key,omitempty"`
     	EndKey        string  `json:"end_key,omitempty"`
     	EncryptionMeta *encryptionpb.EncryptionMeta `json:"encryption_meta,omitempty"`
     }
     ```

     `HistoryHotRegion` wraps hot region info, it is storage format of `hotRegionStorage`. The unserialized size of  mock `HistoryHotRegion` (random value, 20 bytes `StartKey`, 20bytes `Endkey`, without `EncryptionMeta`) is 144 Bytes, after serialization, the size is 353 bytes.

     * Data size estimation 

         The following formulas show data size per day, per week and per month respectively with default write interval 10 minutes, given the maximum number of hotspot regions is 1000:

         one day data write =  24(hours) * 6 (writes/hour)  * 1000 (max regions numbers/write) * 353 B (each region size)= 48.48 MB

         one week data write = 7(days) * 48.48MB(one day data write) = 339.34 MB

         one month data write = 30(days) * 48.48MB(one day data write) = 1454.32 MB

     * Actual data size.

         The following table shows the actual size of data stored in LevelDB per day, per week and per month, again, using 10 minutes as the write interval and using 1000 as the maximum number of hotspot regions :
         
         |  Only Write Data Size Per Day(MB) |  Only Write Data Size Per Week(MB) |  Only Write Data Size Per Month (MB) |
         | --------------------- | ---------------------- | ------------------------ |
         | 22.39                |  139.01                | 586.86                 |
         
         Because of the compression of LevelDB, the actual data sizes of all three time dimensions are close to and slightly smaller than the estimated data sizes. Given the size of the data, we set `HisHotRegionTTL` to 7 days. If the data survival time exceeds the preservation time,it will be delete from LevelDB,but really delete by LevelDB's auto compaction. 

6. PD-CTL
   Support history hot regions in pd-ctl.

### TiDB

1. Add memory table `TIDB_HOT_REGIONS_HISTORY`：

   Create a new memory table `TIDB_HOT_REGIONS_HISTORY` with fileds discussed above in `INFORMATION_SCHEMA` schema.

1. Add `HotRegionsHistoryTableExtractor` to push down some predicates to PD in order to  reduce network IO.

   ```go
   // HistoryHotRegionsRequest wrap conditions push down to PD.
   type HistoryHotRegionsRequest struct {
    StartTime      int64    `json:"start_time,omitempty"`
    EndTime        int64    `json:"end_time,omitempty"`
    RegionIDs      []uint64 `json:"region_ids,omitempty"`
    StoreIDs       []uint64 `json:"store_ids,omitempty"`
    PeerIDs        []uint64 `json:"peer_ids,omitempty"`
    IsLearners     []bool   `json:"is_learners,omitempty"`
    IsLeaders      []bool   `json:"is_leaders,omitempty"`
    HotRegionTypes []string `json:"hot_region_type,omitempty"`
   }
   ```
   
1. Add `hotRegionsHistoryRetriver` to fetch hotspot regions from all  PD servers by HTTP request(GET method),  then supplement fields like `DB_NAME`, `TABLE_NAME`,  `TABLE_ID`,  `INDEX_NAME`, `INDEX_ID` according to the `START_KEY` and `END_KEY`of the hotspot region, and merge the results.

   ```go
   // HistoryHotRegion records each hot region's statistics.
   // it's the response of PD.
   type HistoryHotRegion struct {
    UpdateTime    int64   `json:"update_time,omitempty"`
    RegionID      uint64  `json:"region_id,omitempty"`
    StoreID       uint64  `json:"store_id,omitempty"`
    PeerID        uint64  `json:"peer_id,omitempty"`
    IsLearner     bool    `json:"is_learner,omitempty"`
    IsLeader      bool    `json:"is_leader,omitempty"`
    HotRegionType string  `json:"hot_region_type,omitempty"`
    HotDegree     int64   `json:"hot_degree,omitempty"`
    FlowBytes     float64 `json:"flow_bytes,omitempty"`
    KeyRate       float64 `json:"key_rate,omitempty"`
    QueryRate     float64 `json:"query_rate,omitempty"`
    StartKey      string  `json:"start_key,omitempty"`
    EndKey        string  `json:"end_key,omitempty"`
   }
   ```

## Test Design

### Functional Tests

* /tidb/HotRegionsHistoryTableExtractor:  test with some sql statements.
* /tidb/hotRegionsHistoryRetriver: test with three mock PD http servers.
* /pd/HotRegionStorage: test read and write function.
* /pd/GetHistoryHotRegions: test PD's http server function. 
* /pd/TestHistoryHotRegions: test pd-ctl with mock hot regions.

### Scenario Tests

Run a workload in a cluster and compare the results of `TIDB_HOT_REGIONS_HISTORY` with PD dashboard and grafana. 

### Compatibility Tests

`TiDB_HOT_REGIONS_HISTORY` is compatible with `TiDB_HOT_REGIONS`.

### Benchmark Tests

* Test the time and space overhead to record history hot regions in PD's LevelDB under different parameters.
* Test the time overhead to retrive hot regions from PDs.

## Impacts & Risks

* TiDB can not add fields for deleted Table or Schema.

## Investigation & Alternatives

Write to TiDB like a normal table.
* advantage:
  * Reuse complete push-down function.
  * Data is written into TiKV to provide disaster tolerance.
  * The `Owner` election solves the problem of who pulls data from PD.
  * Can not support scenarios using TiKV independently.
* There may be two place to put this normal table:

     1. Create a table in the `mysql`  :
        * advantage:
          * Reuse complete push-down function.
          * Insert, query and delete can reuse the existing functions of Tidb.
        * disadvantage
          * The content in `INFORMATION_SCHEMA` is stored in the mysql library, which feels strange.
     2. Create a table in `INFORMATION_SCHEMA`: 

        * advantage:
          * No need to change the query entry.
          * It is more unified in design in this library.
        * disadvantage:
          * The creation of the `init()` function itself involves a lot and is difficult to transform.


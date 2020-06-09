# TiDB HTTP API

`TiDBIP` is the ip of the TiDB server. `10080` is the default status port, and you can edit it in tidb.toml when starting the TiDB server.

1. Get the current status of TiDB, including the connections, version and git_hash

    ```shell
    curl http://{TiDBIP}:10080/status
    ```

    ```shell
    $curl http://127.0.0.1:10080/status
    {
        "connections": 0,
        "git_hash": "f572e33854e1c0f942f031e9656d0004f99995c6",
        "version": "5.7.25-TiDB-v2.1.0-rc.3-355-gf572e3385-dirty"
    }
    ```

1. Get all metrics of TiDB

    ```shell
    curl http://{TiDBIP}:10080/metrics
    ```

1. Get the metadata of all regions

    ```shell
    curl http://{TiDBIP}:10080/regions/meta
    ```

    ```shell
    $curl http://127.0.0.1:10080/regions/meta
    [
        {
            "leader": {
                "id": 5,
                "store_id": 1
            },
            "peers": [
                {
                    "id": 5,
                    "store_id": 1
                }
            ],
            "region_epoch": {
                "conf_ver": 1,
                "version": 2
            },
            "region_id": 4
        }
    ]
    ```

1. Get the table/index of hot regions

    ```shell
    curl http://{TiDBIP}:10080/regions/hot
    ```

    ```shell
    $curl http://127.0.0.1:10080/regions/hot
    {
      "read": [

      ],
      "write": [
        {
          "db_name": "sbtest1",
          "table_name": "sbtest13",
          "index_name": "",
          "flow_bytes": 220718,
          "max_hot_degree": 12,
          "region_count": 1
        }
      ]
    }
    ```

1. Get the information of a specific region by ID

    ```shell
    curl http://{TiDBIP}:10080/regions/{regionID}
    ```

    ```shell
    $curl http://127.0.0.1:10080/regions/4001
    {
        "end_key": "dIAAAAAAAAEk",
        "frames": [
            {
                "db_name": "test",
                "is_record": true,
                "table_id": 286,
                "table_name": "t1"
            }
        ],
        "region_id": 4001,
        "start_key": "dIAAAAAAAAEe"
    }
    ```

1. Get regions Information from db.table

    ```shell
    curl http://{TiDBIP}:10080/tables/{db}/{table}/regions
    ```

    ```shell
    $curl http://127.0.0.1:10080/tables/test/t1/regions
    {
        "id": 286,
        "indices": [],
        "name": "t1",
        "record_regions": [
            {
                "leader": {
                    "id": 4002,
                    "store_id": 1
                },
                "peers": [
                    {
                        "id": 4002,
                        "store_id": 1
                    }
                ],
                "region_epoch": {
                    "conf_ver": 1,
                    "version": 83
                },
                "region_id": 4001
            }
        ]
    }
    ```

1. Get schema Information about all db

    ```shell
    curl http://{TiDBIP}:10080/schema
    ```

    ```shell
    $curl http://127.0.0.1:10080/schema
    [
        {
            "charset": "utf8mb4",
            "collate": "utf8mb4_bin",
            "db_name": {
                "L": "test",
                "O": "test"
            },
            "id": 266,
            "state": 5
        },
        .
        .
        .
    ]
    ```

1. Get schema Information about db

    ```shell
    curl http://{TiDBIP}:10080/schema/{db}
    ```

1. Get schema Information about db.table, and you can get schema info by tableID (tableID is the **unique** identifier of table in TiDB)

    ```shell
    curl http://{TiDBIP}:10080/schema/{db}/{table}

    curl http://{TiDBIP}:10080/schema?table_id={tableID}
    ```

1. Get database information, table information and tidb info schema version by tableID.

    ```shell
    curl http://{TiDBIP}:10080/db-table/{tableID}
    ```

1. Get MVCC Information of the key with a specified handle ID

    ```shell
    curl http://{TiDBIP}:10080/mvcc/key/{db}/{table}/{handle}
    ```

    ```shell
    $curl http://127.0.0.1:10080/mvcc/key/test/t1/1
    {
        "info": {
            "writes": [
                {
                    "commit_ts": 405179368526053380,
                    "short_value": "CAICAkE=",
                    "start_ts": 405179368526053377
                }
            ]
        }
    }
    ```

1. Get MVCC Information of the first key in the table with a specified start ts

    ```shell
    curl http://{TiDBIP}:10080/mvcc/txn/{startTS}/{db}/{table}
    ```

    ```shell
    $curl http://127.0.0.1:10080/mvcc/txn/405179368526053377/test/t1
    {
        "info": {
            "writes": [
                {
                    "commit_ts": 405179368526053380,
                    "short_value": "CAICAkE=",
                    "start_ts": 405179368526053377
                }
            ]
        },
        "key": "dIAAAAAAAAEzX3KAAAAAAAAAAQ=="
    }
    ```

1. Get MVCC Information by a hex value

    ```shell
    curl http://{TiDBIP}:10080/mvcc/hex/{hexKey}
    ```

1. Get MVCC Information of a specified index key, argument example: column_name_1=column_value_1&column_name_2=column_value2...

    ```shell
    curl http://{TiDBIP}:10080/mvcc/index/{db}/{table}/{index}/{handle}?${c1}={v1}&${c2}=${v2}
    ```

    *Hint: For the index column which column type is timezone dependent, e.g. `timestamp`, convert its value to UTC
timezone.*

    ```shell
    $curl http://127.0.0.1:10080/mvcc/index/test/t1/idx/1\?a\=A
    {
        "info": {
            "writes": [
                {
                    "commit_ts": 405179523374252037,
                    "short_value": "MA==",
                    "start_ts": 405179523374252036
                }
            ]
        }
    }
    ```

    *Hint: On a partitioned table, use the `table(partition)` pattern as the table name, `test(p1)` for example:*

    ```shell
    $curl http://127.0.0.1:10080/mvcc/index/test(p1)/t1/idx/1\?a\=A
    ```

1. Scatter regions of the specified table, add a `scatter-range` scheduler for the PD and the range is same as the table range.

    ```shell
    curl http://{TiDBIP}:10080/tables/{db}/{table}/scatter
    ```
    *Hint: On a partitioned table, use the `table(partition)` pattern as the table name, `test(p1)` for example:*

    **Note**: The `scatter-range` scheduler may conflict with the global scheduler, do not use it for long periods on the larger table.

1. Stop scatter the regions, disable the `scatter-range` scheduler for the specified table.

    ```shell
    curl http://{TiDBIP}:10080/tables/{db}/{table}/stop-scatter
    ```
    *Hint: On a partitioned table, use the `table(partition)` pattern as the table name, `test(p1)` for example:*

1. Get TiDB server settings

    ```shell
    curl http://{TiDBIP}:10080/settings
    ```

1. Get TiDB server information.

    ```shell
    curl http://{TiDBIP}:10080/info
    ```

    ```shell
    $curl http://127.0.0.1:10080/info
    {
        "ddl_id": "f7e73ed5-63b4-4cb4-ba7c-42b32dc74e77",
        "git_hash": "f572e33854e1c0f942f031e9656d0004f99995c6",
        "ip": "",
        "is_owner": true,
        "lease": "45s",
        "listening_port": 4000,
        "status_port": 10080,
        "version": "5.7.25-TiDB-v2.1.0-rc.3-355-gf572e3385-dirty"
    }
    ```

1. Get TiDB cluster all servers information.

    ```shell
    curl http://{TiDBIP}:10080/info/all
    ```

    ```shell
    $curl http://127.0.0.1:10080/info/all
    {
        "servers_num": 2,
        "owner_id": "29a65ec0-d931-4f9e-a212-338eaeffab96",
        "is_all_server_version_consistent": true,
        "all_servers_info": {
            "29a65ec0-d931-4f9e-a212-338eaeffab96": {
                "version": "5.7.25-TiDB-v4.0.0-alpha-669-g8f2a09a52-dirty",
                "git_hash": "8f2a09a52fdcaf9d9bfd775d2c6023f363dc121e",
                "ddl_id": "29a65ec0-d931-4f9e-a212-338eaeffab96",
                "ip": "",
                "listening_port": 4000,
                "status_port": 10080,
                "lease": "45s",
                "binlog_status": "Off"
            },
            "cd13c9eb-c3ee-4887-af9b-e64f3162d92c": {
                "version": "5.7.25-TiDB-v4.0.0-alpha-669-g8f2a09a52-dirty",
                "git_hash": "8f2a09a52fdcaf9d9bfd775d2c6023f363dc121e",
                "ddl_id": "cd13c9eb-c3ee-4887-af9b-e64f3162d92c",
                "ip": "",
                "listening_port": 4001,
                "status_port": 10081,
                "lease": "45s",
                "binlog_status": "Off"
            }
        }
    }
    ```

1. Enable/Disable TiDB server general log

    ```shell
    curl -X POST -d "tidb_general_log=1" http://{TiDBIP}:10080/settings
    curl -X POST -d "tidb_general_log=0" http://{TiDBIP}:10080/settings
    ```

1. Change TiDB server log level

    ```shell
    curl -X POST -d "log_level=debug" http://{TiDBIP}:10080/settings
    curl -X POST -d "log_level=info" http://{TiDBIP}:10080/settings
    ```

1. Change TiDB DDL slow log threshold

    The unit is millisecond.

    ```shell
    curl -X POST -d "ddl_slow_threshold=300" http://{TiDBIP}:10080/settings
    ```

1. Get the column value by an encoded row and some information that can be obtained from a column of the table schema information. 

    Argument example: rowBin=base64_encoded_row_value

    ```shell
    curl http://{TiDBIP}:10080/tables/{colID}/{colFlag}/{colLen}?rowBin={val}
    ```

    *Hint: For the column which field type is timezone dependent, e.g. `timestamp`, convert its value to UTC timezone.*

1. Resign the ddl owner, let tidb start a new ddl owner election.

    ```shell
    curl -X POST http://{TiDBIP}:10080/ddl/owner/resign
    ```

1. Get all TiDB DDL job history information.

    ```shell
    curl http://{TiDBIP}:10080/ddl/history
    ```

1. Get count {number} TiDB DDL job history information.

    ```shell
    curl http://{TiDBIP}:10080/ddl/history?limit={number}
    ```

    **Note**: If you request a tidb that is not ddl owner, the response will be `This node is not a ddl owner, can't be resigned.` 

1. Download TiDB debug info

    ```shell
    curl http://{TiDBIP}:10080/debug/zip?seconds=60 --output debug.zip
    ```
    
    zip file will include:
    
    - Go heap pprof(after GC)
    - Go cpu pprof(10s)
    - Go mutex pprof
    - Full goroutine
    - TiDB config and version

    Param:
    
    - seconds: profile time(s), default is 10s. 

1. Get statistics data of specified table.

    ```shell
    curl http://{TiDBIP}:10080/stats/dump/{db}/{table}
    ```

1. Get statistics data of specific table and timestamp.

    ```shell
    curl http://{TiDBIP}:10080/stats/dump/{db}/{table}/{yyyyMMddHHmmss}
    ```
    ```shell
    curl http://{TiDBIP}:10080/stats/dump/{db}/{table}/{yyyy-MM-dd HH:mm:ss}
    ```

1. Resume the binlog writing when Pump is recovered.

    ```shell
    curl http://{TiDBIP}:10080/binlog/recover
    ```

    Return value:

    * timeout, return status code: 400, message: `timeout`
    * If it returns normally, status code: 200, message example:
        ```text
        {
          "Skipped": false,
          "SkippedCommitterCounter": 0
        }
        ```
        `Skipped`: false indicates that the current binlog is not in the skipped state, otherwise, it is in the skipped state
        `SkippedCommitterCounter`: Represents how many transactions are currently being committed in the skipped state. By default, the API will return after waiting until all skipped-binlog transactions are committed. If this value is greater than 0, it means that you need to wait until them are committed .

    Param:

    * op=nowait: return after binlog status is recoverd, do not wait until the skipped-binlog transactions are committed.
    * op=reset: reset `SkippedCommitterCounter` to 0 to avoid the problem that `SkippedCommitterCounter` is not cleared due to some unusual cases.
    * op=status: Get the current status of binlog recovery.

# TiDB HTTP API

`TiDBIP` is the ip of the TiDB server. `10080` is the default status port, and you can edit it in tidb.toml when starting the TiDB server.

1. Get the current status of TiDB, including the connections, version and git_hash

    ```shell
    curl http://{TiDBIP}:10080/status
    ```

1. Get all metrics of TiDB

    ```shell
    curl http://{TiDBIP}:10080/metrics
    ```

1. Get the metadata of all regions

    ```shell
    curl http://{TiDBIP}:10080/regions/meta
    ```

1. Get the information of a specific region by ID

    ```shell
    curl http://{TiDBIP}:10080/regions/{regionID}
    ```

1. Get regions Information from db.table

    ```shell
    curl http://{TiDBIP}:10080/tables/{db}/{table}/regions
    ```

1. Get schema Information about all db

    ```shell
    curl http://{TiDBIP}:10080/schema
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

1. Get MVCC Information of the first key in the table with a specified start ts

    ```shell
    curl http://{TiDBIP}:10080/mvcc/txn/{startTS}/{db}/{table}
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

1. Scatter regions of the specified table, add a `scatter-range` scheduler for the PD and the range is same as the table range.

    ```shell
    curl -X POST http://{TiDBIP}:10080/tables/{db}/{table}/scatter
    ```

    **Note**: The `scatter-range` scheduler may conflict with the global scheduler, do not use it for long periods on the larger table.

1. Stop scatter the regions, disable the `scatter-range` scheduler for the specified table.

    ```shell
    curl -X POST http://{TiDBIP}:10080/tables/{db}/{table}/stop-scatter
    ```

1. Get TiDB server settings

    ```shell
    curl http://{TiDBIP}:10080/settings
    ```

1. Get TiDB server information.

    ```shell
    curl http://{TiDBIP}:10080/info
    ```

1. Get TiDB cluster all servers information.

    ```shell
    curl http://{TiDBIP}:10080/info/all
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

    **Note**: If you request a tidb that is not ddl owner, the response will be `This node is not a ddl owner, can't be resigned.`

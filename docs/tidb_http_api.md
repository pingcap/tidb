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

1. Get disk-usage info about db.table

    ```shell
    curl http://{TiDBIP}:10080/tables/{db}/{table}/disk-usage
    ```

1. Get MVCC Information of the key with a specified handle ID

    ```shell
    curl http://{TiDBIP}:10080/mvcc/key/{db}/{table}/{handle}
    ```

1. Get MVCC Information of the first key in the table with a specified start ts

    ```shell
    curl http://{TiDBIP}:10080/mvcc/txn/{startTS}/{db}/{table}
    ```

1. Get MVCC Information of the key with a specified handle ID

    ```shell
    curl http://{TiDBIP}:10080/mvcc/txn/{startTS}
    ```

1. Get MVCC Information by a hex value

    ```shell
    curl http://{TiDBIP}:10080/mvcc/hex/{hexKey}
    ```

1. Get MVCC Information of a specified index key, argument example: column_name_1=column_value_1&column_name_2=column_value2...

    ```shell
    curl http://{TiDBIP}:10080/mvcc/index/{db}/{table}/{index}/{handle}?${c1}={v1}&${c2}=${v2}
    ```

#  TiDB HTTP API

`TiDBIP` is the ip of the TiDB server. `10080` is the default status port, and you can edit it in tidb.toml when starting the TiDB server 

> 1. Get the current status of TiDB, including the connections, version and git_hash

```
curl http://{TiDBIP}:10080/status
```

> 2. Get all metrics of TiDB

```
curl http://{TiDBIP}:10080/metrics
```

> 3. Get the metadata of all regions

```
curl http://{TiDBIP}:10080/regions/meta
```

> 4. Get the information of a specific region by ID

```
curl http://{TiDBIP}:10080/regions/{regionID}
```

> 5. Get regions Information from db.table

```
curl http://{TiDBIP}:10080/tables/{db}/{table}/regions
```

> 6. Get schema Information about all db

```
curl http://{TiDBIP}:10080/schema
```

> 7. Get schema Information about db

```
curl http://{TiDBIP}:10080/schema/{db}
```

> 8. Get schema Information about db.table, and you can get schema info by tableid

```
curl http://{TiDBIP}:10080/schema/{db}/{table}

curl http://{TiDBIP}:10080/schema?tableid={tableid}

```

> 9. Get disk-usage info about db.table

```
curl http://{TiDBIP}:10080/tables/{db}/{table}/disk-usage
```

> 10. Get MVCC Information of the key with a specified handle ID

```
curl http://{TiDBIP}:10080/mvcc/key/{db}/{table}/{handle}
```

> 11. Get MVCC Information of the first key in the table with a specified start ts

```
curl http://{TiDBIP}:10080/mvcc/txn/{startTS}/{db}/{table}
```

> 12. Get MVCC Information of the key with a specified handle ID

```
curl http://{TiDBIP}:10080/mvcc/txn/{startTS}
```

> 13. Get MVCC Information by a hex value

```
curl http://{TiDBIP}:10080/mvcc/hex/{hexKey}
```

> 14. Get MVCC Information of a specified index key, argument example: column_name_1=column_value_1&column_name_2=column_value2...

```
curl http://{TiDBIP}:10080/mvcc/index/{db}/{table}/{index}/{handle}?${c1}={v1}&${c2}=${v2}
```

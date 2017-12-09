#  TiDB HTTP API

`TiDBIP` is the ip of the TiDB server. `10080` is the default status port, and it can be changed in tidb.toml when started. 

> 1. Get the current status of TiDB, including the connections, version and git_hash

```
curl http://{TiDBIP}:10080/status
```

> 2. Get TiDB all metrics

```
curl http://{TiDBIP}:10080/metrics
```

> 3. Get all regions metadata

```
curl http://{TiDBIP}:10080/regions/meta
```

> 4. Get the information of a specific region by ID

```
curl http://{TiDBIP}:10080/regions/{regionID}
```

> 5. Get regions info from db.table

```
curl http://{TiDBIP}:10080/tables/{db}/{table}/regions
```

> 6. Get schema info about all db

```
curl http://{TiDBIP}:10080/schema
```

> 7. Get schema info about db

```
curl http://{TiDBIP}:10080/schema/{db}
```

> 8. Get schema info about db.table

```
curl http://{TiDBIP}:10080/schema/{db}/{table}
```


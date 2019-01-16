---
name: "\U0001F947 Performance Question"
about: Performance question about TiDB which is not caused by bug.

---

## Performance Question

- What version of TiDB are you using?
<!-- You can try `tidb-server -V` or run `select tidb_version();` on TiDB to get this information -->

- What's the difference between the performance you expect and the performance you observe?

- Are you comparing TiDB with some other databases? What's the difference between TiDB and the compared one?

- If it's a specific SQL query, could you please
    - let us know whether you analyzed the tables involved in the query or not. And how long it is after you ran the last `ANALYZE`.
    - tell us whether this SQL always runs slowly, or just occasionally.
    - provide the `EXPLAIN ANALYZE` result of this query if your TiDB version is higher than 2.1, or you can just provide the `EXPLAIN` result.
    - provide the plain text of the SQL and table schema so we can test it locally. It would be better if you can provide the dumped statistics information.
        - use `show create table ${involved_table}\G` to get the table schema.
        - use `curl -G "http://${tidb-server-ip}:${tidb-server-status-port}/stats/dump/${db_name}/${table_name}" > ${table_name}_stats.json` to get the dumped statistics of one involved table.
    - provide the `EXPLAIN` result of the compared database. For mysql, `EXPLAIN format=json`'s result will be more helpful.
    - other information that is useful from your perspective.

- If it's not about a specific SQL query, e.g. the benchmark result you got by yourself is not expected. Could you please
    - show us your cluster's topology architecture
    - a simple description of you workload

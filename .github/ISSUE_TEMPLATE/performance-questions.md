---
name: "\U0001F947 Ask a Database Performance Question"
about: I want to ask a database performance question.
labels: type/question, type/performance
---

## Performance Questions

- What version of TiDB are you using?
<!-- You can try `tidb-server -V` or run `select tidb_version();` on TiDB to get this information -->

- What's the observed and your expected performance respectively?

- Have you compared TiDB with other databases? If yes, what's their difference?

- For a specific slow SQL query, please provide the following information:
    - Whether you analyzed the tables involved in the query and how long it is after you ran the last `ANALYZE`.
    - Whether this SQL query always or occasionally runs slowly.
    - The `EXPLAIN ANALYZE` result of this query if your TiDB version is higher than 2.1, or you can just provide the `EXPLAIN` result.
    - The plain text of the SQL query and table schema so we can test it locally. It would be better if you can provide the dumped statistics information.
        <!-- you can use `show create table ${involved_table}\G` to get the table schema.-->
        <!-- use `curl -G "http://${tidb-server-ip}:${tidb-server-status-port}/stats/dump/${db_name}/${table_name}" > ${table_name}_stats.json` to get the dumped statistics of one involved table.-->
    - The `EXPLAIN` result of the compared database. For MySQL, `EXPLAIN format=json`'s result will be more helpful.
    - Other information that is useful from your perspective.

- For a general performance question, e.g. the benchmark result you got by yourself is not expected, please provide the following information:
    - Your cluster's topology architecture.
    - A simple description of you workload.
    - The metrics PDF generated from Grafana monitor. Remember to set the time range to the performance issue duration.

---
name: "\U0001F947 Performance Question"
about: Performance question about TiDB which is not caused by bug.

---

## Performance Question

- What version of TiDB are you using (`tidb-server -V` or run `select tidb_version();` on TiDB)?

- What's the difference between the performance you expect and the performance you observe?

- Are you comparing TiDB with some other database? What's the difference between TiDB and compared one?

- If it's a specific SQL query, could you please
    - let us know whether you get your result after analyzing the table involved in the query.
    - provide the `EXPLAIN ANALYZE` result of this query if you can wait until this SQL's execution finishes and your TiDB version is no lower than 2.1, or you can just provide the `EXPLAIN` result.
    - provide the plain text of the SQL and table schema so we can test it locally. It'll be more better if you can provide the dumped statistics information.
    - other information that you think it's useful to distinguish the property of this SQL.

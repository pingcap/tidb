---
name: "🐛 Bug Report"
about: Something isn't working as expected
title: ''
labels: 'type/bug '
---

Please answer these questions before submitting your issue. Thanks!

1. What did you do?
If possible, provide a recipe for reproducing the error.


2. What did you expect to see?



3. What did you see instead?



4. What version of BR and TiDB/TiKV/PD are you using?

<!--
br -V
tidb-lightning -V
tidb-server -V
tikv-server -V
pd-server -V
-->

5. Operation logs
   - Please upload `br.log` for BR if possible
   - Please upload `tidb-lightning.log` for TiDB-Lightning if possible
   - Please upload `tikv-importer.log` from TiKV-Importer if possible
   - Other interesting logs


6. Configuration of the cluster and the task
   - `tidb-lightning.toml` for TiDB-Lightning if possible
   - `tikv-importer.toml` for TiKV-Importer if possible
   - `topology.yml` if deployed by TiUP


7. Screenshot/exported-PDF of Grafana dashboard or metrics' graph in Prometheus if possible

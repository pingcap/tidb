# Roadmap

This document defines the roadmap for TiDB development.

##### __SQL Layer__  
- [x] Simple CRUD / DDL
- [x] Index support
- [x] Index optimization
- [x] Query plan optimization
- [x] Transactions
- [x] Functions support  (e.g. MAX / MIN / COUNT / CONCAT ... )
- [x] Aggregation support
    - [x] Group by clause
    - [x] Order by clause
    - [x] Distinct clause
- [x] Join (LEFT JOIN / RIGHT JOIN / CROSS JOIN)
- [x] Simple Subquery
- [x] Asynchronous schema change


##### __API__  
- [x] Embedded Go library
- [x] MySQL protocol server
- [ ] PostgreSQL protocol server
- [ ] JSON support


##### __Application__  
- [x] Gogs
- [x] Wordpress
- [ ] Phabricator


##### __Admin Tool__  
- [x] PhpMyAdmin 
- [ ] Homemade admin tool [WIP]


##### __Storage__  
- [x] BoltDB
- [x] GoLevelDB
- [x] RocksDB
- [x] LMDB
- [x] HBase
- [x] Homemade distributed KV:
    - [ ] Transactions
    - [ ] Replicate log using Paxos/Raft
    - [ ] Auto-Rebalance
    - [ ] Geo replicated

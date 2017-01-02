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
- [x] MPP SQL
    - [x] Push down 


##### __API__  
- [x] MySQL protocol server
- [ ] PostgreSQL protocol server
- [ ] JSON support


##### __Application__  
- [x] Gogs
- [x] Wordpress
- [ ] Phabricator


##### __Admin Tool__  
- [x] PhpMyAdmin 
- [x] Homemade admin tool


##### __Storage__  
- [x] BoltDB
- [x] GoLevelDB
- [x] Homemade distributed KV ([pingcap/tikv](https://github.com/pingcap/tikv)):
    - [x] Transactions
    - [x] Replicate log using Raft
    - [x] Scale-out (Auto-rebalance)
    - [x] Geo replicated

# Integrate LOAD DATA with tidb-lightning
- Author: [lance6716](https://github.com/lance6716), [D3Hunter](https://github.com/D3Hunter)
- Tracking Issue: https://github.com/pingcap/tidb/issues/40499

## Summary

This RFC introduce the similarity and difference of SQL statement "LOAD DATA" and [TiDB lightning](https://docs.pingcap.com/tidb/stable/tidb-lightning-overview), and propose a way to integrate them with the advantage from the both side.

## Background and Motivation

The [LOAD DATA statement](https://docs.pingcap.com/tidb/stable/sql-statement-load-data#load-data) of TiDB aims to be fully compatible with MySQL's one in most scenarios. It has advantages such as
- Easy to use no matter how cluster is deployed
- Supporting linux pipe files
- Supporting modifying the data by [input preprocessing](https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-input-preprocessing)

TiDB lightning is a tool used for importing data at TB scale to TiDB clusters. It is widely used to import data into TiDB cluster in many cases, and it has many powerful and complex features:
- Supporting many file formats like mydumper-like dump files, parquet
- Supporting read files from cloud storage like S3
- Very high importing speed when use [physical import mode](https://docs.pingcap.com/tidb/stable/tidb-lightning-physical-import-mode)

Although they both have some advantanges, merely one of them is limited in data loading sceario. For example, the user tends to load large amount of data using TiDB lightning considering the performance, but if the TiDB cluster is deployed in k8s TiDB lightning need to be launched in a k8s pod to access the cluster. This introduces many problems, and LOAD DATA only requires the user to execute a SQL through MySQL client.

## Detailed Design

In short, we will decouple LOAD DATA statement and TiDB lightning into many components, and try to pick the best parts of both and combine them.

### Data source reader

### Data parser

### KV encoder

### KV writer

## Other Issues

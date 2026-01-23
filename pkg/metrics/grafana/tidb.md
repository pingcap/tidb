# TiDB Dashboard Migration Plan

This document tracks the migration of `tidb.json` to `tidb.jsonnet`.

## Overview

- **Source**: `tidb.json` (793 KB, manually maintained)
- **Target**: `tidb.jsonnet` (programmatic dashboard definition)
- **Reference**: `tidb_summary.jsonnet` (pattern to follow)

## Migration Progress

| Row # | Row Name | Panels | Status | Commit |
|-------|----------|--------|--------|--------|
| 1 | Query Summary | 12 | ✅ Done | 47fc2affb1 |
| 2 | Query Detail | 7 | ✅ Done | 827cec6c41 |
| 3 | Server | 23 | ✅ Done | 9c6e6d4abf |
| 4 | Transaction | 35 | ✅ Done | fd02c6ad66 |
| 5 | Executor | 16 | ✅ Done | TBD |
| 6 | Distsql | 8 | ✅ Done | c36c24b8db |
| 7 | KV Errors | 5 | ✅ Done | b536b3d6e1 |
| 8 | KV Request | 16 | ✅ Done | a0bc3c0270 |
| 9 | PD Client | 15 | ✅ Done | 3e8b635b50 |
| 10 | Schema Load | 11 | ✅ Done | 98eddd5b41 |
| 11 | DDL | 14 | ✅ Done | cd39af60ad |
| 12 | Dist Execute Framework | 7 | ✅ Done | e708549962 |
| 13 | Statistics & Plan Management | 17 | ✅ Done | cb21e62411 |
| 14 | Owner | 2 | ✅ Done | 589b18c287 |
| 15 | Meta | 4 | ✅ Done | 0be0d78c3a |
| 16 | GC | 11 | ✅ Done | ff477e5330 |
| 17 | Batch Client | 4 | ✅ Done | d2a0212889 |
| 18 | TopSQL | 6 | ✅ Done | e227dc8460 |
| 19 | TTL | 17 | ✅ Done | 12627a6187 |
| 20 | Resource Manager | 2 | ✅ Done | b0101f0bba |
| 21 | Follower Read | 3 | ✅ Done | 17b61e9426 |
| 22 | Import Into | 5 | ✅ Done | 20922e8df3 |
| 23 | Global Sort | 6 | ✅ Done | 96eec8a4ec |
| 24 | Network Transmission | 1 | ✅ Done | c5e5eb2ed4 |
| 25 | Memory Arbitrator | 8 | ⏳ Pending | - |

**Total**: 25 rows, ~228 panels

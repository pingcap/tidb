# Mega Test Migration TODO

Total packages to migrate: **302**
Migrated so far: **18**
Remaining: **284**

## Migration Checklist

### Core Planner Packages (60+ packages)

- [ ] pkg/planner/cardinality
- [X] pkg/planner/cascades
- [ ] pkg/planner/cascades/memo
- [ ] pkg/planner/cascades/old
- [ ] pkg/planner/core
- [ ] pkg/planner/core/casetest
- [ ] pkg/planner/core/casetest/binaryplan
- [ ] pkg/planner/core/casetest/cascades
- [ ] pkg/planner/core/casetest/cbotest
- [ ] pkg/planner/core/casetest/ch
- [ ] pkg/planner/core/casetest/correlated
- [ ] pkg/planner/core/casetest/dag
- [ ] pkg/planner/core/casetest/enforcempp
- [ ] pkg/planner/core/casetest/flatplan
- [ ] pkg/planner/core/casetest/hint
- [ ] pkg/planner/core/casetest/index
- [ ] pkg/planner/core/casetest/indexmerge
- [ ] pkg/planner/core/casetest/instanceplancache
- [ ] pkg/planner/core/casetest/join
- [ ] pkg/planner/core/casetest/logicalplan
- [ ] pkg/planner/core/casetest/mpp
- [ ] pkg/planner/core/casetest/parallelapply
- [ ] pkg/planner/core/casetest/partition
- [ ] pkg/planner/core/casetest/physicalplantest
- [ ] pkg/planner/core/casetest/plancache
- [ ] pkg/planner/core/casetest/planstats
- [ ] pkg/planner/core/casetest/pushdown
- [ ] pkg/planner/core/casetest/rule
- [ ] pkg/planner/core/casetest/scalarsubquery
- [ ] pkg/planner/core/casetest/schema
- [ ] pkg/planner/core/casetest/tpcds
- [ ] pkg/planner/core/casetest/tpch
- [ ] pkg/planner/core/casetest/vectorsearch
- [ ] pkg/planner/core/casetest/windows
- [ ] pkg/planner/core/issuetest
- [ ] pkg/planner/core/operator/logicalop/logicalop_test
- [ ] pkg/planner/core/rule
- [ ] pkg/planner/core/tests/analyze
- [ ] pkg/planner/core/tests/cte
- [ ] pkg/planner/core/tests/extractor
- [ ] pkg/planner/core/tests/null
- [ ] pkg/planner/core/tests/pointget
- [ ] pkg/planner/core/tests/prepare
- [ ] pkg/planner/core/tests/redact
- [ ] pkg/planner/core/tests/rewriter
- [ ] pkg/planner/core/tests/subquery
- [X] pkg/planner/extstore
- [X] pkg/planner/funcdep
- [ ] pkg/planner/implementation
- [X] pkg/planner/indexadvisor
- [ ] pkg/planner/memo
- [X] pkg/planner/util
- [ ] pkg/planner/util/fixcontrol

### Executor Packages (50+ packages)

- [ ] pkg/executor
- [ ] pkg/executor/aggfuncs
- [ ] pkg/executor/importer
- [ ] pkg/executor/internal/applycache
- [ ] pkg/executor/internal/calibrateresource
- [ ] pkg/executor/internal/exec
- [ ] pkg/executor/internal/pdhelper
- [ ] pkg/executor/internal/querywatch
- [ ] pkg/executor/internal/vecgroupchecker
- [ ] pkg/executor/join
- [ ] pkg/executor/join/test/indexjoin
- [ ] pkg/executor/join/test/mergejoin
- [ ] pkg/executor/sortexec
- [ ] pkg/executor/staticrecordset
- [ ] pkg/executor/test/admintest
- [ ] pkg/executor/test/aggregate
- [ ] pkg/executor/test/analyzetest
- [ ] pkg/executor/test/analyzetest/columns
- [ ] pkg/executor/test/analyzetest/memorycontrol
- [ ] pkg/executor/test/analyzetest/options
- [ ] pkg/executor/test/analyzetest/panictest
- [ ] pkg/executor/test/autoidtest
- [ ] pkg/executor/test/cte
- [ ] pkg/executor/test/ddl
- [ ] pkg/executor/test/distsqltest
- [ ] pkg/executor/test/executor
- [ ] pkg/executor/test/fktest
- [ ] pkg/executor/test/indexmergereadtest
- [ ] pkg/executor/test/infoschema
- [ ] pkg/executor/test/issuetest
- [ ] pkg/executor/test/jointest
- [ ] pkg/executor/test/jointest/hashjoin
- [ ] pkg/executor/test/loaddatatest
- [ ] pkg/executor/test/loadremotetest
- [ ] pkg/executor/test/memtest
- [ ] pkg/executor/test/oomtest
- [ ] pkg/executor/test/passwordtest
- [ ] pkg/executor/test/plancache
- [ ] pkg/executor/test/planreplayer
- [ ] pkg/executor/test/seqtest
- [ ] pkg/executor/test/showtest
- [ ] pkg/executor/test/simpletest
- [ ] pkg/executor/test/splittest
- [ ] pkg/executor/test/tiflashtest
- [ ] pkg/executor/test/txn
- [ ] pkg/executor/test/unstabletest
- [ ] pkg/executor/test/writetest

### Session Packages

- [ ] pkg/session
- [ ] pkg/sessionctx
- [ ] pkg/sessiontxn

### Expression Packages

- [ ] pkg/expression
- [ ] pkg/expression/aggregation
- [ ] pkg/expression/test/constantpropagation
- [ ] pkg/expression/test/multivaluedindex

### DDL Sub-packages

- [ ] pkg/ddl
- [ ] pkg/ddl/ingest
- [ ] pkg/ddl/label
- [ ] pkg/ddl/notifier
- [ ] pkg/ddl/schemaver
- [ ] pkg/ddl/session
- [ ] pkg/ddl/systable
- [ ] pkg/ddl/util
- [ ] pkg/ddl/tests/adminpause
- [ ] pkg/ddl/tests/fail
- [ ] pkg/ddl/tests/fastcreatetable
- [ ] pkg/ddl/tests/fk
- [ ] pkg/ddl/tests/indexmerge
- [ ] pkg/ddl/tests/metadatalock
- [ ] pkg/ddl/tests/multivaluedindex
- [ ] pkg/ddl/tests/partition
- [ ] pkg/ddl/tests/serial
- [ ] pkg/ddl/tests/tiflash

### Store & KV Packages

- [ ] pkg/store
- [ ] pkg/store/copr
- [ ] pkg/store/copr/copr_test
- [ ] pkg/store/driver
- [ ] pkg/store/driver/error
- [ ] pkg/store/driver/txn
- [ ] pkg/store/gcworker
- [ ] pkg/store/helper
- [ ] pkg/store/mockstore
- [ ] pkg/store/mockstore/mockcopr
- [ ] pkg/store/mockstore/unistore
- [ ] pkg/store/mockstore/unistore/cophandler
- [ ] pkg/store/mockstore/unistore/lockstore
- [ ] pkg/store/mockstore/unistore/tikv
- [ ] pkg/store/mockstore/unistore/util/lockwaiter

### DistSQL & Table Packages

- [ ] pkg/distsql
- [ ] pkg/table
- [ ] pkg/table/tables
- [ ] pkg/table/tables/test/partition
- [ ] pkg/table/temptable
- [ ] pkg/tablecodec
- [ ] pkg/tablecodec/rowindexcodec

### Meta & Autoid Packages

- [ ] pkg/meta
- [ ] pkg/meta/autoid

### Statistics Packages

- [ ] pkg/statistics/handle/syncload
- [ ] pkg/statistics/handle/updatetest
- [ ] pkg/statistics/handle/usage
- [ ] pkg/statistics/handle/util

### Domain Packages

- [ ] pkg/domain/crossks
- [ ] pkg/domain/globalconfigsync
- [ ] pkg/domain/infosync
- [ ] pkg/domain/serverinfo

### Config & Types Packages

- [ ] pkg/config
- [ ] pkg/types
- [ ] pkg/types/parser_driver

### Util Packages (100+ packages)

- [ ] pkg/util/admin
- [ ] pkg/util/arena
- [ ] pkg/util/benchdaily
- [ ] pkg/util/bitmap
- [ ] pkg/util/checksum
- [ ] pkg/util/chunk
- [ ] pkg/util/codec
- [ ] pkg/util/collate
- [ ] pkg/util/cpu
- [ ] pkg/util/cuprofile
- [ ] pkg/util/cteutil
- [ ] pkg/util/dbterror
- [ ] pkg/util/ddl-checker
- [ ] pkg/util/deadlockhistory
- [ ] pkg/util/disjointset
- [ ] pkg/util/disk
- [ ] pkg/util/encrypt
- [ ] pkg/util/execdetails
- [ ] pkg/util/expensivequery
- [ ] pkg/util/fastrand
- [ ] pkg/util/format
- [ ] pkg/util/generatedexpr
- [ ] pkg/util/hack
- [ ] pkg/util/keydecoder
- [ ] pkg/util/kvcache
- [ ] pkg/util/logutil
- [ ] pkg/util/mathutil
- [ ] pkg/util/memory
- [ ] pkg/util/mock
- [ ] pkg/util/mvmap
- [ ] pkg/util/paging
- [ ] pkg/util/parser
- [ ] pkg/util/plancodec
- [ ] pkg/util/printer
- [ ] pkg/util/profile
- [ ] pkg/util/ranger
- [ ] pkg/util/resourcegrouptag
- [ ] pkg/util/rowDecoder
- [ ] pkg/util/rowcodec
- [ ] pkg/util/selection
- [ ] pkg/util/sem
- [ ] pkg/util/sem/compat
- [ ] pkg/util/set
- [ ] pkg/util/slice
- [ ] pkg/util/sqlexec
- [ ] pkg/util/stmtsummary
- [ ] pkg/util/stmtsummary/v2
- [ ] pkg/util/stmtsummary/v2/tests
- [ ] pkg/util/stringutil
- [ ] pkg/util/sys/linux
- [ ] pkg/util/sys/storage
- [ ] pkg/util/systimemon
- [ ] pkg/util/texttree
- [ ] pkg/util/timeutil
- [ ] pkg/util/topsql
- [ ] pkg/util/topsql/collector
- [ ] pkg/util/topsql/reporter
- [ ] pkg/util/topsql/stmtstats
- [ ] pkg/util/tracing
- [ ] pkg/util/vitess
- [ ] pkg/util/workloadrepo

### InfoSchema Packages

- [ ] pkg/infoschema
- [ ] pkg/infoschema/perfschema
- [ ] pkg/infoschema/test/cachetest
- [ ] pkg/infoschema/test/clustertablestest
- [ ] pkg/infoschema/test/infoschemav2test

### Other Packages

- [ ] pkg/autoid_service
- [ ] pkg/bindinfo/tests
- [ ] pkg/dxf/example
- [ ] pkg/dxf/framework/handle
- [ ] pkg/dxf/framework/integrationtests
- [ ] pkg/dxf/framework/planner
- [ ] pkg/dxf/framework/scheduler
- [ ] pkg/dxf/framework/storage
- [ ] pkg/dxf/framework/taskexecutor
- [ ] pkg/dxf/importinto
- [ ] pkg/dxf/importinto/conflictedkv
- [ ] pkg/errno
- [ ] pkg/lightning/backend/external
- [ ] pkg/lightning/backend/local
- [ ] pkg/lightning/checkpoints
- [ ] pkg/lightning/common
- [ ] pkg/lightning/mydump
- [ ] pkg/objstore/s3store
- [ ] pkg/structure
- [ ] pkg/telemetry
- [ ] pkg/timer/api
- [ ] pkg/timer/runtime
- [ ] pkg/ttl/cache
- [ ] pkg/ttl/session
- [ ] pkg/ttl/sqlbuilder
- [ ] pkg/ttl/ttlworker
- [ ] pkg/ttl/ttlworker/integrationtest
- [ ] pkg/workloadlearning

## Migration Progress

- [X] pkg/bindinfo
- [X] pkg/ddl
- [X] pkg/domain
- [X] pkg/executor
- [X] pkg/expression
- [X] pkg/extension
- [X] pkg/infoschema
- [X] pkg/kv
- [X] pkg/metrics
- [X] pkg/owner
- [X] pkg/session
- [X] pkg/sessiontxn
- [X] pkg/timer
- [X] pkg/util

**Total Progress: 14/302 packages (4.6%)**

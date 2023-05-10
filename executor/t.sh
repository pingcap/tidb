#!/bin/bash
set -e
set -x

go test . --tags=intest -v -run TestIndexMergePanicPartialIndexWorker
go test . --tags=intest -v -run TestIndexMergePanicPartialTableWorker
go test . --tags=intest -v -run TestIndexMergePanicPartialProcessWorkerUnion
go test . --tags=intest -v -run TestIndexMergePanicPartialProcessWorkerIntersection
go test . --tags=intest -v -run TestIndexMergePanicPartitionTableIntersectionWorker
go test . --tags=intest -v -run TestIndexMergePanicTableScanWorker
go test . --tags=intest -v -run TestIndexMergeError

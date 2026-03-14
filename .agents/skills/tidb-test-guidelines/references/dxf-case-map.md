# TiDB DXF Test Case Map (pkg/dxf)

## Overview

- Grouped by package directory.
- Each test file has a one-line description based on its primary test/benchmark name.
- Testdata lists files under `testdata/` mapped to their owning package directory.

## pkg/dxf/example

### Tests
- `pkg/dxf/example/app_test.go` - dxf/example: Tests example application.

## pkg/dxf/framework/handle

### Tests
- `pkg/dxf/framework/handle/handle_test.go` - dxf/framework/handle: Tests handle.
- `pkg/dxf/framework/handle/status_test.go` - dxf/framework/handle: Tests calculate required nodes.
- `pkg/dxf/framework/handle/status_testkit_test.go` - dxf/framework/handle: Tests get schedule status.

## pkg/dxf/framework/integrationtests

### Tests
- `pkg/dxf/framework/integrationtests/bench_test.go` - dxf/framework/integrationtests: Tests scheduler overhead.
- `pkg/dxf/framework/integrationtests/framework_err_handling_test.go` - dxf/framework/integrationtests: Tests on task error.
- `pkg/dxf/framework/integrationtests/framework_ha_test.go` - dxf/framework/integrationtests: Tests HA node random shutdown.
- `pkg/dxf/framework/integrationtests/framework_pause_and_resume_test.go` - dxf/framework/integrationtests: Tests framework pause and resume.
- `pkg/dxf/framework/integrationtests/framework_rollback_test.go` - dxf/framework/integrationtests: Tests framework rollback.
- `pkg/dxf/framework/integrationtests/framework_scope_test.go` - dxf/framework/integrationtests: Tests scope basic.
- `pkg/dxf/framework/integrationtests/framework_test.go` - dxf/framework/integrationtests: Tests random owner change with multiple tasks.
- `pkg/dxf/framework/integrationtests/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/dxf/framework/integrationtests/modify_test.go` - dxf/framework/integrationtests: Tests modify task concurrency and meta.
- `pkg/dxf/framework/integrationtests/resource_control_test.go` - dxf/framework/integrationtests: Tests resource control.

## pkg/dxf/framework/metering

### Tests
- `pkg/dxf/framework/metering/data_test.go` - dxf/framework/metering: Tests data equals.
- `pkg/dxf/framework/metering/metering_test.go` - dxf/framework/metering: Tests new meter empty bucket.
- `pkg/dxf/framework/metering/recorder_test.go` - dxf/framework/metering: Tests recorder.

## pkg/dxf/framework/planner

### Tests
- `pkg/dxf/framework/planner/plan_test.go` - dxf/framework/planner: Tests physical plan.
- `pkg/dxf/framework/planner/planner_test.go` - dxf/framework/planner: Tests planner.

## pkg/dxf/framework/proto

### Tests
- `pkg/dxf/framework/proto/step_test.go` - dxf/framework/proto: Tests step.
- `pkg/dxf/framework/proto/subtask_test.go` - dxf/framework/proto: Tests subtask is done.
- `pkg/dxf/framework/proto/task_test.go` - dxf/framework/proto: Tests task step.
- `pkg/dxf/framework/proto/type_test.go` - dxf/framework/proto: Tests task type.

## pkg/dxf/framework/scheduler

### Tests
- `pkg/dxf/framework/scheduler/autoscaler_test.go` - dxf/framework/scheduler: Tests calc max node count by table size.
- `pkg/dxf/framework/scheduler/balancer_test.go` - dxf/framework/scheduler: Tests balance one task.
- `pkg/dxf/framework/scheduler/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/dxf/framework/scheduler/nodes_test.go` - dxf/framework/scheduler: Tests maintain live nodes.
- `pkg/dxf/framework/scheduler/scheduler_manager_nokit_test.go` - dxf/framework/scheduler: Tests manager schedulers ordered.
- `pkg/dxf/framework/scheduler/scheduler_manager_test.go` - dxf/framework/scheduler: Tests clean up routine.
- `pkg/dxf/framework/scheduler/scheduler_nokit_test.go` - dxf/framework/scheduler: Tests scheduler on next stage.
- `pkg/dxf/framework/scheduler/scheduler_test.go` - dxf/framework/scheduler: Tests task fail in manager.
- `pkg/dxf/framework/scheduler/slots_test.go` - dxf/framework/scheduler: Tests slot manager reserve next-gen.

## pkg/dxf/framework/schstatus

### Tests
- `pkg/dxf/framework/schstatus/status_test.go` - dxf/framework/schstatus: Tests status print.

## pkg/dxf/framework/storage

### Tests
- `pkg/dxf/framework/storage/table_test.go` - dxf/framework/storage: Tests task table.
- `pkg/dxf/framework/storage/task_state_test.go` - dxf/framework/storage: Tests task state.
- `pkg/dxf/framework/storage/task_table_test.go` - dxf/framework/storage: Tests task table.

## pkg/dxf/framework/taskexecutor

### Tests
- `pkg/dxf/framework/taskexecutor/main_test.go` - Configures default goleak settings and registers testdata.
- `pkg/dxf/framework/taskexecutor/manager_test.go` - dxf/framework/taskexecutor: Tests manage task executor.
- `pkg/dxf/framework/taskexecutor/register_test.go` - dxf/framework/taskexecutor: Tests register task type.
- `pkg/dxf/framework/taskexecutor/slot_test.go` - dxf/framework/taskexecutor: Tests slot manager.
- `pkg/dxf/framework/taskexecutor/task_executor_test.go` - dxf/framework/taskexecutor: Tests task executor run.
- `pkg/dxf/framework/taskexecutor/task_executor_testkit_test.go` - dxf/framework/taskexecutor: Tests task executor basic.

## pkg/dxf/framework/taskexecutor/execute

### Tests
- `pkg/dxf/framework/taskexecutor/execute/interface_test.go` - dxf/framework/taskexecutor: Tests subtask summary speed.

## pkg/dxf/importinto

### Tests
- `pkg/dxf/importinto/collect_conflicts_test.go` - dxf/importinto: Tests collect conflicts step executor.
- `pkg/dxf/importinto/conflict_resolution_test.go` - dxf/importinto: Tests conflict resolution step executor.
- `pkg/dxf/importinto/encode_and_sort_operator_test.go` - dxf/importinto: Tests encode and sort operator.
- `pkg/dxf/importinto/job_testkit_test.go` - dxf/importinto: Tests submit task next-gen.
- `pkg/dxf/importinto/metrics_test.go` - dxf/importinto: Tests metric manager.
- `pkg/dxf/importinto/planner_test.go` - dxf/importinto: Tests logical plan.
- `pkg/dxf/importinto/proto_test.go` - dxf/importinto: Tests KV conflict info aggregation.
- `pkg/dxf/importinto/scheduler_test.go` - dxf/importinto: Tests import into.
- `pkg/dxf/importinto/scheduler_testkit_test.go` - dxf/importinto: Tests scheduler external local sort.
- `pkg/dxf/importinto/task_executor_test.go` - dxf/importinto: Tests import task executor.
- `pkg/dxf/importinto/task_executor_testkit_test.go` - dxf/importinto: Tests post process step executor.
- `pkg/dxf/importinto/wrapper_test.go` - dxf/importinto: Tests chunk convert.

## pkg/dxf/importinto/conflictedkv

### Tests
- `pkg/dxf/importinto/conflictedkv/collector_test.go` - dxf/importinto/conflictedkv: Tests collect result merge.
- `pkg/dxf/importinto/conflictedkv/deleter_test.go` - dxf/importinto/conflictedkv: Tests deleter.
- `pkg/dxf/importinto/conflictedkv/handler_test.go` - dxf/importinto/conflictedkv: Tests handler.
- `pkg/dxf/importinto/conflictedkv/row_handle_test.go` - dxf/importinto/conflictedkv: Tests handle filter.

## pkg/dxf/operator

### Tests
- `pkg/dxf/operator/pipeline_test.go` - dxf/operator: Tests pipeline async multi operators without error.

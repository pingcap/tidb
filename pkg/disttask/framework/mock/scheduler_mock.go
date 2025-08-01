// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/tidb/pkg/disttask/framework/scheduler (interfaces: Scheduler,CleanUpRoutine,TaskManager)
//
// Generated by this command:
//
//	mockgen -package mock github.com/pingcap/tidb/pkg/disttask/framework/scheduler Scheduler,CleanUpRoutine,TaskManager
//

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	proto "github.com/pingcap/tidb/pkg/disttask/framework/proto"
	storage "github.com/pingcap/tidb/pkg/disttask/framework/storage"
	execute "github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/sessionctx"
	gomock "go.uber.org/mock/gomock"
)

// MockScheduler is a mock of Scheduler interface.
type MockScheduler struct {
	ctrl     *gomock.Controller
	recorder *MockSchedulerMockRecorder
}

// MockSchedulerMockRecorder is the mock recorder for MockScheduler.
type MockSchedulerMockRecorder struct {
	mock *MockScheduler
}

// NewMockScheduler creates a new mock instance.
func NewMockScheduler(ctrl *gomock.Controller) *MockScheduler {
	mock := &MockScheduler{ctrl: ctrl}
	mock.recorder = &MockSchedulerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockScheduler) EXPECT() *MockSchedulerMockRecorder {
	return m.recorder
}

// ISGOMOCK indicates that this struct is a gomock mock.
func (m *MockScheduler) ISGOMOCK() struct{} {
	return struct{}{}
}

// Close mocks base method.
func (m *MockScheduler) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockSchedulerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockScheduler)(nil).Close))
}

// GetEligibleInstances mocks base method.
func (m *MockScheduler) GetEligibleInstances(arg0 context.Context, arg1 *proto.Task) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetEligibleInstances", arg0, arg1)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetEligibleInstances indicates an expected call of GetEligibleInstances.
func (mr *MockSchedulerMockRecorder) GetEligibleInstances(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetEligibleInstances", reflect.TypeOf((*MockScheduler)(nil).GetEligibleInstances), arg0, arg1)
}

// GetNextStep mocks base method.
func (m *MockScheduler) GetNextStep(arg0 *proto.TaskBase) proto.Step {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNextStep", arg0)
	ret0, _ := ret[0].(proto.Step)
	return ret0
}

// GetNextStep indicates an expected call of GetNextStep.
func (mr *MockSchedulerMockRecorder) GetNextStep(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNextStep", reflect.TypeOf((*MockScheduler)(nil).GetNextStep), arg0)
}

// GetTask mocks base method.
func (m *MockScheduler) GetTask() *proto.Task {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTask")
	ret0, _ := ret[0].(*proto.Task)
	return ret0
}

// GetTask indicates an expected call of GetTask.
func (mr *MockSchedulerMockRecorder) GetTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTask", reflect.TypeOf((*MockScheduler)(nil).GetTask))
}

// Init mocks base method.
func (m *MockScheduler) Init() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Init")
	ret0, _ := ret[0].(error)
	return ret0
}

// Init indicates an expected call of Init.
func (mr *MockSchedulerMockRecorder) Init() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Init", reflect.TypeOf((*MockScheduler)(nil).Init))
}

// IsRetryableErr mocks base method.
func (m *MockScheduler) IsRetryableErr(arg0 error) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsRetryableErr", arg0)
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsRetryableErr indicates an expected call of IsRetryableErr.
func (mr *MockSchedulerMockRecorder) IsRetryableErr(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsRetryableErr", reflect.TypeOf((*MockScheduler)(nil).IsRetryableErr), arg0)
}

// ModifyMeta mocks base method.
func (m *MockScheduler) ModifyMeta(arg0 []byte, arg1 []proto.Modification) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ModifyMeta", arg0, arg1)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ModifyMeta indicates an expected call of ModifyMeta.
func (mr *MockSchedulerMockRecorder) ModifyMeta(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ModifyMeta", reflect.TypeOf((*MockScheduler)(nil).ModifyMeta), arg0, arg1)
}

// OnDone mocks base method.
func (m *MockScheduler) OnDone(arg0 context.Context, arg1 storage.TaskHandle, arg2 *proto.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OnDone", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// OnDone indicates an expected call of OnDone.
func (mr *MockSchedulerMockRecorder) OnDone(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnDone", reflect.TypeOf((*MockScheduler)(nil).OnDone), arg0, arg1, arg2)
}

// OnNextSubtasksBatch mocks base method.
func (m *MockScheduler) OnNextSubtasksBatch(arg0 context.Context, arg1 storage.TaskHandle, arg2 *proto.Task, arg3 []string, arg4 proto.Step) ([][]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OnNextSubtasksBatch", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].([][]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// OnNextSubtasksBatch indicates an expected call of OnNextSubtasksBatch.
func (mr *MockSchedulerMockRecorder) OnNextSubtasksBatch(arg0, arg1, arg2, arg3, arg4 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnNextSubtasksBatch", reflect.TypeOf((*MockScheduler)(nil).OnNextSubtasksBatch), arg0, arg1, arg2, arg3, arg4)
}

// OnTick mocks base method.
func (m *MockScheduler) OnTick(arg0 context.Context, arg1 *proto.Task) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "OnTick", arg0, arg1)
}

// OnTick indicates an expected call of OnTick.
func (mr *MockSchedulerMockRecorder) OnTick(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnTick", reflect.TypeOf((*MockScheduler)(nil).OnTick), arg0, arg1)
}

// ScheduleTask mocks base method.
func (m *MockScheduler) ScheduleTask() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ScheduleTask")
}

// ScheduleTask indicates an expected call of ScheduleTask.
func (mr *MockSchedulerMockRecorder) ScheduleTask() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ScheduleTask", reflect.TypeOf((*MockScheduler)(nil).ScheduleTask))
}

// MockCleanUpRoutine is a mock of CleanUpRoutine interface.
type MockCleanUpRoutine struct {
	ctrl     *gomock.Controller
	recorder *MockCleanUpRoutineMockRecorder
}

// MockCleanUpRoutineMockRecorder is the mock recorder for MockCleanUpRoutine.
type MockCleanUpRoutineMockRecorder struct {
	mock *MockCleanUpRoutine
}

// NewMockCleanUpRoutine creates a new mock instance.
func NewMockCleanUpRoutine(ctrl *gomock.Controller) *MockCleanUpRoutine {
	mock := &MockCleanUpRoutine{ctrl: ctrl}
	mock.recorder = &MockCleanUpRoutineMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCleanUpRoutine) EXPECT() *MockCleanUpRoutineMockRecorder {
	return m.recorder
}

// ISGOMOCK indicates that this struct is a gomock mock.
func (m *MockCleanUpRoutine) ISGOMOCK() struct{} {
	return struct{}{}
}

// CleanUp mocks base method.
func (m *MockCleanUpRoutine) CleanUp(arg0 context.Context, arg1 *proto.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CleanUp", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CleanUp indicates an expected call of CleanUp.
func (mr *MockCleanUpRoutineMockRecorder) CleanUp(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CleanUp", reflect.TypeOf((*MockCleanUpRoutine)(nil).CleanUp), arg0, arg1)
}

// MockTaskManager is a mock of TaskManager interface.
type MockTaskManager struct {
	ctrl     *gomock.Controller
	recorder *MockTaskManagerMockRecorder
}

// MockTaskManagerMockRecorder is the mock recorder for MockTaskManager.
type MockTaskManagerMockRecorder struct {
	mock *MockTaskManager
}

// NewMockTaskManager creates a new mock instance.
func NewMockTaskManager(ctrl *gomock.Controller) *MockTaskManager {
	mock := &MockTaskManager{ctrl: ctrl}
	mock.recorder = &MockTaskManagerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTaskManager) EXPECT() *MockTaskManagerMockRecorder {
	return m.recorder
}

// ISGOMOCK indicates that this struct is a gomock mock.
func (m *MockTaskManager) ISGOMOCK() struct{} {
	return struct{}{}
}

// AwaitingResolveTask mocks base method.
func (m *MockTaskManager) AwaitingResolveTask(arg0 context.Context, arg1 int64, arg2 proto.TaskState, arg3 error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AwaitingResolveTask", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// AwaitingResolveTask indicates an expected call of AwaitingResolveTask.
func (mr *MockTaskManagerMockRecorder) AwaitingResolveTask(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AwaitingResolveTask", reflect.TypeOf((*MockTaskManager)(nil).AwaitingResolveTask), arg0, arg1, arg2, arg3)
}

// CancelTask mocks base method.
func (m *MockTaskManager) CancelTask(arg0 context.Context, arg1 int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// CancelTask indicates an expected call of CancelTask.
func (mr *MockTaskManagerMockRecorder) CancelTask(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CancelTask", reflect.TypeOf((*MockTaskManager)(nil).CancelTask), arg0, arg1)
}

// DeleteDeadNodes mocks base method.
func (m *MockTaskManager) DeleteDeadNodes(arg0 context.Context, arg1 []string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteDeadNodes", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteDeadNodes indicates an expected call of DeleteDeadNodes.
func (mr *MockTaskManagerMockRecorder) DeleteDeadNodes(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteDeadNodes", reflect.TypeOf((*MockTaskManager)(nil).DeleteDeadNodes), arg0, arg1)
}

// FailTask mocks base method.
func (m *MockTaskManager) FailTask(arg0 context.Context, arg1 int64, arg2 proto.TaskState, arg3 error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FailTask", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// FailTask indicates an expected call of FailTask.
func (mr *MockTaskManagerMockRecorder) FailTask(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FailTask", reflect.TypeOf((*MockTaskManager)(nil).FailTask), arg0, arg1, arg2, arg3)
}

// GCSubtasks mocks base method.
func (m *MockTaskManager) GCSubtasks(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GCSubtasks", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// GCSubtasks indicates an expected call of GCSubtasks.
func (mr *MockTaskManagerMockRecorder) GCSubtasks(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GCSubtasks", reflect.TypeOf((*MockTaskManager)(nil).GCSubtasks), arg0)
}

// GetActiveSubtasks mocks base method.
func (m *MockTaskManager) GetActiveSubtasks(arg0 context.Context, arg1 int64) ([]*proto.SubtaskBase, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetActiveSubtasks", arg0, arg1)
	ret0, _ := ret[0].([]*proto.SubtaskBase)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetActiveSubtasks indicates an expected call of GetActiveSubtasks.
func (mr *MockTaskManagerMockRecorder) GetActiveSubtasks(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetActiveSubtasks", reflect.TypeOf((*MockTaskManager)(nil).GetActiveSubtasks), arg0, arg1)
}

// GetAllNodes mocks base method.
func (m *MockTaskManager) GetAllNodes(arg0 context.Context) ([]proto.ManagedNode, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllNodes", arg0)
	ret0, _ := ret[0].([]proto.ManagedNode)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllNodes indicates an expected call of GetAllNodes.
func (mr *MockTaskManagerMockRecorder) GetAllNodes(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllNodes", reflect.TypeOf((*MockTaskManager)(nil).GetAllNodes), arg0)
}

// GetAllSubtaskSummaryByStep mocks base method.
func (m *MockTaskManager) GetAllSubtaskSummaryByStep(arg0 context.Context, arg1 int64, arg2 proto.Step) ([]*execute.SubtaskSummary, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllSubtaskSummaryByStep", arg0, arg1, arg2)
	ret0, _ := ret[0].([]*execute.SubtaskSummary)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllSubtaskSummaryByStep indicates an expected call of GetAllSubtaskSummaryByStep.
func (mr *MockTaskManagerMockRecorder) GetAllSubtaskSummaryByStep(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllSubtaskSummaryByStep", reflect.TypeOf((*MockTaskManager)(nil).GetAllSubtaskSummaryByStep), arg0, arg1, arg2)
}

// GetAllSubtasks mocks base method.
func (m *MockTaskManager) GetAllSubtasks(arg0 context.Context) ([]*proto.SubtaskBase, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllSubtasks", arg0)
	ret0, _ := ret[0].([]*proto.SubtaskBase)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllSubtasks indicates an expected call of GetAllSubtasks.
func (mr *MockTaskManagerMockRecorder) GetAllSubtasks(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllSubtasks", reflect.TypeOf((*MockTaskManager)(nil).GetAllSubtasks), arg0)
}

// GetAllSubtasksByStepAndState mocks base method.
func (m *MockTaskManager) GetAllSubtasksByStepAndState(arg0 context.Context, arg1 int64, arg2 proto.Step, arg3 proto.SubtaskState) ([]*proto.Subtask, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllSubtasksByStepAndState", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]*proto.Subtask)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllSubtasksByStepAndState indicates an expected call of GetAllSubtasksByStepAndState.
func (mr *MockTaskManagerMockRecorder) GetAllSubtasksByStepAndState(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllSubtasksByStepAndState", reflect.TypeOf((*MockTaskManager)(nil).GetAllSubtasksByStepAndState), arg0, arg1, arg2, arg3)
}

// GetAllTasks mocks base method.
func (m *MockTaskManager) GetAllTasks(arg0 context.Context) ([]*proto.TaskBase, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllTasks", arg0)
	ret0, _ := ret[0].([]*proto.TaskBase)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllTasks indicates an expected call of GetAllTasks.
func (mr *MockTaskManagerMockRecorder) GetAllTasks(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllTasks", reflect.TypeOf((*MockTaskManager)(nil).GetAllTasks), arg0)
}

// GetSubtaskCntGroupByStates mocks base method.
func (m *MockTaskManager) GetSubtaskCntGroupByStates(arg0 context.Context, arg1 int64, arg2 proto.Step) (map[proto.SubtaskState]int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSubtaskCntGroupByStates", arg0, arg1, arg2)
	ret0, _ := ret[0].(map[proto.SubtaskState]int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSubtaskCntGroupByStates indicates an expected call of GetSubtaskCntGroupByStates.
func (mr *MockTaskManagerMockRecorder) GetSubtaskCntGroupByStates(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSubtaskCntGroupByStates", reflect.TypeOf((*MockTaskManager)(nil).GetSubtaskCntGroupByStates), arg0, arg1, arg2)
}

// GetSubtaskErrors mocks base method.
func (m *MockTaskManager) GetSubtaskErrors(arg0 context.Context, arg1 int64) ([]error, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSubtaskErrors", arg0, arg1)
	ret0, _ := ret[0].([]error)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSubtaskErrors indicates an expected call of GetSubtaskErrors.
func (mr *MockTaskManagerMockRecorder) GetSubtaskErrors(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSubtaskErrors", reflect.TypeOf((*MockTaskManager)(nil).GetSubtaskErrors), arg0, arg1)
}

// GetTaskBaseByID mocks base method.
func (m *MockTaskManager) GetTaskBaseByID(arg0 context.Context, arg1 int64) (*proto.TaskBase, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskBaseByID", arg0, arg1)
	ret0, _ := ret[0].(*proto.TaskBase)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTaskBaseByID indicates an expected call of GetTaskBaseByID.
func (mr *MockTaskManagerMockRecorder) GetTaskBaseByID(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskBaseByID", reflect.TypeOf((*MockTaskManager)(nil).GetTaskBaseByID), arg0, arg1)
}

// GetTaskByID mocks base method.
func (m *MockTaskManager) GetTaskByID(arg0 context.Context, arg1 int64) (*proto.Task, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTaskByID", arg0, arg1)
	ret0, _ := ret[0].(*proto.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTaskByID indicates an expected call of GetTaskByID.
func (mr *MockTaskManagerMockRecorder) GetTaskByID(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTaskByID", reflect.TypeOf((*MockTaskManager)(nil).GetTaskByID), arg0, arg1)
}

// GetTasksInStates mocks base method.
func (m *MockTaskManager) GetTasksInStates(arg0 context.Context, arg1 ...any) ([]*proto.Task, error) {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "GetTasksInStates", varargs...)
	ret0, _ := ret[0].([]*proto.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTasksInStates indicates an expected call of GetTasksInStates.
func (mr *MockTaskManagerMockRecorder) GetTasksInStates(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTasksInStates", reflect.TypeOf((*MockTaskManager)(nil).GetTasksInStates), varargs...)
}

// GetTopUnfinishedTasks mocks base method.
func (m *MockTaskManager) GetTopUnfinishedTasks(arg0 context.Context) ([]*proto.TaskBase, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTopUnfinishedTasks", arg0)
	ret0, _ := ret[0].([]*proto.TaskBase)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetTopUnfinishedTasks indicates an expected call of GetTopUnfinishedTasks.
func (mr *MockTaskManagerMockRecorder) GetTopUnfinishedTasks(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTopUnfinishedTasks", reflect.TypeOf((*MockTaskManager)(nil).GetTopUnfinishedTasks), arg0)
}

// GetUsedSlotsOnNodes mocks base method.
func (m *MockTaskManager) GetUsedSlotsOnNodes(arg0 context.Context) (map[string]int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetUsedSlotsOnNodes", arg0)
	ret0, _ := ret[0].(map[string]int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetUsedSlotsOnNodes indicates an expected call of GetUsedSlotsOnNodes.
func (mr *MockTaskManagerMockRecorder) GetUsedSlotsOnNodes(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetUsedSlotsOnNodes", reflect.TypeOf((*MockTaskManager)(nil).GetUsedSlotsOnNodes), arg0)
}

// ModifiedTask mocks base method.
func (m *MockTaskManager) ModifiedTask(arg0 context.Context, arg1 *proto.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ModifiedTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ModifiedTask indicates an expected call of ModifiedTask.
func (mr *MockTaskManagerMockRecorder) ModifiedTask(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ModifiedTask", reflect.TypeOf((*MockTaskManager)(nil).ModifiedTask), arg0, arg1)
}

// PauseTask mocks base method.
func (m *MockTaskManager) PauseTask(arg0 context.Context, arg1 string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PauseTask", arg0, arg1)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PauseTask indicates an expected call of PauseTask.
func (mr *MockTaskManagerMockRecorder) PauseTask(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PauseTask", reflect.TypeOf((*MockTaskManager)(nil).PauseTask), arg0, arg1)
}

// PausedTask mocks base method.
func (m *MockTaskManager) PausedTask(arg0 context.Context, arg1 int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PausedTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// PausedTask indicates an expected call of PausedTask.
func (mr *MockTaskManagerMockRecorder) PausedTask(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PausedTask", reflect.TypeOf((*MockTaskManager)(nil).PausedTask), arg0, arg1)
}

// ResumeSubtasks mocks base method.
func (m *MockTaskManager) ResumeSubtasks(arg0 context.Context, arg1 int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResumeSubtasks", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ResumeSubtasks indicates an expected call of ResumeSubtasks.
func (mr *MockTaskManagerMockRecorder) ResumeSubtasks(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResumeSubtasks", reflect.TypeOf((*MockTaskManager)(nil).ResumeSubtasks), arg0, arg1)
}

// ResumedTask mocks base method.
func (m *MockTaskManager) ResumedTask(arg0 context.Context, arg1 int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ResumedTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// ResumedTask indicates an expected call of ResumedTask.
func (mr *MockTaskManagerMockRecorder) ResumedTask(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResumedTask", reflect.TypeOf((*MockTaskManager)(nil).ResumedTask), arg0, arg1)
}

// RevertTask mocks base method.
func (m *MockTaskManager) RevertTask(arg0 context.Context, arg1 int64, arg2 proto.TaskState, arg3 error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RevertTask", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(error)
	return ret0
}

// RevertTask indicates an expected call of RevertTask.
func (mr *MockTaskManagerMockRecorder) RevertTask(arg0, arg1, arg2, arg3 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RevertTask", reflect.TypeOf((*MockTaskManager)(nil).RevertTask), arg0, arg1, arg2, arg3)
}

// RevertedTask mocks base method.
func (m *MockTaskManager) RevertedTask(arg0 context.Context, arg1 int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RevertedTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// RevertedTask indicates an expected call of RevertedTask.
func (mr *MockTaskManagerMockRecorder) RevertedTask(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RevertedTask", reflect.TypeOf((*MockTaskManager)(nil).RevertedTask), arg0, arg1)
}

// SucceedTask mocks base method.
func (m *MockTaskManager) SucceedTask(arg0 context.Context, arg1 int64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SucceedTask", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// SucceedTask indicates an expected call of SucceedTask.
func (mr *MockTaskManagerMockRecorder) SucceedTask(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SucceedTask", reflect.TypeOf((*MockTaskManager)(nil).SucceedTask), arg0, arg1)
}

// SwitchTaskStep mocks base method.
func (m *MockTaskManager) SwitchTaskStep(arg0 context.Context, arg1 *proto.Task, arg2 proto.TaskState, arg3 proto.Step, arg4 []*proto.Subtask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SwitchTaskStep", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// SwitchTaskStep indicates an expected call of SwitchTaskStep.
func (mr *MockTaskManagerMockRecorder) SwitchTaskStep(arg0, arg1, arg2, arg3, arg4 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SwitchTaskStep", reflect.TypeOf((*MockTaskManager)(nil).SwitchTaskStep), arg0, arg1, arg2, arg3, arg4)
}

// SwitchTaskStepInBatch mocks base method.
func (m *MockTaskManager) SwitchTaskStepInBatch(arg0 context.Context, arg1 *proto.Task, arg2 proto.TaskState, arg3 proto.Step, arg4 []*proto.Subtask) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SwitchTaskStepInBatch", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// SwitchTaskStepInBatch indicates an expected call of SwitchTaskStepInBatch.
func (mr *MockTaskManagerMockRecorder) SwitchTaskStepInBatch(arg0, arg1, arg2, arg3, arg4 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SwitchTaskStepInBatch", reflect.TypeOf((*MockTaskManager)(nil).SwitchTaskStepInBatch), arg0, arg1, arg2, arg3, arg4)
}

// TransferTasks2History mocks base method.
func (m *MockTaskManager) TransferTasks2History(arg0 context.Context, arg1 []*proto.Task) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TransferTasks2History", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// TransferTasks2History indicates an expected call of TransferTasks2History.
func (mr *MockTaskManagerMockRecorder) TransferTasks2History(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TransferTasks2History", reflect.TypeOf((*MockTaskManager)(nil).TransferTasks2History), arg0, arg1)
}

// UpdateSubtasksExecIDs mocks base method.
func (m *MockTaskManager) UpdateSubtasksExecIDs(arg0 context.Context, arg1 []*proto.SubtaskBase) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateSubtasksExecIDs", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateSubtasksExecIDs indicates an expected call of UpdateSubtasksExecIDs.
func (mr *MockTaskManagerMockRecorder) UpdateSubtasksExecIDs(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateSubtasksExecIDs", reflect.TypeOf((*MockTaskManager)(nil).UpdateSubtasksExecIDs), arg0, arg1)
}

// WithNewSession mocks base method.
func (m *MockTaskManager) WithNewSession(arg0 func(sessionctx.Context) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithNewSession", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithNewSession indicates an expected call of WithNewSession.
func (mr *MockTaskManagerMockRecorder) WithNewSession(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithNewSession", reflect.TypeOf((*MockTaskManager)(nil).WithNewSession), arg0)
}

// WithNewTxn mocks base method.
func (m *MockTaskManager) WithNewTxn(arg0 context.Context, arg1 func(sessionctx.Context) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WithNewTxn", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// WithNewTxn indicates an expected call of WithNewTxn.
func (mr *MockTaskManagerMockRecorder) WithNewTxn(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WithNewTxn", reflect.TypeOf((*MockTaskManager)(nil).WithNewTxn), arg0, arg1)
}

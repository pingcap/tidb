package utils

import "sync"

type TaskManager struct {
	mu struct {
		sync.Mutex
		pendingTasks map[string]*Task
	}
	execMu struct {
		sync.Mutex
		runningTasks map[string]*Task
	}
	ID string
}

type Task struct {
	ID     string
	Holder string
	State  int32
}

// RefreshTasks refreshes the tasks from the upper layer and filters by ID
func (t *TaskManager) GetAllMVTasks() error {
	sql := "SELECT id, holder, state FROM mv"

}

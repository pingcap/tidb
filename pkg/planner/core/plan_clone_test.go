package core

import (
	"bytes"
	"os"
	"testing"
)

func TestPlanClone(t *testing.T) {
	updatedCode, err := genPlanCloneForPlanCacheCode()
	if err != nil {
		t.Errorf("Generate CloneForPlanCache code error: %v", err)
	}
	currentCode, err := os.ReadFile("./plan_clone_generated.go")
	if err != nil {
		t.Errorf("Read current plan_clone_generated.go code error: %v", err)
	}
	if !bytes.Equal(updatedCode, currentCode) {
		t.Errorf("plan_clone_generated.go should be updated, please run TestUpdatePlanCloneCode to update it.")
	}
}

func TestUpdatePlanCloneCode(t *testing.T) {
	updatedCode, err := genPlanCloneForPlanCacheCode()
	if err != nil {
		t.Errorf("Generate CloneForPlanCache code error: %v", err)
	}
	if err := os.WriteFile("./plan_clone_generated.go", updatedCode, 0644); err != nil {
		t.Errorf("Write plan_clone_generated.go error: %v", err)
	}
}

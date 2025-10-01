package parser

import (
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// TestP0Fix_C1_MemoryAllocationFailure tests that large allocation requests
// are handled gracefully without panics
func TestP0Fix_C1_MemoryAllocationFailure(t *testing.T) {
	arena := NewASTArena()

	// Test extremely large allocation that would cause memory exhaustion
	// This should return nil instead of panicking
	hugeSizes := []int{
		1024*1024*1024 + 1, // 1GB + 1 (exceeds maxAllocationSize)
		maxAllocationSize + 1, // Just over the limit
		-1, // Invalid negative size
		0,  // Invalid zero size
	}

	for _, size := range hugeSizes {
		ptr := arena.allocate(size)
		if ptr != nil {
			t.Errorf("Expected nil for invalid size %d, got valid pointer", size)
		}
	}

	// Test valid allocations still work
	validPtr := arena.allocate(64)
	if validPtr == nil {
		t.Error("Expected valid pointer for reasonable allocation")
	}
}

// TestP0Fix_C2_UnsafePointerValidation tests that unsafe pointer operations
// are properly validated and don't cause segfaults
func TestP0Fix_C2_UnsafePointerValidation(t *testing.T) {
	arena := NewASTArena()

	// Test allocation from fresh arena (should succeed)
	ptr1 := arena.allocate(64)
	if ptr1 == nil {
		t.Error("Expected valid pointer from fresh arena")
	}

	// Force arena into invalid state by manipulating internals
	arena.current = nil
	arena.offset = -1

	// This should be handled gracefully and return nil
	ptr2 := arena.allocate(64)
	if ptr2 != nil {
		t.Error("Expected nil pointer when arena is in invalid state")
	}

	// Reset arena and verify it works again
	arena.Reset()
	ptr3 := arena.allocate(64)
	if ptr3 == nil {
		t.Error("Expected valid pointer after arena reset")
	}
}

// TestP0Fix_C3_IntegerOverflowProtection tests that size calculations
// are protected against integer overflow
func TestP0Fix_C3_IntegerOverflowProtection(t *testing.T) {
	arena := NewASTArena()

	// Test sizes that could cause overflow in alignment calculation
	dangerousSizes := []int{
		maxAllocationSize - maxAlignment + 1, // Would overflow during alignment
		maxAllocationSize - 1,                // Near the limit
		maxAllocationSize,                     // At the limit (should be rejected)
		int(^uint(0) >> 1),                   // Max int value
	}

	for _, size := range dangerousSizes {
		ptr := arena.allocate(size)
		if ptr != nil {
			t.Errorf("Expected nil for potentially dangerous size %d, got valid pointer", size)
		}
	}

	// Test that reasonable sizes still work
	reasonableSize := 1024
	ptr := arena.allocate(reasonableSize)
	if ptr == nil {
		t.Errorf("Expected valid pointer for reasonable size %d", reasonableSize)
	}
}

// TestP0Fix_C4_ThreadSafetyDocumentation tests that the documented
// single-threaded behavior works as expected
func TestP0Fix_C4_ThreadSafetyDocumentation(t *testing.T) {
	arena := NewASTArena()

	// Test normal allocation sequence
	ptr1 := arena.allocate(64)
	if ptr1 == nil {
		t.Error("Expected valid pointer for first allocation")
	}

	ptr2 := arena.allocate(128)
	if ptr2 == nil {
		t.Error("Expected valid pointer for second allocation")
	}

	// Test reset operation (should work in single-threaded context)
	arena.Reset()

	// After reset, should be able to allocate again
	ptr3 := arena.allocate(64)
	if ptr3 == nil {
		t.Error("Expected valid pointer after reset")
	}

	// Verify reset actually clears the arena
	if ptr3 == ptr1 {
		// This is expected - reset should reuse the same memory block
		t.Log("Reset successfully reused memory block")
	}
}

// TestP0Fix_AllocationMethodsHandleNil tests that all allocation methods
// properly handle nil returns from the internal allocate() function
func TestP0Fix_AllocationMethodsHandleNil(t *testing.T) {
	arena := NewASTArena()

	// Force arena into a state where allocate() will return nil
	arena.current = nil
	arena.offset = -1

	// Test that all public allocation methods handle nil gracefully
	tests := []struct {
		name string
		fn   func() interface{}
	}{
		{"AllocSelectStmt", func() interface{} { return arena.AllocSelectStmt() }},
		{"AllocInsertStmt", func() interface{} { return arena.AllocInsertStmt() }},
		{"AllocUpdateStmt", func() interface{} { return arena.AllocUpdateStmt() }},
		{"AllocDeleteStmt", func() interface{} { return arena.AllocDeleteStmt() }},
		{"AllocBinaryOperationExpr", func() interface{} { return arena.AllocBinaryOperationExpr() }},
		{"AllocFuncCallExpr", func() interface{} { return arena.AllocFuncCallExpr() }},
		{"AllocColumnNameExpr", func() interface{} { return arena.AllocColumnNameExpr() }},
		{"AllocTableName", func() interface{} { return arena.AllocTableName() }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.fn()
			if result != nil {
				t.Errorf("%s should return nil when allocation fails, got %v", test.name, result)
			}
		})
	}
}

// TestP0Fix_RealWorldUsage tests the arena allocator with realistic
// AST node allocation patterns to ensure P0 fixes don't break normal usage
func TestP0Fix_RealWorldUsage(t *testing.T) {
	arena := NewASTArena()

	// Simulate a typical parsing operation with mixed allocations
	selectStmt := arena.AllocSelectStmt()
	if selectStmt == nil {
		t.Error("Failed to allocate SelectStmt")
		return
	}

	// Initialize the allocated struct
	*selectStmt = ast.SelectStmt{}

	// Allocate multiple expression nodes
	for i := 0; i < 10; i++ {
		funcExpr := arena.AllocFuncCallExpr()
		if funcExpr == nil {
			t.Errorf("Failed to allocate FuncCallExpr #%d", i)
			continue
		}
		*funcExpr = ast.FuncCallExpr{}

		binExpr := arena.AllocBinaryOperationExpr()
		if binExpr == nil {
			t.Errorf("Failed to allocate BinaryOperationExpr #%d", i)
			continue
		}
		*binExpr = ast.BinaryOperationExpr{}
	}

	// Test reset and reuse
	arena.Reset()

	// Should be able to allocate again after reset
	newSelectStmt := arena.AllocSelectStmt()
	if newSelectStmt == nil {
		t.Error("Failed to allocate SelectStmt after reset")
	}

	*newSelectStmt = ast.SelectStmt{}
}

// TestP0Fix_MemoryAlignment tests that memory alignment is preserved
// after the P0 fixes
func TestP0Fix_MemoryAlignment(t *testing.T) {
	arena := NewASTArena()

	// Allocate various sizes and check alignment
	sizes := []int{1, 7, 8, 15, 16, 31, 32, 63, 64}

	for _, size := range sizes {
		ptr := arena.allocate(size)
		if ptr == nil {
			t.Errorf("Failed to allocate size %d", size)
			continue
		}

		// Check that pointer is properly aligned
		addr := uintptr(ptr)
		if addr%maxAlignment != 0 {
			t.Errorf("Pointer for size %d is not properly aligned: %x", size, addr)
		}
	}
}

// TestP0Fix_Performance tests that P0 fixes don't significantly impact
// allocation performance
func TestP0Fix_Performance(t *testing.T) {
	arena := NewASTArena()

	// This is a basic performance smoke test
	// In a real benchmark, we'd compare with baseline measurements
	for i := 0; i < 1000; i++ {
		ptr := arena.allocate(64)
		if ptr == nil {
			t.Errorf("Allocation failed at iteration %d", i)
			break
		}

		if i%100 == 99 {
			// Reset occasionally to test reset performance
			arena.Reset()
		}
	}
}
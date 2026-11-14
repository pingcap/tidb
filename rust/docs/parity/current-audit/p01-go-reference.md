# P01 Go reference probes

Captured on 2026-11-14 from Go in this worktree. The temporary package was
`zzprobe/p01`, outside `rust/`; it is not part of the change to integrate.
The tests use a testkit session as the plan context and invoke Go's actual
operator constructors, clone methods, and memory accounting. The receiver
probe performs the same pointer conversion as `executorBuilder.buildShuffleReceiverStub`;
the Rust counterpart additionally exercises actual receiver executors.

Command from the repository root:

    go test --tags=intest -p 6 -v ./zzprobe/p01

Relevant output (testkit startup/shutdown logs omitted):

    === RUN   TestPhysicalCTEClassSharing
    shared=true seed_cloned=true operator=data:CTE_9 limit=2,5 in_apply=true storage_shared=true
    --- PASS: TestPhysicalCTEClassSharing
    === RUN   TestPhysicalShuffleReceiverPointer
    first=33 second=22 independent=true
    --- PASS: TestPhysicalShuffleReceiverPointer
    PASS

Assertions also pin the empty CTEClass memory usage to 160 bytes on 64-bit,
producer plan accounting, and the receiver pointer/interface contribution to
24 bytes. A table scan is used because Go PhysicalTableDual does not support
Clone; the first probe attempt established that restriction.

Rust regressions are `test_physical_cte_class_sharing` in tidb-planner's
physical_tests and `test_physical_shuffle_receiver_pointer` in tidb-executor's
shuffle tests. The former checks Send + Sync, class mutations across threads,
physical clone identity, and storage/recursive paths. The latter calls the
actual executor builder and observes mutations through the retained receiver.

Go source SHA-256:

- `pkg/planner/core/operator/physicalop/physical_cte.go`: `6223410e99a11ab15400dc218786c4cb4d300ea58370a461704c5b86217401f9`
- `pkg/planner/core/operator/physicalop/physical_shuffle.go`: `6721de010c25ba7aee259abcbcc5914a64ab167ccdbf7fb096de199ce85c183d`
- `pkg/planner/core/operator/logicalop/logical_cte.go`: `3d70b7660b4146a04a536224b2862d0427965e09590cad7a2b789862beef1053`
- `pkg/executor/builder.go`: `f866d4fc8105352ac508d2bacece2d2da2fb490a471650153e1318c84d8da128`

Reproducer (write to `zzprobe/p01/p01_test.go` at the repository root):

```go
package p01

import (
	"testing"
	"unsafe"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestPhysicalCTEClassSharing(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := tk.Session().GetPlanCtx()
	require.Equal(t, int64(160), (&logicalop.CTEClass{}).MemoryUsage())
	class := &logicalop.CTEClass{IDForStorage: 7, IsDistinct: true}
	seed := physicalop.PhysicalTableScan{}.Init(ctx, 0)
	seed.SetStats(&property.StatsInfo{RowCount: 1})
	seed.SetSchema(expression.NewSchema())
	class.SeedPartPhysicalPlan = seed
	p := physicalop.PhysicalCTE{CTE: class, SeedPlan: seed, CteName: ast.NewCIStr("c")}.Init(ctx, &property.StatsInfo{RowCount: 1})
	p.SetSchema(expression.NewSchema())
	cloned, err := p.Clone(ctx)
	require.NoError(t, err)
	c := cloned.(*physicalop.PhysicalCTE)
	require.Same(t, class, c.CTE)
	require.NotSame(t, p.SeedPlan, c.SeedPlan)
	require.Equal(t, p.PhysicalSchemaProducer.MemoryUsage()+p.CteAsName.MemoryUsage()+seed.MemoryUsage()+class.MemoryUsage(), p.MemoryUsage())
	class.IDForStorage = 9
	class.HasLimit, class.LimitBeg, class.LimitEnd = true, 2, 5
	class.IsInApply = true
	require.Equal(t, "data:CTE_9", c.OperatorInfo(false))
	require.Equal(t, int64(160)+seed.MemoryUsage(), class.MemoryUsage())
	require.True(t, c.CTE.IsInApply)
	require.Equal(t, uint64(5), c.CTE.LimitEnd)
	logical := logicalop.LogicalCTE{Cte: class, OnlyUsedAsStorage: true}.Init(ctx, 0)
	logical.SetSchema(expression.NewSchema())
	plans, _, err := physicalop.ExhaustPhysicalPlans4LogicalCTE(logical, &property.PhysicalProperty{})
	require.NoError(t, err)
	storage := plans[0].(*physicalop.PhysicalCTEStorage)
	require.Same(t, class, storage.CTE)
	require.Equal(t, storage.PhysicalSchemaProducer.MemoryUsage()+storage.CteAsName.MemoryUsage()+class.MemoryUsage(), storage.MemoryUsage())
	clonedStorage, err := storage.Clone(ctx)
	require.NoError(t, err)
	require.Same(t, class, clonedStorage.(*physicalop.PhysicalCTEStorage).CTE)
	p.RecurPlan = seed
	class.RecursivePartPhysicalPlan = seed
	recursiveClone, err := p.Clone(ctx)
	require.NoError(t, err)
	require.Same(t, class, recursiveClone.(*physicalop.PhysicalCTE).CTE)
	require.NotSame(t, p.RecurPlan, recursiveClone.(*physicalop.PhysicalCTE).RecurPlan)
	t.Logf("shared=true seed_cloned=true operator=%s limit=%d,%d in_apply=%t storage_shared=true", c.OperatorInfo(false), c.CTE.LimitBeg, c.CTE.LimitEnd, c.CTE.IsInApply)
}

func TestPhysicalShuffleReceiverPointer(t *testing.T) {
	// Same pointer round trip as executorBuilder.buildShuffleReceiverStub.
	first, second := new(int), new(int)
	*first, *second = 11, 22
	stub := physicalop.PhysicalShuffleReceiverStub{}
	require.Nil(t, stub.Receiver)
	require.Equal(t, stub.PhysicalSchemaProducer.MemoryUsage()+int64(24), stub.MemoryUsage())
	stub.Receiver = unsafe.Pointer(first)
	builtFirst := (*int)(stub.Receiver)
	stub.Receiver = unsafe.Pointer(second)
	builtSecond := (*int)(stub.Receiver)
	*builtFirst = 33
	require.Equal(t, 33, *first)
	require.Equal(t, 22, *builtSecond)
	require.NotSame(t, builtFirst, builtSecond)
	t.Logf("first=%d second=%d independent=true", *builtFirst, *builtSecond)
}
```

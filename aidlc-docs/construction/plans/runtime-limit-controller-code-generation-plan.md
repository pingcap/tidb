# Code Generation Plan

1. Add a concurrency-safe controller to `pkg/executor/internal/exec` with no
   dependency on session or executor implementations.
2. Add the opt-in system variable and session field.
3. Mark ordered IndexLookUpJoin executors eligible during executor building.
4. Attach one controller from LIMIT to the eligible join and outer IndexLookup.
5. Add interruptible reservations to outer and lookup producers and consumption
   feedback to the corresponding consumers.
6. Add focused tests and observable runtime snapshot data.
7. Build, deploy, run at least ten E2E rounds, then tune only from collected
   evidence.

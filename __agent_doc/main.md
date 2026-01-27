# Your epic

You need to implement a feature -- segmented restore.

Now, `br restore point` requires the user to finish restore to a point-in-time within oneshot.

We want `br restore point` can be "segmented", like:

```
br restore point --pd 127.0.0.1:2379 -s local:///Volumes/eXternal/Cache/tmp/20260122_003925/incr --restored-ts 463736294698909699 --full-backup-storage local:///Volumes/eXternal/Cache/tmp/20260122_003925/full --last=false
/br restore point --pd 127.0.0.1:2379 -s local:///Volumes/eXternal/Cache/tmp/20260122_003925/incr --restored-ts 463736295708426243 --start-ts 463736294698909699 --last=false
/br restore point --pd 127.0.0.1:2379 -s local:///Volumes/eXternal/Cache/tmp/20260122_003925/incr --restored-ts ... --start-ts 463736295708426243 --last=true
```

But for now, it is impossible. There are something we have known: tiflash replicas may be added back too early, indices may not be properly recreated... But more are unknown unknown.

This is an epic to make it work. Be patient. You cannot fix all within one edition. You have git access, commit or rollback your work when need. `AGENT.md` may told you to enable failpoint before running test, don't follow that as we are running "integration" tests, also you may ignore all `bazel` related requirements, repeat, don't enable failpoints, don't try to build with bazel or `make bazel_prepare`.

You may find that something is fully out of your scope. Say, the test environment was broken. In that scenario, **don't** try to hack, just stop and ask for help. Again, don't try to hack.

If you have made some progress, record them in `__agent_doc/`, for those who come later.

Suggestions:
- You may not be able to solve the problem in one "sprint". Always record your plan to `__agent_doc` before start. For your next run.

## To budld BR

```
make build_br
```

## To run our test cases

```
bash /Volumes/eXternal/Developer/seg-pitr-workload/scripts/run.sh
```

This command can only be run without sandbox. Request access to it before you start to do anything.

This command runs for minutes, again, this command runs for minutes. Don't set a too short time out.

Once this test script passes, our epic reaches its happy ending.

Reading its content and record its behavior can be a good start point.

## After Happy Ending (1)

All tests are passed. It is time to tidy up our codebase. Inspect recent commits you did, and refactor your modifications with DRY principle.

## Integrated Test (2026-01-23)
- The segmented PiTR external test is now integrated as `br/tests/br_pitr_segmented_restore/run.sh`.
- The workload source lives under `br/tests/seg_pitr_workload`; the test builds it via `go build`.
- Run via `TEST_NAME=br_pitr_segmented_restore br/tests/run.sh` or include it in `br/tests/run_group_br_tests.sh` (G07).

# TODO

# DONE

- [x] Refactor the three added methods with PMD cognitive complexity >= 15 (24→14, 23→3, 19→12). Extend the root test runner and validate all compaction and changed branch test classes: 973 passed, 10 skipped, including three shutdown tests and 1,000 generated queue cases. Production/test Checkstyle passed. See [.reviews/tombstone-complexity-refactor-20260910.md](.reviews/tombstone-complexity-refactor-20260910.md).

- [x] Add production shutdown integration tests for active and queued reactive compactions: node-drain interruption/discard with restart checks, graceful-helper waiting, and BUSY termination. Three integration tests and 17 queue-manager tests passed twice with JDK 17 and JaCoCo on 2026-09-07; test Checkstyle passed. See [.plans/tombstone-compaction-shutdown-integration.md](.plans/tombstone-compaction-shutdown-integration.md).
- [x] Add a bounded 60-second per-table/key reactive compaction cooldown; deterministic expiry/failure tests, seeded property checks, production integration, read tests, and build/Checkstyle passed on 2026-09-06.
- [x] Complete tombstone-triggered partition compaction review fixes and expanded regression coverage; eight focused test classes and build/Checkstyle passed with JDK 21 on 2026-09-05.
- [x] Implement tombstone-triggered partition compaction.

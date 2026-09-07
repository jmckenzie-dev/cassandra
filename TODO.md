# TODO

# DONE

- [x] Add production shutdown integration tests for active and queued reactive compactions: node-drain interruption/discard with restart checks, graceful-helper waiting, and BUSY termination. Three integration tests and 17 queue-manager tests passed twice with JDK 17 and JaCoCo on 2026-09-07; test Checkstyle passed. See [.plans/tombstone-compaction-shutdown-integration.md](.plans/tombstone-compaction-shutdown-integration.md).
- [x] Add a bounded 60-second per-table/key reactive compaction cooldown; deterministic expiry/failure tests, seeded property checks, production integration, read tests, and build/Checkstyle passed on 2026-09-06.
- [x] Complete tombstone-triggered partition compaction review fixes and expanded regression coverage; eight focused test classes and build/Checkstyle passed with JDK 21 on 2026-09-05.
- [x] Implement tombstone-triggered partition compaction.

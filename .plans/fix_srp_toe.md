    Fix short read protection across tombstone paging checkpoints

    Replica tombstone paging could make short read protection treat an incomplete
    source response as exhausted. This could under-fill limited reads, expose stale
    rows during reconciliation, or reuse an old pager checkpoint.

    Carry explicit checkpoint metadata through message parameters. Supported
    single-partition resolvers continue the affected source, while unsupported
    paths fail safely. Transfer pager ownership before full-data read repair so a
    late local read cannot restore stale state.

    Add in-JVM and unit coverage for remote paging, transient replicas, command
    ownership, and read repair.

    Key Items:
    - Propagate tombstone checkpoints through coordinator read responses.
    - Continue incomplete replica reads through short read protection.
    - Protect pager ownership during reconciliation and read repair.
    - Add distributed and unit regression coverage.

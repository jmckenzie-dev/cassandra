# Many-tables profiling harness — lessons learned

- `ant` init hard-fails when the main checkout's `.git/hooks` is read-only
  (worktree setups). Fix: no-op `install-git-defaults.sh` stub + `-Dant.build.src`.
  With accord jars prebuilt, direct ant also needs `-Dno-build-accord=true`.
- This workstation has read-only `~/.gradle` and `~/.m2`. Redirect via
  `GRADLE_USER_HOME`, `GRADLE_OPTS=-Dmaven.repo.local=...`, and
  `-Dlocal.repository=...` (the wrapper does this automatically under tmp/).
- async-profiler 4.2 allows ONE profiler session per process, and multi-event
  output only as JFR ("Only JFR output supports multiple events"). Record once
  per phase to `<phase>.ap.jfr`; derive HTML flame graphs post-run with jfrconv
  (`-o html` + required `--alloc|--wall|--cpu` flag).
- JDK 21 in-JVM dtest JMX needs the full ant test JPMS flag set (notably
  `--add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED`), or the JMX feature
  fails with an IllegalAccessError at node start.
- `jdk.ExecutionSample` is NOT enabled by default in a programmatic Recording;
  enable it (plus NativeMethodSample) or `jfr view hot-methods` is empty.
- The JDK `jfr` tool reads async-profiler .ap.jfr directly; allocation samples
  surface as standard JDK TLAB event types, so `jfr view allocation-by-site`
  works with zero extra installs.
- Repeatability held across three design refactors (heap checkpoints within
  0.2%) — validate instrumentation changes by re-running the same N and
  comparing summary numbers before trusting new data.

Building and Testing with the helper sripts
-------------------------------------------

Information on building and testing beyond the use of ant.
All scripts also print help if the first argument is `-h`.

`build-jars.sh`, `check-code.sh` and `run-tests.sh` accept `-s` (`--summary`).
That prints a summary of the failures in place of the full ant output, and keeps
the full output in a log file under the build directory.  The scripts name that
file when a run fails.

Building JARs
-------------

Build in docker:

    .build/docker/build-jars.sh


Build without docker:

    .build/build-jars.sh


Remove the artifacts of a previous build first:

    .build/build-jars.sh --clean


Code Checks and Lints
---------------------

Run in docker:

    .build/docker/check-code.sh


Run without docker:

    .build/check-code.sh


Run in docker with a specific jdk.
The following applies to all build scripts.

    .build/docker/check-code.sh 11


Run in docker with a specific build path.
This permits parallel builds off the same source path.
The following applies to all build scripts.

    build_dir=/tmp/cass_Mtu462n .build/docker/check-code.sh


Building Artifacts (tarball and maven)
-------------------------------------

Build with docker:

    .build/docker/build-artifacts.sh


Build without docker:

    .build/build-artifacts.sh


Build in docker with a specific jdk:

    .build/docker/build-artifacts.sh 11


Building Debian and RedHat packages
-----------------------------------

The packaging scripts are only intended to be used with docker.

Build:

    .build/docker/build-debian.sh
    .build/docker/build-redhat.sh


Build with a specific jdk:

    .build/docker/build-debian.sh 11
    .build/docker/build-redhat.sh rpm 11



Running Tests
-------------

Running unit tests with docker:

    .build/docker/run-tests.sh -a test


Running unittests without docker:

    .build/run-tests.sh -a test


Running only a split of unittests, with docker:

    .build/docker/run-tests.sh -a test -c 1/64


Running unittests with a specific jdk with docker:

    .build/docker/run-tests.sh -a test -c 1/64 -j 11


Running only unit tests matching a regexp, with docker:

    .build/docker/run-tests.sh -a test -t VerifyTest -j 11
    .build/docker/run-tests.sh -a test -t "Compaction*Test$" -j 11


Running other types of tests with docker:

    .build/docker/run-tests.sh -a test
    .build/docker/run-tests.sh -a stress-test
    .build/docker/run-tests.sh -a fqltool-test
    .build/docker/run-tests.sh -a sstableloader-test
    .build/docker/run-tests.sh -a microbench
    .build/docker/run-tests.sh -a test-cdc
    .build/docker/run-tests.sh -a test-compression
    .build/docker/run-tests.sh -a test-oa
    .build/docker/run-tests.sh -a test-system-keyspace-directory
    .build/docker/run-tests.sh -a test-tries
    .build/docker/run-tests.sh -a test-burn
    .build/docker/run-tests.sh -a long-test
    .build/docker/run-tests.sh -a cqlsh-test
    .build/docker/run-tests.sh -a jvm-dtest
    .build/docker/run-tests.sh -a jvm-dtest-upgrade
    .build/docker/run-tests.sh -a dtest
    .build/docker/run-tests.sh -a dtest-novnode
    .build/docker/run-tests.sh -a dtest-offheap
    .build/docker/run-tests.sh -a dtest-large
    .build/docker/run-tests.sh -a dtest-large-novnode
    .build/docker/run-tests.sh -a dtest-upgrade
    .build/docker/run-tests.sh -a dtest-upgrade-large

Repeating tests

Just add the '-repeat' suffix to the test type and pass in the repeat arguments

    .build/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2
    .build/docker/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2 -j 11

Fail fast with repeating tests is done with REPEATED_TESTS_STOP_ON_FAILURE

    .build/run-tests.sh -a jvm-dtest-repeat -t BooleanTest -e REPEATED_TESTS_COUNT=2 -e REPEATED_TESTS_STOP_ON_FAILURE=false

Targeted paging coverage
-----------------------

Use a Java 17 build with the bundled JaCoCo 0.8.8. Rebuild generated classes when
switching Java versions. Select a fully qualified test class:

    bash run_tests.sh build
    bash run_tests.sh coverage org.apache.cassandra.service.pager.PagingAcrossTombstonesTest -Dno-build-test=true
    bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dno-build-test=true -DmaxMemory=2G

The wrapper writes console output to a dated file in `logs/`. Each coverage run
uses a separate directory in `build/paging-coverage/`, containing JUnit results,
execution data, and HTML, CSV, and XML reports. It generates a report even when
tests fail and preserves the failure exit code. Coverage from a failed run is
partial evidence, not a passing verification result.

Run the paging-state properties, checkpoint properties, continuation serialization property, and bounded
three-node fuzzer:

    bash run_property_tests.sh -Dno-build-test=true -DmaxMemory=2G

The distributed fuzzer uses native-protocol clients pinned to each coordinator.
It compares timestamp-ordered inserts, row deletes, and range deletes against an
independent model at consistency ALL. Before enabling tombstone paging, it checks
the model against a single-replica replay of every mutation, unpaged ALL reads,
and an all-disabled paging control. The separate `mutationModelMatchesCassandra`
test runs those controls alone. Disabled nodes use a high failure threshold;
this does not test rejection by an overwhelmed disabled node.

Enabled cases check ascending and descending reads, memtables and flushed data,
and matching, differing, and partly disabled configurations. Fetch size must be
strictly below LIMIT so Cassandra creates a pager. Every enabled configuration
must cross a replica tombstone threshold. The bounded native-page reader checks
state progress, row/page budgets, and a deadline before comparing all returned
rows; empty pages with continuation state are valid.

The deterministic `PagingAcrossTombstonesDistributedTest` checks empty pages with
matching digests and continuations, full-data reconciliation on mismatch,
blocking read repair, reverse composite clustering, static data, and a remote
data replica. Its original ALL regressions now pass in memory and after flush.
Run it with the same test or coverage wrapper as the fuzzer.

`PagingAcrossTombstonesInteractionTest` adds concurrent writes, speculation,
timeout/retry, replica shutdown, TTL expiry, collection deletions, GROUP BY,
partition-key IN, and safe range-checkpoint rejection. Its IN and GROUP BY
regressions exposed separate defects. The IN fix now passes the original case
and 112 additional variants covering LIMIT, per-partition limits, repeated
checkpoints, deleted partitions, descending clustering, static data, and
reconciliation, in memory and after flush. All 16 methods now pass. The grouped
cases also cover static data, both clustering orders, per-partition limits,
empty checkpoints in later IN partitions, and divergent-replica refills. See
[the grouped paging fix](../.debug/fix-grouped-checkpoint-paging.md).

`QueryPagerTest` now checks cursor advancement on the same pager, serialized
state restoration, and unfiltered exhaustion. The distributed fuzzer also has
`partitionKeyInMatchesModel`: 12 small histories per seed, with identical
replicas, independent row membership, disabled LIMIT controls, and bounded
native-page traversal. Seeds 18424 and 1 pass. This IN property uses coordinator
1; the existing divergent-replica property checks all three coordinators.
See the [IN fix report](../.debug/fix-in-paging-checkpoint-reuse.md) for commands
and current versus cumulative JaCoCo results.

The extended pager tests cover aggregation sub-pages, range endpoint bounds,
per-partition limits, early close, exhausted pagers, and paging-state validation.
The new round-trip property uses 1,024 fixed-seed cases across protocols V3–V6.
The three unit suites take about eight seconds with JaCoCo enabled; the new
passing methods account for about 0.6 seconds of that run.

    bash run_tests.sh coverage org.apache.cassandra.service.pager.PagingStateTest -Dno-build-test=true
    bash run_tests.sh coverage org.apache.cassandra.service.pager.RandomizedPagingStateTest -Dno-build-test=true
    bash run_tests.sh coverage org.apache.cassandra.service.QueryPagerTest -Dno-build-test=true

All 22 `QueryPagerTest` methods pass, including the forward/reverse GROUP BY
checkpoint regressions. `PagingStateTest` now runs the read-only heap buffer
regression after the shared byte-copy fix. The property includes read-only heap
views again. See the [coverage extension report](../.debug/extended-pager-coverage.md)
for the historical 95.7% line / 89.7% branch measurement and the
[fix report](../.debug/fix-grouped-checkpoint-paging.md) for current validation.

The latest [coverage follow-up](../.debug/close-paging-coverage-gaps.md) adds nine
methods for static cutoffs, empty-page iteration, legacy static boundaries,
continuation rejection, transient reconciliation, state equality, and management
accessors. It also strengthens the state round-trip property. At that revision,
pager coverage was 96.37% of lines and 93.12% of branches. The new methods took about 9.1 seconds,
excluding shared class/JVM setup, and add no sleeps. A static cutoff without a
row cursor now returns a typed read failure instead of a generic server error.

The [correctness audit](../.debug/paging-correctness-and-performance.md) adds native
ONE/LOCAL_ONE failure checks without clustering columns and a latched speculative
transient-replica regression. Default local reads now reject tombstone overflow;
replica handlers and pagers explicitly enable resumable checkpoints. A local-read
regression covers filtered and unfiltered APIs, including cluster metadata.

Run the bounded three-node performance comparison without JaCoCo instrumentation:

    bash run_tests.sh test org.apache.cassandra.distributed.test.PagingAcrossTombstonesPerformanceTest -Dno-build-test=true -DmaxMemory=8G -Dtest.timeout=180000

It measures complete native ALL queries with fetch size 100 and LIMIT 101 in
memory and after flush. The four modes are clean reads with paging disabled or
enabled, matching checkpoints, and full-data reconciliation. It asserts results
and replica read counts, and reports timing without a latency pass/fail threshold.
Measurements are written to `logs/paging-performance-<UTC timestamp>.log` and stdout.
The audit records the measurement design and its limits.

`PagingAcrossTombstonesTransientTest` exercises an actual 3/1 replication factor
and all three coordinators. `MixedConfigPagingAcrossTombstonesTest` now applies
four real configurations and runs 32 populated cases. The mixed-version class
uses `UpgradeTestBase` for a real 5.0-to-current upgrade. It requires historical
and current `dtest-*.jar` artifacts in `build/`; compile each with `ant dtest-jar`.
All four deterministic tests pass with 5.0.10 and this branch. They check stages
with one and two upgraded nodes, all coordinator roles, GROUP BY, static columns,
TTL, partition-key IN, LIMIT, and PER PARTITION LIMIT. Rebuild the current branch's
dtest JAR after implementation changes; a trunk 7.0 JAR does not contain the
feature. See the [upgrade results](../.debug/mixed-version-tombstone-paging.md).
Complete builds before starting tests because Ant rebuilds shared Accord artifacts.
Run Ant test invocations sequentially: they also remove shared test data directories.

The default seed is `18424` with six histories. Override the seed through Ant's test
JVM arguments, for example:

    bash run_tests.sh coverage org.apache.cassandra.distributed.test.PagingAcrossTombstonesFuzzTest -Dno-build-test=true '-Dtest.jvm.args=-Dcassandra.test.seed=18424'

Run the mixed-version fuzzer explicitly when historical and current dtest JARs
are available. It checks generated replica divergence, forward/reverse reads,
and partition-key IN after each of two node upgrades:

    bash run_property_tests.sh --mixed-version -Dno-build-test=true -DmaxMemory=8G -Dtest.timeout=480000 '-Dtest.jvm.args=-Dcassandra.test.seed=18424'

Run the existing single-node synchronous/asynchronous fuzzer separately:

    bash run_property_tests.sh --long -Dno-build-test=true '-Dtest.jvm.args=-Dcassandra.test.paging_fuzz_duration_min=1'

Replay one command-sequence iteration per validator profile with a fixed seed:

    bash run_property_tests.sh --long -Dno-build-test=true '-Dtest.jvm.args=-Dcassandra.test.seed=18424'

The long suite also runs a fixed-seed live-row model property with bounded
paging. Use the JVM property for replay: some shell launchers drop the legacy
`cassandra.test.seed` environment variable because its name contains dots.

Inspect the pager package and the changed read/repair classes in the report.
Line coverage and branch coverage measure executed code; neither proves correct
reconciliation or that every paging mode was exercised.

Running python dtests without docker:

    .build/run-python-dtests.sh dtest


Other test types without docker:

    .build/run-tests.sh jvm-test


Other python dtest types without docker:

    .build/run-python-dtests.sh dtest-upgrade-large

Debugging test scripts:

    DEBUG=true .build/docker/run-tests.sh -a test


Testing the helper scripts
--------------------------

The tests of the helper scripts need no JDK build.  A stub `ant` and a stub `docker`
replay canned output, so each suite runs in seconds.

Run them:

    python3 -m unittest discover -s .build/sh/test    # the log summarizers
    .build/sh/test/stub-ant-cases.sh                  # .build/*.sh
    .build/sh/test/stub-docker-cases.sh               # .build/docker/*.sh


Running Sonar analysis (experimental)
-------------------------------------

Run:

    ant sonar

Sonar analysis requires the SonarQube server to be available. If there
is already some SonarQube server, it can be used by setting the
following env variables:

    SONAR_HOST_URL=http://sonar.example.com
    SONAR_CASSANDRA_TOKEN=cassandra-project-analysis-token
    SONAR_PROJECT_KEY=<key of the Cassandra project in SonarQube>

If SonarQube server is not available, one can be started locally in
a Docker container. The following command will create a SonarQube
container and start the server:

    ant sonar-create-server

The server will be available at http://localhost:9000 with admin
credentials admin/password. The Docker container named `sonarqube`
is created and left running. When using this local SonarQube server,
no env variables to configure url, token, or project key are needed,
and the analysis can be run right away with `ant sonar`.

After the analysis, the server remains running so that one can
inspect the results.

To stop the local SonarQube server:

    ant sonar-stop-server

However, this command just stops the Docker container without removing
it. It allows to start the container later with:

    docker container start sonarqube

and access previous analysis results. To drop the container, run:

    docker container rm sonarqube

When `SONAR_HOST_URL` is not provided, the script assumes a dedicated
local instance of the SonarQube server and sets it up automatically,
which includes creating a project, setting up the quality profile, and
quality gate from the configuration stored in
[sonar-quality-profile.xml](sonar%2Fsonar-quality-profile.xml) and
[sonar-quality-gate.json](sonar%2Fsonar-quality-gate.json)
respectively. To run the analysis with a custom quality profile, start
the server using `ant sonar-create-server`, create a project manually,
and set up a desired quality profile for it. Then, create the analysis
token for the project and export the following env variables:

    SONAR_HOST_URL="http://127.0.0.1:9000"
    SONAR_CASSANDRA_TOKEN="<token>"
    SONAR_PROJECT_KEY="<key of the Cassandra project in SonarQube>"

The analysis can be run with `ant sonar`.

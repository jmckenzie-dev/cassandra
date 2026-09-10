<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for additional
information regarding copyright ownership. The ASF licenses this file to you
under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# Bounded idle admission validation

- Never edit a running shell script, even just help text. Bash can resume reading
  at an obsolete file offset. An early wrapper completed its tests but then tried
  to execute `en`; the final unchanged-script rerun exited 0.
- The repository uses JUnit 4.12. Use the existing AssertJ exception assertions.
- JaCoCo 0.8.8 cannot instrument this Java 21 build. The JVM can continue running
  tests after instrumentation fails. Check agent diagnostics before interpreting
  an exit-zero test process or an exec file as coverage evidence.
  The subsequent user-authorized upgrade to 0.8.11 resolved this limitation; all
  74 tests pass instrumented and Ant reporting succeeds.
- Use the test logger for distributed measurement output; System.out did not appear
  in the archived test output. Preserve the timestamped class logs with the XML.
- Use injected monotonic time for budget tests. Real flush duration must not refill
  a supposedly exhausted test budget and make assertions depend on machine speed.

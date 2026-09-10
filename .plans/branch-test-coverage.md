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

# Branch test coverage

Measure the current moar_tables checkout, including uncommitted work, against its
merge base with origin/trunk: 88fd0f6a0eaed8943f05ac9e8f947882b8ddc8f1.

- Inventory all changed production Java files, all added/modified JUnit test
  sources, nested property tests, and existing concrete core memtable test classes.
- Run the selected unit and distributed classes through repository wrappers under
  JaCoCo 0.8.11 on Java 21. Run wrappers sequentially and preserve each batch's XML
  and logs before another wrapper cleans shared outputs. Do not run the full suite.
- Run branch verification entrypoints for nested properties and supporting Java /
  Python checks. Do not treat benchmark mains or JMH workloads as JUnit tests.
- Instrument changed production classes and nested classes. Report merged line and
  branch coverage per changed source file, plus coverage of executable lines in
  added/modified diff hunks. Whole-file percentages include untouched code.
- Identify uncovered files, material gaps, failed/skipped tests, and instrumentation
  exclusions/mismatches. Do not infer branch coverage from line coverage.
- Keep source, test, configuration, dependency, and Git history unchanged. Generated
  orchestration scripts and reports live under tmp/ and logs/. No commit requested.

The test manifest and captured diff are the exact scope record. The intended
result is a coverage assessment, not a merge approval or a claim of full database
correctness. Non-Java files are outside JaCoCo's numeric coverage.

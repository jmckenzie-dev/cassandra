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

# Metric exports

Source: 319995ce5c178f5eb0dbaf135b98d395c40a045c.
Base: f7d5a7e2c0fcf769293ee27073630a3574584b06.

Review units: adaptive scrape history; compact metric registration;
transient ObjectName handling. Keep all switches opt-in.

Build with Java 21 using ai-build. Run six test classes using run_tests.sh.
Preserve XML reports. Run boundary diagnostics and isolated benchmarks.
Do not run timing measurements alongside other builds or tests.

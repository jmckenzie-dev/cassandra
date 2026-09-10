<!--
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements. See the NOTICE file distributed with this work for
additional information regarding copyright ownership. The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
-->

# UCS hierarchy measurement lessons

- Run Ant test wrappers sequentially in a shared checkout. Separate test classes
  still share output and data directories. An overlapping ControllerTest rerun
  lost its XML output when another wrapper cleaned the directory. Repeat the
  affected run in isolation; the failed attempt is not a database test result.
- Never rebuild shared classes or the JAR while a benchmark JVM is running.
- Trace the full producer path before interpreting a metric. Flush logs include
  empty per-disk writers, but ColumnFamilyStore removes them before it updates
  the observed flush-size average.
- Record average and peak settled file counts. A final checkpoint just after a
  large tier merge can conceal higher reader residency during earlier cycles.

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

# Shared-base extraction

Source: 319995ce5c178f5eb0dbaf135b98d395c40a045c.
Parent: 50a10d42a5d633150fbf4ae518b4da85466472b2.

Import generic build/test wrappers and profiling tools. Remove optional feature
hooks from the profile launcher until their owning branches add them.
Upgrade JaCoCo to 0.8.11. Add shared metric interfaces and constructor overloads
without changing recording algorithms or runtime defaults.

Run the build, Checkstyle, five existing metric test classes, and a three-table
profile harness smoke test before freezing this base. See the coordinator's
.agentlogs/plans/many-tables-split/01-shared-base.md for the complete gate.

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Tombstone continuation tests

- Require fetch size below LIMIT when a test needs a client pager. Accept empty
  pages with continuation and compare the complete ordered result.
- Check generated expectations against single-replica mutation replay and
  disabled paging controls before attributing failures to reconciliation.
- Compare completion status and clustering alongside digests. Tombstone counts
  do not identify a position.
- Test static-only output both before and at partition exhaustion. Cursor
  progress does not establish that an earlier page returned data.
- Give parallel runs separate logs and output directories. Do not edit a shell
  wrapper while it executes. Keep long method lists out of Ant output filenames.

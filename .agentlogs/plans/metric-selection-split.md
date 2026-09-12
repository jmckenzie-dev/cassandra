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

# Metric selection split

Source: `319995ce5c178f5eb0dbaf135b98d395c40a045c`

Base: `f7d5a7e2c0fcf769293ee27073630a3574584b06`

1. Apply complete diffs for SELECT-only paths.
2. Apply SELECT-owned hunks in shared files.
3. Remove STORE, EXPORT, MEM, and EXTRA assumptions from standalone tests.
4. Record source allocation and integration-only assertions.
5. Run generator, packaging, build, and targeted tests.
6. Review and commit the branch in small units.

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

# Expiration test diagnosis

The mock Controller returned a zero expiration-check interval. The real strategy
requires elapsed milliseconds > interval. Two selections in one clock tick
could omit the second expiration scan and return a null compaction selection.

Set only the injected mock intervals to -1 in testDropExpiredFromBucket and
testDropExpiredAndCompactNonExpired. Both fixtures test selection, not scan
scheduling. No production behavior, assertions, clock state, or sleeps change.

<!-- Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file for copyright ownership.
Licensed under the Apache License, Version 2.0. You may obtain a copy at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software is
distributed on an AS IS BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
See the License for permissions and limitations under the License. -->

# Restore analyzer executable modes

The integration path audit found that two unchanged scripts lost mode 100755
during extraction: analyze-reservoir-measurements.py and
analyze-resident-reservoirs.py. Restore their original executable modes.
Their contents and all compiled sources remain unchanged. Prior benchmark and
test results still apply; those runs invoked Python explicitly.

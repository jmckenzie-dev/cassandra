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

# Shared-base build environment

Use Java Development Kit (JDK) 21.0.12.1 and Apache Ant 1.10.17 through the
ai-build and ai-ci-test wrappers. Set JAVA_HOME explicitly to
/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home.

The first sandboxed build failed while installing a shared Git hook. An approved
escalated build passed. No source workaround or dependency installation was
needed. The wrapper skips generated documentation and Release Audit Tool checks;
its successful check-code stage includes main and test Checkstyle.

Build logs: logs/20260911-150101-ai-build.log (sandbox failure) and
logs/20260911-152611-ai-build.log (successful build and checks).

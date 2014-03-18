#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#-----------------------------------------------------------------------------
Function SetCassandraHome()
{
    if (! $env:CASSANDRA_HOME)
    {
        $cwd = [System.IO.Directory]::GetCurrentDirectory()
        $cwd = Split-Path $cwd -parent
        $env:CASSANDRA_HOME = $cwd -replace "\\", "/"
        echo "Setting cassandra home to $env:CASSANDRA_HOME"
    }
}

#-----------------------------------------------------------------------------
Function SetCassandraMain()
{
    if (! $env:CASSANDRA_MAIN)
    {
        $env:CASSANDRA_MAIN="org.apache.cassandra.service.CassandraDaemon"
    }
}

#-----------------------------------------------------------------------------
Function SetJavaOpts
{
    # kep multi-line for ease of use
    $rawOpts=@"
 -ea
 -javaagent:"$env:CASSANDRA_HOME/lib/jamm-0.2.6.jar"
 -Xms2G
 -Xmx2G
 -XX:+HeapDumpOnOutOfMemoryError
 -XX:+UseParNewGC
 -XX:+UseConcMarkSweepGC
 -XX:+CMSParallelRemarkEnabled
 -XX:SurvivorRatio=8
 -XX:MaxTenuringThreshold=1
 -XX:CMSInitiatingOccupancyFraction=75
 -XX:+UseCMSInitiatingOccupancyOnly
 -Dcom.sun.management.jmxremote.port=7199
 -Dcom.sun.management.jmxremote.ssl=false
 -Dcom.sun.management.jmxremote.authenticate=false
 -Dlogback.configurationFile=logback.xml
"@
    # Strip back out newlines added above
    $env:JAVA_OPTS = $rawOpts -replace [Environment]::NewLine, ""
}

#-----------------------------------------------------------------------------
Function BuildClassPath
{
    $cp = "$env:CASSANDRA_HOME/conf"
    foreach ($file in Get-ChildItem "$env:CASSANDRA_HOME/lib/*.jar")
    {
        $file = $file -replace "\\", "/"
        $cp = $cp + ";" + "$file"
    }

    # Add build/classes/main so it works in development
    $cp = $cp + ";" + "$env:CASSANDRA_HOME/build/classes/main;$env:CASSANDRA_HOME/build/classes/thrift"
    $env:CLASSPATH=$cp
}

#-----------------------------------------------------------------------------
Function SetCassandraEnvironment
{
    echo "Setting up Cassandra environment"
    if (! $env:JAVA_HOME)
    {
        echo "JAVA_HOME environment variable must be set.  Aborting!"
        exit
    }
    SetCassandraHome
    $env:CASSANDRA_CONF = "$env:CASSANDRA_HOME/conf"
    $env:CASSANDRA_PARAMS="-Dcassandra -Dcassandra-foreground=yes"
    SetCassandraMain
    BuildClassPath
    SetJavaOpts
}

#-----------------------------------------------------------------------------
Function PrintEnvironment
{
    Param($argumentOne)
    echo "`n[Environment Values at identifier: $argumentOne]"
    echo "   CASSANDRA_HOME: $env:CASSANDRA_HOME"
    echo "   CASSANDRA_CONF: $env:CASSANDRA_CONF"
    echo "   CASSANDRA_MAIN: $env:CASSANDRA_MAIN"
    echo "   CLASSPATH: $env:CLASSPATH"
    echo "   JAVA_OPTS: $env:JAVA_OPTS"
}

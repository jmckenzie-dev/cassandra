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
param (
    [switch]$install,
    [switch]$uninstall,
    [switch]$help
 )

#-----------------------------------------------------------------------------
Function ValidateArguments
{
    if ($install -and $uninstall)
    {
        exit
    }
    if ($help)
    {
        PrintUsage
    }
}

#-----------------------------------------------------------------------------
Function PrintUsage
{
    echo @"
usage: cassandra.ps1 [-install | -uninstall] [-help]
    -install        install cassandra as a service
    -uninstall      remove cassandra service
    -help           print this message

    NOTE: no arguments will run cassandra in the background
    NOTE: cassandra as a service requires Commons Daemon Service Runner
        available at http://commons.apache.org/proper/commons-daemon/"
"@
    exit
}

#-----------------------------------------------------------------------------
Function HandleInstallation
{
    $SERVICE_JVM = "cassandra"
    $PATH_PRUNSRV = "$env:CASSANDRA_HOME/bin/daemon/"
    $PR_LOGPATH = $serverPath

    if (!$env:PRUNSRV)
    {
        $env:PRUNSRV="$PATH_PRUNSRV/prunsrv"
    }

    echo "Attempting to delete service if already existing..."
    & "$prunsrv //DS//$SERVICE_JVM"

    # Quit out if this is uninstall only
    if ($uninstall)
    {
        return
    }

    echo "Installing $SERVICE_JVM. If you get registry warnings, re-run as an Administrator"
    & "$prunsrv //IS//$SERVICE_JVM"

    echo "Setting the parameters for $SERVICE_JVM"

    # Broken multi-line for convenience - glued back together in a bit
    $cmd = @"
$PRUNSRV //US//%SERVICE_JVM%
 --Jvm=auto --StdOutput auto --StdError auto
 --Classpath=$env:CASSANDRA_CLASSPATH
 --StartMode=jvm --StartClass=$env:CASSANDRA_MAIN --StartMethod=main
 --StopMode=jvm --StopClass=$env:CASSANDRA_MAIN  --StopMethod=stop
 ++JvmOptions=$env:JAVA_OPTS ++JvmOptions=-DCassandra
 --PidFile pid.txt
"@
    $cmd = $cmd -replace [Environment]::NewLine, ""
    & $cmd

    echo "Installation of $SERVICE_JVM is complete"
}

#-----------------------------------------------------------------------------
Function RunCassandra
{
    echo "Starting cassandra server"
    $cmd = @"
$env:JAVA_HOME/bin/java
"@
    $arg1 = $env:JAVA_OPTS
    $arg2 = $env:CASSANDRA_PARAMS
    $arg3 = "-cp $env:CLASSPATH"
    $arg4 = @"
"$env:CASSANDRA_MAIN"
"@

    $proc = Start-Process -FilePath "$cmd" -ArgumentList $arg1,$arg2,$arg3,"$arg4" -PassThru -WindowStyle Hidden

    # store the pid
    echo $proc.Id > $env:CASSANDRA_HOME/cassandra.pid

    $cassPid = $proc.Id
    echo "Started cassandra successfully with pid: $cassPid"
}

#-----------------------------------------------------------------------------
# Main
$file = [System.IO.Directory]::GetCurrentDirectory() + "/windows-env.ps1"
. $file

ValidateArguments

SetCassandraEnvironment

if ($install -or $uninstall)
{
    HandleInstallation
}
else
{
    RunCassandra
}

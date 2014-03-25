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
    [switch]$help,
    # single switch args
    [switch]$f,
    [string]$p,
    [string]$H,
    [string]$E
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
usage: cassandra.ps1 [-f] [-h] [-p pidfile] [-H dumpfile] [-E errorfile] [-install | -uninstall] [-help]
    -f              Run cassandra in foreground
    -install        install cassandra as a service
    -uninstall      remove cassandra service
    -p              pidfile tracked by server and removed on close
    -H              change JVM HeapDumpPath
    -E              change JVM ErrorFile
    -help           print this message

    NOTE: installing cassandra as a service requires Commons Daemon Service Runner
        available at http://commons.apache.org/proper/commons-daemon/"
"@
    exit
}

#-----------------------------------------------------------------------------
# Note: throughout these scripts we're replacing \ with /.  This allows clean
# operation on both command-prompt and cygwin-based environments.
Function Main
{
    ValidateArguments

    # Order of preference on grabbing environment settings:
    #   CASSANDRA_CONF
    #   CASSANDRA_HOME
    #   Relative to current working directory
    if (Test-Path Env:\CASSANDRA_CONF)
    {
        $file = "$env:CASSANDRA_CONF/windows-env.ps1"
    }
    elseif (Test-Path Env:\CASSANDRA_HOME)
    {
        $file = "$env:CASSANDRA_HOME/conf/windows-env.ps1"
    }
    else
    {
        $file = [System.IO.Directory]::GetCurrentDirectory() + "/../conf/windows-env.ps1"
    }
    $file = $file -replace "\\", "/"

    if (Test-Path $file)
    {
        . $file
    }
    else
    {
        echo "Error with environment file resolution.  Path: $file not found."
        exit
    }

    SetCassandraEnvironment

    # Other command line params
    if ($H)
    {
        $env:JAVA_OPTS = $env:JAVA_OPTS + " -XX:HeapDumpPath=$H"
    }
    if ($E)
    {
        $env:JAVA_OPTS = $env:JAVA_OPTS + " -XX:ErrorFile=$E"
    }
    if ($p)
    {
        $env:CASSANDRA_PARAMS = $env:CASSANDRA_PARAMS + " -pidfile=$p"
    }

    if ($install -or $uninstall)
    {
        HandleInstallation
    }
    else
    {
        RunCassandra($f)
    }
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
Function RunCassandra([string]$foreground)
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

    if ($foreground -ne "False")
    {
        Start-Process -FilePath "$cmd" -ArgumentList $arg1,$arg2,$arg3,"$arg4" -NoNewWindow
    }
    else
    {
        $proc = Start-Process -FilePath "$cmd" -ArgumentList $arg1,$arg2,$arg3,"$arg4" -PassThru -WindowStyle Hidden

        # store the pid
        echo $proc.Id > $env:CASSANDRA_HOME/cassandra.pid

        $cassPid = $proc.Id
        if ($cassPid -eq "")
        {
            echo "Error starting cassandra."
        }
        else
        {
            echo "Started cassandra successfully with pid: $cassPid"
        }
    }
}

#-----------------------------------------------------------------------------
Main

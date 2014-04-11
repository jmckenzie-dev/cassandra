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
    [string]$p,
    [switch]$help
)

#-----------------------------------------------------------------------------
Function ValidateArguments
{
    if (!$p)
    {
        PrintUsage
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
usage: stop-server.ps1 -p pidfile [-help]
    -p              pidfile tracked by server and removed on close
"@
    exit
}

#-----------------------------------------------------------------------------
Function KillProcess
{
    if (-Not (Test-Path $p))
    {
        echo "Error - pidfile not found.  Aborting."
        exit
    }
    # coerce to integer
    $a = Get-Content $p
    taskkill /pid $a

    echo "Sent WM_CLOSE to cassandra"
}

#-----------------------------------------------------------------------------
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

    KillProcess
}

#-----------------------------------------------------------------------------
Main

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
    [switch]$f,
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
usage: stop-server.ps1 -p pidfile -f[-help]
    -p      pidfile tracked by server and removed on close
    -f      force kill.
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

    $a = Get-Content $p
    if (Get-Process -Id $a -EA SilentlyContinue)
    {
        $pinfo = New-Object System.Diagnostics.ProcessStartInfo
        $pinfo.FileName = "taskkill.exe"
        $pinfo.RedirectStandardError = $true
        $pinfo.RedirectStandardOutput = $true
        $pinfo.UseShellExecute = $false
        $pinfo.Arguments = "/pid $a"

        if ($f)
        {
            $pinfo.Arguments = $pinfo.Arguments + " /f"
        }

        $killer = New-Object System.Diagnostics.Process
        $killer.StartInfo = $pinfo
        $killer.Start() | Out-Null
        $killer.WaitForExit()
        $stderr = $killer.StandardError.ReadToEnd()
        if ($stderr.Contains("forcefully"))
        {
            echo "Failed to terminate process with pid $a.  Re-run with -f to forcefully terminate."
            exit
        }
        elseif ($stderr -ne "")
        {
            echo $stderr
            exit
        }
    }
    else
    {
        echo "No cassandra process running with pid: $a.  Exiting."
        exit
    }

    echo "Sent WM_CLOSE termination signal to cassandra with pid $a"
}

#-----------------------------------------------------------------------------
ValidateArguments
KillProcess

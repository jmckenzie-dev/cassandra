@REM
@REM  Licensed to the Apache Software Foundation (ASF) under one or more
@REM  contributor license agreements.  See the NOTICE file distributed with
@REM  this work for additional information regarding copyright ownership.
@REM  The ASF licenses this file to You under the Apache License, Version 2.0
@REM  (the "License"); you may not use this file except in compliance with
@REM  the License.  You may obtain a copy of the License at
@REM
@REM      http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM  Unless required by applicable law or agreed to in writing, software
@REM  distributed under the License is distributed on an "AS IS" BASIS,
@REM  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM  See the License for the specific language governing permissions and
@REM  limitations under the License.

@echo off
if "%OS%" == "Windows_NT" setlocal

REM Blind pass all arguments through here to either cassandra.ps1 or cassandra.bat
for %%i in (%*) do call :appendArg %%i
goto :okArgs

REM -----------------------------------------------------------------------------
:appendArg
set ARGS=%ARGS% %1
goto :eof

REM -----------------------------------------------------------------------------
:okArgs
REM See if we have the capabilities of running the powershell scripts
for /F "delims=" %%i in ('powershell Get-ExecutionPolicy') do set PERMISSION=%%i
if "%PERMISSION%" == "Unrestricted" goto runPowerShell
goto runLegacy

REM -----------------------------------------------------------------------------
:runPowerShell
echo Detected powershell execution permissions.  Running with enhanced startup scripts.
powershell /file cassandra.ps1 %ARGS%
goto finally

REM -----------------------------------------------------------------------------
:runLegacy
echo WARNING! Powershell script execution unavailable.
echo    Please use 'powershell Set-ExecutionPolicy Unrestricted'
echo    on this user-account to run cassandra with fully featured
echo    functionality on this platform.

echo Starting with legacy cassandra.bat
cassandra_legacy.bat %ARGS%
goto finally

:finally
ENDLOCAL

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
usage: cassandra.ps1 [-install|-uninistall]
    -install        Install the cassandra server as a service
    -uninistall     Uninstall the cassandra server as a service
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
    PrintEnvironment "Pre Run"
    $cmd = @"
$env:JAVA_HOME/bin/java
"@
    $arg1 = $env:JAVA_OPTS
    $arg2 = $env:CASSANDRA_PARAMS
    $arg3 = "-cp $env:CLASSPATH"
    $arg4 = @"
"$env:CASSANDRA_MAIN"
"@

    echo "Running command: $cmd $arg1 $arg2 $arg3 $arg4"
    $proc = Start-Process -FilePath "$cmd" -ArgumentList $arg1,$arg2,$arg3,"$arg4" 2>&1 >> output.log
    $cassPid = $proc.Id
    echo "Pid: $cassPid"
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

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ "x$CASSANDRA_HOME" = "x" ]; then
    CASSANDRA_HOME="`dirname "$0"`/.."
fi

# The directory where Cassandra's configs live (required)
if [ "x$CASSANDRA_CONF" = "x" ]; then
    CASSANDRA_CONF="$CASSANDRA_HOME/conf"
fi

# The java classpath (required)
CLASSPATH="$CASSANDRA_CONF"

# This can be the path to a jar file, or a directory containing the 
# compiled classes. NOTE: This isn't needed by the startup script,
# it's just used here in constructing the classpath.
if [ -d $CASSANDRA_HOME/build ] ; then
    jars_cnt="`ls -1 $CASSANDRA_HOME/build/apache-cassandra*.jar | grep -v 'javadoc.jar' | grep -v 'sources.jar' | wc -l | xargs echo`"
    if [ "$jars_cnt" -gt 1 ]; then
        dir="`cd $CASSANDRA_HOME/build; pwd`"
        echo "There are JAR artifacts for multiple versions in the $dir directory. Please clean the project with 'ant realclean' and build it again." 1>&2
        exit 1
    fi

    if [ "$jars_cnt" = "1" ]; then
        cassandra_bin="`ls -1 $CASSANDRA_HOME/build/apache-cassandra*.jar | grep -v javadoc | grep -v sources`"
        CLASSPATH="$CLASSPATH:$cassandra_bin"
    fi
fi

# the default location for commitlogs, sstables, and saved caches
# if not set in cassandra.yaml
cassandra_storagedir="$CASSANDRA_HOME/data"

# JAVA_HOME can optionally be set here
# JAVA_HOME=/usr/local/jdk11

for jar in "$CASSANDRA_HOME"/lib/*.jar; do
    CLASSPATH="$CLASSPATH:$jar"
done

# JSR223 - collect all JSR223 engines' jars
for jsr223jar in "$CASSANDRA_HOME"/lib/jsr223/*/*.jar; do
    CLASSPATH="$CLASSPATH:$jsr223jar"
done

CLASSPATH="$CLASSPATH:$EXTRA_CLASSPATH"

# JSR223/JRuby - set ruby lib directory
if [ -d "$CASSANDRA_HOME"/lib/jsr223/jruby/ruby ] ; then
    export JVM_OPTS="$JVM_OPTS -Djruby.lib=$CASSANDRA_HOME/lib/jsr223/jruby"
fi
# JSR223/JRuby - set ruby JNI libraries root directory
if [ -d "$CASSANDRA_HOME"/lib/jsr223/jruby/jni ] ; then
    export JVM_OPTS="$JVM_OPTS -Djffi.boot.library.path=$CASSANDRA_HOME/lib/jsr223/jruby/jni"
fi
# JSR223/Jython - set python.home system property
if [ -f "$CASSANDRA_HOME"/lib/jsr223/jython/jython.jar ] ; then
    export JVM_OPTS="$JVM_OPTS -Dpython.home=$CASSANDRA_HOME/lib/jsr223/jython"
fi
# JSR223/Scala - necessary system property
if [ -f "$CASSANDRA_HOME"/lib/jsr223/scala/scala-compiler.jar ] ; then
    export JVM_OPTS="$JVM_OPTS -Dscala.usejavacp=true"
fi

# set JVM javaagent opts to avoid warnings/errors
JAVA_AGENT="$JAVA_AGENT -javaagent:$CASSANDRA_HOME/lib/jamm-0.4.0.jar"

platform=$(uname -m)
if [ -d "$CASSANDRA_HOME"/lib/"$platform" ]; then
    for jar in "$CASSANDRA_HOME"/lib/"$platform"/*.jar ; do
        CLASSPATH="$CLASSPATH:${jar}"
    done
fi

#
# Java executable and per-Java version JVM settings
#

# Use JAVA_HOME if set, otherwise look for java in PATH
if [ -n "$JAVA_HOME" ]; then
    # Why we can't have nice things: Solaris combines x86 and x86_64
    # installations in the same tree, using an unconventional path for the
    # 64bit JVM.  Since we prefer 64bit, search the alternate path first,
    # (see https://issues.apache.org/jira/browse/CASSANDRA-4638).
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=`command -v java 2> /dev/null`
fi

if [ -z $JAVA ] ; then
    echo Unable to find java executable. Check JAVA_HOME and PATH environment variables. >&2
    exit 1;
fi

# Matches variable 'java.supported' in build.xml
java_versions_supported=(11 17 21)
java_version_string=$(IFS=" "; echo "${java_versions_supported[*]}")

# Determine the sort of JVM we'll be running on.
JAVA_VERSION=$(java -version 2>&1 | grep '[openjdk|java] version' | cut -d '"' -f2 | cut -d '.' -f1)

supported=0
highest_supported=-1
# Both capture highest supported so we can support newer JDK's, and compare to see if ours is explicitly supported
for version in "${java_versions_supported[@]}"; do
  if [ "$version" -gt "$highest_supported" ]; then
    highest_supported=$version
  fi
  if [ "$version" -eq "$JAVA_VERSION" ]; then
      supported=1
  fi
done

if [ "$JAVA_VERSION" -gt "$highest_supported" ]; then
  if [ -z "$CASSANDRA_JDK_UNSUPPORTED" ]; then
    echo "######################################################################"
    echo "Warning! You are using JDK$JAVA_VERSION. This Cassandra version only supports $java_version_string"
    echo "######################################################################"
  else
    echo "Unsupported Java $JAVA_VERSION. Supported are $java_version_string"
    echo "If you would like to test with newer Java versions set CASSANDRA_JDK_UNSUPPORTED to any value (for example, CASSANDRA_JDK_UNSUPPORTED=true). Unset the parameter for default behavior"
    exit 1
  fi
fi

if [ "$supported" -eq "0" ]; then
  echo "Unsupported Java $JAVA_VERSION. Supported are $java_version_string"
  exit 1
fi

# TODO: Either remove the JVM_VENDOR and JVM_ARCH variables or explain where they're used. Appear vestigial.
java_ver_output=$("${JAVA:-java}" -version 2>&1)
jvm=$(echo "$java_ver_output" | grep -A 1 '[openjdk|java] version' | awk 'NR==2 {print $1}')
case "$jvm" in
    OpenJDK)
        JVM_VENDOR=OpenJDK
        # this will be "64-Bit" or "32-Bit"
        JVM_ARCH=$(echo "$java_ver_output" | awk 'NR==3 {print $2}')
        ;;
    "Java(TM)")
        JVM_VENDOR=Oracle
        # this will be "64-Bit" or "32-Bit"
        JVM_ARCH=$(echo "$java_ver_output" | awk 'NR==3 {print $3}')
        ;;
    *)
        # Help fill in other JVM values
        JVM_VENDOR=other
        JVM_ARCH=unknown
        ;;
esac

# Read user-defined JVM options from jvm-server.options file
JVM_OPTS_FILE=$CASSANDRA_CONF/jvm${jvmoptions_variant:--clients}.options
if [ $JAVA_VERSION -ge 21 ] ; then
    JVM_DEP_OPTS_FILE=$CASSANDRA_CONF/jvm21${jvmoptions_variant:--clients}.options
elif [ $JAVA_VERSION -ge 17 ] ; then
    JVM_DEP_OPTS_FILE=$CASSANDRA_CONF/jvm17${jvmoptions_variant:--clients}.options
    JVM_DEP_OPTS_FILE=$CASSANDRA_CONF/jvm17${jvmoptions_variant:--clients}.options
elif [ $JAVA_VERSION -ge 11 ] ; then
    JVM_DEP_OPTS_FILE=$CASSANDRA_CONF/jvm11${jvmoptions_variant:--clients}.options
fi

for opt in `grep "^-" $JVM_OPTS_FILE` `grep "^-" $JVM_DEP_OPTS_FILE`
do
  JVM_OPTS="$JVM_OPTS $opt"
done

# Append additional options when using JDK17+ (CASSANDRA-19001)
USING_JDK=$(command -v javac || command -v "${JAVA_HOME:-/usr}/bin/javac")
if [ -n "$USING_JDK" ] && [ "$JAVA_VERSION" -ge 17 ]; then
  JVM_OPTS="$JVM_OPTS --add-exports jdk.attach/sun.tools.attach=ALL-UNNAMED"
  JVM_OPTS="$JVM_OPTS --add-exports jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED"
  JVM_OPTS="$JVM_OPTS --add-opens jdk.compiler/com.sun.tools.javac=ALL-UNNAMED"
fi

#!/usr/bin/env python

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

import fnmatch, os, sys
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-w", "--working", action="store_true", dest="working", help="Working mode - on first swallowed exception, print details and exit.")
parser.add_option("-p", "--print", action="store_true", dest="print_swallowed", help="Print swallowed exceptions")
parser.add_option("-e", "--exceptions", action="store_true", dest="exceptions", help="Include swallowing Exceptions in output along with Throwable.")
parser.add_option("-v", "--verbose", action="store_true", dest="verbose", help="Print results of parse to stdout")
parser.add_option("-d", "--debug", action="store_true", dest="debug", help="Print debug information concerning parsing logic")
(options, args) = parser.parse_args()

def main():
    matches = []
    for root, dirnames, filenames in os.walk('src'):
        for filename in fnmatch.filter(filenames, '*.java'):
            matches.append(os.path.join(root, filename))

    file_count = {}
    total_delegated = 0
    total_runtimed = 0
    total_rethrown = 0
    total_missed = 0
    total_catch_analyzed = 0
    total_throwable_analyzed = 0
    total_exception_analyzed = 0

    for match in matches:
        f = open(match)
        index = 0

        active = 0
        nesting = 0
        delegated = 0
        runtimed = 0
        rethrown = 0

        exception_block = []
        for line in f:
            index += 1
            if active == 0:
                if "catch (Throwable " in line or (options.exceptions and "catch (Exception " in line):
                    active = 1
                    if "catch (Throwable " in line:
                        total_throwable_analyzed += 1
                    elif "catch (Exception " in line:
                        total_exception_analyzed += 1
                    total_catch_analyzed += 1
                    if match in file_count:
                        file_count[match] += 1
                    else:
                        file_count[match] = 1
                    exception_block.append(str(index) + " -- " + line)
                    continue
            if active == 1:
                exception_block.append(str(index) + " -- " + line)
                if "{" in line:
                    nesting += 1
                if "}" in line:
                    nesting -= 1

                if "throw new Runtime" in line:
                    runtimed = 1
                    continue
                elif "throw" in line:
                    rethrown = 1
                    continue
                elif "JVMStabilityInspector.inspectThrowable" in line:
                    delegated = 1
                    continue

            if nesting == 0 and active == 1:
                active = 0
                dbg_print("----------------------------------------------------------------------")
                for exception_line in exception_block:
                    dbg_print(exception_line.rstrip())
                if runtimed == 0 and rethrown == 0 and delegated == 0:
                    if options.working or options.print_swallowed:
                        print f.name
                        for exception_line in exception_block:
                            print exception_line.rstrip()
                        if options.working:
                            sys.exit(-1)
                    dbg_print("SWALLOWED")
                    total_missed += 1
                elif runtimed == 1:
                    dbg_print("RUNTIMED")
                    total_runtimed += 1
                elif rethrown == 1:
                    dbg_print("RETHROWN")
                    total_rethrown += 1
                elif delegated == 1:
                    dbg_print("DELEGATED")
                    total_delegated += 1
                exception_block = []

                runtimed = 0
                rethrown = 0

    cond_print("Exceptions analyzed by file:")
    for key in sorted(file_count.iterkeys()):
        cond_print("   " + key + ": " + str(file_count[key]))
    cond_print("----------------------------------------------------------------------")
    cond_print("Total caught and rethrown as something other than Runtime: " + str(total_rethrown))
    cond_print("Total caught and rethrown as Runtime: " + str(total_runtimed))
    cond_print("Total Swallowed: " + str(total_missed))
    cond_print("Total delegated to JVMStabilityInspector: " + str(total_delegated))
    cond_print("Total 'catch (Throwable ...)' analyzed: " + str(total_throwable_analyzed))
    cond_print("Total 'catch (Exception ...)' analyzed: " + str(total_exception_analyzed))
    cond_print("Total catch clauses analyzed: " + str(total_catch_analyzed))

    if options.working:
        print 'Check complete.  No swallowed exceptions found.'
    sys.exit(total_missed)

def cond_print(input):
    if options.verbose == True:
        print input

def dbg_print(input):
    if options.debug == True:
        print input

main()

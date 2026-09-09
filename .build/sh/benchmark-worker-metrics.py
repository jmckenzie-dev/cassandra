# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Run inside the dev container; compare original eager, current eager, and lazy IDs."""
import argparse
import collections
import datetime
import json
import os
import statistics
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--original-jar', type=Path)
    parser.add_argument('--summarize', type=Path)
    parser.add_argument('--forks', type=int, default=3)
    args = parser.parse_args()
    if args.summarize:
        return summarize(args.summarize)
    if args.original_jar is None or not args.original_jar.is_file() or args.forks < 1:
        parser.error('An existing original JAR and a positive fork count are required')
    Path('logs').mkdir(exist_ok=True)
    output = Path('logs') / (datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '-worker-metrics-benchmark')
    samples = []
    with output.with_suffix('.log').open('w') as log:
        def emit(line):
            print(line, end='', flush=True)
            log.write(line)
            log.flush()

        for fork in range(args.forks):
            for workers in (8, 64):
                for access in ('shared', 'partitioned'):
                    modes = ('original', 'eager', 'lazy') if fork % 2 == 0 else ('lazy', 'eager', 'original')
                    for mode in modes:
                        env = os.environ.copy()
                        env['PROFILE_SKIP_BUILD'] = 'true'
                        env['PROFILE_MAIN_CLASS'] = 'org.apache.cassandra.metrics.WorkerMetricResidencyBenchmark'
                        env['PROFILE_LAZY_METRIC_IDS'] = str(mode == 'lazy').lower()
                        env['PROFILE_JAR'] = str(args.original_jar.resolve() if mode == 'original'
                                                 else Path('build/apache-cassandra-7.0-SNAPSHOT.jar').resolve())
                        command = ['env']
                        command.extend(f'{key}={env[key]}' for key in ('PROFILE_SKIP_BUILD', 'PROFILE_MAIN_CLASS',
                                                                     'PROFILE_LAZY_METRIC_IDS', 'PROFILE_JAR'))
                        command.extend(['.build/sh/ai-profile-many-tables', str(workers), access, mode])
                        emit(json.dumps({'fork': fork, 'command': command}) + '\n')
                        with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                              text=True, env=env) as process:
                            for line in process.stdout:
                                emit(line)
                                for prefix in ('recording,', 'residency,', 'construction,', 'verified_count='):
                                    if prefix in line:
                                        samples.append({'fork': fork, 'mode': mode, 'workers': workers,
                                                        'access': access, 'result': line[line.index(prefix):].strip()})
                            status = process.wait()
                        emit(f'case_exit_status={status}\n')
                        output.with_suffix('.json').write_text(json.dumps(samples, indent=2) + '\n')
                        if status:
                            return status
        emit(f'Output: {output.with_suffix(".json")}\n')
    return 0


def summarize(path):
    groups = collections.defaultdict(lambda: collections.defaultdict(list))
    for row in json.loads(path.read_text()):
        group = groups[(row['workers'], row['access'], row['mode'])]
        fields = row['result'].split(',')
        if fields[0] == 'recording':
            values = dict(field.split('=') for field in fields[4:])
            round_number = int(values['round'])
            if round_number >= 3:
                group['threadNsPerOp'].append(float(values['thread_ns_per_op']))
                group['wallNsPerOp'].append(float(values['wall_ns']) / int(values['operations']))
                group['steadyBytesPerOp'].append(float(values['bytes_per_op']))
            elif round_number == 0:
                group['firstUseThreadNsPerOp'].append(float(values['thread_ns_per_op']))
                group['firstUseBytesPerOp'].append(float(values['bytes_per_op']))
        elif fields[0] == 'residency':
            values = dict(field.split('=') for field in fields[4:])
            for key, value in values.items():
                group[key].append(int(value))
        elif fields[0] == 'construction':
            group['constructionNs'].append(int(fields[4]))
            group['constructionBytes'].append(int(fields[5]))
    summary = [{'workers': workers, 'access': access, 'mode': mode,
                'statistics': {key: {'n': len(values), 'mean': statistics.mean(values),
                                     'stdev': statistics.stdev(values) if len(values) > 1 else 0,
                                     'median': statistics.median(values), 'min': min(values), 'max': max(values)}
                               for key, values in group.items()}}
               for (workers, access, mode), group in sorted(groups.items())]
    text = json.dumps(summary, indent=2) + '\n'
    output = Path('logs') / (datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f') + '-worker-metrics-summary')
    output.with_suffix('.json').write_text(text)
    with output.with_suffix('.log').open('w') as log:
        for row in summary:
            stats = row['statistics']
            line = (f"{row['workers']} {row['access']} {row['mode']}: "
                    f"thread ns/op {stats['threadNsPerOp']['mean']:.3f} +/- {stats['threadNsPerOp']['stdev']:.3f}, "
                    f"wall ns/op {stats['wallNsPerOp']['mean']:.3f} +/- {stats['wallNsPerOp']['stdev']:.3f}, "
                    f"worker slots {stats['worker_slots']['mean']:.1f}, heap B {stats['heap_bytes']['mean']:.0f}")
            print(line)
            log.write(line + '\n')
        print('Output:', output.with_suffix('.json'))
        log.write(f'Output: {output.with_suffix(".json")}\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())

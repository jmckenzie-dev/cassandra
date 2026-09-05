#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for
# additional information regarding copyright ownership. The ASF licenses this
# file to you under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Summarize matched real-table runs after ai-compare-resident-metrics completes."""
import collections
import csv
import datetime
import json
import logging
import pathlib
import statistics
import sys


def analyze(batch):
    with (batch / 'runs.tsv').open() as manifest:
        rows = list(csv.DictReader(manifest, delimiter='\t'))
    if not rows or len(rows) % 4:
        raise ValueError('Expected complete legacy/optimized pairs for both scenarios')
    groups = collections.defaultdict(list)
    for row in rows:
        if row['status'] != '0':
            raise ValueError('Failed run: ' + row['summary'])
        path = pathlib.Path(row['summary'])
        data = json.loads(path.read_text())
        if data['failure'] or data['failedWriteRequests']:
            raise ValueError('Workload failure: ' + str(path))
        if not 1 <= data['tables'] <= 1000 or data['scenario'] != row['scenario']:
            raise ValueError('Workload mismatch: ' + str(path))
        optimized = row['mode'] == 'optimized'
        if data['optimizedMetricsEnabled'] != optimized or data['effectiveConfiguration']['optimizedMetricsEnabled'] != optimized:
            raise ValueError('Metrics implementation mismatch: ' + str(path))
        for phase in data['phases']:
            if phase['errors']:
                raise ValueError('Phase failure: ' + str(path))
        settled = data['checkpoints']['settled']
        if not settled['settledPostGc']:
            raise ValueError('Missing post-GC checkpoint: ' + str(path))
        if settled['live_memtables'] != data['tables'] or settled['dirty_memtables'] or settled['flushing_memtables']:
            raise ValueError('Tables are not clean and resident: ' + str(path))
        expected_writes = 0 if row['scenario'] == 'never-written' else data['tables'] * data['rowsPerTablePerCycle'] * data['cycles']
        if data['completedWrites'] != expected_writes or data['completedReadQueries'] < data['tables']:
            raise ValueError('Incomplete workload: ' + str(path))
        create = next(phase for phase in data['phases'] if phase['name'] == '01-create')
        groups[(row['mode'], row['scenario'])].append({
            'summary': str(path), 'tables': data['tables'],
            'baselineHeapBytes': data['checkpoints']['baseline']['heapUsedBytes'],
            'createdHeapBytes': data['checkpoints']['created']['heapUsedBytes'],
            'settledHeapBytes': settled['heapUsedBytes'],
            'createSeconds': create['elapsedNanos'] / 1e9,
            'writes': data['completedWrites'], 'reads': data['completedReadQueries'],
            'sstables': settled['sstables'],
        })
    expected_groups = {(mode, scenario) for mode in ('legacy', 'optimized')
                       for scenario in ('never-written', 'written-flushed')}
    if set(groups) != expected_groups or len({len(values) for values in groups.values()}) != 1:
        raise ValueError('Unequal comparison groups')
    report = {'batch': str(batch), 'groups': {}}
    for key, values in sorted(groups.items()):
        measurements = {}
        for metric in ('baselineHeapBytes', 'createdHeapBytes', 'settledHeapBytes', 'createSeconds'):
            samples = [row[metric] for row in values]
            measurements[metric] = {'median': statistics.median(samples), 'min': min(samples), 'max': max(samples)}
        report['groups']['/'.join(key)] = {'runs': values, 'measurements': measurements}
    return report


def main():
    stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')
    directory = pathlib.Path('logs')
    directory.mkdir(exist_ok=True)
    output = directory / (stamp + '-analyze-resident-metrics.json')
    logging.basicConfig(level=logging.INFO, format='%(message)s',
                        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(output.with_suffix('.log'))])
    try:
        if len(sys.argv) != 2:
            raise ValueError('Supply one completed comparison batch directory')
        report = analyze(pathlib.Path(sys.argv[1]))
        output.write_text(json.dumps(report, indent=2) + '\n')
        logging.info(json.dumps(report, indent=2))
        logging.info('Report: %s', output)
    except Exception:
        logging.exception('Resident metrics analysis failed')
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())

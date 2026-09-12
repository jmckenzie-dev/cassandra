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
"""Read metric-ID occupancy from a captured heap; no Cassandra state is modified."""
import collections
import contextlib
import datetime
import json
from pathlib import Path
import struct
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hprof_reader import Hprof
from importlib import import_module

Tee = import_module('analyze-heap-ownership').Tee


def probe(path):
    with Hprof(path) as heap:
        def field(oid, key):
            return heap.values(oid).get(key, (2, 0))[1] if oid else 0

        def array(oid):
            kind, typ, count, offset = heap.objects[oid]
            assert (kind, typ) == (35, 11)
            return [value[0] for value in struct.iter_unpack('>q', heap.data[offset:offset + count * 8])]

        def string(oid):
            backing = field(oid, 'value')
            _, typ, size, offset = heap.objects[backing]
            assert typ == 8
            return heap.data[offset:offset + size].decode('utf-16-le' if field(oid, 'coder') else 'latin1')

        types = {}
        for cid in heap.classes:
            parent = cid
            while parent:
                name = heap.names.get(parent, '')
                if name.rsplit('.', 1)[-1] in ('ThreadLocalCounter', 'ThreadLocalHistogram', 'ThreadLocalMeter', 'GeometricThreadLocalMeter'):
                    types[cid] = name.rsplit('.', 1)[-1]
                    break
                parent = heap.classes[parent][0]

        owners = collections.defaultdict(lambda: collections.defaultdict(set))
        contexts = {}
        workers = []
        for oid, (kind, cid, _, _) in heap.objects.items():
            if kind != 33:
                continue
            name = heap.names[cid]
            loader = heap.loaders[cid]
            if cid in types:
                values = heap.values(oid)
                for key in ('metricId', 'countMetricId', 'uncountedMetricId',
                            'lazyMetricId', 'lazyCountMetricId', 'lazyUncountedMetricId'):
                    if key in values:
                        metric_id = values[key][1]
                        if metric_id < 2**31:
                            owners[loader][types[cid] + '.' + key].add(metric_id)
            if name == 'org.apache.cassandra.metrics.ThreadLocalMetrics':
                values = array(field(oid, 'counterValues'))
                nonzero = [i for i, value in enumerate(values) if value]
                contexts[oid] = {'loader': hex(loader), 'capacity': len(values), 'payloadBytes': 8 * len(values),
                                 'nonzeroSlots': len(nonzero), 'highestNonzeroSlot': max(nonzero, default=-1),
                                 'nonzeroFraction': len(nonzero) / len(values) if values else 0,
                                 'nonzeroValues': dict(collections.Counter(value for value in values if value)),
                                 'pagedArrayEstimates': {page: {'occupiedPages': len({i // page for i in nonzero}),
                                     'shallowBytes': len({i // page for i in nonzero}) * (16 + 8 * page)
                                         + ((16 + 4 * ((len(values) + page - 1) // page) + 7) // 8 * 8)}
                                     for page in (8, 16, 32, 64, 128, 256)}}
            if name.endswith('HeapOwnershipCensusHarness$Worker'):
                workers.append({'name': string(field(field(oid, 'thread'), 'name')),
                                'contextId': field(oid, 'counters'), 'recordedTables': field(oid, 'recordedTables')})

        statics = []
        for cid, values in heap.statics.items():
            if heap.names.get(cid) != 'org.apache.cassandra.metrics.ThreadLocalMetrics':
                continue
            loader = heap.loaders[cid]
            summary = array(field(values['summaryValues'][1], 'array'))
            by_type = owners[loader]
            ids = set().union(*by_type.values()) if by_type else set()
            statics.append({'loader': hex(loader), 'idGenerator': field(values['idGenerator'][1], 'value'),
                            'summaryCapacity': len(summary), 'summaryPayloadBytes': 8 * len(summary),
                            'liveObjectIdCount': len(ids), 'highestLiveObjectId': max(ids, default=-1),
                            'liveIdsByType': {key: len(ids) for key, ids in by_type.items()}})
        return {'heap': str(path), 'statics': statics, 'contexts': contexts,
                'workers': [{**row, 'counterArray': contexts.get(row['contextId'])} for row in workers],
                'limits': 'Zero-valued slots may be valid counters. Live-object IDs exclude cleaner-only lifecycle state. No capacity forecast assumes all zero slots can be deleted.'}


stamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')
Path('logs').mkdir(exist_ok=True)
out = Path('logs') / (stamp + '-worker-metric-probe')
with out.with_suffix('.log').open('w') as log, contextlib.redirect_stdout(Tee(sys.stdout, log)), contextlib.redirect_stderr(Tee(sys.stderr, log)):
    result = probe(Path(sys.argv[1]))
    out.with_suffix('.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'statics': result['statics'],
                      'workerArrays': [worker['counterArray'] for worker in result['workers']]}, indent=2))
    print('Output:', out.with_suffix('.json'))

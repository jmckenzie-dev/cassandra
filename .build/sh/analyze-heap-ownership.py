#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor
# license agreements. See the NOTICE file distributed with this work for additional
# information regarding copyright ownership. The ASF licenses this file to you
# under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Count indexed heap objects and bounded direct-reference ownership, not dominators."""

import argparse
import collections
import contextlib
import datetime
import functools
import json
from pathlib import Path
import re
import sys
import time
import traceback

from hprof_reader import Hprof


METRICS = "org.apache.cassandra.metrics."
CFS = "org.apache.cassandra.db.ColumnFamilyStore"
SCHEMA = "org.apache.cassandra.schema."
PRIMITIVES = {4: "[Z", 5: "[C", 6: "[F", 7: "[D", 8: "[B", 9: "[S", 10: "[I", 11: "[J"}
HISTORY_TYPES = {8: ("byte", 1), 9: ("short", 2), 10: ("int", 4), 11: ("long", 8)}
CFS_FIELDS = {"data", "readOrdering", "sstableIdGenerator", "indexManager", "viewManager",
              "minCompactionThreshold", "maxCompactionThreshold", "crcCheckChance",
              "compactionStrategyManager", "directories", "writeHandler", "streamManager",
              "repairManager", "topPartitions", "sstableImporter", "compressionDictionaryManager",
              "partitionKeySetIgnoreGcGrace", "diskBoundaryManager", "cachedShardBoundaries",
              "paxosRepairHistory"}
BACK_EDGES = {"this$0", "val$cfs", "cfs", "keyspace", "schema", "parents", "children", "all", "global",
              "executor", "scheduler", "subscribers", "classLoader", "contextClassLoader"}
COLLECTIONS = ("java.util.Hash", "java.util.LinkedHash", "java.util.WeakHash", "java.util.Tree", "java.util.Array",
               "java.util.Collections$", "java.util.ImmutableCollections$", "java.util.EnumMap",
               "java.util.concurrent.ConcurrentHashMap", "java.util.concurrent.ConcurrentSkipList",
               "java.util.concurrent.CopyOnWriteArray", "java.util.concurrent.atomic.",
               "com.google.common.collect.")
LEAVES = ("java.lang.String", "java.lang.Integer", "java.lang.Long", "java.lang.Double",
          "java.lang.Boolean", "java.lang.Object", "java.net.", "java.nio.HeapByteBuffer")


def history_array_storage(record):
    if record is None:
        return "null", 0
    kind, typ, length, _ = record
    if kind not in (35, 195) or typ not in HISTORY_TYPES:
        raise ValueError("JMX history must be null or a byte, short, int, or long array")
    name, width = HISTORY_TYPES[typ]
    return name, length * width


def histogram(path):
    result = collections.defaultdict(lambda: {"count": 0, "bytes": 0})
    if path:
        for line in path.read_text().splitlines():
            match = re.match(r"\s*\d+:\s+(\d+)\s+(\d+)\s+(\S+)", line)
            if match:
                count, size, name = match.groups()
                row = result[name.replace("/", ".")]
                row["count"] += int(count)
                row["bytes"] += int(size)
        if not result:
            raise ValueError(f"No class histogram records in {path}")
    return dict(result)


def category(name):
    if name.startswith("["):
        return "primitive_arrays" if len(name) == 2 else "reference_arrays"
    if name == "java.lang.String":
        return "strings"
    if name.startswith(("javax.management.", "com.sun.jmx.")):
        return "jmx_infrastructure"
    if name.startswith(METRICS):
        return "metric_implementations"
    if name.startswith(SCHEMA):
        return "schema"
    if "ThreadLocal" in name or name == "io.netty.util.internal.InternalThreadLocalMap":
        return "thread_local_infrastructure"
    if name.startswith(("org.apache.cassandra.db.", "org.apache.cassandra.io.sstable.",
                        "org.apache.cassandra.index.", "org.apache.cassandra.repair.")):
        return "table_and_storage_runtime"
    if name.startswith(COLLECTIONS):
        return "collections_and_atomics"
    return "other"


def inspect(args):
    reference = histogram(args.class_histogram)
    with Hprof(args.heap) as heap:
        def align(value):
            return (value + args.alignment - 1) // args.alignment * args.alignment

        def name(oid):
            kind, cid, _, _ = heap.objects[oid]
            return PRIMITIVES[cid] if kind in (35, 195) else heap.names[cid]

        @functools.lru_cache(maxsize=200000)
        def fields(oid):
            return heap.values(oid)

        def refs(oid):
            kind = heap.objects[oid][0]
            if kind == 34:
                return [("[]", target) for target in heap.references(oid) if target]
            if kind == 33:
                return [(key, value) for key, (typ, value) in fields(oid).items() if typ == 2 and value]
            return []

        def field(oid, key):
            return fields(oid).get(key, (2, 0))[1]

        def string(oid):
            if not oid:
                return None
            value = field(oid, "value")
            kind, typ, length, start = heap.objects[value]
            if kind != 35:
                raise ValueError("String backing is not a primitive array")
            if typ == 5:
                return heap.data[start:start + length * 2].decode("utf-16-be")
            if typ != 8:
                raise ValueError("Unsupported String backing")
            codec = "latin1" if not field(oid, "coder") else "utf-16-" + ("le" if args.byte_order == "little" else "be")
            return heap.data[start:start + length].decode(codec)

        class_sizes = collections.defaultdict(set)
        for cid, (_, size, _) in heap.classes.items():
            class_sizes[heap.names[cid]].add(size)

        @functools.lru_cache(maxsize=None)
        def estimated_instance_size(cid):
            size = 8 + args.class_pointer_bytes
            while cid:
                cid, _, definitions = heap.classes[cid]
                size += sum(args.reference_bytes if typ == 2 else heap.sizes[typ] for _, typ in definitions)
            return align(size)

        calibrated = {}
        for cls, row in reference.items():
            if not cls.startswith("[") and row["count"] and row["bytes"] % row["count"] == 0:
                size = row["bytes"] // row["count"]
                if size % args.alignment == 0 and len(class_sizes.get(cls, ())) == 1:
                    calibrated[cls] = size

        array_header = (8 + args.class_pointer_bytes + 4 + 7) // 8 * 8

        def shallow(oid):
            kind, cid, length, _ = heap.objects[oid]
            if kind == 34:
                return align(array_header + length * args.reference_bytes)
            if kind in (35, 195):
                return align(array_header + length * heap.sizes[cid])
            return calibrated.get(heap.names[cid], estimated_instance_size(cid))

        def totals(ids):
            counts = collections.defaultdict(lambda: {"count": 0, "shallowBytes": 0})
            total = 0
            for oid in ids:
                size = shallow(oid)
                total += size
                row = counts[name(oid)]
                row["count"] += 1
                row["shallowBytes"] += size
            return {"objects": len(ids), "shallowBytes": total,
                    "classes": dict(sorted(counts.items(), key=lambda item: -item[1]["shallowBytes"]))}

        census = totals(heap.objects)
        for cls, row in census["classes"].items():
            row["category"] = category(cls)
            row["sizeMethod"] = "array_layout" if cls.startswith("[") else "histogram_calibrated" if cls in calibrated else "estimated_field_layout"
            if not cls.startswith("["):
                row["hprofClassSizes"] = sorted(class_sizes[cls])
            if cls in reference:
                row["histogramCount"] = reference[cls]["count"]
                row["histogramBytes"] = reference[cls]["bytes"]
                row["countDifference"] = row["count"] - row["histogramCount"]
                row["byteDifference"] = row["shallowBytes"] - row["histogramBytes"]
        categories = collections.defaultdict(lambda: {"objects": 0, "shallowBytes": 0})
        for row in census["classes"].values():
            categories[row["category"]]["objects"] += row["count"]
            categories[row["category"]]["shallowBytes"] += row["shallowBytes"]
        census["categories"] = dict(categories)
        coverage = collections.defaultdict(lambda: {"objects": 0, "shallowBytes": 0})
        for row in census["classes"].values():
            coverage[row["sizeMethod"]]["objects"] += row["count"]
            coverage[row["sizeMethod"]]["shallowBytes"] += row["shallowBytes"]
        for row in coverage.values():
            row["objectFraction"] = row["objects"] / census["objects"] if census["objects"] else 0
            row["byteFraction"] = row["shallowBytes"] / census["shallowBytes"] if census["shallowBytes"] else 0
        census["sizeCoverage"] = dict(coverage)
        census["histogramOnlyClasses"] = {cls: row for cls, row in reference.items() if cls not in census["classes"]}
        census["classDumpRecordsWithoutInstanceRecords"] = len(set(heap.classes) - set(heap.objects))

        claims = collections.defaultdict(set)
        walks = []
        metric_owners = collections.defaultdict(set)
        tables = {}

        def allowed(cls, role):
            if cls.startswith("[") or cls.startswith(COLLECTIONS + LEAVES):
                return True
            if role == "table_metrics":
                return cls.startswith((METRICS, "com.codahale.metrics."))
            if role == "metric_names":
                return cls.startswith((METRICS + "CassandraMetricsRegistry$MetricName", "javax.management.ObjectName"))
            if role == "jmx":
                return cls.startswith((METRICS + "CassandraMetricsRegistry$Jmx",
                                       METRICS + "CassandraMetricsRegistry$DynamicJmx", "javax.management.", "com.sun.jmx."))
            if role in ("threadlocal_state", "meter_state"):
                return cls.startswith((METRICS + "ThreadLocalMetrics", METRICS + "ThreadLocalMeter$",
                                       METRICS + "GeometricThreadLocalMeter$", "java.lang.ref.",
                                       "org.apache.cassandra.utils.Free"))
            if role == "scrape_state":
                return (cls.startswith(METRICS) and "Snapshot" in cls
                        or cls.startswith("org.apache.cassandra.db.virtual.model."))
            if role == "schema":
                return cls.startswith((SCHEMA, "org.apache.cassandra.db.marshal.", "org.apache.cassandra.cql3.",
                                       "org.apache.cassandra.utils.btree."))
            if role == "cfs_nonmetrics":
                return cls.startswith(("org.apache.cassandra.db.", "org.apache.cassandra.io.sstable.",
                                       "org.apache.cassandra.index.", "org.apache.cassandra.streaming.",
                                       "org.apache.cassandra.utils.", "org.apache.cassandra.repair.",
                                       "com.github.benmanes.caffeine."))
            return False

        def walk(roots, role, owner):
            stack = [(oid, 0) for oid in roots if oid in heap.objects]
            seen = {}
            stopped = collections.Counter()
            depth_stops = set()
            while stack:
                oid, depth = stack.pop()
                if oid in seen and seen[oid] <= depth:
                    continue
                seen[oid] = depth
                cls = name(oid)
                claims[oid].add((role, owner))
                if depth == args.max_depth:
                    if refs(oid):
                        depth_stops.add(oid)
                    continue
                depth_stops.discard(oid)
                for key, target in refs(oid):
                    if target not in heap.objects:
                        continue
                    target_class = name(target)
                    if key in BACK_EDGES or key in ("referent", "discovered", "queue", "clock", "last", "metadata", "metric"):
                        continue
                    if role == "table_metrics" and cls.endswith("LatencyMetrics") and key not in ("latency", "totalLatency"):
                        continue
                    if target_class in (CFS, "org.apache.cassandra.db.Keyspace", METRICS + "TableMetrics"):
                        continue
                    if "Executor" in target_class or "ClassLoader" in target_class or target_class in ("java.lang.Class", "java.lang.Thread"):
                        continue
                    if not allowed(target_class, role):
                        stopped[target_class] += 1
                        continue
                    stack.append((target, depth + 1))
            walks.append({"role": role, "owner": owner, "rootCount": len(roots), "objects": len(seen),
                          "depthFrontierObjects": len(depth_stops), "excludedTargetClasses": dict(stopped)})
            return set(seen)

        for oid, (kind, cid, _, _) in heap.objects.items():
            if kind != 33 or heap.names[cid] != CFS:
                continue
            metadata = field(oid, "metadata")
            keyspace, table = string(field(metadata, "keyspace")), string(field(metadata, "name"))
            if keyspace != args.keyspace:
                continue
            if table in tables:
                raise ValueError(f"Multiple live CFS objects for {keyspace}.{table}")
            tables[table] = {"objectId": hex(oid), "metricRoot": hex(field(oid, "metric"))}
            metric_ids = walk([field(oid, "metric")], "table_metrics", table)
            for target in metric_ids:
                if name(target).startswith((METRICS, "com.codahale.metrics.")):
                    metric_owners[target].add(table)
            walk([metadata], "schema", table)
            claims[oid].add(("cfs_nonmetrics", table))
            walk([target for key, target in refs(oid) if key in CFS_FIELDS], "cfs_nonmetrics", table)
        if len(tables) != args.expected_tables:
            raise ValueError(f"Expected {args.expected_tables} tables in {args.keyspace}, found {len(tables)}")

        thread_contexts = []
        census_workers = []
        wrappers = []
        named_owner_conflicts = []
        table_wrappers = collections.defaultdict(set)
        table_last_arrays = collections.defaultdict(set)
        table_history_wrappers = collections.defaultdict(collections.Counter)
        object_name_owners = collections.defaultdict(set)
        registry_roots = []
        for oid, (kind, cid, _, _) in heap.objects.items():
            if kind != 33:
                continue
            cls = heap.names[cid]
            if cls.startswith((METRICS + "CassandraMetricsRegistry$Jmx", METRICS + "CassandraMetricsRegistry$DynamicJmx")):
                object_name = field(oid, "objectName")
                canonical = string(field(object_name, "_canonicalName") or field(object_name, "canonicalName"))
                properties = dict(re.findall(r'(?:^|,)([^=,]+)=("(?:\\.|[^"\\])*"|[^,]*)',
                                             canonical.split(":", 1)[1])) if canonical else {}
                properties = {key: re.sub(r"\\(.)", r"\1", value.strip('"')) for key, value in properties.items()}
                user_keyspace = properties.get("keyspace") == args.keyspace
                named_table = properties.get("scope", properties.get("table")) if user_keyspace else None
                metric_tables = metric_owners.get(field(oid, "metric"), set())
                owners = {named_table} if named_table in tables else metric_tables or {"global_or_other"}
                if named_table in tables and metric_tables and metric_tables != {named_table}:
                    named_owner_conflicts.append({"objectName": canonical, "metricOwners": sorted(metric_tables)})
                last = field(oid, "last")
                history_type = history_array_storage(heap.objects[last] if last else None)[0] if "last" in fields(oid) else None
                for owner in owners:
                    walk([oid], "jmx", owner)
                    if last:
                        walk([last], "scrape_state", owner)
                    if owner in tables:
                        table_wrappers[owner].add(oid)
                        object_name_owners[field(oid, "objectName")].add(owner)
                        if history_type is not None:
                            table_history_wrappers[owner][history_type] += 1
                        if last:
                            table_last_arrays[owner].add(last)
                wrappers.append({"class": cls, "owners": sorted(owners), "userKeyspace": user_keyspace,
                                 "namedTable": named_table, "hasLastValues": bool(last), "historyType": history_type})
            elif cls == METRICS + "CassandraMetricsRegistry$MetricName":
                walk([oid], "metric_names", "global_or_shared")
            elif cls == METRICS + "CassandraMetricsRegistry":
                registry_roots.append(field(oid, "metrics"))
            elif cls == METRICS + "ThreadLocalMetrics":
                walk([oid], "threadlocal_state", "shared_across_tables")
                counter = field(oid, "counterValues")
                thread_contexts.append({"objectId": hex(oid), "counterArrayId": hex(counter),
                                        "counterCapacity": heap.objects[counter][2] if counter else 0})
            elif cls in ("org.apache.cassandra.db.virtual.model.HistogramMetricRow",
                         "org.apache.cassandra.db.virtual.model.TimerMetricRow"):
                walk([oid], "scrape_state", "global_or_other")
            elif cls.endswith("HeapOwnershipCensusHarness$Worker"):
                thread = field(oid, "thread")
                counters = field(oid, "counters")
                census_workers.append({"threadName": string(field(thread, "name")),
                                       "threadId": field(thread, "tid"), "counterContextId": hex(counters),
                                       "recordedTables": field(oid, "recordedTables")})
        static_roots = []
        for cid, values in heap.statics.items():
            cls = heap.names.get(cid, "")
            for key, (typ, oid) in values.items():
                if typ != 2 or oid not in heap.objects:
                    continue
                role = None
                if cls == METRICS + "ThreadLocalMetrics" and key in ("allThreadLocalMetrics", "summaryValues", "phantomReferences", "freeMetricIdSetTracker"):
                    role = "threadlocal_state"
                if cls in (METRICS + "ThreadLocalMeter", METRICS + "GeometricThreadLocalMeter") and key in ("rates", "allMeters", "freeRateGroupIdSet"):
                    role = "meter_state"
                if cls == METRICS + "TableMetrics" and key == "ALL_TABLE_METRICS":
                    registry_roots.append(oid)
                if role:
                    walk([oid], role, "shared_across_tables")
                    static_roots.append({"class": cls, "field": key, "objectId": hex(oid), "role": role})
        walk(registry_roots, "registry_structure", "shared_across_tables")

        # JMX repository objects point toward wrappers; count their own supporting graph separately.
        jmx_roots = [oid for oid, (kind, cid, _, _) in heap.objects.items()
                     if kind == 33 and heap.names[cid].startswith("com.sun.jmx.mbeanserver.")]
        walk(jmx_roots, "jmx", "global_or_shared")

        property_maps = set()
        property_objects = set()
        table_property_maps = collections.defaultdict(set)
        table_property_objects = collections.defaultdict(set)
        property_name_count = 0
        for oid, (kind, cid, _, _) in heap.objects.items():
            if kind != 33 or heap.names[cid] != "javax.management.ObjectName":
                continue
            cache = field(oid, "_propertyList")
            if not cache:
                continue
            property_name_count += 1
            property_maps.add(cache)
            for owner in object_name_owners.get(oid, {"global_or_other"}):
                reached = walk([cache], "jmx_property_lists", owner)
                property_objects.update(reached)
                if owner in tables:
                    table_property_maps[owner].add(cache)
                    table_property_objects[owner].update(reached)

        partitions = collections.defaultdict(set)
        groups = collections.defaultdict(set)
        per_table = collections.defaultdict(lambda: {"exclusiveObjects": 0, "exclusiveShallowBytes": 0,
                                                      "sharedClaimedObjects": 0, "sharedClaimedShallowBytes": 0})
        for oid in heap.objects:
            owners = claims.get(oid, set())
            roles = {role for role, _ in owners}
            partition = next(iter(roles)) if len(roles) == 1 else "shared_between_roles" if roles else "unclaimed"
            partitions[partition].add(oid)
            for role in roles:
                groups[role].add(oid)
            for table in {owner for _, owner in owners} & tables.keys():
                exclusive = {owner for _, owner in owners} == {table}
                prefix = "exclusive" if exclusive else "sharedClaimed"
                per_table[table][prefix + "Objects"] += 1
                per_table[table][prefix + "ShallowBytes"] += shallow(oid)
        partition_summary = {key: totals(ids) for key, ids in partitions.items()}
        assert sum(row["objects"] for row in partition_summary.values()) == census["objects"]
        assert sum(row["shallowBytes"] for row in partition_summary.values()) == census["shallowBytes"]
        return {"heap": str(args.heap.resolve()), "classHistogram": str(args.class_histogram.resolve()) if args.class_histogram else None,
                "layout": {"referenceBytes": args.reference_bytes, "classPointerBytes": args.class_pointer_bytes,
                           "alignment": args.alignment, "markWordBytes": 8, "arrayHeaderBytes": array_header,
                           "byteOrder": args.byte_order, "source": "explicit arguments; not auto-detected"},
                "limits": ["Shallow bytes are not retained size. Bounded field reachability is not proof of exclusive GC ownership.",
                           "The disjoint census covers indexed instance and array records. CLASS_DUMP mirrors and native/metaspace bytes are excluded.",
                           "Instance sizes use homogeneous class histogram averages where available; otherwise compressed fields plus header/alignment are estimates that omit VM padding such as Contended.",
                           "Histogram and HPROF can be separate instants. Per-class count/byte differences are reported rather than hidden.",
                           "Ownership groups overlap; only ownershipPartition and class categories are disjoint.",
                           "Per-table exclusive means exclusive among these bounded claims, not among all incoming heap references.",
                           "Traversal excludes back-references, reference referents, clocks, executors, class loaders, and unapproved classes; frontier counts report depth limits.",
                           "Thread-local and meter arrays remain shared. Whole arrays are never charged independently to each table."],
                "keyspace": args.keyspace, "tableCount": len(tables), "maxTraversalDepth": args.max_depth,
                "census": census, "ownershipPartition": partition_summary,
                "ownershipGroupsOverlapping": {key: totals(ids) for key, ids in groups.items()},
                "tables": {table: {**row, **per_table[table],
                                    "jmx": {"wrapperObjects": len(table_wrappers[table]),
                                            "wrapperShallowBytes": sum(shallow(oid) for oid in table_wrappers[table]),
                                            "lastArrays": len(table_last_arrays[table]),
                                            "lastArrayPayloadBytes": sum(history_array_storage(heap.objects[oid])[1] for oid in table_last_arrays[table]),
                                            "lastArrayShallowBytes": sum(shallow(oid) for oid in table_last_arrays[table]),
                                            "lastByteArrays": sum(history_array_storage(heap.objects[oid])[0] == "byte" for oid in table_last_arrays[table]),
                                            "lastShortArrays": sum(history_array_storage(heap.objects[oid])[0] == "short" for oid in table_last_arrays[table]),
                                            "lastIntArrays": sum(history_array_storage(heap.objects[oid])[0] == "int" for oid in table_last_arrays[table]),
                                            "lastLongArrays": sum(history_array_storage(heap.objects[oid])[0] == "long" for oid in table_last_arrays[table]),
                                            "nullHistoryWrappers": table_history_wrappers[table]["null"],
                                            "objectNamePropertyMaps": len(table_property_maps[table]),
                                            "propertyMapGraphObjects": len(table_property_objects[table]),
                                            "propertyMapGraphShallowBytes": sum(shallow(oid) for oid in table_property_objects[table])}}
                           for table, row in tables.items()},
                "threadLocalContexts": thread_contexts, "censusWorkers": census_workers, "metricStaticRoots": static_roots,
                "jmxWrapperCounts": dict(collections.Counter(row["class"] for row in wrappers)),
                "jmxUserKeyspaceWrappers": sum(row["userKeyspace"] for row in wrappers),
                "jmxUserKeyspaceWrappersWithoutTableScope": sum(row["userKeyspace"] and row["namedTable"] not in tables for row in wrappers),
                "jmxNamedOwnerConflicts": named_owner_conflicts,
                "jmxWrappersWithLastValues": sum(row["hasLastValues"] for row in wrappers),
                "jmxHistoryWrappersByType": dict(collections.Counter(row["historyType"] for row in wrappers if row["historyType"] is not None)),
                "objectNamePropertyLists": {"objectNamesWithCache": property_name_count,
                                            "uniqueMaps": len(property_maps), **totals(property_objects)},
                "scrapeInterpretation": "Check summary.json inspectNameProperties for the scrape mode. ObjectName key-property inspection can create property maps; do not attribute that growth to getAttribute alone.",
                "traversals": walks}


class Tee:
    def __init__(self, console, log):
        self.console, self.log = console, log

    def write(self, value):
        self.console.write(value)
        self.log.write(value)
        self.flush()

    def flush(self):
        self.console.flush()
        self.log.flush()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("heap", type=Path)
    parser.add_argument("--keyspace", default="heap_census")
    parser.add_argument("--expected-tables", type=int, required=True)
    parser.add_argument("--class-histogram", type=Path)
    parser.add_argument("--reference-bytes", type=int, choices=(4, 8), default=4)
    parser.add_argument("--class-pointer-bytes", type=int, choices=(4, 8), default=4)
    parser.add_argument("--alignment", type=int, choices=(8, 16, 32, 64, 128, 256), default=8)
    parser.add_argument("--byte-order", choices=("little", "big"), default="little")
    parser.add_argument("--max-depth", type=int, default=12)
    args = parser.parse_args()
    if not 0 <= args.expected_tables <= 1000 or not 1 <= args.max_depth <= 64:
        raise ValueError("Expected tables must be 0..1000 and max depth 1..64")
    if args.class_histogram is None:
        candidate = args.heap.with_name("histogram-" + args.heap.stem + ".txt")
        if candidate.is_file():
            args.class_histogram = candidate
    print(f"Reading {args.heap}; histogram={args.class_histogram}")
    started = time.monotonic()
    result = inspect(args)
    result["elapsedSeconds"] = time.monotonic() - started
    destination = Path("logs") / f"{datetime.datetime.now():%Y%m%d-%H%M%S-%f}-{args.heap.stem}-heap-ownership.json"
    destination.write_text(json.dumps(result, indent=2) + "\n")
    concise = {"heap": result["heap"], "tableCount": result["tableCount"], "layout": result["layout"],
               "elapsedSeconds": result["elapsedSeconds"],
               "indexedObjects": result["census"]["objects"], "indexedShallowBytes": result["census"]["shallowBytes"],
               "sizeCoverage": result["census"]["sizeCoverage"],
               "classCategories": result["census"]["categories"],
               "ownershipPartition": {key: {k: v for k, v in row.items() if k != "classes"}
                                      for key, row in result["ownershipPartition"].items()},
               "largestClasses": dict(list(result["census"]["classes"].items())[:20]),
               "threadLocalContexts": len(result["threadLocalContexts"]),
               "jmxWrappersWithLastValues": result["jmxWrappersWithLastValues"],
               "limits": result["limits"], "details": str(destination.resolve())}
    print(json.dumps(concise, indent=2))


if __name__ == "__main__":
    logs = Path("logs")
    logs.mkdir(exist_ok=True)
    path = logs / f"{datetime.datetime.now():%Y%m%d-%H%M%S-%f}-analyze-heap-ownership.log"
    with path.open("x") as log, contextlib.redirect_stdout(Tee(sys.stdout, log)), contextlib.redirect_stderr(Tee(sys.stderr, log)):
        try:
            print(f"Analysis log: {path.resolve()}")
            main()
        except SystemExit:
            raise
        except Exception:
            traceback.print_exc()
            raise SystemExit(1)

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
"""Attribute reservoir counter payload to table metrics in a live HotSpot heap dump."""
import argparse
import collections
import contextlib
import datetime
import functools
import json
import pathlib
import sys
import traceback

from hprof_reader import Hprof


PREFIX = "org.apache.cassandra.metrics."
LEGACY = PREFIX + "DecayingEstimatedHistogramReservoir"
COMPACT = PREFIX + "CompactDecayingEstimatedHistogramReservoir"


def inspect(path, expected_tables=100, user_keyspace="memtable_residency"):
    with Hprof(path) as heap:
        fields = heap.values

        def name(oid):
            return heap.names[heap.objects[oid][1]]

        def ref(values, key):
            typ, oid = values.get(key, (2, 0))
            if typ != 2:
                raise ValueError(f"Expected reference field {key}, found type {typ}")
            return oid

        def string(oid):
            value = fields(oid)
            kind, typ, length, start = heap.objects[ref(value, "value")]
            if kind != 35 or typ != 8 or value["coder"][1] != 0:
                raise ValueError("Table-name decoding requires a Latin-1 HotSpot String")
            return heap.data[start:start + length].decode("latin1")

        @functools.lru_cache(maxsize=None)
        def array(oid):
            kind, typ, length, start = heap.objects[oid]
            if kind != 35 or typ not in (10, 11):
                raise ValueError(f"Expected int[] or long[] at {oid:#x}, found record/type {kind}/{typ}")
            width = heap.sizes[typ]
            return {"length": length, "cellBytes": width, "payloadBytes": length * width,
                    "nonzeroCells": sum(heap.u(start + i * width, width) != 0 for i in range(length))}

        def atomic_array(oid):
            if not oid:
                return set()
            if name(oid) == PREFIX + "AdaptiveCounterArray":
                oid = ref(fields(oid), "values")
            if name(oid) not in ("java.util.concurrent.atomic.AtomicLongArray",
                                 "java.util.concurrent.atomic.AtomicIntegerArray"):
                raise ValueError(f"Unsupported atomic counter array: {name(oid)}")
            values = fields(oid)
            backing = ref(values, "array")
            if not backing:
                raise ValueError(f"Missing atomic-array backing at {oid:#x}")
            array(backing)
            return {backing}

        def storage(oid, configured_stripes=1):
            if not oid:
                return {"arrays": set(), "active": set(), "directories": set(), "representation": "empty",
                        "physicalStripes": 0, "counterAllocatedStripes": 0}
            cls = name(oid)
            if cls == "java.util.concurrent.atomic.AtomicLongArray":
                arrays = atomic_array(oid)
                return {"arrays": arrays, "active": arrays, "directories": set(), "representation": "dense",
                        "physicalStripes": configured_stripes, "counterAllocatedStripes": configured_stripes}
            if cls not in (COMPACT + "$PagedBuckets", COMPACT + "$StripedBuckets"):
                raise ValueError(f"Unsupported reservoir counter storage: {cls}")
            values = fields(oid)
            dense = atomic_array(ref(values, "dense"))
            pages = ref(values, "pages")
            paged_arrays, directories = set(), set()
            if pages:
                directory = ref(fields(pages), "array")
                directories.add(directory)
                for page in heap.references(directory):
                    paged_arrays.update(atomic_array(page))
            arrays = dense | paged_arrays
            representation = "dense+pages" if dense and paged_arrays else "dense" if dense else "paged" if pages else "empty"
            striped = cls == COMPACT + "$StripedBuckets"
            result = {"arrays": arrays, "active": dense if dense else paged_arrays,
                      "directories": directories, "representation": representation,
                      "physicalStripes": 1 if striped or configured_stripes == 1 else None,
                      "counterAllocatedStripes": int(bool(arrays)) if striped or configured_stripes == 1 else None if arrays else 0}
            if striped:
                representations = {representation}
                secondary = ref(values, "secondary")
                if secondary:
                    directory = ref(fields(secondary), "array")
                    result["directories"].add(directory)
                    children = list(heap.references(directory))
                    if len(children) != configured_stripes - 1:
                        raise ValueError("Secondary stripe directory does not match configured stripe count")
                    for child in children:
                        if not child:
                            continue
                        extra = storage(child)
                        for key in ("arrays", "active", "directories"):
                            result[key].update(extra[key])
                        result["physicalStripes"] += extra["physicalStripes"]
                        result["counterAllocatedStripes"] += extra["counterAllocatedStripes"]
                        representations.add(extra["representation"])
                result["representation"] = "striped:" + ",".join(sorted(representations))
            return result

        reservoirs = {}
        class_counts = collections.Counter()
        for oid, (kind, cid, _, _) in heap.objects.items():
            if kind != 33:
                continue
            cls = heap.names[cid]
            if cls in (LEGACY, COMPACT) or cls.startswith((LEGACY + "$", COMPACT + "$")):
                class_counts[cls] += 1
            if cls not in (LEGACY, COMPACT):
                continue
            values = fields(oid)
            stripes = values["nStripes"][1]
            cumulative = storage(ref(values, "buckets"), stripes)
            decaying_fields = fields(ref(values, "decayingBuckets"))
            decay_field = "decayBuckets" if "decayBuckets" in decaying_fields else "values"
            decaying = storage(ref(decaying_fields, decay_field), stripes)
            offset = ref(values, "bucketOffsets")
            array(offset)
            reservoirs[oid] = {"class": cls, "offsets": offset, "stripes": stripes,
                               "cumulative": cumulative, "decaying": decaying,
                               "arrays": cumulative["arrays"] | decaying["arrays"],
                               "active": cumulative["active"] | decaying["active"],
                               "directories": cumulative["directories"] | decaying["directories"]}

        static_offsets = set()
        static_offset_fields = {}
        for cid, values in heap.statics.items():
            if heap.names.get(cid) not in (LEGACY, COMPACT):
                continue
            for key, (typ, oid) in values.items():
                if typ == 2 and oid and heap.objects.get(oid, ())[:2] == (35, 11):
                    static_offsets.add(oid)
                    static_offset_fields[f"{heap.names[cid]}@{cid:x}.{key}"] = {"objectId": hex(oid), **array(oid)}
        offset_owners = collections.defaultdict(set)
        for oid, row in reservoirs.items():
            offset_owners[row["offsets"]].add(oid)
        shared_offsets = static_offsets | {oid for oid, owners in offset_owners.items() if len(owners) > 1}

        def walk(oid, route, visited):
            if not oid or oid in visited:
                return {}
            if oid in reservoirs:
                return {oid: route}
            visited.add(oid)
            if heap.objects[oid][0] != 33:
                return {}
            cls = name(oid)
            value = fields(oid)
            if cls in (PREFIX + "TableMetrics$TableHistogram", PREFIX + "TableMetrics$TableTimer"):
                edges = ("cf",)
            elif cls == PREFIX + "LatencyMetrics":
                edges = ("latency",)
            elif cls == PREFIX + "ScalingReservoir":
                edges = ("delegate",)
            elif "histogram" in value and cls.startswith(PREFIX):
                edges = ("histogram",)
            elif "reservoir" in value and cls.startswith((PREFIX, "com.codahale.metrics.")):
                edges = ("reservoir",)
            else:
                return {}
            result = {}
            for edge in edges:
                result.update(walk(ref(value, edge), route + "." + edge, visited))
            return result

        def roots(oid):
            result = {}
            for key, (typ, target) in fields(oid).items():
                if typ == 2 and target:
                    result.update(walk(target, key, set()))
            return result

        all_tables = {}
        for oid, (kind, cid, _, _) in heap.objects.items():
            if kind != 33 or heap.names[cid] != "org.apache.cassandra.db.ColumnFamilyStore":
                continue
            value = fields(oid)
            metadata = fields(ref(value, "metadata"))
            key = (string(ref(metadata, "keyspace")), string(ref(metadata, "name")))
            if key in all_tables:
                raise ValueError(f"Multiple live ColumnFamilyStores for {key}; use a single-node settled heap")
            all_tables[key] = {"tableMetrics": roots(ref(value, "metric")), "trieMetrics": {}}
        for oid, (kind, cid, _, _) in heap.objects.items():
            if kind != 33 or heap.names[cid] != "org.apache.cassandra.db.memtable.TrieMemtable":
                continue
            value = fields(oid)
            metadata = fields(ref(value, "metadata"))
            key = (string(ref(metadata, "keyspace")), string(ref(metadata, "name")))
            if key in all_tables:
                all_tables[key]["trieMetrics"].update(roots(ref(value, "metrics")))

        def summarize(ids):
            arrays = {aid for oid in ids for aid in reservoirs[oid]["arrays"]}
            offsets = {reservoirs[oid]["offsets"] for oid in ids}
            directories = {aid for oid in ids for aid in reservoirs[oid]["directories"]}
            empty = {oid for oid in ids if not any(array(aid)["nonzeroCells"] for aid in reservoirs[oid]["active"])}
            empty_arrays = {aid for oid in empty for aid in reservoirs[oid]["arrays"]}
            zero_arrays = {aid for aid in arrays if not array(aid)["nonzeroCells"]}
            private_offsets = offsets - shared_offsets
            return {"reservoirs": len(ids), "classCounts": dict(collections.Counter(reservoirs[oid]["class"] for oid in ids)),
                    "emptyReservoirs": len(empty), "nonemptyReservoirs": len(ids) - len(empty),
                    "uniqueBackingArrays": len(arrays), "backingArrayPayloadBytes": sum(array(oid)["payloadBytes"] for oid in arrays),
                    "emptyBackingArrayPayloadBytes": sum(array(oid)["payloadBytes"] for oid in empty_arrays),
                    "zeroBackingArrays": len(zero_arrays), "zeroBackingArrayPayloadBytes": sum(array(oid)["payloadBytes"] for oid in zero_arrays),
                    "nonzeroBackingArrayPayloadBytes": sum(array(oid)["payloadBytes"] for oid in arrays - zero_arrays),
                    "nonzeroPhysicalCells": sum(array(oid)["nonzeroCells"] for oid in arrays),
                    "uniqueDirectoryArrays": len(directories),
                    "directoryReferenceSlots": sum(heap.objects[oid][2] for oid in directories),
                    "uniqueOffsetsArrays": len(offsets), "offsetsPayloadBytes": sum(array(oid)["payloadBytes"] for oid in offsets),
                    "privateOffsetsPayloadBytes": sum(array(oid)["payloadBytes"] for oid in private_offsets),
                    "sharedOffsetsPayloadBytes": sum(array(oid)["payloadBytes"] for oid in offsets & shared_offsets),
                    "offsetsArrayLengthCounts": dict(collections.Counter(array(oid)["length"] for oid in offsets)),
                    "backingArrayLengthCounts": dict(collections.Counter(array(oid)["length"] for oid in arrays)),
                    "backingArrayCellBytesCounts": dict(collections.Counter(array(oid)["cellBytes"] for oid in arrays)),
                    "backingArrayPayloadBytesByCellWidth": {
                        str(width): sum(array(oid)["payloadBytes"] for oid in arrays if array(oid)["cellBytes"] == width)
                        for width in (4, 8)},
                    "stripeCounts": dict(collections.Counter(reservoirs[oid]["stripes"] for oid in ids)),
                    "physicalStripeCountsBySide": {
                        side: {key: dict(collections.Counter("unavailable" if reservoirs[oid][side][key] is None
                                                           else str(reservoirs[oid][side][key]) for oid in ids))
                               for key in ("physicalStripes", "counterAllocatedStripes")}
                        for side in ("cumulative", "decaying")},
                    "storageRepresentations": dict(collections.Counter(reservoirs[oid][side]["representation"] for oid in ids for side in ("cumulative", "decaying")))}

        user_table_ids, user_trie_ids, other_table_ids = set(), set(), set()
        user_tables, all_user_arrays = [], set()
        per_field = collections.defaultdict(lambda: {"reservoirs": 0, "empty": 0, "payloadBytes": 0})
        for (keyspace, table), value in sorted(all_tables.items()):
            table_ids, trie_ids = set(value["tableMetrics"]), set(value["trieMetrics"])
            if keyspace != user_keyspace:
                other_table_ids.update(table_ids | trie_ids)
                continue
            if not table_ids:
                raise ValueError(f"No reservoir ownership found for user table {table}; inspect metric field routing")
            user_table_ids.update(table_ids)
            user_trie_ids.update(trie_ids)
            arrays = {aid for oid in table_ids | trie_ids for aid in reservoirs[oid]["arrays"]}
            if arrays & all_user_arrays:
                raise ValueError("Counter arrays are shared between user tables")
            all_user_arrays.update(arrays)
            user_tables.append({"table": table, "tableMetrics": summarize(table_ids), "trieMetrics": summarize(trie_ids)})
            for group, mapping in value.items():
                for oid, route in mapping.items():
                    row = per_field[group + "." + route]
                    row["reservoirs"] += 1
                    row["empty"] += int(not any(array(aid)["nonzeroCells"] for aid in reservoirs[oid]["active"]))
                    row["payloadBytes"] += sum(array(aid)["payloadBytes"] for aid in reservoirs[oid]["arrays"])
        actual_tables = len(user_tables)
        if actual_tables != expected_tables and not (path.stem == "baseline" and actual_tables == 0):
            raise ValueError(f"Expected {expected_tables} user tables (or zero in baseline.hprof), found {actual_tables}")
        if user_table_ids & user_trie_ids:
            raise ValueError("Table and trie metric roots share a reservoir")
        users = user_table_ids | user_trie_ids
        if users & other_table_ids:
            raise ValueError("User and system table roots share a reservoir")
        unassigned = set(reservoirs) - users - other_table_ids
        private_user_offsets = {oid for oid, owners in offset_owners.items()
                                if oid not in static_offsets and owners <= users}
        per_table_payload = collections.Counter(row["tableMetrics"]["backingArrayPayloadBytes"]
                                               + row["trieMetrics"]["backingArrayPayloadBytes"] for row in user_tables)
        return {"file": str(path.resolve()), "userKeyspace": user_keyspace, "userTables": actual_tables,
                "scope": "Exact unique int/long-array payload reachable through reservoir fields; excludes object headers, wrappers and reference-array byte widths. Ownership is field reachability, not dominator retained size.",
                "stripeCountScope": "Configured counts describe each reservoir. Physical counts describe each counter side, including empty primary and secondary stores; counterAllocatedStripes counts stores with arrays. Multi-stripe phase-1 paged storage has no separate physical stores, so its physical allocation counts are unavailable.",
                "classInstanceCounts": dict(sorted(class_counts.items())),
                "allReservoirs": summarize(set(reservoirs)), "userTableMetrics": summarize(user_table_ids),
                "userTrieMetrics": summarize(user_trie_ids), "otherTableMetricsAndTrie": summarize(other_table_ids),
                "notAssignedToTables": summarize(unassigned), "userCombined": summarize(users),
                "fixedStaticOffsets": {"uniqueArrays": len(static_offsets), "payloadBytes": sum(array(oid)["payloadBytes"] for oid in static_offsets),
                                       "fields": static_offset_fields},
                "exclusiveUserCounterAndOffsetsPayloadBytes": sum(array(oid)["payloadBytes"] for oid in all_user_arrays | private_user_offsets),
                "userMetricFields": dict(sorted(per_field.items())), "perTablePayloadCounts": dict(per_table_payload),
                "tables": user_tables}


class Tee:
    def __init__(self, console, log):
        self.console, self.log = console, log

    def write(self, text):
        self.console.write(text)
        self.log.write(text)
        self.flush()

    def flush(self):
        self.console.flush()
        self.log.flush()


def main():
    logs = pathlib.Path("logs")
    logs.mkdir(exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    log_path = logs / (stamp + "-analyze-resident-reservoirs.log")
    json_path = log_path.with_suffix(".json")
    with log_path.open("w") as log, contextlib.redirect_stdout(Tee(sys.stdout, log)), contextlib.redirect_stderr(Tee(sys.stderr, log)):
        try:
            parser = argparse.ArgumentParser(description=__doc__)
            parser.add_argument("--expected-tables", type=int, default=100)
            parser.add_argument("--keyspace", default="memtable_residency")
            parser.add_argument("heaps", nargs="+", type=pathlib.Path)
            args = parser.parse_args()
            if not 1 <= args.expected_tables <= 1000:
                raise ValueError("Expected table count must be between 1 and 1000")
            results = []
            for path in args.heaps:
                print("Inspecting " + str(path), file=sys.stderr)
                row = inspect(path, args.expected_tables, args.keyspace)
                results.append(row)
                print(json.dumps({key: value for key, value in row.items() if key not in ("tables", "userMetricFields")}, indent=2))
            json_path.write_text(json.dumps(results, indent=2) + "\n")
            print("Report: " + str(json_path.resolve()), file=sys.stderr)
        except SystemExit:
            raise
        except BaseException:
            traceback.print_exc()
            raise SystemExit(1)


if __name__ == "__main__":
    main()

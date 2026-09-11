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
import mmap
import struct


class Hprof:
    """Index HotSpot heap records while retaining their original byte offsets."""

    def __init__(self, path):
        self.stream = path.open('rb')
        self.data = mmap.mmap(self.stream.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            self._index()
        except BaseException:
            self.close()
            raise

    def close(self):
        self.data.close()
        self.stream.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def u(self, pos, width=None):
        return int.from_bytes(self.data[pos:pos + (width or self.ids)], 'big')

    def _index(self):
        data = self.data
        h = data.find(b'\0')
        self.ids = ids = struct.unpack_from('>I', data, h + 1)[0]
        assert ids in (4, 8)
        self.sizes = sizes = {2: ids, 4: 1, 5: 2, 6: 4, 7: 8, 8: 1, 9: 2, 10: 4, 11: 8}
        roots = {255: ids, 1: 2 * ids, 2: ids + 8, 3: ids + 8, 4: ids + 4, 5: ids,
                 6: ids + 4, 7: ids, 8: ids + 8, 0x89: ids, 0x8a: ids, 0x8b: ids,
                 0x8c: ids, 0x8d: ids, 0x8e: ids + 8, 0x90: ids, 0xfe: ids + 4}
        self.names, self.strings, self.classes, self.objects = {}, {}, {}, {}
        self.statics, self.loaders = {}, {}
        u = self.u
        pos = h + 13
        while pos < len(data):
            tag, _, length = struct.unpack_from('>BII', data, pos)
            pos += 9
            end = pos + length
            assert end <= len(data)
            if tag == 1:
                self.strings[u(pos)] = data[pos + ids:end].decode('utf-8', errors='replace')
            elif tag == 2:
                self.names[u(pos + 4)] = self.strings[u(pos + 8 + ids)].replace('/', '.')
            if tag not in (12, 28):
                pos = end
                continue
            while pos < end:
                kind = data[pos]
                pos += 1
                if kind in roots:
                    pos += roots[kind]
                elif kind == 32:
                    cid, sid = u(pos), u(pos + ids + 4)
                    self.loaders[cid] = u(pos + 2 * ids + 4)
                    shallow = u(pos + 7 * ids + 4, 4)
                    pos += 7 * ids + 8
                    count = u(pos, 2)
                    pos += 2
                    for _ in range(count):
                        pos += 3 + sizes[data[pos + 2]]
                    count = u(pos, 2)
                    pos += 2
                    static_fields = {}
                    for _ in range(count):
                        name, typ = self.strings[u(pos)], data[pos + ids]
                        static_fields[name] = typ, u(pos + ids + 1, sizes[typ])
                        pos += ids + 1 + sizes[typ]
                    self.statics[cid] = static_fields
                    count = u(pos, 2)
                    pos += 2
                    fields = []
                    for _ in range(count):
                        fields.append((self.strings[u(pos)], data[pos + ids]))
                        pos += ids + 1
                    self.classes[cid] = sid, shallow, fields
                elif kind == 33:
                    oid, cid, length = u(pos), u(pos + ids + 4), u(pos + 2 * ids + 4, 4)
                    pos += 2 * ids + 8
                    self.objects[oid] = kind, cid, length, pos
                    pos += length
                elif kind == 34:
                    oid, length, cid = u(pos), u(pos + ids + 4, 4), u(pos + ids + 8)
                    pos += 2 * ids + 8
                    self.objects[oid] = kind, cid, length, pos
                    pos += length * ids
                elif kind in (35, 195):
                    oid, length, typ = u(pos), u(pos + ids + 4, 4), data[pos + ids + 8]
                    pos += ids + 9
                    self.objects[oid] = kind, typ, length, pos
                    if kind == 35:
                        pos += length * sizes[typ]
                else:
                    raise ValueError((kind, pos))
            assert pos == end
        assert pos == len(data)

    def values(self, oid):
        kind, cid, length, start = self.objects[oid]
        assert kind == 33
        fields = {}
        offset = start
        while cid:
            cid, _, definitions = self.classes[cid]
            for name, typ in definitions:
                width = self.sizes[typ]
                fields.setdefault(name, (typ, self.u(offset, width)))
                offset += width
        assert offset == start + length
        return fields

    def references(self, oid):
        kind, _, length, start = self.objects[oid]
        if kind != 34:
            raise ValueError(f"Expected object array at {oid:#x}, found record {kind}")
        return [self.u(start + index * self.ids) for index in range(length)]

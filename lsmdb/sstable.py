"""SSTable -- Sorted String Table: an immutable, sorted, on-disk dump of a
frozen memtable. Once written it is never modified (LSM's core invariant),
which is what makes reads and later compaction simple.

File layout (v0.2 -- a single sorted run of records; a sparse index comes in v0.3):
    repeated records, in ascending key order:
        type:    1 byte   (0 = value, 1 = tombstone)
        key_len: 4 bytes  (uint32, big-endian)
        key:     key_len bytes
        val_len: 4 bytes  (uint32)   -- 0 for a tombstone
        val:     val_len bytes
    footer (last 4 bytes): uint32 count of records
"""
import os
import struct

from .memtable import TOMBSTONE

T_VALUE = 0
T_TOMB = 1
_REC = struct.Struct(">B I")   # type, key_len
_VLEN = struct.Struct(">I")    # val_len
_COUNT = struct.Struct(">I")   # footer record count


def write_sstable(path: str, items) -> None:
    """Write (key, value) pairs -- value is bytes or TOMBSTONE -- to `path`.
    `items` MUST be produced in ascending key order (the memtable already is)."""
    tmp = path + ".tmp"
    count = 0
    with open(tmp, "wb") as f:
        for key, value in items:
            if value is TOMBSTONE:
                f.write(_REC.pack(T_TOMB, len(key)) + key + _VLEN.pack(0))
            else:
                f.write(_REC.pack(T_VALUE, len(key)) + key + _VLEN.pack(len(value)) + value)
            count += 1
        f.write(_COUNT.pack(count))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)   # atomic: the SSTable appears complete or not at all


class SSTable:
    """Read-only handle over one SSTable file."""

    def __init__(self, path: str) -> None:
        self.path = path

    def get(self, key: bytes):
        """Return bytes for a live value, TOMBSTONE for a delete, or None if
        this SSTable doesn't contain the key. v0.2 does a full scan; v0.3
        replaces this with a sparse-index seek + binary search."""
        with open(self.path, "rb") as f:
            data = f.read()
        n = len(data) - _COUNT.size   # everything before the footer
        i = 0
        while i < n:
            typ, klen = _REC.unpack_from(data, i); i += _REC.size
            k = data[i:i + klen]; i += klen
            (vlen,) = _VLEN.unpack_from(data, i); i += _VLEN.size
            v = data[i:i + vlen]; i += vlen
            if k == key:
                return TOMBSTONE if typ == T_TOMB else v
            if k > key:
                break   # sorted file: we've passed where the key would be
        return None

    def items(self):
        """Yield (key, value|TOMBSTONE) in key order -- used by compaction later."""
        with open(self.path, "rb") as f:
            data = f.read()
        n = len(data) - _COUNT.size
        i = 0
        while i < n:
            typ, klen = _REC.unpack_from(data, i); i += _REC.size
            k = data[i:i + klen]; i += klen
            (vlen,) = _VLEN.unpack_from(data, i); i += _VLEN.size
            v = data[i:i + vlen]; i += vlen
            yield k, (TOMBSTONE if typ == T_TOMB else v)

"""SSTable -- Sorted String Table: an immutable, sorted, on-disk run of records
with a SPARSE INDEX for fast lookups.

File layout (v0.3):
    [data block]   repeated records in ascending key order:
        type:    1 byte   (0 = value, 1 = tombstone)
        key_len: 4 bytes  (uint32, big-endian)
        key:     key_len bytes
        val_len: 4 bytes  (uint32)   -- 0 for a tombstone
        val:     val_len bytes
    [index block] repeated, ascending, one entry per Nth data record:
        key_len: 4 bytes
        key:     key_len bytes
        offset:  8 bytes  (uint64) -- byte offset of that record in the data block
    [footer] fixed 16 bytes:
        index_offset: 8 bytes (uint64) -- where the index block starts
        index_count:  4 bytes (uint32) -- number of index entries
        rec_count:    4 bytes (uint32) -- number of data records

To read: load the (small) index into memory, binary-search it for the largest
indexed key <= target, seek to that offset, and scan forward through one block
(at most INDEX_INTERVAL records) until the key is found or passed.
"""
import os
import struct
import bisect

from .memtable import TOMBSTONE

T_VALUE = 0
T_TOMB = 1
INDEX_INTERVAL = 128            # index every 128th record

_REC = struct.Struct(">B I")   # type, key_len
_VLEN = struct.Struct(">I")    # val_len
_IDX = struct.Struct(">I")     # index key_len
_OFF = struct.Struct(">Q")     # index offset (uint64)
_FOOTER = struct.Struct(">Q I I")   # index_offset, index_count, rec_count


def write_sstable(path: str, items) -> None:
    """Write (key, value|TOMBSTONE) pairs in ascending key order to `path`."""
    tmp = path + ".tmp"
    index = []          # list of (key, offset) sampled every INDEX_INTERVAL
    with open(tmp, "wb") as f:
        offset = 0
        count = 0
        for key, value in items:
            if count % INDEX_INTERVAL == 0:
                index.append((key, offset))
            if value is TOMBSTONE:
                rec = _REC.pack(T_TOMB, len(key)) + key + _VLEN.pack(0)
            else:
                rec = _REC.pack(T_VALUE, len(key)) + key + _VLEN.pack(len(value)) + value
            f.write(rec)
            offset += len(rec)
            count += 1

        index_offset = offset
        for key, off in index:
            f.write(_IDX.pack(len(key)) + key + _OFF.pack(off))
        f.write(_FOOTER.pack(index_offset, len(index), count))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)       # atomic


class SSTable:
    """Read-only handle. The sparse index is loaded once and cached in memory."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._index_keys = []      # ascending indexed keys
        self._index_offs = []      # parallel byte offsets
        self._data_end = 0         # first byte of the index block
        self._load_index()

    def _load_index(self) -> None:
        with open(self.path, "rb") as f:
            f.seek(-_FOOTER.size, os.SEEK_END)
            index_offset, index_count, _rec = _FOOTER.unpack(f.read(_FOOTER.size))
            self._data_end = index_offset
            f.seek(index_offset)
            blob = f.read()        # index block + footer; footer is tiny
            i = 0
            for _ in range(index_count):
                (klen,) = _IDX.unpack_from(blob, i); i += _IDX.size
                key = blob[i:i + klen]; i += klen
                (off,) = _OFF.unpack_from(blob, i); i += _OFF.size
                self._index_keys.append(key)
                self._index_offs.append(off)

    def get(self, key: bytes):
        """bytes for a live value, TOMBSTONE for a delete, None if not present."""
        if not self._index_keys or key < self._index_keys[0]:
            return None            # smaller than the smallest key in this file
        # largest indexed key <= target
        pos = bisect.bisect_right(self._index_keys, key) - 1
        start = self._index_offs[pos]
        with open(self.path, "rb") as f:
            f.seek(start)
            block = f.read(self._data_end - start)   # scan from here to index start
        i = 0
        while i < len(block):
            typ, klen = _REC.unpack_from(block, i); i += _REC.size
            k = block[i:i + klen]; i += klen
            (vlen,) = _VLEN.unpack_from(block, i); i += _VLEN.size
            v = block[i:i + vlen]; i += vlen
            if k == key:
                return TOMBSTONE if typ == T_TOMB else v
            if k > key:
                return None        # passed it within the block -> absent
        return None

    def items(self):
        """Yield (key, value|TOMBSTONE) in key order (used by compaction later)."""
        with open(self.path, "rb") as f:
            data = f.read(self._data_end) if self._data_end else f.read()
        i = 0
        while i < len(data):
            typ, klen = _REC.unpack_from(data, i); i += _REC.size
            k = data[i:i + klen]; i += klen
            (vlen,) = _VLEN.unpack_from(data, i); i += _VLEN.size
            v = data[i:i + vlen]; i += vlen
            yield k, (TOMBSTONE if typ == T_TOMB else v)

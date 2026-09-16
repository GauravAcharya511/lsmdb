"""Write-Ahead Log: every mutation is appended here (and fsync'd) BEFORE
the memtable is updated, so a crash can't lose an acknowledged write.
On open, the WAL is replayed to rebuild the memtable.

Record format (length-prefixed binary, so keys/values may contain any bytes):
    op:      1 byte   (0 = put, 1 = delete)
    key_len: 4 bytes  (uint32, big-endian)
    key:     key_len bytes
    val_len: 4 bytes  (uint32, big-endian)  -- 0 for delete
    val:     val_len bytes
"""
import os
import struct

OP_PUT = 0
OP_DELETE = 1
_HEADER = struct.Struct(">B I")   # op, key_len
_VLEN = struct.Struct(">I")       # val_len


class WAL:
    def __init__(self, path: str, sync: bool = True) -> None:
        self.path = path
        self.sync = sync
        self._f = open(path, "ab", buffering=0)

    def append_put(self, key: bytes, value: bytes) -> None:
        self._write(OP_PUT, key, value)

    def append_delete(self, key: bytes) -> None:
        self._write(OP_DELETE, key, b"")

    def _write(self, op: int, key: bytes, value: bytes) -> None:
        rec = _HEADER.pack(op, len(key)) + key + _VLEN.pack(len(value)) + value
        self._f.write(rec)
        if self.sync:
            os.fsync(self._f.fileno())

    def close(self) -> None:
        self._f.close()

    @staticmethod
    def replay(path: str):
        """Yield (op, key, value) for each record. Stops cleanly at a torn
        tail record (a crash mid-write), which is safe: that write was never
        acknowledged."""
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            data = f.read()
        i, n = 0, len(data)
        while i < n:
            if i + _HEADER.size > n:
                break
            op, klen = _HEADER.unpack_from(data, i)
            i += _HEADER.size
            if i + klen + _VLEN.size > n:
                break
            key = data[i:i + klen]; i += klen
            (vlen,) = _VLEN.unpack_from(data, i); i += _VLEN.size
            if i + vlen > n:
                break
            val = data[i:i + vlen]; i += vlen
            yield op, key, val

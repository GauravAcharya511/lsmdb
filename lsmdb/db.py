"""LSMDB v0.1 -- the public key/value API.

Write path:  append to WAL (durable)  ->  update the in-memory memtable.
Read path:   consult the memtable (a tombstone reads back as "not found").
Recovery:    on open, replay the WAL to reconstruct the memtable.

v0.1 keeps everything in one memtable; v0.2 introduces flushing the
memtable to on-disk SSTables once it grows past a threshold.
"""
import os

from .memtable import Memtable, TOMBSTONE
from .wal import WAL, OP_PUT, OP_DELETE


def _as_bytes(x) -> bytes:
    if isinstance(x, bytes):
        return x
    if isinstance(x, str):
        return x.encode("utf-8")
    raise TypeError(f"keys/values must be str or bytes, got {type(x).__name__}")


class LSMDB:
    def __init__(self, path: str, sync: bool = True) -> None:
        self.path = path
        os.makedirs(path, exist_ok=True)
        self._wal_path = os.path.join(path, "wal.log")
        self._memtable = Memtable()
        self._recover()
        self._wal = WAL(self._wal_path, sync=sync)

    def _recover(self) -> None:
        for op, key, val in WAL.replay(self._wal_path):
            if op == OP_PUT:
                self._memtable.put(key, val)
            elif op == OP_DELETE:
                self._memtable.delete(key)

    def put(self, key, value) -> None:
        key, value = _as_bytes(key), _as_bytes(value)
        self._wal.append_put(key, value)
        self._memtable.put(key, value)

    def delete(self, key) -> None:
        key = _as_bytes(key)
        self._wal.append_delete(key)
        self._memtable.delete(key)

    def get(self, key):
        key = _as_bytes(key)
        try:
            v = self._memtable.get(key)
        except KeyError:
            return None
        return None if v is TOMBSTONE else v

    def close(self) -> None:
        self._wal.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

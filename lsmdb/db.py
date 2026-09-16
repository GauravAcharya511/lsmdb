"""LSMDB v0.2 -- key/value API with memtable flushing to on-disk SSTables.

Write path:  append to WAL (durable) -> update memtable
             -> if the memtable is full, FLUSH it to a new SSTable.
Read path:   memtable  ->  SSTables newest-to-oldest  (first hit wins;
             a tombstone anywhere in that order means "deleted").
Flush:       freeze the memtable to an immutable sorted SSTable, fsync it,
             then reset the WAL (its writes are now durable in the SSTable)
             and start a fresh empty memtable.
Recovery:    load existing SSTables (newest last on disk), then replay the
             WAL -- which holds only the writes since the last flush.
"""
import os
import glob

from .memtable import Memtable, TOMBSTONE
from .wal import WAL, OP_PUT, OP_DELETE
from .sstable import SSTable, write_sstable

_SST_GLOB = "sst-*.db"
_SST_FMT = "sst-{:06d}.db"


def _as_bytes(x) -> bytes:
    if isinstance(x, bytes):
        return x
    if isinstance(x, str):
        return x.encode("utf-8")
    raise TypeError(f"keys/values must be str or bytes, got {type(x).__name__}")


class LSMDB:
    def __init__(self, path: str, sync: bool = True, memtable_limit: int = 1000) -> None:
        self.path = path
        os.makedirs(path, exist_ok=True)
        self._wal_path = os.path.join(path, "wal.log")
        self._sync = sync
        self._memtable_limit = memtable_limit

        # SSTables, ordered NEWEST-FIRST for reads.
        paths = sorted(glob.glob(os.path.join(path, _SST_GLOB)))
        self._sstables = [SSTable(p) for p in reversed(paths)]
        self._next_seq = (
            max((self._seq_of(p) for p in paths), default=0) + 1
        )

        self._memtable = Memtable()
        self._recover()
        self._wal = WAL(self._wal_path, sync=sync)

    @staticmethod
    def _seq_of(p: str) -> int:
        return int(os.path.basename(p)[4:10])

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
        self._maybe_flush()

    def delete(self, key) -> None:
        key = _as_bytes(key)
        self._wal.append_delete(key)
        self._memtable.delete(key)
        self._maybe_flush()

    def get(self, key):
        key = _as_bytes(key)
        try:
            v = self._memtable.get(key)
            return None if v is TOMBSTONE else v
        except KeyError:
            pass
        for sst in self._sstables:          # newest -> oldest
            v = sst.get(key)
            if v is None:
                continue                    # not in this file; keep looking
            return None if v is TOMBSTONE else v
        return None

    def _maybe_flush(self) -> None:
        if len(self._memtable) >= self._memtable_limit:
            self.flush()

    def flush(self) -> None:
        if len(self._memtable) == 0:
            return
        sst_path = os.path.join(self.path, _SST_FMT.format(self._next_seq))
        write_sstable(sst_path, self._memtable.items())   # durable + atomic
        self._next_seq += 1
        self._sstables.insert(0, SSTable(sst_path))       # newest at front
        # Those writes now live in the SSTable, so the WAL can be reset.
        self._wal.close()
        open(self._wal_path, "wb").close()                # truncate to empty
        self._wal = WAL(self._wal_path, sync=self._sync)
        self._memtable = Memtable()

    def close(self) -> None:
        self._wal.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

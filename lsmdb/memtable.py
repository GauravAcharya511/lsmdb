"""In-memory sorted table of the most recent writes.

Keys are kept sorted so a later flush to an SSTable (v0.2) is a single
in-order scan. A delete is stored as a TOMBSTONE marker rather than by
removing the key, because older on-disk data may still hold the key and
the tombstone is what shadows it.
"""
from sortedcontainers import SortedDict


class _Tombstone:
    __slots__ = ()

    def __repr__(self) -> str:
        return "<TOMBSTONE>"


TOMBSTONE = _Tombstone()


class Memtable:
    def __init__(self) -> None:
        self._data: SortedDict = SortedDict()
        self._nbytes = 0

    def put(self, key: bytes, value: bytes) -> None:
        self._account(key, value)
        self._data[key] = value

    def delete(self, key: bytes) -> None:
        self._account(key, b"")
        self._data[key] = TOMBSTONE

    def get(self, key: bytes):
        # Raises KeyError if the key is absent from THIS memtable.
        # Returns bytes for a live value, or TOMBSTONE for a delete.
        return self._data[key]

    def _account(self, key: bytes, value: bytes) -> None:
        if key not in self._data:
            self._nbytes += len(key) + len(value)

    def __len__(self) -> int:
        return len(self._data)

    @property
    def nbytes(self) -> int:
        return self._nbytes

    def items(self):
        return self._data.items()

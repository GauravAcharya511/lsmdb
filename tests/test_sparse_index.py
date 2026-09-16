import struct
from lsmdb import LSMDB, TOMBSTONE
from lsmdb.sstable import write_sstable, SSTable, INDEX_INTERVAL, _FOOTER


def test_large_sstable_all_keys_found(tmp_path):
    # More records than the index interval, so multiple index entries exist
    # and lookups must seek to the right block.
    n = INDEX_INTERVAL * 3 + 7
    items = [(f"key{i:05d}".encode(), f"val{i}".encode()) for i in range(n)]
    p = str(tmp_path / "big.db")
    write_sstable(p, items)
    sst = SSTable(p)

    # every key resolves correctly...
    for i in range(n):
        assert sst.get(f"key{i:05d}".encode()) == f"val{i}".encode()
    # ...and absent keys return None (below, between, above)
    assert sst.get(b"aaaaa") is None
    assert sst.get(b"key00000zzz") is None
    assert sst.get(b"zzzzz") is None


def test_index_has_multiple_entries(tmp_path):
    n = INDEX_INTERVAL * 2
    items = [(f"k{i:05d}".encode(), b"v") for i in range(n)]
    p = str(tmp_path / "idx.db")
    write_sstable(p, items)
    with open(p, "rb") as f:
        f.seek(-_FOOTER.size, 2)
        _off, index_count, rec_count = _FOOTER.unpack(f.read(_FOOTER.size))
    assert rec_count == n
    assert index_count == 2            # ceil(n / INDEX_INTERVAL) sampled keys
    # the cached index in the reader matches
    assert len(SSTable(p)._index_keys) == 2


def test_tombstone_via_index(tmp_path):
    items = [(f"k{i:05d}".encode(), (TOMBSTONE if i == 200 else b"v")) for i in range(300)]
    p = str(tmp_path / "t.db")
    write_sstable(p, items)
    sst = SSTable(p)
    assert sst.get(b"k00200") is TOMBSTONE
    assert sst.get(b"k00199") == b"v"


def test_end_to_end_many_flushes(tmp_path):
    # Force several flushes, then verify reads across many SSTables still work.
    db = LSMDB(str(tmp_path), memtable_limit=50)
    for i in range(500):
        db.put(f"k{i:05d}", f"v{i}")
    for i in range(0, 500, 37):
        assert db.get(f"k{i:05d}") == f"v{i}".encode()
    db.close()

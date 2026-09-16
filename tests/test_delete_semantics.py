"""v0.4 -- exhaustive tombstone/delete correctness across the read path.

The invariant under test: for any key, the value returned is the one from the
NEWEST source that mentions it (memtable, then SSTables newest->oldest), where a
tombstone means 'deleted'. Re-inserting after a delete must un-shadow the key.
"""
from lsmdb import LSMDB, TOMBSTONE


def test_delete_then_reinsert_in_memtable(tmp_path):
    with LSMDB(str(tmp_path)) as db:
        db.put("k", "v1")
        db.delete("k")
        assert db.get("k") is None
        db.put("k", "v2")            # re-insert un-shadows
        assert db.get("k") == b"v2"


def test_delete_in_memtable_over_value_in_sstable(tmp_path):
    with LSMDB(str(tmp_path), memtable_limit=2) as db:
        db.put("k", "v"); db.put("x", "x")   # flush: k=v on disk
        db.delete("k")                        # tombstone in memtable
        assert db.get("k") is None


def test_reinsert_in_memtable_over_tombstone_in_sstable(tmp_path):
    with LSMDB(str(tmp_path), memtable_limit=2) as db:
        db.put("k", "v"); db.put("a", "a")    # flush #1: k=v
        db.delete("k"); db.put("b", "b")      # flush #2: k=tombstone
        assert db.get("k") is None
        db.put("k", "again")                  # newest write in memtable
        assert db.get("k") == b"again"


def test_delete_nonexistent_key_is_safe(tmp_path):
    with LSMDB(str(tmp_path)) as db:
        db.delete("ghost")
        assert db.get("ghost") is None


def test_delete_survives_recovery(tmp_path):
    db = LSMDB(str(tmp_path), memtable_limit=2)
    db.put("k", "v"); db.put("x", "x")        # flush: k=v
    db.delete("k")                            # tombstone only in WAL
    db.close()
    db2 = LSMDB(str(tmp_path), memtable_limit=2)
    assert db2.get("k") is None               # tombstone replayed from WAL
    db2.close()


def test_tombstone_shadows_multiple_older_sstables(tmp_path):
    with LSMDB(str(tmp_path), memtable_limit=2) as db:
        db.put("k", "v1"); db.put("a", "a")   # flush #1: k=v1
        db.put("k", "v2"); db.put("b", "b")   # flush #2: k=v2
        assert db.get("k") == b"v2"
        db.delete("k"); db.put("c", "c")      # flush #3: k=tombstone
        assert db.get("k") is None            # newest (tombstone) wins over both

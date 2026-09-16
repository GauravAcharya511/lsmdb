import pytest
from lsmdb import LSMDB, TOMBSTONE
from lsmdb.sstable import write_sstable, SSTable


def test_sstable_roundtrip(tmp_path):
    p = str(tmp_path / "t.db")
    write_sstable(p, [(b"a", b"1"), (b"b", b"2"), (b"c", TOMBSTONE)])
    sst = SSTable(p)
    assert sst.get(b"a") == b"1"
    assert sst.get(b"b") == b"2"
    assert sst.get(b"c") is TOMBSTONE
    assert sst.get(b"z") is None          # absent -> None


def test_flush_creates_sstable_and_clears_memtable(tmp_path):
    db = LSMDB(str(tmp_path), memtable_limit=3)
    db.put("a", "1")
    db.put("b", "2")
    db.put("c", "3")          # 3rd write hits the limit -> flush
    assert len(db._memtable) == 0
    assert len(db._sstables) == 1
    # data still readable, now served from the SSTable
    assert db.get("a") == b"1"
    assert db.get("c") == b"3"
    db.close()


def test_reads_span_memtable_and_sstables(tmp_path):
    db = LSMDB(str(tmp_path), memtable_limit=2)
    db.put("a", "1")
    db.put("b", "2")          # flush #1 -> sst with a,b
    db.put("c", "3")          # still in memtable
    assert db.get("a") == b"1"    # from sstable
    assert db.get("c") == b"3"    # from memtable
    db.close()


def test_newest_write_shadows_older_sstable(tmp_path):
    db = LSMDB(str(tmp_path), memtable_limit=2)
    db.put("k", "old")
    db.put("x", "x")          # flush #1: k=old
    db.put("k", "new")        # newer value, in memtable
    assert db.get("k") == b"new"
    db.put("y", "y")          # flush #2: k=new now in a newer sstable
    assert db.get("k") == b"new"   # newest sstable wins
    db.close()


def test_delete_shadows_value_in_older_sstable(tmp_path):
    db = LSMDB(str(tmp_path), memtable_limit=2)
    db.put("k", "v")
    db.put("x", "x")          # flush #1: k=v
    db.delete("k")            # tombstone in memtable
    assert db.get("k") is None
    db.put("y", "y")          # flush #2: tombstone now in newer sstable
    assert db.get("k") is None    # tombstone shadows older value
    db.close()


def test_recovery_across_sstables_and_wal(tmp_path):
    db = LSMDB(str(tmp_path), memtable_limit=2)
    db.put("a", "1")
    db.put("b", "2")          # flushed to sstable
    db.put("c", "3")          # only in WAL/memtable (not yet flushed)
    db.close()

    db2 = LSMDB(str(tmp_path), memtable_limit=2)
    assert db2.get("a") == b"1"   # recovered from sstable
    assert db2.get("c") == b"3"   # recovered from WAL replay
    db2.close()

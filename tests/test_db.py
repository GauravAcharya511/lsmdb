import pytest
from lsmdb import LSMDB


def test_put_get(tmp_path):
    with LSMDB(str(tmp_path)) as db:
        db.put("k1", "v1")
        assert db.get("k1") == b"v1"


def test_get_missing_returns_none(tmp_path):
    with LSMDB(str(tmp_path)) as db:
        assert db.get("nope") is None


def test_overwrite_newest_wins(tmp_path):
    with LSMDB(str(tmp_path)) as db:
        db.put("k", "first")
        db.put("k", "second")
        assert db.get("k") == b"second"


def test_delete(tmp_path):
    with LSMDB(str(tmp_path)) as db:
        db.put("k", "v")
        db.delete("k")
        assert db.get("k") is None


def test_binary_keys_and_values(tmp_path):
    with LSMDB(str(tmp_path)) as db:
        db.put(b"\x00\x01", b"\xff\xfe")
        assert db.get(b"\x00\x01") == b"\xff\xfe"


def test_recovery_after_reopen(tmp_path):
    # Simulate a crash: write, drop the object without a graceful shutdown,
    # then reopen the same directory and expect the data back via WAL replay.
    db = LSMDB(str(tmp_path))
    db.put("a", "1")
    db.put("b", "2")
    db.delete("a")
    db.close()

    db2 = LSMDB(str(tmp_path))
    assert db2.get("a") is None      # tombstone survived
    assert db2.get("b") == b"2"      # value survived
    db2.close()


def test_recovery_ignores_torn_tail(tmp_path):
    db = LSMDB(str(tmp_path))
    db.put("k", "v")
    db.close()
    # Corrupt the tail to mimic a crash mid-write: append a partial record.
    wal = tmp_path / "wal.log"
    with open(wal, "ab") as f:
        f.write(b"\x00\x00\x00")  # truncated header, no body
    db2 = LSMDB(str(tmp_path))
    assert db2.get("k") == b"v"   # good record still recovered
    db2.close()

import os, sys, json, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from minidb.wal import WriteAheadLog
from minidb.collection import Database


class TestWAL:

    def setup_method(self):
        os.makedirs("data", exist_ok=True)
        self.log_path = "data/test_wal.log"
        if os.path.exists(self.log_path):
            os.remove(self.log_path)
        self.wal = WriteAheadLog(self.log_path)

    def test_append_creates_file(self):
        self.wal.append("SET", "users", "u:1", {"name": "Alice"})
        assert os.path.exists(self.log_path)

    def test_entries_are_valid_json(self):
        self.wal.append("SET", "users", "u:1", {"name": "Alice"})
        with open(self.log_path) as f:
            entry = json.loads(f.readline())
        assert entry["op"] == "SET"
        assert entry["key"] == "u:1"

    def test_read_all_returns_entries(self):
        self.wal.append("SET", "users", "u:1", {})
        self.wal.append("SET", "users", "u:2", {})
        self.wal.append("DELETE", "users", "u:1")
        assert self.wal.size() == 3

    def test_checkpoint_clears_log(self):
        self.wal.append("SET", "users", "u:1", {})
        self.wal.checkpoint()
        assert self.wal.size() == 0

    def test_read_empty_log_returns_empty_list(self):
        assert self.wal.read_all() == []


class TestCrashRecovery:

    def setup_method(self):
        os.makedirs("data", exist_ok=True)
        self.log_path = "data/recovery_test.log"
        if os.path.exists(self.log_path):
            os.remove(self.log_path)

    def test_wal_is_written_on_set(self):
        wal = WriteAheadLog(self.log_path)
        db = Database.__new__(Database)
        db.name = "testdb"
        db._wal = wal
        db._collections = {}
        db["users"].set("u:1", {"name": "Alice"})
        entries = wal.read_all()
        assert len(entries) == 1
        assert entries[0]["op"] == "SET"
        assert entries[0]["key"] == "u:1"

    def test_wal_written_on_delete(self):
        wal = WriteAheadLog(self.log_path)
        wal.append("SET",    "users", "u:1", {"name": "Alice"})
        wal.append("DELETE", "users", "u:1", None)
        entries = wal.read_all()
        assert entries[1]["op"] == "DELETE"
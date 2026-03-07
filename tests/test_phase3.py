import os, sys, json, shutil, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from minidb.storage import StorageEngine
from minidb.collection import Database

TEST_DIR = "data/test_phase3"


class TestStorageEngine:

    def setup_method(self):
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)
        self.storage = StorageEngine(TEST_DIR)

    def test_save_creates_snapshot(self):
        self.storage.save({"users": {"u:1": {"name": "Alice"}}})
        assert self.storage.exists()

    def test_load_returns_saved_data(self):
        data = {"users": {"u:1": {"name": "Alice"}, "u:2": {"name": "Bob"}}}
        self.storage.save(data)
        loaded = self.storage.load()
        assert loaded["collections"]["users"]["u:1"]["name"] == "Alice"

    def test_load_returns_empty_when_no_snapshot(self):
        result = self.storage.load()
        assert result == {"collections": {}}

    def test_atomic_write_no_tmp_file_left(self):
        self.storage.save({"users": {}})
        assert not os.path.exists(self.storage.snapshot_path + ".tmp")

    def test_size_bytes_nonzero_after_save(self):
        self.storage.save({"users": {"u:1": {"name": "Alice"}}})
        assert self.storage.size_bytes() > 0


class TestPersistence:

    def setup_method(self):
        if os.path.exists(TEST_DIR):
            shutil.rmtree(TEST_DIR)

    def test_data_survives_restart(self):
        # Session 1 — write and save
        db = Database(data_dir=TEST_DIR)
        db["users"].set("u:1", {"name": "Alice", "age": 28})
        db["users"].set("u:2", {"name": "Bob",   "age": 34})
        db.save()

        # Session 2 — new Database instance (simulates restart)
        db2 = Database(data_dir=TEST_DIR)
        assert db2["users"].get("u:1")["name"] == "Alice"
        assert db2["users"].get("u:2")["age"] == 34
        assert db2["users"].count() == 2

    def test_wal_replayed_after_crash(self):
        # Session 1 — write but DON'T save (simulates crash)
        db = Database(data_dir=TEST_DIR)
        db["users"].set("u:99", {"name": "Crash Test"})
        # No db.save() — WAL has the entry, no snapshot

        # Session 2 — restart, WAL should be replayed
        db2 = Database(data_dir=TEST_DIR)
        assert db2["users"].get("u:99")["name"] == "Crash Test"

    def test_save_clears_wal(self):
        db = Database(data_dir=TEST_DIR)
        db["users"].set("u:1", {"name": "Alice"})
        assert db._wal.size() > 0
        db.save()
        assert db._wal.size() == 0

    def test_multiple_collections_persist(self):
        db = Database(data_dir=TEST_DIR)
        db["users"].set("u:1",    {"name": "Alice"})
        db["products"].set("p:1", {"name": "Laptop", "price": 999})
        db.save()

        db2 = Database(data_dir=TEST_DIR)
        assert db2["users"].get("u:1")["name"] == "Alice"
        assert db2["products"].get("p:1")["price"] == 999

    def test_stats(self):
        db = Database(data_dir=TEST_DIR)
        db["users"].set("u:1", {"name": "Alice"})
        db.save()
        s = db.stats()
        assert s["snapshot_exists"] is True
        assert s["wal_entries"] == 0
        assert s["collections"]["users"] == 1
import sys, os, shutil, pytest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from minidb.index import Index, IndexManager
from minidb.collection import Collection, Database

TEST_DIR = "data/test_phase4"


class TestIndex:

    def test_add_and_lookup_eq(self):
        idx = Index("city")
        idx.add("u:1", "New York")
        idx.add("u:2", "London")
        idx.add("u:3", "New York")
        assert idx.lookup_eq("New York") == {"u:1", "u:3"}
        assert idx.lookup_eq("London")   == {"u:2"}
        assert idx.lookup_eq("Tokyo")    == set()

    def test_remove_cleans_up(self):
        idx = Index("city")
        idx.add("u:1", "NY")
        idx.remove("u:1", "NY")
        assert idx.lookup_eq("NY") == set()
        assert "NY" not in idx._data    # empty set removed

    def test_lookup_range(self):
        idx = Index("age")
        idx.add("u:1", 25)
        idx.add("u:2", 34)
        idx.add("u:3", 19)
        idx.add("u:4", 40)
        assert idx.lookup_range(low=25, high=35) == {"u:1", "u:2"}
        assert idx.lookup_range(low=30)          == {"u:2", "u:4"}
        assert idx.lookup_range(high=20)         == {"u:3"}

    def test_serialization(self):
        idx = Index("age")
        idx.add("u:1", 25)
        idx.add("u:2", 30)
        data = idx.to_dict()
        idx2 = Index("age")
        idx2.from_dict(data)
        assert idx2.lookup_eq(25) == {"u:1"}
        assert idx2.lookup_eq(30) == {"u:2"}


class TestIndexManager:

    def _make_docs(self):
        return {
            "u:1": {"name": "Alice", "age": 28, "city": "New York"},
            "u:2": {"name": "Bob",   "age": 34, "city": "London"},
            "u:3": {"name": "Carol", "age": 22, "city": "New York"},
            "u:4": {"name": "David", "age": 45, "city": "Tokyo"},
        }

    def test_create_and_rebuild(self):
        mgr = IndexManager("users")
        mgr.create("city")
        mgr.rebuild(self._make_docs())
        result = mgr.query("city", "==", "New York")
        assert result == {"u:1", "u:3"}

    def test_on_set_updates_index(self):
        mgr = IndexManager("users")
        mgr.create("city")
        mgr.on_set("u:5", None, {"city": "Paris"})
        assert "u:5" in mgr.query("city", "==", "Paris")

    def test_on_delete_removes_from_index(self):
        mgr = IndexManager("users")
        mgr.create("city")
        mgr.rebuild(self._make_docs())
        mgr.on_delete("u:1", {"city": "New York"})
        assert "u:1" not in mgr.query("city", "==", "New York")

    def test_no_index_returns_none(self):
        mgr = IndexManager("users")
        assert mgr.query("city", "==", "NY") is None

    def test_range_query(self):
        mgr = IndexManager("users")
        mgr.create("age")
        mgr.rebuild(self._make_docs())
        result = mgr.query("age", ">=", 30)
        assert result == {"u:2", "u:4"}


class TestCollectionFind:

    def test_find_without_index(self):
        col = Collection("users")
        col.set("u:1", {"name": "Alice", "age": 28})
        col.set("u:2", {"name": "Bob",   "age": 34})
        col.set("u:3", {"name": "Carol", "age": 22})
        result = col.find("age", ">=", 25)
        keys = [k for k, _ in result]
        assert "u:1" in keys
        assert "u:2" in keys
        assert "u:3" not in keys

    def test_find_with_index(self):
        col = Collection("users")
        col.set("u:1", {"name": "Alice", "city": "NY"})
        col.set("u:2", {"name": "Bob",   "city": "LA"})
        col.set("u:3", {"name": "Carol", "city": "NY"})
        col.create_index("city")
        result = col.find("city", "==", "NY")
        keys = [k for k, _ in result]
        assert set(keys) == {"u:1", "u:3"}

    def test_index_stays_consistent_on_update(self):
        col = Collection("users")
        col.set("u:1", {"city": "NY"})
        col.create_index("city")
        col.set("u:1", {"city": "LA"})    # update city
        assert col.find("city", "==", "NY") == []
        keys = [k for k, _ in col.find("city", "==", "LA")]
        assert "u:1" in keys

    def test_index_consistent_on_delete(self):
        col = Collection("users")
        col.set("u:1", {"city": "NY"})
        col.create_index("city")
        col.delete("u:1")
        assert col.find("city", "==", "NY") == []

    def test_create_index_returns_message(self):
        col = Collection("users")
        assert col.create_index("age") == "Index created on 'age'"
        assert col.create_index("age") == "Index on 'age' already exists"
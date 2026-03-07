# import pytest
# from minidb.kvstore import KVStore
# from minidb.collection import Database

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import pytest 
from minidb.kvstore import KVStore
from minidb.collection import Collection, Database


def test_set_and_get():
    store = KVStore()
    store.set("name", "Alice")
    assert store.get("name") == "Alice"


def test_delete():
    store = KVStore()
    store.set("x", 10)
    store.delete("x")
    assert store.get("x") is None


def test_exists():
    store = KVStore()
    store.set("a", 1)
    assert store.exists("a") is True


def test_keys():
    store = KVStore()
    store.set("a", 1)
    store.set("b", 2)
    assert set(store.keys()) == {"a", "b"}


def test_items():
    store = KVStore()
    store.set("a", 1)
    assert ("a", 1) in store.items()


def test_count():
    store = KVStore()
    store.set("a", 1)
    store.set("b", 2)
    assert store.count() == 2


def test_clear():
    store = KVStore()
    store.set("a", 1)
    store.clear()
    assert store.count() == 0


def test_invalid_key():
    store = KVStore()
    with pytest.raises(TypeError):
        store.set(123, "invalid")


def test_database_collections():
    db = Database()

    users = db["users"]
    products = db["products"]

    users.set("u1", "Alice")
    products.set("p1", "Laptop")

    assert users.get("u1") == "Alice"
    assert products.get("p1") == "Laptop"


def test_collection_isolation():
    db = Database()

    users = db["users"]
    orders = db["orders"]

    users.set("id1", "User")
    assert orders.get("id1") is None
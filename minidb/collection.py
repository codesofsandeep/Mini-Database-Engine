



import os
from typing import Any
from .kvstore import KVStore
from .wal import WriteAheadLog
from .storage import StorageEngine
from .index import IndexManager
from .query import QueryExecutor


class Collection:
    
    def __init__(self, name: str, wal: WriteAheadLog = None):
        self.name = name
        self._store = KVStore()
        self._wal = wal
        self._indexes = IndexManager(name)
        self._executor = QueryExecutor(self._indexes)

        

    def set(self, key: str, value: Any) -> None:
        if self._wal:
            self._wal.append("SET", self.name, key, value)
        old_doc = self._store.get(key)
        self._indexes.on_set(key, old_doc, value)
        self._store.set(key, value)

    def delete(self, key: str) -> bool:
        if self._wal:
            self._wal.append("DELETE", self.name, key)
        old_doc = self._store.get(key)
        self._indexes.on_delete(key, old_doc)
        return self._store.delete(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self._store.get(key, default)

    def exists(self, key: str) -> bool:
        return self._store.exists(key)

    def keys(self) -> list:
        return self._store.keys()

    def items(self) -> list:
        return self._store.items()

    def all(self) -> dict:
        return self._store.all()

    def count(self) -> int:
        return self._store.count()

    def clear(self) -> None:
        self._store.clear()

    def create_index(self, field: str) -> str:
        created = self._indexes.create(field)
        if created:
            self._indexes.rebuild(self._store.all())
            return f"Index created on '{field}'"
        return f"Index on '{field}' already exists"

    def drop_index(self, field: str) -> str:
        dropped = self._indexes.drop(field)
        return f"Index on '{field}' dropped" if dropped else f"No index on '{field}'"

    def list_indexes(self) -> list:
        return self._indexes.list()

    def find(self, field: str, op: str, value: Any) -> list:
        candidate_keys = self._indexes.query(field, op, value)
        if candidate_keys is not None:
            docs = []
            for key in candidate_keys:
                doc = self._store.get(key)
                if doc is not None:
                    docs.append((key, doc))
            return docs
        return self._filter_scan(field, op, value)

    def _filter_scan(self, field: str, op: str, value: Any) -> list:
        results = []
        ops = {
            "==": lambda a, b: a == b,
            "=" : lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">" : lambda a, b: a >  b,
            ">=": lambda a, b: a >= b,
            "<" : lambda a, b: a <  b,
            "<=": lambda a, b: a <= b,
        }
        fn = ops.get(op)
        if fn is None:
            raise ValueError(f"Unknown operator: {op}")
        for key, doc in self._store.items():
            if isinstance(doc, dict):
                doc_val = doc.get(field)
                if doc_val is not None:
                    try:
                        if fn(type(value)(doc_val), value):
                            results.append((key, doc))
                    except (TypeError, ValueError):
                        pass
        return results

    def _load_data(self, data: dict) -> None:
        for key, value in data.items():
            self._store.set(key, value)
        self._indexes.rebuild(self._store.all())

    def _replay(self, op: str, key: str, value: Any) -> None:
        if op == "SET":
            old_doc = self._store.get(key)
            self._indexes.on_set(key, old_doc, value)
            self._store.set(key, value)
        elif op == "DELETE":
            old_doc = self._store.get(key)
            self._indexes.on_delete(key, old_doc)
            self._store.delete(key)

    def __repr__(self) -> str:
        return f'Collection("{self.name}", {self.count()} docs)'

    def query(self, query_str: str = "",
              limit: int = None,
              order_by: str = None,
              order_desc: bool = False):
        """
        Run a query string against this collection.

        Examples:
            col.query('age >= 25')
            col.query('city == "NY" AND active == true')
            col.query('score > 90 OR age < 20')
            col.query('name ~= "ali"')
            col.query('age >= 18', order_by='age', limit=5)
        """
        return self._executor.execute(
            query_str,
            self._store.all(),
            limit=limit,
            order_by=order_by,
            order_desc=order_desc
        )

    def find_one(self, query_str: str):
        """Return first matching document or None."""
        result = self.query(query_str, limit=1)
        return result.docs[0] if result.docs else None

class Database:
    def __init__(self, name: str = "minidb", data_dir: str = "data"):
        self.name = name
        self._storage = StorageEngine(data_dir)
        self._wal = WriteAheadLog(os.path.join(data_dir, "wal.log"))
        self._collections = {}
        self._boot()

    def _boot(self) -> None:
        if self._storage.exists():
            state = self._storage.load()
            for col_name, col_data in state.get("collections", {}).items():
                col = self._get_or_create(col_name)
                col._load_data(col_data)
        for entry in self._wal.read_all():
            col = self._get_or_create(entry["col"])
            col._replay(entry["op"], entry["key"], entry.get("val"))

    def _get_or_create(self, name: str) -> Collection:
        if name not in self._collections:
            self._collections[name] = Collection(name, wal=self._wal)
        return self._collections[name]

    def collection(self, name: str) -> Collection:
        return self._get_or_create(name)

    def __getitem__(self, name: str) -> Collection:
        return self.collection(name)

    def save(self) -> None:
        all_data = {name: col.all() for name, col in self._collections.items()}
        self._storage.save(all_data)
        self._wal.checkpoint()

    def list_collections(self) -> list:
        return list(self._collections.keys())

    def drop_collection(self, name: str) -> bool:
        if name in self._collections:
            del self._collections[name]
            return True
        return False

    def stats(self) -> dict:
        return {
            "database": self.name,
            "snapshot_exists": self._storage.exists(),
            "snapshot_size_bytes": self._storage.size_bytes(),
            "wal_entries": self._wal.size(),
            "collections": {
                name: col.count()
                for name, col in self._collections.items()
            }
        }

    def __repr__(self) -> str:
        return f'Database("{self.name}", collections={self.list_collections()})'
    
    
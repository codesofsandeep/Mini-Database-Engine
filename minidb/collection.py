# import os
# from typing import Any
# from .kvstore import KVStore
# from .wal import WriteAheadLog
# from .storage import StorageEngine
# from .index import IndexManager




# ## Phase : 1

# class Collection:
#     def __init__(self, name):
#         self.name = name
#         self.store = KVStore()

#     def set(self, key, value):
#         self.store.set(key, value)

#     def get(self, key):
#         return self.store.get(key)

#     def delete(self, key):
#         self.store.delete(key)

#     def exists(self, key):
#         return self.store.exists(key)

#     def keys(self):
#         return self.store.keys()

#     def items(self):
#         return self.store.items()

#     def count(self):
#         return self.store.count()

#     def clear(self):
#         self.store.clear()


# class Database:
#     def __init__(self):
#         self.collections = {}

#     def create_collection(self, name):
#         if name not in self.collections:
#             self.collections[name] = Collection(name)
#         return self.collections[name]

#     def __getitem__(self, name):
#         return self.create_collection(name)




# # Phase 2:

# # from .wal import WriteAheadLog
# # from typing import Any
# # from .kvstore import KVStore
# # from .wal import WriteAheadLog

# class Collection:
#     def __init__(self, name: str, wal: WriteAheadLog = None):
#         self.name = name
#         self._store = KVStore()
#         self._wal = wal  # injected from Database

#     def set(self, key: str, value) -> None:
#         if self._wal:
#             self._wal.append("SET", self.name, key, value)  # log FIRST
#         self._store.set(key, value)                          # then write

#     def delete(self, key: str) -> bool:
#         if self._wal:
#             self._wal.append("DELETE", self.name, key)      # log FIRST
#         return self._store.delete(key)                       # then delete
    

# class Database:
#     def __init__(self, name: str = "minidb", data_dir: str = "data"):
#         self.name = name
#         self._wal = WriteAheadLog(f"{data_dir}/wal.log")
#         self._collections: dict[str, Collection] = {}

#     def collection(self, name: str) -> Collection:
#         if name not in self._collections:
#             self._collections[name] = Collection(name, wal=self._wal)
#         return self._collections[name]
    
#     def __getitem__(self, name: str):
#         return self.collection(name)



# ## Phase 3:

# # from typing import Any
# # from .kvstore import KVStore
# # from .wal import WriteAheadLog
# # from .storage import StorageEngine


# class Collection:
#     def __init__(self, name: str, wal: WriteAheadLog = None):
#         self.name = name
#         self._store = KVStore()
#         self._wal = wal

#     def set(self, key: str, value: Any) -> None:
#         if self._wal:
#             self._wal.append("SET", self.name, key, value)
#         self._store.set(key, value)

#     def delete(self, key: str) -> bool:
#         if self._wal:
#             self._wal.append("DELETE", self.name, key)
#         return self._store.delete(key)

#     def get(self, key: str, default: Any = None) -> Any:
#         return self._store.get(key, default)

#     def exists(self, key: str) -> bool:
#         return self._store.exists(key)

#     def keys(self) -> list:
#         return self._store.keys()

#     def items(self) -> list:
#         return self._store.items()

#     def all(self) -> dict:
#         return self._store.all()

#     def count(self) -> int:
#         return self._store.count()

#     def clear(self) -> None:
#         self._store.clear()

#     def _load_data(self, data: dict) -> None:
#         """Called during boot to populate from snapshot."""
#         for key, value in data.items():
#             self._store.set(key, value)

#     def _replay(self, op: str, key: str, value: Any) -> None:
#         """Called during WAL replay — bypasses WAL logging to avoid double-write."""
#         if op == "SET":
#             self._store.set(key, value)
#         elif op == "DELETE":
#             self._store.delete(key)

#     def __repr__(self) -> str:
#         return f'Collection("{self.name}", {self.count()} docs)'


# # class Database:
# #     def __init__(self, name: str = "minidb", data_dir: str = "data"):
# #         self.name = name
# #         self._storage = StorageEngine(data_dir)
# #         self._wal = WriteAheadLog(os.path.join(data_dir, "wal.log"))
# #         self._collections: dict[str, Collection] = {}
# #         self._boot()

# #     def _boot(self) -> None:
# #         """
# #         Startup sequence — runs every time Database() is created:
# #           1. Load snapshot from disk
# #           2. Replay any WAL entries written after the snapshot
# #         """
# #         # Step 1 — load snapshot
# #         if self._storage.exists():
# #             state = self._storage.load()
# #             for col_name, col_data in state.get("collections", {}).items():
# #                 col = self._get_or_create(col_name)
# #                 col._load_data(col_data)

# #         # Step 2 — replay WAL (catches writes after last snapshot)
# #         for entry in self._wal.read_all():
# #             col = self._get_or_create(entry["col"])
# #             col._replay(entry["op"], entry["key"], entry.get("val"))

# #     def _get_or_create(self, name: str) -> Collection:
# #         if name not in self._collections:
# #             self._collections[name] = Collection(name, wal=self._wal)
# #         return self._collections[name]

# #     def collection(self, name: str) -> Collection:
# #         return self._get_or_create(name)

# #     def __getitem__(self, name: str) -> Collection:
# #         return self.collection(name)

# #     def save(self) -> None:
# #         """
# #         Flush all data to disk, then checkpoint the WAL.
# #         After this call, WAL is empty — snapshot is the source of truth.
# #         """
# #         all_data = {name: col.all() for name, col in self._collections.items()}
# #         self._storage.save(all_data)
# #         self._wal.checkpoint()

# #     def list_collections(self) -> list:
# #         return list(self._collections.keys())

# #     def drop_collection(self, name: str) -> bool:
# #         if name in self._collections:
# #             del self._collections[name]
# #             return True
# #         return False

# #     def stats(self) -> dict:
# #         return {
# #             "database": self.name,
# #             "snapshot_exists": self._storage.exists(),
# #             "snapshot_size_bytes": self._storage.size_bytes(),
# #             "wal_entries": self._wal.size(),
# #             "collections": {
# #                 name: col.count()
# #                 for name, col in self._collections.items()
# #             }
# #         }

# #     def __repr__(self) -> str:
# #         return f'Database("{self.name}", collections={self.list_collections()})'


# # # needed for os.path inside Database.__init__
# # import os


# ## Phase 4: 

# # from typing import Any
# # from .kvstore import KVStore
# # from .wal import WriteAheadLog
# # from .storage import StorageEngine
# # from .index import IndexManager

# class Collection:
#     def __init__(self, name: str, wal=None):
#         self.name = name
#         self._store = KVStore()
#         self._wal = wal
#         self._indexes = IndexManager(name)    # ← add this

#     def set(self, key: str, value: Any) -> None:
#         if self._wal:
#             self._wal.append("SET", self.name, key, value)
#         old_doc = self._store.get(key)        # ← get old value BEFORE overwrite
#         self._indexes.on_set(key, old_doc, value)   # ← update indexes
#         self._store.set(key, value)

#     def delete(self, key: str) -> bool:
#         if self._wal:
#             self._wal.append("DELETE", self.name, key)
#         old_doc = self._store.get(key)        # ← get value BEFORE delete
#         self._indexes.on_delete(key, old_doc)       # ← update indexes
#         return self._store.delete(key)

#     def create_index(self, field: str) -> str:
#         """Create an index on a field and populate it immediately."""
#         created = self._indexes.create(field)
#         if created:
#             self._indexes.rebuild(self._store.all())
#             return f"Index created on '{field}'"
#         return f"Index on '{field}' already exists"

#     def drop_index(self, field: str) -> str:
#         dropped = self._indexes.drop(field)
#         return f"Index on '{field}' dropped" if dropped else f"No index on '{field}'"

#     def list_indexes(self) -> list:
#         return self._indexes.list()

#     def find(self, field: str, op: str, value: Any) -> list:
#         """
#         Find all documents matching field op value.
#         Uses index if available, falls back to full scan.
#         Returns list of (key, doc) tuples.
#         """
#         candidate_keys = self._indexes.query(field, op, value)

#         if candidate_keys is not None:
#             # ── Fast path: index hit ──
#             docs = []
#             for key in candidate_keys:
#                 doc = self._store.get(key)
#                 if doc is not None:
#                     docs.append((key, doc))
#             return docs
#         else:
#             # ── Slow path: full scan ──
#             return self._filter_scan(field, op, value)

#     def _filter_scan(self, field: str, op: str, value: Any) -> list:
#         """Full collection scan — used when no index exists."""
#         results = []
#         ops = {
#             "==": lambda a, b: a == b,
#             "=" : lambda a, b: a == b,
#             "!=": lambda a, b: a != b,
#             ">" : lambda a, b: a >  b,
#             ">=": lambda a, b: a >= b,
#             "<" : lambda a, b: a <  b,
#             "<=": lambda a, b: a <= b,
#         }
#         fn = ops.get(op)
#         if fn is None:
#             raise ValueError(f"Unknown operator: {op}")
#         for key, doc in self._store.items():
#             if isinstance(doc, dict):
#                 doc_val = doc.get(field)
#                 if doc_val is not None:
#                     try:
#                         if fn(type(value)(doc_val), value):
#                             results.append((key, doc))
#                     except (TypeError, ValueError):
#                         pass
#         return results


# class Database:
#     def __init__(self, name: str = "minidb", data_dir: str = "data"):
#         self.name = name
#         self._storage = StorageEngine(data_dir)
#         self._wal = WriteAheadLog(os.path.join(data_dir, "wal.log"))
#         self._collections: dict[str, Collection] = {}
#         self._boot()

#     def _boot(self) -> None:
#         """
#         Startup sequence — runs every time Database() is created:
#           1. Load snapshot from disk
#           2. Replay any WAL entries written after the snapshot
#         """
#         # Step 1 — load snapshot
#         if self._storage.exists():
#             state = self._storage.load()
#             for col_name, col_data in state.get("collections", {}).items():
#                 col = self._get_or_create(col_name)
#                 col._load_data(col_data)

#         # Step 2 — replay WAL (catches writes after last snapshot)
#         for entry in self._wal.read_all():
#             col = self._get_or_create(entry["col"])
#             col._replay(entry["op"], entry["key"], entry.get("val"))

#     def _get_or_create(self, name: str) -> Collection:
#         if name not in self._collections:
#             self._collections[name] = Collection(name, wal=self._wal)
#         return self._collections[name]

#     def collection(self, name: str) -> Collection:
#         return self._get_or_create(name)

#     def __getitem__(self, name: str) -> Collection:
#         return self.collection(name)

#     def save(self) -> None:
#         """
#         Flush all data to disk, then checkpoint the WAL.
#         After this call, WAL is empty — snapshot is the source of truth.
#         """
#         all_data = {name: col.all() for name, col in self._collections.items()}
#         self._storage.save(all_data)
#         self._wal.checkpoint()

#     def list_collections(self) -> list:
#         return list(self._collections.keys())

#     def drop_collection(self, name: str) -> bool:
#         if name in self._collections:
#             del self._collections[name]
#             return True
#         return False

#     def stats(self) -> dict:
#         return {
#             "database": self.name,
#             "snapshot_exists": self._storage.exists(),
#             "snapshot_size_bytes": self._storage.size_bytes(),
#             "wal_entries": self._wal.size(),
#             "collections": {
#                 name: col.count()
#                 for name, col in self._collections.items()
#             }
#         }

#     def __repr__(self) -> str:
#         return f'Database("{self.name}", collections={self.list_collections()})'


# # needed for os.path inside Database.__init__
# import os




import os
from typing import Any
from .kvstore import KVStore
from .wal import WriteAheadLog
from .storage import StorageEngine
from .index import IndexManager


class Collection:
    def __init__(self, name: str, wal: WriteAheadLog = None):
        self.name = name
        self._store = KVStore()
        self._wal = wal
        self._indexes = IndexManager(name)

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
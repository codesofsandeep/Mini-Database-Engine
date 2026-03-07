# minidb/kvstore.py
from typing import Any, Iterator


class KVStore:
    def __init__(self):
        self._store = {}

    def set(self, key: str, value: Any) -> None:
        if not isinstance(key, str):
            raise TypeError(f"Key must be a string, got {type(key).__name__}")
        self._store[key] = value

    def get(self, key: str, default: Any = None) -> Any:   # ← fixed: added default
        return self._store.get(key, default)

    def delete(self, key: str) -> bool:                    # ← fixed: now returns bool
        if key in self._store:
            del self._store[key]
            return True
        return False

    def exists(self, key: str) -> bool:
        return key in self._store

    def keys(self) -> list:
        return list(self._store.keys())

    def values(self) -> list:
        return list(self._store.values())

    def items(self) -> list:
        return list(self._store.items())

    def all(self) -> dict:                                 # ← added: needed for save()
        return dict(self._store)

    def count(self) -> int:
        return len(self._store)

    def clear(self) -> None:
        self._store.clear()

    def __len__(self) -> int:
        return self.count()

    def __contains__(self, key: str) -> bool:
        return self.exists(key)

    def __iter__(self) -> Iterator:
        return iter(self._store)

    def __repr__(self) -> str:
        return f"KVStore({self.count()} docs)"
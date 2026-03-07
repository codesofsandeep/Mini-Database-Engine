# minidb/index.py
from typing import Any


class Index:
    """
    A single field index.
    Internal structure: { field_value → set of doc keys }

    Example after indexing 3 users on 'city':
        _data = {
            "New York": {"u:1", "u:3"},
            "London":   {"u:2"},
        }
    """

    def __init__(self, field: str):
        self.field = field
        self._data: dict[Any, set] = {}

    def add(self, doc_key: str, field_value: Any) -> None:
        """Register that doc_key has this field_value."""
        if field_value is None:
            return
        if field_value not in self._data:
            self._data[field_value] = set()
        self._data[field_value].add(doc_key)

    def remove(self, doc_key: str, field_value: Any) -> None:
        """Remove doc_key from the index."""
        if field_value in self._data:
            self._data[field_value].discard(doc_key)
            if not self._data[field_value]:        # clean up empty sets
                del self._data[field_value]

    def lookup_eq(self, value: Any) -> set:
        """Find all doc keys where field == value. O(1)."""
        return set(self._data.get(value, set()))

    def lookup_range(self, low=None, high=None) -> set:
        """
        Find all doc keys where low <= field <= high.
        Works on numbers and strings. O(k) where k = unique values.
        """
        result = set()
        for val, keys in self._data.items():
            try:
                if low  is not None and val < low:  continue
                if high is not None and val > high: continue
                result.update(keys)
            except TypeError:
                pass  # skip mixed types
        return result

    def all_keys(self) -> set:
        """Return every doc key in this index."""
        result = set()
        for keys in self._data.values():
            result.update(keys)
        return result

    def to_dict(self) -> dict:
        """Serialize for persistence."""
        return {str(k): list(v) for k, v in self._data.items()}

    def from_dict(self, data: dict) -> None:
        """Deserialize from persistence."""
        self._data = {}
        for k, v in data.items():
            # restore numeric types
            try:    key = int(k)
            except (ValueError, TypeError):
                try:    key = float(k)
                except (ValueError, TypeError): key = k
            self._data[key] = set(v)

    def __repr__(self) -> str:
        return f"Index(field={self.field!r}, unique_values={len(self._data)})"


class IndexManager:
    """
    Manages all indexes for one collection.
    Keeps every index consistent as documents are written/deleted.
    """

    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self._indexes: dict[str, Index] = {}

    # ── Manage indexes ──────────────────────────────

    def create(self, field: str) -> bool:
        """Create a new index. Returns True if it was newly created."""
        if field not in self._indexes:
            self._indexes[field] = Index(field)
            return True
        return False

    def drop(self, field: str) -> bool:
        """Remove an index."""
        if field in self._indexes:
            del self._indexes[field]
            return True
        return False

    def has(self, field: str) -> bool:
        return field in self._indexes

    def list(self) -> list:
        return list(self._indexes.keys())

    # ── Keep indexes in sync with writes ────────────

    def on_set(self, doc_key: str, old_doc: Any, new_doc: Any) -> None:
        """
        Called every time a document is written.
        Removes old field values, adds new ones.
        """
        for field, idx in self._indexes.items():
            old_val = old_doc.get(field) if isinstance(old_doc, dict) else None
            new_val = new_doc.get(field) if isinstance(new_doc, dict) else None
            if old_val is not None:
                idx.remove(doc_key, old_val)
            if new_val is not None:
                idx.add(doc_key, new_val)

    def on_delete(self, doc_key: str, old_doc: Any) -> None:
        """Called every time a document is deleted."""
        if not isinstance(old_doc, dict):
            return
        for field, idx in self._indexes.items():
            val = old_doc.get(field)
            if val is not None:
                idx.remove(doc_key, val)

    def rebuild(self, all_docs: dict) -> None:
        """
        Rebuild all indexes from scratch.
        Called after loading a snapshot from disk.
        """
        for idx in self._indexes.values():
            idx._data = {}
        for doc_key, doc in all_docs.items():
            if isinstance(doc, dict):
                for field, idx in self._indexes.items():
                    val = doc.get(field)
                    if val is not None:
                        idx.add(doc_key, val)

    # ── Query using indexes ──────────────────────────

    def query(self, field: str, op: str, value: Any):
        """
        Use an index to get candidate doc keys.
        Returns None if no index exists for this field — caller must full scan.

        Supported ops: == = != > >= < <=
        """
        if field not in self._indexes:
            return None     # no index — signal to caller: do full scan
        idx = self._indexes[field]
        if op in ("==", "="):
            return idx.lookup_eq(value)
        elif op == "!=":
            return idx.all_keys() - idx.lookup_eq(value)
        elif op == ">":
            return idx.lookup_range(low=value) - idx.lookup_eq(value)
        elif op == ">=":
            return idx.lookup_range(low=value)
        elif op == "<":
            return idx.lookup_range(high=value) - idx.lookup_eq(value)
        elif op == "<=":
            return idx.lookup_range(high=value)
        return None

    # ── Persistence helpers ──────────────────────────

    def to_dict(self) -> dict:
        return {field: idx.to_dict() for field, idx in self._indexes.items()}

    def from_dict(self, data: dict) -> None:
        for field, idx_data in data.items():
            self._indexes[field] = Index(field)
            self._indexes[field].from_dict(idx_data)

    def __repr__(self) -> str:
        return f"IndexManager({self.collection_name!r}, indexes={self.list()})"
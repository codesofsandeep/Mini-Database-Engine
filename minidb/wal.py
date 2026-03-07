# minidb/wal.py
import json, os, time

class WriteAheadLog:
    def __init__(self, log_path: str = "data/wal.log"):
        self.log_path = log_path
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

    def append(self, op: str, collection: str, key: str, value=None):
        """Write intent to disk BEFORE the actual write."""
        entry = {"ts": time.time(), "op": op, "col": collection, "key": key, "val": value}
        with open(self.log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")

    def read_all(self) -> list:
        """Read every log entry for replay."""
        if not os.path.exists(self.log_path):
            return []
        entries = []
        with open(self.log_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass  # skip corrupted lines
        return entries

    def checkpoint(self):
        """Truncate log after a successful snapshot save."""
        open(self.log_path, "w").close()

    def size(self) -> int:
        return len(self.read_all())
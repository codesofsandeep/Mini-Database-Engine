# minidb/storage.py
import json, os, time, shutil

class StorageEngine:
    """
    Saves the full database state to disk atomically.
    Uses write-to-tmp + os.replace() to prevent corruption on crash.

    File layout:
        data/
          snapshot.db      ← full DB snapshot
          wal.log          ← write-ahead log (already built)
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.snapshot_path = os.path.join(data_dir, "snapshot.db")
        os.makedirs(data_dir, exist_ok=True)

    def save(self, collections: dict) -> None:
        """
        Atomically write all collections to disk.
        Write to .tmp first, then rename — safe against mid-write crashes.
        """
        payload = {
            "version": 1,
            "saved_at": time.time(),
            "collections": collections
        }
        tmp_path = self.snapshot_path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        os.replace(tmp_path, self.snapshot_path)   # atomic!

    def load(self) -> dict:
        """
        Load the last snapshot from disk.
        Returns empty state if no snapshot exists yet.
        """
        if not os.path.exists(self.snapshot_path):
            return {"collections": {}}
        with open(self.snapshot_path) as f:
            return json.load(f)

    def exists(self) -> bool:
        return os.path.exists(self.snapshot_path)

    def size_bytes(self) -> int:
        if not self.exists():
            return 0
        return os.path.getsize(self.snapshot_path)
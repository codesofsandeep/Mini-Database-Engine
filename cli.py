#!/usr/bin/env python3
# cli.py — Interactive MiniDB Shell
import sys, os, json, shutil
sys.path.insert(0, os.path.dirname(__file__))

from minidb import Database

BANNER = """
╔══════════════════════════════════════════╗
║          MiniDB Interactive Shell        ║
║          Built from scratch in Python    ║
╚══════════════════════════════════════════╝
Type  help  to see all commands.
"""

HELP = """
COMMANDS
────────────────────────────────────────────
  use <collection>          switch collection
  set <key> <json>          insert/update doc
  get <key>                 fetch by key
  delete <key>              remove a doc
  find <query>              query with filter
  find <query> | order <field> | limit <n>
  index <field>             create index
  indexes                   list indexes
  count                     doc count
  collections               list collections
  stats                     db statistics
  save                      flush to disk
  clear                     clear collection
  exit                      quit

QUERY EXAMPLES
────────────────────────────────────────────
  find age >= 25
  find city == "New York" AND active == true
  find score > 90 OR age < 20
  find name ~= "ali"
  find age >= 18 | order age | limit 5
  find age > 0 | order score desc | limit 3
"""


def parse_find_command(args_str: str):
    """
    Parse:  find age >= 25 | order age | limit 10
    Returns (query_str, order_by, order_desc, limit)
    """
    parts      = [p.strip() for p in args_str.split("|")]
    query_str  = parts[0].strip()
    order_by   = None
    order_desc = False
    limit      = None

    for part in parts[1:]:
        tokens = part.strip().split()
        if tokens[0].lower() == "order" and len(tokens) >= 2:
            order_by   = tokens[1]
            order_desc = len(tokens) >= 3 and tokens[2].lower() == "desc"
        elif tokens[0].lower() == "limit" and len(tokens) >= 2:
            try:
                limit = int(tokens[1])
            except ValueError:
                pass

    return query_str, order_by, order_desc, limit


def print_doc(key, doc, idx=None):
    prefix = f"[{idx}] " if idx is not None else ""
    if isinstance(doc, dict):
        fields = "  ".join(f"{k}: {v!r}" for k, v in doc.items()
                           if not k.startswith("_"))
        print(f"  {prefix}{key}  →  {fields}")
    else:
        print(f"  {prefix}{key}  →  {doc!r}")


def run_cli(data_dir: str = "data"):
    print(BANNER)
    db  = Database(data_dir=data_dir)
    col = db["default"]
    print(f"  Connected. Active collection: default\n")

    while True:
        try:
            active = col.name
            line   = input(f"minidb [{active}]> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Bye!")
            break

        if not line:
            continue

        parts   = line.split(None, 1)
        command = parts[0].lower()
        args    = parts[1].strip() if len(parts) > 1 else ""

        # ── use ──────────────────────────────────────
        if command == "use":
            if not args:
                print("  Usage: use <collection>")
            else:
                col = db[args]
                print(f"  Switched to collection: {args!r}")

        # ── set ──────────────────────────────────────
        elif command == "set":
            tokens = args.split(None, 1)
            if len(tokens) < 2:
                print("  Usage: set <key> <json>")
                print("  Example: set user:1 {\"name\": \"Alice\", \"age\": 25}")
            else:
                key, raw = tokens[0], tokens[1]
                try:
                    value = json.loads(raw)
                    col.set(key, value)
                    print(f"  ✓ Set {key!r}")
                except json.JSONDecodeError as e:
                    print(f"  ✗ Invalid JSON: {e}")

        # ── get ──────────────────────────────────────
        elif command == "get":
            if not args:
                print("  Usage: get <key>")
            else:
                doc = col.get(args)
                if doc is None:
                    print(f"  ✗ Key {args!r} not found")
                else:
                    print_doc(args, doc)

        # ── delete ───────────────────────────────────
        elif command == "delete":
            if not args:
                print("  Usage: delete <key>")
            else:
                ok = col.delete(args)
                print(f"  {'✓ Deleted' if ok else '✗ Key not found'}: {args!r}")

        # ── find ─────────────────────────────────────
        elif command == "find":
            try:
                query_str, order_by, order_desc, limit = \
                    parse_find_command(args)
                result = col.query(
                    query_str,
                    order_by=order_by,
                    order_desc=order_desc,
                    limit=limit
                )
                if not result.docs:
                    print("  (no results)")
                else:
                    for i, doc in enumerate(result.docs, 1):
                        key = doc.pop("_key", "?")
                        print_doc(key, doc, i)
                print(f"\n  {result.total} result(s)  |  "
                      f"scanned {result.scanned}  |  "
                      f"index: {result.index_used}")
            except Exception as e:
                print(f"  ✗ Query error: {e}")

        # ── index ────────────────────────────────────
        elif command == "index":
            if not args:
                print("  Usage: index <field>")
            else:
                print(f"  {col.create_index(args)}")

        # ── indexes ──────────────────────────────────
        elif command == "indexes":
            idxs = col.list_indexes()
            print(f"  Indexes: {idxs if idxs else '(none)'}")

        # ── count ────────────────────────────────────
        elif command == "count":
            print(f"  {col.count()} documents in {col.name!r}")

        # ── collections ──────────────────────────────
        elif command == "collections":
            print(f"  {db.list_collections()}")

        # ── stats ────────────────────────────────────
        elif command == "stats":
            s = db.stats()
            print(f"  Database      : {s['database']}")
            print(f"  Snapshot      : {s['snapshot_exists']}")
            print(f"  Snapshot size : {s['snapshot_size_bytes']} bytes")
            print(f"  WAL entries   : {s['wal_entries']}")
            for name, cnt in s["collections"].items():
                print(f"  {name:20} : {cnt} docs")

        # ── save ─────────────────────────────────────
        elif command == "save":
            db.save()
            print("  ✓ Saved to disk")

        # ── clear ────────────────────────────────────
        elif command == "clear":
            col.clear()
            print(f"  ✓ Cleared collection {col.name!r}")

        # ── help ─────────────────────────────────────
        elif command == "help":
            print(HELP)

        # ── exit ─────────────────────────────────────
        elif command in ("exit", "quit", "q"):
            db.save()
            print("  ✓ Saved. Bye!")
            break

        else:
            print(f"  Unknown command: {command!r}. Type help.")


if __name__ == "__main__":
    data_dir = sys.argv[1] if len(sys.argv) > 1 else "data"
    run_cli(data_dir)
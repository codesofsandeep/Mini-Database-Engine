# MiniDB

A minimal document database built from scratch in pure Python.
Zero external dependencies. Built to understand how databases work internally.

## Architecture
```
minidb/
├── kvstore.py     Phase 1 — Hash map storage primitive
├── wal.py         Phase 2 — Write-Ahead Log (crash recovery)
├── storage.py     Phase 3 — Atomic snapshot persistence
├── index.py       Phase 4 — Inverted index + range queries
├── query.py       Phase 5 — Tokenizer → AST → Query executor
└── collection.py  — Ties all components together
```

## Boot sequence (every startup)
```
1. Load snapshot.db    → restore last saved state
2. Replay wal.log      → recover writes since last save
3. Ready               → zero data loss guaranteed
```

## Usage
```python
from minidb import Database

db = Database()
db["users"].set("u:1", {"name": "Alice", "age": 28, "city": "New York"})
db["users"].set("u:2", {"name": "Bob",   "age": 34, "city": "London"})

# Query engine
result = db["users"].query('age >= 25 AND city == "New York"')
result = db["users"].query('name ~= "ali"')
result = db["users"].query('score > 90', order_by='score', limit=5)

# Indexes (100x+ faster on large collections)
db["users"].create_index("city")
result = db["users"].query('city == "New York"')
print(result.index_used)   # "city"
print(result.scanned)      # only matching docs

# Persist to disk
db.save()
```

## Query syntax

| Operator | Meaning          | Example                    |
|----------|------------------|----------------------------|
| `==`     | equals           | `city == "NY"`             |
| `!=`     | not equals       | `status != "inactive"`     |
| `>`      | greater than     | `age > 18`                 |
| `>=`     | greater or equal | `score >= 90`              |
| `<`      | less than        | `price < 100`              |
| `<=`     | less or equal    | `age <= 65`                |
| `~=`     | contains         | `name ~= "ali"`            |
| `AND`    | both conditions  | `age > 18 AND active == true` |
| `OR`     | either condition | `city == "NY" OR city == "LA"` |
| `NOT`    | negate           | `NOT active == false`      |

## Run the benchmark
```bash
python benchmark.py
```

Sample output (100,000 docs):
```
Without index :  85.00 ms
With index    :   0.3 ms
Speedup       :  280x faster with index
```

## Run the interactive CLI
```bash
python cli.py
```
```
minidb [default]> use users
minidb [users]> set u:1 {"name": "Alice", "age": 28}
minidb [users]> find age >= 25
minidb [users]> find city == "NY" | order age | limit 5
minidb [users]> index age
minidb [users]> save
```

## Test suite
```bash
pytest tests/ -v        # 78 tests across 5 phases
```

## What this teaches

| Component       | CS Concept                        |
|-----------------|-----------------------------------|
| KVStore         | Hash maps, O(1) lookup            |
| WAL             | Durability, append-only logs      |
| StorageEngine   | Atomic writes, file systems       |
| IndexManager    | Inverted indexes, range scans     |
| QueryEngine     | Lexer, AST, recursive descent     |

# benchmark.py
import sys, os, time, shutil, random, string
sys.path.insert(0, os.path.dirname(__file__))

from minidb import Database

BENCH_DIR  = "data/benchmark"
NUM_DOCS   = 100_000
CITIES     = ["New York", "London", "Tokyo", "Paris", "Sydney"]
QUERY_CITY = "Tokyo"

def random_name(n=8):
    return "".join(random.choices(string.ascii_lowercase, k=n)).capitalize()

def banner(title):
    print(f"\n{'─'*55}")
    print(f"  {title}")
    print('─'*55)

def fmt(n):
    return f"{n:,}"

# ── Setup ──────────────────────────────────────────────
if os.path.exists(BENCH_DIR):
    shutil.rmtree(BENCH_DIR)

db = Database(data_dir=BENCH_DIR)
col = db["users"]

# ── Insert ─────────────────────────────────────────────
banner(f"INSERT {fmt(NUM_DOCS)} documents")

t0 = time.perf_counter()
for i in range(NUM_DOCS):
    col.set(f"u:{i}", {
        "name":   random_name(),
        "age":    random.randint(18, 70),
        "city":   random.choice(CITIES),
        "score":  random.randint(1, 100),
        "active": random.choice([True, False]),
    })
insert_time = time.perf_counter() - t0

print(f"  Inserted : {fmt(NUM_DOCS)} docs")
print(f"  Time     : {insert_time:.3f}s")
print(f"  Rate     : {fmt(int(NUM_DOCS / insert_time))} docs/sec")

# ── Query WITHOUT index (full scan) ────────────────────
banner(f'QUERY city == "{QUERY_CITY}" — NO INDEX (full scan)')

times = []
for _ in range(5):
    t0 = time.perf_counter()
    result = col.query(f'city == "{QUERY_CITY}"')
    times.append(time.perf_counter() - t0)

no_idx_avg  = sum(times) / len(times)
no_idx_docs = result.total

print(f"  Matched  : {fmt(no_idx_docs)} docs")
print(f"  Scanned  : {fmt(result.scanned)} docs")
print(f"  Avg time : {no_idx_avg*1000:.2f} ms  (5 runs)")
print(f"  Index    : {result.index_used}")

# ── Create index ───────────────────────────────────────
banner("CREATE INDEX on 'city'")

t0  = time.perf_counter()
msg = col.create_index("city")
idx_build = time.perf_counter() - t0

print(f"  {msg}")
print(f"  Build time: {idx_build*1000:.2f} ms")

# ── Query WITH index ────────────────────────────────────
banner(f'QUERY city == "{QUERY_CITY}" — WITH INDEX')

times = []
for _ in range(5):
    t0 = time.perf_counter()
    result = col.query(f'city == "{QUERY_CITY}"')
    times.append(time.perf_counter() - t0)

idx_avg = sum(times) / len(times)

print(f"  Matched  : {fmt(result.total)} docs")
print(f"  Scanned  : {fmt(result.scanned)} docs")
print(f"  Avg time : {idx_avg*1000:.3f} ms  (5 runs)")
print(f"  Index    : {result.index_used}")

# ── Speedup ────────────────────────────────────────────
banner("RESULTS")
speedup = no_idx_avg / idx_avg if idx_avg > 0 else float("inf")
print(f"  Without index : {no_idx_avg*1000:.2f} ms")
print(f"  With index    : {idx_avg*1000:.3f} ms")
print(f"  Speedup       : {speedup:.0f}x faster with index")
print(f"  Docs scanned  : {fmt(NUM_DOCS)} → {fmt(result.scanned)}")

# ── More query types ───────────────────────────────────
banner("BONUS — Other query types on 100k docs")

col.create_index("age")
col.create_index("score")

t0 = time.perf_counter()
r  = col.query("age >= 30 AND age <= 40")
print(f"  age 30–40          : {fmt(r.total):>7} docs  "
      f"({(time.perf_counter()-t0)*1000:.2f} ms)")

t0 = time.perf_counter()
r  = col.query("score > 90")
print(f"  score > 90         : {fmt(r.total):>7} docs  "
      f"({(time.perf_counter()-t0)*1000:.2f} ms)")

t0 = time.perf_counter()
r  = col.query("active == true", order_by="score",
               order_desc=True, limit=10)
print(f"  Top 10 active/score: {fmt(r.total):>7} docs  "
      f"({(time.perf_counter()-t0)*1000:.2f} ms)")

print()
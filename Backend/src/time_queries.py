import time
from config import get_db

db = get_db()
for (name, fn) in [
    ("known_nodes", lambda: db.get_known_nodes(hours=1)),
    ("history", lambda: db.query_history(hours=1)),
    ("latest node1", lambda: db.get_latest_reading("node1", hours=1)),
]:
    t0 = time.time(); r = fn(); dt = time.time() - t0
    print(f"{name:14s} {dt*1000:7.0f} ms  -> {len(r)} results")
db.close()

"""Keep the marts fresh: `dbt run` every --interval seconds, `dbt test` every Nth cycle.

The dashboard reads marts, so this loop sets the end-to-end freshness floor:
loader flush (~10s) + this interval + dashboard refresh.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"


def dbt(*args: str) -> int:
    env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)}
    cmd = ["dbt", *args, "--project-dir", str(DBT_DIR)]
    return subprocess.call(cmd, env=env, cwd=DBT_DIR)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--interval", type=float, default=30)
    p.add_argument("--test-every", type=int, default=10, help="run `dbt test` every N cycles")
    args = p.parse_args()

    cycle = 0
    while True:
        started = time.monotonic()
        rc = dbt("run", "--quiet")
        print(f"dbt_loop: cycle {cycle} run rc={rc} ({time.monotonic() - started:.1f}s)", flush=True)
        if rc == 0 and cycle % args.test_every == 0:
            rc = dbt("test", "--quiet")
            print(f"dbt_loop: cycle {cycle} test rc={rc}", flush=True)
        cycle += 1
        time.sleep(max(0.0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    sys.exit(main())

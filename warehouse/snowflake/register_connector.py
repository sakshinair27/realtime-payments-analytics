"""Register (or update) the Snowflake sink connector on the Kafka Connect worker.

Reads Snowflake settings from .env and the private key from SNOWFLAKE_PRIVATE_KEY_PATH,
so no secret is committed to the repo. Idempotent: PUT /connectors/<name>/config.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
NAME = "snowflake-transactions-sink"


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def private_key_body(path: Path) -> str:
    lines = path.read_text().strip().splitlines()
    return "".join(line for line in lines if not line.startswith("-----"))


def main() -> int:
    load_env(ROOT / ".env")
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_PRIVATE_KEY_PATH"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        print(f"missing settings in .env: {', '.join(missing)}", file=sys.stderr)
        return 1

    config = json.loads((Path(__file__).parent / "connector.json").read_text())
    config.update({
        "snowflake.url.name": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com:443",
        "snowflake.private.key": private_key_body(ROOT / os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]),
    })
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
        config["snowflake.private.key.passphrase"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]

    url = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")
    req = urllib.request.Request(f"{url}/connectors/{NAME}/config", method="PUT",
                                 data=json.dumps(config).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(f"{resp.status} registered {NAME}")
    # The status record appears a few seconds after registration; poll instead of failing.
    for _ in range(15):
        try:
            with urllib.request.urlopen(f"{url}/connectors/{NAME}/status", timeout=30) as resp:
                status = json.loads(resp.read())
        except urllib.error.HTTPError:
            time.sleep(2)
            continue
        tasks = [t["state"] for t in status["tasks"]]
        print(f"connector {status['connector']['state']}, tasks {tasks}")
        if status["connector"]["state"] == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
            return 0
        time.sleep(2)
    print("connector did not reach RUNNING; check: docker compose logs kafka-connect", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())

"""Write keys/setup_filled.sql: setup.sql with both public keys substituted in.

The output lives in keys/ (git-ignored) so the filled script is never committed.
Public keys aren't secret, but keeping the repo free of account-specific values keeps it reusable.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def key_body(path: Path) -> str:
    return "".join(line for line in path.read_text().strip().splitlines() if not line.startswith("-----"))


sql = (Path(__file__).parent / "setup.sql").read_text()
sql = sql.replace("'<LOADER_PUBLIC_KEY>'", "'" + key_body(ROOT / "keys" / "rsa_key.pub") + "'")
sql = sql.replace("'<DBT_PUBLIC_KEY>'", "'" + key_body(ROOT / "keys" / "dbt_key.pub") + "'")
out = ROOT / "keys" / "setup_filled.sql"
out.write_text(sql)
print(f"wrote {out}")


from types import SimpleNamespace

ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "bizcredit.db"

SPEC = SimpleNamespace(
    slug="bizcredit",
    fmt="html",
    engine="rpa",
    endpoint="items",
    columns=["id", "name", "value", "status", "ts", "holder"],
    page_size=4,
    int_fields=["value"],
    ts_field="ts",
)

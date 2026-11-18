import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("REPO_ROOT", str(ROOT))
os.environ.setdefault("AUTH_DEMO_ENABLED", "1")
os.environ.pop("MOCK", None)
for p in (ROOT, ROOT / "api", ROOT / "etl"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

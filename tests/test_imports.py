"""Every backend module imports FIRST, in a fresh interpreter — no import-order
dependence. ``backend.persistence.ports`` once imported service dataclasses at
module level while the services import ``backend.persistence.records`` (whose
package ``__init__`` loads ``ports``): importing a service first raised a circular
ImportError that the suite never saw, because it always loaded the persistence
package first. Run in a subprocess (``docs/testing-gotchas.md``: never purge
``sys.modules`` inside the pytest process)."""
from __future__ import annotations

import pkgutil
import subprocess
import sys
from pathlib import Path

import backend

ROOT = Path(__file__).resolve().parents[1]

_PROBE = """
import importlib, sys
failed = []
for name in sys.argv[1:]:
    for k in [k for k in sys.modules if k == "backend" or k.startswith("backend.")]:
        del sys.modules[k]
    try:
        importlib.import_module(name)
    except ImportError as e:
        if "fastapi" in str(e) or "starlette" in str(e) or "mysql" in str(e) or "gspread" in str(e):
            continue  # a web / driver dependency this interpreter lacks: not an import-order defect
        failed.append(f"{name}: {type(e).__name__}: {e}")
print("\\n".join(failed))
sys.exit(1 if failed else 0)
"""


def _modules():
    return sorted(m.name for m in pkgutil.walk_packages(backend.__path__, "backend.") if not m.ispkg)


def test_every_backend_module_imports_first_in_a_fresh_interpreter():
    mods = _modules()
    assert "backend.services.pricing_service" in mods and "backend.persistence.ports" in mods
    run = subprocess.run([sys.executable, "-c", _PROBE, *mods], cwd=ROOT, capture_output=True, text=True,
                         timeout=120)
    assert run.returncode == 0, run.stdout + run.stderr

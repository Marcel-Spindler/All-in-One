import os
import runpy
from pathlib import Path


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"

os.environ["PDL_FAST_FORCE_COMPANY"] = "Factor"
try:
    runpy.run_path(str(APP_PATH), run_name="__main__")
finally:
    os.environ.pop("PDL_FAST_FORCE_COMPANY", None)
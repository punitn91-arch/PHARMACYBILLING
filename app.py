import importlib.util
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.join(BASE_DIR, "billingwebapp")
APP_FILE = os.path.join(APP_DIR, "app.py")

if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

if not os.path.exists(APP_FILE):
    raise RuntimeError(f"Expected application file not found: {APP_FILE}")

spec = importlib.util.spec_from_file_location("billingwebapp_main", APP_FILE)
if not spec or not spec.loader:
    raise RuntimeError("Unable to load billingwebapp/app.py")

module = importlib.util.module_from_spec(spec)
# Register module before execution so Flask can resolve root/template paths correctly.
sys.modules[spec.name] = module
spec.loader.exec_module(module)
app = module.app

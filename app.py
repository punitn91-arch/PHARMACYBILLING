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


if __name__ == "__main__":
    debug_flag = (os.environ.get("FLASK_DEBUG") or os.environ.get("DEBUG") or "1").strip().lower()
    debug = debug_flag not in {"0", "false", "no", "off"}
    is_production = bool(getattr(module, "IS_PROD", False))
    port = module.prepare_local_server(is_production=is_production)
    module.schedule_browser_open(is_production=is_production)
    app.run(host="0.0.0.0", port=port, debug=debug)

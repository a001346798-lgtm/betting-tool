import os
from pathlib import Path
from lottery_analyzer import _create_flask_app

data_dir    = Path(os.environ.get("DATA_DIR", "."))
output_path = Path(os.environ.get("OUTPUT_PATH", "index.html"))
app = _create_flask_app(data_dir, output_path, run_init=True)

import os
from pathlib import Path
from lottery_analyzer import _create_flask_app

data_dir    = Path(os.environ.get("DATA_DIR", "/tmp"))
output_path = Path(os.environ.get("OUTPUT_PATH", "/tmp/index.html"))

# run_init=False：啟動時不連 Supabase，gunicorn 先綁定 port
# 第一次有人開網頁時，index() 路由會自動產生報告
app = _create_flask_app(data_dir, output_path, run_init=False)

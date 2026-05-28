import os
from pathlib import Path
from flask import Response
from lottery_analyzer import _create_flask_app, LotteryAnalyzer, AutoSyncManager, DataWriter

_DATA_DIR    = Path(os.environ.get("DATA_DIR", "/tmp"))
_OUTPUT_PATH = Path(os.environ.get("OUTPUT_PATH", "/tmp/index.html"))

# run_init=False: 不在啟動時連 Supabase，避免啟動失敗
app = _create_flask_app(_DATA_DIR, _OUTPUT_PATH, run_init=False)

# 第一次有人開網頁時才執行同步 + 產生報告
@app.route("/", endpoint="_wsgi_index")
def _wsgi_index():
    if not _OUTPUT_PATH.exists():
        writer = DataWriter(_DATA_DIR)
        syncer = AutoSyncManager(_DATA_DIR, writer)
        syncer.sync_all()
        LotteryAnalyzer(_DATA_DIR).run(_OUTPUT_PATH, server_mode=True)
    html = _OUTPUT_PATH.read_text(encoding="utf-8")
    return Response(html, mimetype="text/html",
                    headers={"Cache-Control": "no-store, no-cache"})

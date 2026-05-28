#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
lottery_analyzer.py v5.0
  - 智能爬蟲自動同步（Gap Detection + Bulk Fetch）
  - 時光機歷史任意期數回測（/api/backtest）
  - 三色球 + 多期遺漏回測（承襲 v4.0）
"""

import io
import sys
import re
import time

if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import os
import argparse
import csv as _csv
from pathlib import Path
from datetime import datetime, date as _date
from collections import defaultdict
from typing import Optional, List, Dict, Any, Tuple

try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("[ERROR] Run: pip install pandas numpy openpyxl flask requests beautifulsoup4")
    sys.exit(1)


# ============================================================
# SECTION 0 — CONFIGURATION
# ============================================================

LOTTERY_CONFIG: Dict[str, Dict] = {
    "taiwan_539": {
        "name":          "台灣今彩539",
        "short_name":    "539",
        "pool_size":     39,
        "pick_count":    5,
        "draws_per_day": 1,
        "file_patterns": ["*539*history*", "*539歷史*", "*taiwan*539*", "*taiwan539*"],
        "csv_filename":  "taiwan_539.csv",
        "theme":         {"primary": "#4338ca", "light": "#eef2ff",
                          "cold_bg": "#1e3a8a", "cold_fg": "#dbeafe"},
    },
    "michigan_fantasy5": {
        "name":          "密西根天天樂",
        "short_name":    "密西根",
        "pool_size":     39,
        "pick_count":    5,
        "draws_per_day": 1,
        "file_patterns": ["*michigan*result*", "*michigan*fantasy*", "*michigan*history*",
                          "*Michigan*Result*", "*michigan*"],
        "csv_filename":  "michigan_fantasy5.csv",
        "theme":         {"primary": "#0e7490", "light": "#ecfeff",
                          "cold_bg": "#164e63", "cold_fg": "#cffafe"},
    },
    "california_fantasy5": {
        "name":          "加州天天樂",
        "short_name":    "加州",
        "pool_size":     39,
        "pick_count":    5,
        "draws_per_day": 1,
        "file_patterns": ["*ca_fantasy5*real*", "*california*fantasy5*real*",
                          "*ca_fantasy5*", "*california*real*"],
        "csv_filename":  "california_fantasy5.csv",
        "theme":         {"primary": "#15803d", "light": "#f0fdf4",
                          "cold_bg": "#14532d", "cold_fg": "#dcfce7"},
    },
    "newyork_take5": {
        "name":          "紐約天天樂",
        "short_name":    "紐約",
        "pool_size":     39,
        "pick_count":    5,
        "draws_per_day": 1,
        "file_patterns": ["*take5*evening*", "*take5_evening*", "*newyork*take5*", "*ny_take5*"],
        "csv_filename":  "newyork_take5.csv",
        "theme":         {"primary": "#b91c1c", "light": "#fef2f2",
                          "cold_bg": "#7f1d1d", "cold_fg": "#fecaca"},
    },
}

MAX_T: int = 50
TOP_N: int = 8
PERIOD_BACKTEST_WINDOW: int = 300  # rolling window for cold-period stats (target draws only)
BET_LOG_MATCH_WINDOW: int = 500     # draw history exposed to JS for bet-log settlement

MISS_WINDOWS: List[str]        = ["10", "30", "50", "100", "300", "500", "all"]
DEFAULT_MISS_WINDOW: str        = "100"
MISS_WIN_LABELS: Dict[str, str] = {
    "10": "10期", "30": "30期", "50": "50期", "100": "100期",
    "300": "300期", "500": "500期", "all": "全歷史",
}
WIN_MIN_SAMPLE: Dict[str, int] = {
    "10": 2, "30": 3, "50": 5, "100": 8, "300": 12, "500": 15, "all": 15,
}


# ============================================================
# SECTION 0.5 — THREE-COLOR BALL HELPER
# ============================================================

def ball_cls(n: int) -> str:
    r = n % 3
    if r == 1:
        return "b-red"
    elif r == 2:
        return "b-blue"
    return "b-green"


# ============================================================
# SECTION 1 — DATA LOADER  (Supabase backend)
# ============================================================

# ── Supabase REST API helpers ─────────────────────────────────
import requests as _requests

def _supa_base() -> str:
    url = os.environ.get("SUPABASE_URL", "").rstrip("/")
    if not url:
        raise RuntimeError("請設定環境變數 SUPABASE_URL")
    # strip any accidentally included path so we always end with /rest/v1
    for suffix in ("/rest/v1", "/rest"):
        if url.endswith(suffix):
            url = url[: -len(suffix)]
    return url.rstrip("/") + "/rest/v1"

def _supa_rhdrs() -> Dict[str, str]:
    key = os.environ.get("SUPABASE_KEY", "")
    if not key:
        raise RuntimeError("請設定環境變數 SUPABASE_KEY")
    return {"apikey": key, "Authorization": f"Bearer {key}"}

def _supa_whdrs() -> Dict[str, str]:
    h = _supa_rhdrs()
    h["Content-Type"] = "application/json"
    h["Prefer"] = "resolution=merge-duplicates"
    return h


class DataLoader:
    """從 Supabase lottery_draws 表載入開獎紀錄，回傳標準化 DataFrame。"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir  # 保留供 betlog 匯出等本機操作使用

    # ── Public load methods ──────────────────────────────────────

    def load(self, lottery_key: str, config: Dict,
             cutoff_date: Optional[pd.Timestamp] = None) -> Optional[pd.DataFrame]:
        try:
            df = self._fetch(lottery_key, cutoff_date)
        except Exception as e:
            print(f"  [SKIP] {config['name']}：Supabase 讀取失敗 — {e}")
            return None
        if df is None or df.empty:
            print(f"  [SKIP] {config['name']}：Supabase 無資料（{lottery_key}）")
            return None
        return df

    def load_silent(self, lottery_key: str, config: Dict,
                    cutoff_date: Optional[pd.Timestamp] = None) -> Optional[pd.DataFrame]:
        """無輸出的靜默版本，供自動同步、時光機使用。"""
        try:
            return self._fetch(lottery_key, cutoff_date)
        except Exception:
            return None

    # ── Internal Supabase fetch with pagination ──────────────────

    def _fetch(self, key: str,
               cutoff_date: Optional[pd.Timestamp] = None) -> Optional[pd.DataFrame]:
        base  = _supa_base()
        hdrs  = _supa_rhdrs()
        PAGE  = 1000
        offset = 0
        all_rows: List[Dict] = []
        while True:
            params: Dict[str, Any] = {
                "lottery_type": f"eq.{key}",
                "select": "draw_date,num1,num2,num3,num4,num5",
                "order": "draw_date.asc",
                "offset": str(offset),
                "limit": str(PAGE),
            }
            if cutoff_date is not None:
                params["draw_date"] = f"lte.{cutoff_date.strftime('%Y-%m-%d')}"
            r = _requests.get(f"{base}/lottery_draws", headers=hdrs, params=params, timeout=30)
            r.raise_for_status()
            page = r.json()
            if not page:
                break
            all_rows.extend(page)
            if len(page) < PAGE:
                break
            offset += PAGE
        if not all_rows:
            return None
        df = pd.DataFrame(all_rows).rename(columns={
            "draw_date": "date",
            "num1": "n1", "num2": "n2", "num3": "n3",
            "num4": "n4", "num5": "n5",
        })
        df["date"] = pd.to_datetime(df["date"])
        for c in ("n1", "n2", "n3", "n4", "n5"):
            df[c] = df[c].astype(int)
        return df.sort_values("date").reset_index(drop=True)

    # ── Gap detection (pure pandas, no I/O) ─────────────────────

    def detect_gaps(self, df: pd.DataFrame, draws_per_day: int = 1) -> List[Dict]:
        """Return date gaps exceeding expected interval × 1.5 (max 10 reported)."""
        if df is None or len(df) < 2:
            return []
        dates = df["date"].sort_values().reset_index(drop=True)
        threshold_days = (1.0 / draws_per_day) * 1.5
        gaps: List[Dict] = []
        for i in range(1, len(dates)):
            delta = (dates[i] - dates[i - 1]).days
            if delta > threshold_days:
                gaps.append({
                    "from": dates[i - 1].strftime("%Y-%m-%d"),
                    "to":   dates[i].strftime("%Y-%m-%d"),
                    "days": delta,
                })
        return gaps[:10]


# ============================================================
# SECTION 2 — PERIOD REPETITION ANALYZER
# ============================================================

class PeriodRepetitionAnalyzer:

    def analyze(self, df: pd.DataFrame, max_t: int = MAX_T) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        dates = df["date"].dt.strftime("%Y-%m-%d").tolist()
        n = len(draws)

        # Only use the most recent PERIOD_BACKTEST_WINDOW draws as target draws.
        # target_start >= max_t ensures i - t >= 0 for all valid t values.
        target_start = max(max_t, n - PERIOD_BACKTEST_WINDOW)
        target_indices = range(target_start, n)

        t_stats: List[Dict] = []
        for t in range(1, max_t + 1):
            overlaps = [len(draws[i] & draws[i - t]) for i in target_indices if i - t >= 0]
            if not overlaps:
                continue
            arr = np.array(overlaps, dtype=float)
            t_stats.append({
                "t":            t,
                "overlap_prob": round(float((arr > 0).mean()) * 100, 2),
                "avg_overlap":  round(float(arr.mean()), 3),
                "sample_size":  int(len(arr)),
            })

        top8 = sorted(t_stats, key=lambda x: (x["overlap_prob"], x["t"]))[:TOP_N]
        latest_idx = n - 1
        for item in top8:
            ref_idx = latest_idx - item["t"]
            if 0 <= ref_idx < n:
                item["ref_numbers"] = sorted(list(draws[ref_idx]))
                item["ref_date"]    = dates[ref_idx]
            else:
                item["ref_numbers"] = []
                item["ref_date"]    = "資料不足"

        pool_size = max(max(d) for d in draws) if draws else 39

        # Build streak history: streak_at[i] = {num: consecutive_streak_as_of_draw_i}
        cur_stk = {num: 0 for num in range(1, pool_size + 1)}
        streak_at: List[Dict[int, int]] = []
        for i in range(n):
            for num in range(1, pool_size + 1):
                if num in draws[i]:
                    cur_stk[num] += 1
                else:
                    cur_stk[num] = 0
            streak_at.append({num: cur_stk[num] for num in draws[i]})

        recent_8: List[Dict] = []
        for i in range(max(0, n - 8), n):
            nums = sorted(list(draws[i]))
            odd_c  = sum(1 for x in nums if x % 2 == 1)
            red_c  = sum(1 for x in nums if x % 3 == 1)
            blue_c = sum(1 for x in nums if x % 3 == 2)
            recent_8.append({
                "date":    dates[i],
                "numbers": nums,
                "odd":     odd_c,
                "even":    5 - odd_c,
                "red":     red_c,
                "blue":    blue_c,
                "green":   5 - red_c - blue_c,
                "streaks": streak_at[i],
            })
        recent_8.reverse()

        recent_match: List[Dict] = []
        for i in range(max(0, n - BET_LOG_MATCH_WINDOW), n):
            recent_match.append({
                "date":    dates[i],
                "numbers": sorted(list(draws[i])),
            })
        recent_match.reverse()

        return {
            "t_stats":        t_stats,
            "top8_lowest":    top8,
            "latest_date":    dates[latest_idx],
            "latest_numbers": sorted(list(draws[latest_idx])),
            "total_draws":    n,
            "recent_8":       recent_8,
            "recent_match":   recent_match,
        }


# ============================================================
# SECTION 3 — MISS VALUE ANALYZER
# ============================================================

class MissValueAnalyzer:
    def analyze(self, df: pd.DataFrame, pool_size: int) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        all_numbers = list(range(1, pool_size + 1))
        full_miss_stats, current_misses = self._compute(df, num_cols, all_numbers)

        window_data: Dict[str, Dict] = {}
        for wkey in MISS_WINDOWS:
            if wkey == "all":
                miss_stats = full_miss_stats
            else:
                w = int(wkey)
                subset = df.iloc[-w:] if len(df) >= w else df
                miss_stats, _ = self._compute(subset, num_cols, all_numbers)

            min_s = WIN_MIN_SAMPLE.get(wkey, 10)
            candidates = [v for v in miss_stats.values() if v["total_count"] >= min_s]
            top8 = sorted(candidates, key=lambda x: (-x["no_show_rate"], x["miss_value"]))[:TOP_N]

            top8_with_numbers: List[Dict] = []
            for item in top8:
                mv = item["miss_value"]
                matching = [{"number": num} for num in all_numbers if current_misses[num] == mv]
                top8_with_numbers.append({**item, "matching_numbers": matching})

            all_num_probs: List[Dict] = []
            for num in all_numbers:
                mv   = current_misses[num]
                stat = miss_stats.get(mv, {"no_show_rate": 0.0, "no_show_count": 0, "total_count": 0})
                all_num_probs.append({
                    "number":        num,
                    "current_miss":  mv,
                    "no_show_rate":  stat["no_show_rate"],
                    "no_show_count": stat["no_show_count"],
                    "total_count":   stat["total_count"],
                })

            window_data[wkey] = {
                "miss_stats":           miss_stats,
                "top8_highest_no_show": top8_with_numbers,
                "all_number_probs":     all_num_probs,
            }

        return {"window_data": window_data, "current_misses": current_misses}

    def _compute(self, df: pd.DataFrame, num_cols: List[str], all_numbers: List[int]):
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        n = len(draws)
        miss = {num: 0 for num in all_numbers}
        bucket: Dict[int, Dict] = defaultdict(lambda: {"no_show": 0, "total": 0})

        for i in range(n - 1):
            draw_set, next_set = draws[i], draws[i + 1]
            for num in all_numbers:
                mv = miss[num]
                bucket[mv]["total"] += 1
                if num not in next_set:
                    bucket[mv]["no_show"] += 1
            for num in all_numbers:
                miss[num] = 0 if num in draw_set else miss[num] + 1

        if draws:
            for num in all_numbers:
                miss[num] = 0 if num in draws[-1] else miss[num] + 1

        miss_stats: Dict[int, Dict] = {}
        for mv, data in bucket.items():
            rate = data["no_show"] / data["total"] if data["total"] > 0 else 0.0
            miss_stats[mv] = {
                "miss_value":    mv,
                "no_show_rate":  round(rate * 100, 2),
                "no_show_count": data["no_show"],
                "total_count":   data["total"],
            }
        return miss_stats, dict(miss)


# ============================================================
# SECTION 3.4 — CONSECUTIVE DRAW ANALYZER  (v8.0 新增)
# ============================================================

class ConsecutiveDrawAnalyzer:
    """計算每個號碼的歷史連開條件機率：連k期後再開機率。"""

    def analyze(self, df: pd.DataFrame, pool_size: int = 39) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        n = len(draws)

        streak_stats: Dict[int, Dict[int, Dict]] = {num: {} for num in range(1, pool_size + 1)}
        cur_streak = {num: 0 for num in range(1, pool_size + 1)}

        for i in range(n):
            if i > 0:
                for num in range(1, pool_size + 1):
                    k = cur_streak[num]
                    if k > 0:
                        hit = 1 if num in draws[i] else 0
                        if k not in streak_stats[num]:
                            streak_stats[num][k] = {"samples": 0, "hit": 0}
                        streak_stats[num][k]["samples"] += 1
                        streak_stats[num][k]["hit"] += hit
            for num in range(1, pool_size + 1):
                if num in draws[i]:
                    cur_streak[num] += 1
                else:
                    cur_streak[num] = 0

        result: Dict[int, Dict] = {}
        for num in range(1, pool_size + 1):
            stats: Dict[int, Dict] = {}
            for k, data in streak_stats[num].items():
                prob = round(data["hit"] / data["samples"] * 100, 2) if data["samples"] > 0 else 0.0
                stats[k] = {"samples": data["samples"], "hit": data["hit"], "prob": prob}
            max_streak = max(stats.keys()) if stats else 0
            result[num] = {
                "stats":      stats,
                "max_streak": max_streak,
                "cur_streak": cur_streak[num],
            }
        return result


# ============================================================
# SECTION 3.5 — TAIL MISS ANALYZER  (v6.0+; v8.0 加入條件機率)
# ============================================================

class TailMissAnalyzer:
    """0尾~9尾 當前連續未出期數 + 條件機率（miss=m 時下期開出機率）。"""

    def analyze(self, df: pd.DataFrame) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        n = len(draws)

        tail_miss: Dict[int, int] = {}
        for t in range(10):
            miss = 0
            for i in range(n - 1, -1, -1):
                tails_in_draw = {x % 10 for x in draws[i]}
                if t in tails_in_draw:
                    break
                miss += 1
            tail_miss[t] = miss

        # 條件機率：給定尾數 t 遺漏 m 期，下期開出機率
        tail_cond: Dict[int, Dict[int, Dict]] = {t: {} for t in range(10)}
        cur_miss = {t: 0 for t in range(10)}
        for i in range(n - 1):
            tails_draw = {x % 10 for x in draws[i]}
            tails_next = {x % 10 for x in draws[i + 1]}
            for t in range(10):
                m = cur_miss[t]
                hit = 1 if t in tails_next else 0
                if m not in tail_cond[t]:
                    tail_cond[t][m] = {"samples": 0, "hit": 0}
                tail_cond[t][m]["samples"] += 1
                tail_cond[t][m]["hit"] += hit
            for t in range(10):
                cur_miss[t] = 0 if t in tails_draw else cur_miss[t] + 1

        for t in range(10):
            for m, data in tail_cond[t].items():
                data["prob"] = round(data["hit"] / data["samples"] * 100, 2) if data["samples"] > 0 else 0.0

        tail_numbers: Dict[int, List[int]] = {
            t: [x for x in range(1, 40) if x % 10 == t]
            for t in range(10)
        }
        return {"tail_miss": tail_miss, "tail_numbers": tail_numbers, "tail_cond_prob": tail_cond}


# ============================================================
# SECTION 3.6 — NUMBER HISTORY ANALYZER  (v9.0)
# ============================================================

class NumberHistoryAnalyzer:
    """Per-number: max_miss, avg_gap, danger_pct, recent_freq (last 20 draws)."""

    def analyze(self, df: pd.DataFrame, pool_size: int = 39) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        n = len(draws)
        result: Dict[int, Dict] = {}
        # Latest draw context for in_latest_draw / is_neighbor
        latest_nums: set = draws[-1] if draws else set()
        neighbor_set: set = set()
        for ln in latest_nums:
            if ln > 1:        neighbor_set.add(ln - 1)
            if ln < pool_size: neighbor_set.add(ln + 1)
        neighbor_set -= latest_nums
        for num in range(1, pool_size + 1):
            gaps: List[int] = []
            current_gap = 0
            for draw in draws:
                if num in draw:
                    if current_gap > 0:
                        gaps.append(current_gap)
                    current_gap = 0
                else:
                    current_gap += 1
            all_gaps = gaps + [current_gap]
            max_miss = max(all_gaps) if all_gaps else 0
            avg_gap  = round(sum(gaps) / len(gaps), 1) if gaps else 0.0
            danger   = min(round(current_gap / max_miss * 100) if max_miss > 0 else 0, 100)
            recent   = sum(1 for d in draws[max(0, n - 20):] if num in d)
            recent50 = sum(1 for d in draws[max(0, n - 50):] if num in d)
            # score_trend: compare recent-20 activity vs recent-50 baseline
            expected_in_20 = round(recent50 / 50 * 20, 1) if recent50 > 0 else 0
            if recent <= 1 and danger >= 30:
                trend = "up"     # cooling off + building miss → exclude score rising
            elif recent >= 4 or (recent50 >= 6 and recent >= 3):
                trend = "down"   # appearing actively → exclude score falling
            else:
                trend = "stable"
            result[num] = {
                "max_miss":       max_miss,
                "avg_gap":        avg_gap,
                "current_miss":   current_gap,
                "danger_pct":     danger,
                "recent_freq":    recent,
                "in_latest_draw": num in latest_nums,
                "is_neighbor":    num in neighbor_set,
                "score_trend":    trend,
            }
        return result


# ============================================================
# SECTION 3.7 — OE/COLOR STATS ANALYZER  (v9.0)
# ============================================================

class OEColorStatsAnalyzer:
    """Historical odd/even and color-combination distribution."""

    def analyze(self, df: pd.DataFrame, pick_count: int = 5) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [[int(x) for x in row] for row in df[num_cols].values.tolist()]
        n = len(draws)
        if n == 0:
            return {"oe_pcts": {}, "color_dist": [], "total_draws": 0}
        oe_cnt: Dict[int, int] = defaultdict(int)
        col_cnt: Dict[str, int] = defaultdict(int)
        for draw in draws:
            odd_c  = sum(1 for x in draw if x % 2 == 1)
            red_c  = sum(1 for x in draw if x % 3 == 1)
            blue_c = sum(1 for x in draw if x % 3 == 2)
            green_c = pick_count - red_c - blue_c
            oe_cnt[odd_c] += 1
            col_cnt[f"{red_c}:{blue_c}:{green_c}"] += 1
        oe_pcts = {
            k: {"count": oe_cnt.get(k, 0),
                "pct":   round(oe_cnt.get(k, 0) / n * 100, 1)}
            for k in range(pick_count + 1)
        }
        top_colors = sorted(col_cnt.items(), key=lambda x: -x[1])[:10]
        color_dist = [
            {"key": k, "count": v, "pct": round(v / n * 100, 1)}
            for k, v in top_colors
        ]
        return {"oe_pcts": oe_pcts, "color_dist": color_dist, "total_draws": n}


# ============================================================
# SECTION 3.8 — STRATEGY BACKTESTER  (v9.5)
# ============================================================

class StrategyBacktester:
    """Smart-scoring batch backtest (v9.5).

    Formula identical to sidebar _updateSmartRec:
      score = 100 - danger_pct * 0.6 - recent_freq * 8 * 0.4
    Higher score = safer pick (less likely to appear) → top-5 chosen for 五不中.
    Strict time cutoff: at period T only draws[0..T-1] are visible.
    """

    def backtest(self, df: pd.DataFrame, pool_size: int = 39,
                 top_n: int = 5, lookback: int = 500) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        n = len(draws)
        min_warm = max(pool_size, 20)
        empty = {"wins": 0, "losses": 0, "win_rate": 0.0,
                 "max_win_streak": 0, "max_loss_streak": 0,
                 "recent_20": [], "total_tested": 0}
        if n < min_warm + 2:
            return empty
        start = max(min_warm, n - lookback - 1)

        # Warmup: build cur_miss, gap_lists, max_miss through draws[0..start]
        cur_miss:  Dict[int, int]       = {num: 0 for num in range(1, pool_size + 1)}
        gap_lists: Dict[int, List[int]] = {num: [] for num in range(1, pool_size + 1)}
        for i in range(start + 1):
            for num in range(1, pool_size + 1):
                if num in draws[i]:
                    if cur_miss[num] > 0:
                        gap_lists[num].append(cur_miss[num])
                    cur_miss[num] = 0
                else:
                    cur_miss[num] += 1
        max_miss: Dict[int, int] = {}
        for num in range(1, pool_size + 1):
            all_gaps = gap_lists[num] + ([cur_miss[num]] if cur_miss[num] > 0 else [])
            max_miss[num] = max(all_gaps) if all_gaps else 0

        results: List[bool] = []
        for i in range(start, n - 1):
            # recent_freq: appearances in draws[i-19..i] (up to 20 draws)
            win_start = max(0, i - 19)
            rec_freq: Dict[int, int] = {num: 0 for num in range(1, pool_size + 1)}
            for j in range(win_start, i + 1):
                for num in draws[j]:
                    if 1 <= num <= pool_size:
                        rec_freq[num] += 1

            def _score(num: int, _cm=cur_miss, _mm=max_miss, _rf=rec_freq) -> float:
                mm = _mm[num]
                dp = min(_cm[num] / mm * 100.0 if mm > 0 else 0.0, 100.0)
                return 100.0 - dp * 0.6 - _rf[num] * 8 * 0.4

            top_nums = sorted(range(1, pool_size + 1), key=_score, reverse=True)[:top_n]
            win = all(num not in draws[i + 1] for num in top_nums)
            results.append(win)

            # Advance state with draws[i+1]
            for num in range(1, pool_size + 1):
                if num in draws[i + 1]:
                    if cur_miss[num] > 0:
                        gap_lists[num].append(cur_miss[num])
                        if cur_miss[num] > max_miss[num]:
                            max_miss[num] = cur_miss[num]
                    cur_miss[num] = 0
                else:
                    cur_miss[num] += 1

        if not results:
            return empty
        wins   = sum(1 for r in results if r)
        losses = len(results) - wins
        win_rate = round(wins / len(results) * 100, 2)
        max_win = max_loss = cw = cl = 0
        for r in results:
            if r:
                cw += 1; cl = 0
            else:
                cl += 1; cw = 0
            if cw > max_win:  max_win  = cw
            if cl > max_loss: max_loss = cl
        return {
            "wins": wins, "losses": losses, "win_rate": win_rate,
            "max_win_streak": max_win, "max_loss_streak": max_loss,
            "recent_20": [1 if r else 0 for r in results[-20:]],
            "total_tested": len(results),
        }


# ============================================================
# SECTION 3.85 — MULTI-STRATEGY BACKTESTER  (v9.7)
# ============================================================

class MultiStrategyBacktester:
    """Runs 4 scoring strategies + random baseline, ranks by win_rate and stability."""

    STRATEGIES = [
        {"id": "smart",   "label": "智能複合評分", "desc": "危險分×0.6 + 近期頻率×8×0.4"},
        {"id": "miss",    "label": "純遺漏優先",   "desc": "當前遺漏值最高的 5 號"},
        {"id": "freq",    "label": "純低頻優先",   "desc": "近20期出現最少的 5 號"},
        {"id": "danger",  "label": "純危險分優先", "desc": "逼近最大遺漏比例最低的 5 號"},
    ]
    # Theoretical random baseline for 5-from-39: C(34,5)/C(39,5)
    RANDOM_WIN_RATE = round(278256 / 575757 * 100, 1)  # ≈ 48.3%

    def backtest_all(self, df: pd.DataFrame, pool_size: int = 39,
                     top_n: int = 5, lookback: int = 300,
                     segments: int = 5) -> List[Dict]:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        n = len(draws)
        min_warm = max(pool_size, 20)
        if n < min_warm + segments * 5:
            return []

        start = max(min_warm, n - lookback - 1)
        # Warmup state
        cur_miss: Dict[int, int]       = {num: 0 for num in range(1, pool_size + 1)}
        gap_lists: Dict[int, List[int]] = {num: [] for num in range(1, pool_size + 1)}
        for i in range(start + 1):
            for num in range(1, pool_size + 1):
                if num in draws[i]:
                    if cur_miss[num] > 0:
                        gap_lists[num].append(cur_miss[num])
                    cur_miss[num] = 0
                else:
                    cur_miss[num] += 1
        max_miss: Dict[int, int] = {
            num: max(gap_lists[num] + ([cur_miss[num]] if cur_miss[num] > 0 else []), default=0)
            for num in range(1, pool_size + 1)
        }

        # Collect per-period results for each strategy
        all_results: Dict[str, List[bool]] = {s["id"]: [] for s in self.STRATEGIES}

        for i in range(start, n - 1):
            win_start = max(0, i - 19)
            rec_freq: Dict[int, int] = {num: 0 for num in range(1, pool_size + 1)}
            for j in range(win_start, i + 1):
                for num in draws[j]:
                    if 1 <= num <= pool_size:
                        rec_freq[num] += 1

            def _dp(num: int) -> float:
                mm = max_miss[num]
                return min(cur_miss[num] / mm * 100.0 if mm > 0 else 0.0, 100.0)

            scores: Dict[str, Dict[int, float]] = {
                "smart":  {num: 100.0 - _dp(num) * 0.6 - rec_freq[num] * 8 * 0.4
                           for num in range(1, pool_size + 1)},
                "miss":   {num: float(cur_miss[num]) for num in range(1, pool_size + 1)},
                "freq":   {num: -float(rec_freq[num]) for num in range(1, pool_size + 1)},
                "danger": {num: 100.0 - _dp(num) for num in range(1, pool_size + 1)},
            }
            for sid, sc in scores.items():
                top_nums = sorted(range(1, pool_size + 1), key=lambda x: sc[x], reverse=True)[:top_n]
                win = all(num not in draws[i + 1] for num in top_nums)
                all_results[sid].append(win)

            # Advance state
            for num in range(1, pool_size + 1):
                if num in draws[i + 1]:
                    if cur_miss[num] > 0:
                        gap_lists[num].append(cur_miss[num])
                        if cur_miss[num] > max_miss[num]:
                            max_miss[num] = cur_miss[num]
                    cur_miss[num] = 0
                else:
                    cur_miss[num] += 1

        output: List[Dict] = []
        for strat in self.STRATEGIES:
            raw = all_results[strat["id"]]
            if not raw:
                continue
            wr = round(sum(raw) / len(raw) * 100, 1)
            # Near-100 win rate for trend stability comparison
            r100_slice = raw[-100:] if len(raw) >= 100 else []
            recent_100_rate: Optional[float] = (
                round(sum(r100_slice) / len(r100_slice) * 100, 1) if r100_slice else None
            )
            seg_size = max(len(raw) // segments, 1)
            seg_rates = []
            for s in range(segments):
                chunk = raw[s * seg_size:(s + 1) * seg_size]
                if chunk:
                    seg_rates.append(round(sum(chunk) / len(chunk) * 100, 1))
            import statistics as _stats
            stability = round(_stats.stdev(seg_rates), 1) if len(seg_rates) >= 2 else 0.0
            delta = round(wr - self.RANDOM_WIN_RATE, 1)
            output.append({
                **strat,
                "win_rate": wr,
                "total": len(raw),
                "stability_std": stability,
                "delta_vs_random": delta,
                "recent_10": [1 if r else 0 for r in raw[-10:]],
                "recent_100_rate": recent_100_rate,
            })

        # Sort by win_rate desc, stability asc
        output.sort(key=lambda x: (-x["win_rate"], x["stability_std"]))
        return output


# ============================================================
# SECTION 3.87 — EXCLUDE SCORE TUNER  (v9.7)
# ============================================================

class ExcludeScoreTuner:
    """Backtests 5 weight presets for calcExcludeScore and returns the best."""

    # Each preset: (dp_weight, freq_weight, latest_bonus, neighbor_bonus, cold_bonus)
    PRESETS = [
        {"id": "balanced", "label": "均衡型",
         "dp": 0.35, "freq": 5, "latest": 25, "neighbor": 12, "cold": 10},
        {"id": "heavy_dp", "label": "危險分主導",
         "dp": 0.55, "freq": 3, "latest": 20, "neighbor": 8,  "cold": 8},
        {"id": "heavy_freq", "label": "頻率主導",
         "dp": 0.20, "freq": 9, "latest": 22, "neighbor": 10, "cold": 8},
        {"id": "latest_focus", "label": "最新期重視",
         "dp": 0.30, "freq": 4, "latest": 35, "neighbor": 18, "cold": 10},
        {"id": "conservative", "label": "保守型",
         "dp": 0.45, "freq": 6, "latest": 18, "neighbor": 8,  "cold": 6},
    ]

    def tune(self, df: pd.DataFrame, pool_size: int = 39,
             top_n: int = 5, lookback: int = 300) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        n = len(draws)
        min_warm = max(pool_size, 20)
        empty: Dict = {"presets": [], "best_id": "", "best_label": ""}
        if n < min_warm + 10:
            return empty

        start = max(min_warm, n - lookback - 1)
        cur_miss:  Dict[int, int]       = {num: 0 for num in range(1, pool_size + 1)}
        gap_lists: Dict[int, List[int]] = {num: [] for num in range(1, pool_size + 1)}
        latest_draw_nums: set            = set()
        for i in range(start + 1):
            latest_draw_nums = draws[i]
            for num in range(1, pool_size + 1):
                if num in draws[i]:
                    if cur_miss[num] > 0:
                        gap_lists[num].append(cur_miss[num])
                    cur_miss[num] = 0
                else:
                    cur_miss[num] += 1
        max_miss: Dict[int, int] = {
            num: max(gap_lists[num] + ([cur_miss[num]] if cur_miss[num] > 0 else []), default=0)
            for num in range(1, pool_size + 1)
        }
        # Neighbors of latest draw (±1, ±2, ±10)
        def _neighbors(nums: set) -> set:
            nb: set = set()
            for n_ in nums:
                for d in (1, -1, 2, -2, 10, -10):
                    x = n_ + d
                    if 1 <= x <= pool_size and x not in nums:
                        nb.add(x)
            return nb

        per_preset: Dict[str, List[bool]] = {p["id"]: [] for p in self.PRESETS}

        for i in range(start, n - 1):
            win_start = max(0, i - 19)
            rec_freq: Dict[int, int] = {num: 0 for num in range(1, pool_size + 1)}
            for j in range(win_start, i + 1):
                for num in draws[j]:
                    if 1 <= num <= pool_size:
                        rec_freq[num] += 1
            neighbors = _neighbors(latest_draw_nums)
            for p in self.PRESETS:
                def _exc(num: int, _cm=cur_miss, _mm=max_miss, _rf=rec_freq,
                         _ld=latest_draw_nums, _nb=neighbors, _p=p) -> float:
                    mm = _mm[num]; cm = _cm[num]; rf = _rf[num]
                    dp = min(cm / mm * 100.0 if mm > 0 else 0.0, 100.0)
                    s = 0.0
                    if cm <= 3: s += 20
                    elif cm <= 8: s += 10
                    s += min(rf * _p["freq"], 25)
                    s += dp * _p["dp"]
                    if num in _ld: s += _p["latest"]
                    if num in _nb: s += _p["neighbor"]
                    return s
                # High exclude score = dangerous → AVOID → pick opposite (low score = safe)
                # We test: exclude top-5-by-score, check 五不中 on remaining
                # i.e., pick 5 with LOWEST exclude score as safe
                top5 = sorted(range(1, pool_size + 1), key=_exc)[:top_n]
                win = all(num not in draws[i + 1] for num in top5)
                per_preset[p["id"]].append(win)
            # Advance
            latest_draw_nums = draws[i + 1]
            for num in range(1, pool_size + 1):
                if num in draws[i + 1]:
                    if cur_miss[num] > 0:
                        gap_lists[num].append(cur_miss[num])
                        if cur_miss[num] > max_miss[num]:
                            max_miss[num] = cur_miss[num]
                    cur_miss[num] = 0
                else:
                    cur_miss[num] += 1

        results = []
        for p in self.PRESETS:
            raw = per_preset[p["id"]]
            if not raw:
                continue
            wr = round(sum(raw) / len(raw) * 100, 1)
            delta = round(wr - MultiStrategyBacktester.RANDOM_WIN_RATE, 1)
            results.append({**p, "win_rate": wr, "total": len(raw),
                            "delta": delta,
                            "recent_10": [1 if r else 0 for r in raw[-10:]]})
        results.sort(key=lambda x: -x["win_rate"])
        best = results[0] if results else {"id": "balanced", "label": "均衡型"}
        return {"presets": results, "best_id": best["id"], "best_label": best["label"]}


# ============================================================
# SECTION 3.9 — RECENT HEAT PROBABILITY ANALYZER  (v9.6)
# ============================================================

class RecentHeatProbabilityAnalyzer:
    """Rolling backtest: for each number n, compute P(n appears next | n appeared k times in last W draws).

    Strict time integrity: each target draw i only uses draws[i-window..i-1].
    """

    def analyze(self, df: pd.DataFrame, pool_size: int = 39,
                window: int = 20, backtest_samples: int = 500) -> Dict:
        num_cols = [c for c in df.columns if c.startswith("n") and c[1:].isdigit()]
        draws = [set(int(x) for x in row) for row in df[num_cols].values.tolist()]
        n_draws = len(draws)
        empty: Dict = {"window": window, "backtest_samples": 0, "numbers": {}, "matrix": {}}
        if n_draws < window + 1:
            return empty

        # matrix[num][k] = {"hit": int, "total": int}
        matrix: Dict[int, Dict[int, Dict[str, int]]] = {
            num: {} for num in range(1, pool_size + 1)
        }
        start_idx = max(window, n_draws - backtest_samples)

        for i in range(start_idx, n_draws):
            win_draws = draws[i - window: i]   # strictly before draw[i]
            target    = draws[i]
            for num in range(1, pool_size + 1):
                k = sum(1 for d in win_draws if num in d)
                if k not in matrix[num]:
                    matrix[num][k] = {"hit": 0, "total": 0}
                matrix[num][k]["total"] += 1
                if num in target:
                    matrix[num][k]["hit"] += 1

        # Current heat: appearances in the last `window` draws
        recent_draws = draws[max(0, n_draws - window):]
        result_numbers: Dict[int, Dict] = {}
        for num in range(1, pool_size + 1):
            recent_count = sum(1 for d in recent_draws if num in d)
            entry = matrix[num].get(recent_count, {"hit": 0, "total": 0})
            hit   = entry["hit"]
            total = entry["total"]
            rate  = round(hit / total * 100, 1) if total >= 1 else None
            result_numbers[num] = {
                "recent_count":  recent_count,
                "next_hit_rate": rate,
                "hit_count":     hit,
                "sample_count":  total,
            }

        matrix_out: Dict[int, Dict] = {}
        for num in range(1, pool_size + 1):
            matrix_out[num] = {
                k: {
                    "rate":  round(v["hit"] / v["total"] * 100, 1) if v["total"] > 0 else None,
                    "hit":   v["hit"],
                    "total": v["total"],
                }
                for k, v in sorted(matrix[num].items())
                if v["total"] > 0
            }

        return {
            "window":            window,
            "backtest_samples":  n_draws - start_idx,
            "numbers":           result_numbers,
            "matrix":            matrix_out,
        }


# ============================================================
# SECTION 4 — SCRAPERS  (single + bulk)
# ============================================================

class LotteryScrapers:
    TIMEOUT = 20
    CHROME_UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/136.0.0.0 Safari/537.36"
    )
    HEADERS = {
        "User-Agent": CHROME_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "application/json;q=0.8,*/*;q=0.7",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    JSON_HEADERS = {
        **HEADERS,
        "Accept": "application/json,text/plain,*/*",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    WEEKDAY_DATE_RE = re.compile(
        r"^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\s+"
        r"[A-Z][a-z]+\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s+\d{4}"
        r"(?:\s+-\s+\d{1,2}:\d{2}\s*(?:am|pm))?",
        re.I,
    )
    WEEKDAY_ONLY_RE = re.compile(
        r"^(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)$",
        re.I,
    )
    MONTH_DATE_RE = re.compile(
        r"^[A-Z][a-z]+\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s+\d{4}"
        r"(?:\s+-\s+\d{1,2}:\d{2}\s*(?:am|pm))?$",
        re.I,
    )
    _SESSION = None

    @classmethod
    def _session(cls):
        if cls._SESSION is not None:
            return cls._SESSION
        import requests
        session = requests.Session()
        session.headers.update(cls.HEADERS)
        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            retry = Retry(
                total=2,
                connect=2,
                read=2,
                status=2,
                backoff_factor=0.45,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET"]),
            )
            adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
        except Exception:
            pass
        cls._SESSION = session
        return session

    _BLOCK_SIGNALS = ("just a moment", "cloudflare", "access denied",
                      "enable javascript", "please wait", "checking your browser")

    @classmethod
    def _is_blocked(cls, r) -> bool:
        if r is None:
            return True
        if r.status_code in (403, 429, 503):
            return True
        try:
            low = r.text[:1200].lower()
            return any(s in low for s in cls._BLOCK_SIGNALS)
        except Exception:
            return False

    @classmethod
    def _get(cls, url: str, headers: Optional[Dict[str, str]] = None,
             referer: Optional[str] = None, timeout: Optional[int] = None):
        try:
            req_headers = dict(cls.HEADERS)
            if headers:
                req_headers.update(headers)
            if referer:
                req_headers["Referer"] = referer
            r = cls._session().get(url, headers=req_headers,
                                   timeout=timeout or cls.TIMEOUT)
            if r.status_code >= 400 or cls._is_blocked(r):
                return None
            return r
        except Exception:
            return None

    @classmethod
    def _json(cls, url: str, headers: Optional[Dict[str, str]] = None,
              referer: Optional[str] = None):
        r = cls._get(url, headers=headers or cls.JSON_HEADERS, referer=referer)
        if not r:
            return None
        try:
            return r.json()
        except Exception:
            return None

    @staticmethod
    def _response_text(response) -> str:
        try:
            if not response.encoding:
                response.encoding = response.apparent_encoding or "utf-8"
        except Exception:
            pass
        return response.text if response else ""

    @staticmethod
    def _normalize_date(value: Any) -> Optional[str]:
        if value is None:
            return None
        s = str(value).strip()
        roc = re.match(r"^(\d{2,3})[/-](\d{1,2})[/-](\d{1,2})", s)
        if roc:
            y = int(roc.group(1)) + 1911
            return f"{y:04d}-{int(roc.group(2)):02d}-{int(roc.group(3)):02d}"
        s = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", s, flags=re.I)
        s = re.split(r"\s+-\s+\d{1,2}:\d{2}\s*(?:am|pm)", s, flags=re.I)[0]
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def _numbers_from_any(value: Any) -> List[int]:
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            raw = []
            for item in value:
                raw.extend(re.findall(r"\d+", str(item)))
        else:
            raw = re.findall(r"\d+", str(value))
        nums = [int(x) for x in raw[:5]]
        if len(nums) != 5 or len(set(nums)) != 5:
            return []
        if not all(1 <= n <= 39 for n in nums):
            return []
        return sorted(nums)

    @classmethod
    def _draw(cls, date_value: Any, numbers_value: Any) -> Optional[Dict]:
        date_s = cls._normalize_date(date_value)
        nums = cls._numbers_from_any(numbers_value)
        if not date_s or not nums:
            return None
        return {"date": date_s, "numbers": nums}

    @classmethod
    def _clean_draws(cls, draws: List[Dict], count: int) -> List[Dict]:
        seen = set()
        clean: List[Dict] = []
        for d in draws:
            if not d or "error" in d:
                continue
            draw = cls._draw(d.get("date"), d.get("numbers"))
            if not draw or draw["date"] in seen:
                continue
            seen.add(draw["date"])
            clean.append(draw)
        clean.sort(key=lambda x: x["date"], reverse=True)
        return clean[:count]

    @classmethod
    def _parse_lotto_archive_html(cls, html: str,
                                  time_filter: Optional[str] = None) -> List[Dict]:
        draws: List[Dict] = []
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            lines = [
                line.strip()
                for line in soup.get_text("\n").splitlines()
                if line.strip()
            ]
        except Exception:
            return draws

        for i, line in enumerate(lines):
            m = cls.WEEKDAY_DATE_RE.match(line)
            scan_from = i + 1
            if m:
                date_line = m.group(0)
            elif (
                cls.WEEKDAY_ONLY_RE.match(line)
                and i + 1 < len(lines)
                and cls.MONTH_DATE_RE.match(lines[i + 1])
            ):
                date_line = line + " " + lines[i + 1]
                scan_from = i + 2
            else:
                continue
            if time_filter and time_filter.lower() not in date_line.lower():
                continue
            date_s = cls._normalize_date(date_line)
            if not date_s:
                continue
            nums: List[int] = []
            for nxt in lines[scan_from:scan_from + 28]:
                if cls.WEEKDAY_DATE_RE.match(nxt) or cls.WEEKDAY_ONLY_RE.match(nxt):
                    break
                if re.fullmatch(r"\d{1,2}", nxt):
                    n = int(nxt)
                    if 1 <= n <= 39:
                        nums.append(n)
                        if len(nums) == 5:
                            break
            draw = cls._draw(date_s, nums)
            if draw:
                draws.append(draw)
        return draws

    @classmethod
    def _scrape_year_archives(cls, url_template: str, count: int,
                              min_year: int = 2010,
                              time_filter: Optional[str] = None) -> List[Dict]:
        results: List[Dict] = []
        year = datetime.now().year
        for y in range(year, min_year - 1, -1):
            url = url_template.format(year=y)
            r = cls._get(url)
            if not r:
                continue
            results.extend(cls._parse_lotto_archive_html(
                cls._response_text(r), time_filter=time_filter
            ))
            if len(results) >= count:
                break
            time.sleep(0.15)
        return cls._clean_draws(results, count)

    # ── Single-draw scrapers ──────────────────────────────────

    @classmethod
    def scrape_taiwan_539(cls) -> Dict:
        draws = cls.scrape_taiwan_539_bulk(count=1)
        return draws[0] if draws else {"error": "爬取失敗"}

    @classmethod
    def scrape_michigan_fantasy5(cls) -> Dict:
        draws = cls.scrape_michigan_fantasy5_bulk(count=1)
        return draws[0] if draws else {"error": "爬取失敗"}

    @classmethod
    def scrape_california_fantasy5(cls) -> Dict:
        draws = cls.scrape_california_fantasy5_bulk(count=1)
        return draws[0] if draws else {"error": "爬取失敗"}

    @classmethod
    def scrape_newyork_take5(cls) -> Dict:
        draws = cls.scrape_newyork_take5_bulk(count=1)
        return draws[0] if draws else {"error": "爬取失敗"}

    # ── Bulk scrapers ─────────────────────────────────────────

    @classmethod
    def scrape_taiwan_539_bulk(cls, count: int = 30) -> List[Dict]:
        """台灣 539：優先使用輕量鏡像頁，失敗再退回台彩歷史頁。"""
        results: List[Dict] = []

        r = cls._get("https://api.lottery.com.tw/l539?c=list")
        if r:
            text = cls._response_text(r)
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(text, "html.parser")
                for box in soup.select(".balls_div_list"):
                    date_el = box.select_one(".date")
                    nums = [s.get_text(strip=True) for s in box.select(".button_yellowball_list")]
                    draw = cls._draw(date_el.get_text(strip=True) if date_el else "", nums)
                    if draw:
                        results.append(draw)
            except Exception:
                pass
            if not results:
                compact = re.sub(r"<[^>]+>", " ", text)
                pattern = re.compile(
                    r"(?P<date>\d{2,3}[/-]\d{1,2}[/-]\d{1,2}).{0,90}?"
                    r"頭獎[:：]\s*\d+\s+"
                    r"(?P<nums>\d{1,2}\s+\d{1,2}\s+\d{1,2}\s+\d{1,2}\s+\d{1,2})",
                    re.S,
                )
                for m in pattern.finditer(compact):
                    draw = cls._draw(m.group("date"), m.group("nums"))
                    if draw:
                        results.append(draw)

        if not results:
            try:
                from bs4 import BeautifulSoup
                r = cls._get(
                    "https://www.taiwanlottery.com.tw/lotto/lotto539/history.aspx",
                    referer="https://www.taiwanlottery.com.tw/",
                )
                if r:
                    soup = BeautifulSoup(cls._response_text(r), "html.parser")
                    for tr in soup.find_all("tr"):
                        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td", "th"])]
                        if len(cells) < 6:
                            continue
                        for i, cell in enumerate(cells):
                            date_s = cls._normalize_date(cell)
                            if not date_s:
                                continue
                            nums = []
                            for nxt in cells[i + 1:i + 12]:
                                if re.fullmatch(r"\d{1,2}", nxt):
                                    nums.append(int(nxt))
                                if len(nums) == 5:
                                    break
                            draw = cls._draw(date_s, nums)
                            if draw:
                                results.append(draw)
                                break
            except Exception:
                pass

        return cls._clean_draws(results, count)

    @classmethod
    def scrape_michigan_fantasy5_bulk(cls, count: int = 30) -> List[Dict]:
        """Michigan Fantasy 5：依序嘗試多個來源，任一成功即回傳。"""
        for url_tmpl in (
            "https://www.lottery.net/michigan/fantasy-5/numbers/{year}",
            "https://www.lotto.net/michigan-fantasy-5/numbers/{year}",
            "https://www.lotterycorner.com/mi/f5/{year}.html",
        ):
            results = cls._scrape_year_archives(url_tmpl, count=count, min_year=2010)
            if results:
                print(f"[SCRAPER] Michigan Fantasy5: 使用來源 {url_tmpl.split('/')[2]}")
                return results
        print("[SCRAPER] Michigan Fantasy5: 所有來源均無回應（Render IP 可能被封鎖）")
        return []

    @classmethod
    def scrape_california_fantasy5_bulk(cls, count: int = 30) -> List[Dict]:
        """California Fantasy 5：官方 API 被擋時退到 Lottery.net 年度歸檔。"""
        results: List[Dict] = []
        per_page = 50
        pages = max(1, min(120, -(-count // per_page)))
        for page in range(1, pages + 1):
            url = (
                "https://www.calottery.com/api/DrawGameApi/"
                f"DrawGamePastDrawResults/9/{per_page}/{page}"
            )
            data = cls._json(url, referer="https://www.calottery.com/en/draw-games/fantasy-5")
            if not data:
                break
            rows = data.get("DrawGamePastDrawResults", []) if isinstance(data, dict) else data
            if not rows:
                break
            for d in rows:
                if not isinstance(d, dict):
                    continue
                draw = cls._draw(
                    d.get("DrawDate") or d.get("drawDate") or d.get("date"),
                    d.get("WinningNumbers") or d.get("winningNumbers") or d.get("numbers"),
                )
                if draw:
                    results.append(draw)
            if len(results) >= count:
                break

        if results:
            print("[SCRAPER] California Fantasy5: 使用官方 API")
            return cls._clean_draws(results, count)

        for url_tmpl in (
            "https://www.lottery.net/california/fantasy-5/numbers/{year}",
            "https://www.lotto.net/california-fantasy-5/numbers/{year}",
        ):
            html_results = cls._scrape_year_archives(url_tmpl, count=count, min_year=2010)
            if html_results:
                print(f"[SCRAPER] California Fantasy5: 使用來源 {url_tmpl.split('/')[2]}")
                return html_results
        print("[SCRAPER] California Fantasy5: 所有來源均無回應（Render IP 可能被封鎖）")
        return []

    @classmethod
    def scrape_newyork_take5_bulk(cls, count: int = 30) -> List[Dict]:
        """NY Take 5 晚盤（Evening）。

        策略：同時抓 lotto.net 與 NY Open Data，合併去重後取最新。
        lotto.net 通常當天即更新；NY Open Data 可能延遲 1 天以上。
        兩源互為 fallback，防止任一來源落後時漏抓最新一期。
        """
        pool: List[Dict] = []

        # ── Source A: lotto.net（通常最即時）──────────────────
        lotto_draws: List[Dict] = []
        cur_year = datetime.now().year
        for y in range(cur_year, cur_year - 2, -1):
            url = f"https://www.lotto.net/new-york-take-5/numbers/{y}"
            r = cls._get(url)
            if r:
                # Use "10:30" (without am/pm suffix) so it matches both
                # "10:30pm" and "10:30 PM" — midday Take5 is at 2:30pm.
                parsed = cls._parse_lotto_archive_html(
                    cls._response_text(r), time_filter="10:30"
                )
                lotto_draws.extend(parsed)
                if len(lotto_draws) >= count:
                    break
            time.sleep(0.15)
        if lotto_draws:
            lotto_latest = max(d["date"] for d in lotto_draws)
            print(f"[SCRAPER] NY Take5 evening: lotto.net latest {lotto_latest}")
        else:
            print("[SCRAPER] NY Take5 evening: lotto.net 無回應或無資料 — "
                  "time_filter 可能需要更新（預期篩選字串：'10:30'）")
        pool.extend(lotto_draws)

        # ── Source B: NY Open Data（可能延遲，但資料齊全）────────
        nyod_draws: List[Dict] = []
        limit = min(max(count + 20, 100), 50000)
        nyod_url = (
            "https://data.ny.gov/resource/dg63-4siq.json"
            f"?$limit={limit}&$order=draw_date%20DESC"
        )
        data = cls._json(nyod_url, headers=cls.JSON_HEADERS,
                         referer="https://data.ny.gov/d/dg63-4siq")
        if isinstance(data, list):
            for d in data:
                if not isinstance(d, dict):
                    continue
                draw = cls._draw(
                    d.get("draw_date"),
                    d.get("evening_winning_numbers"),
                )
                if draw:
                    nyod_draws.append(draw)
        if nyod_draws:
            nyod_latest = max(d["date"] for d in nyod_draws)
            print(f"[SCRAPER] NY Take5 evening: NY Open Data latest {nyod_latest}")
        else:
            print("[SCRAPER] NY Take5 evening: NY Open Data 無回應或無資料")
        pool.extend(nyod_draws)

        if pool:
            return cls._clean_draws(pool, count)

        print("[SCRAPER] NY Take5 evening: 兩個來源均無資料")
        return []

    @classmethod
    def bulk(cls, key: str, count: int) -> List[Dict]:
        """統一入口：根據 key 呼叫對應的 bulk 方法"""
        method_map = {
            "taiwan_539":          cls.scrape_taiwan_539_bulk,
            "michigan_fantasy5":   cls.scrape_michigan_fantasy5_bulk,
            "california_fantasy5": cls.scrape_california_fantasy5_bulk,
            "newyork_take5":       cls.scrape_newyork_take5_bulk,
        }
        method = method_map.get(key)
        return method(count=count) if method else []

    @classmethod
    def scrape_all(cls) -> Dict[str, Dict]:
        return {
            "taiwan_539":          cls.scrape_taiwan_539(),
            "michigan_fantasy5":   cls.scrape_michigan_fantasy5(),
            "california_fantasy5": cls.scrape_california_fantasy5(),
            "newyork_take5":       cls.scrape_newyork_take5(),
        }


# ============================================================
# SECTION 5 — AUTO SYNC MANAGER  (v5.0 新增)
# ============================================================

class AutoSyncManager:
    """
    啟動時自動偵測本地資料落後天數，
    透過 Bulk Scraper 補齊漏掉的開獎期數。
    """

    def __init__(self, data_dir: Path, writer: "DataWriter"):
        self.data_dir = data_dir
        self.writer   = writer
        self.loader   = DataLoader(data_dir)

    def sync_all(self) -> Dict[str, Dict]:
        """同步全部 4 種彩票，回傳各自的同步結果"""
        report: Dict[str, Dict] = {}
        for key, cfg in LOTTERY_CONFIG.items():
            report[key] = self._sync_one(key, cfg)
        return report

    def _sync_one(self, key: str, cfg: Dict) -> Dict:
        today_d = datetime.now().date()

        # 取得本地最新日期
        df = self.loader.load_silent(key, cfg)
        if df is None or df.empty:
            latest_local = None
            gap_days = 999
        else:
            latest_local = df["date"].max().date()
            gap_days = (today_d - latest_local).days

        if gap_days <= 0:
            return {"new_count": 0, "message": "資料已是最新", "gap_days": 0}

        print(f"  [SYNC] {cfg['name']}：本地落後 {gap_days} 天，嘗試補齊...")

        # 估算需抓取期數（加緩衝）
        dpd         = cfg.get("draws_per_day", 1)
        need_count  = max(1, gap_days * dpd + 10)
        draws_raw   = LotteryScrapers.bulk(key, need_count)

        if not draws_raw:
            return {
                "new_count": 0,
                "message":   "爬蟲無回應，請稍後手動更新",
                "gap_days":  gap_days,
            }

        # 只保留比本地更新的資料，並由舊到新寫入，避免 CSV 時序倒插。
        new_draws: List[Dict] = []
        for draw in draws_raw:
            if "error" in draw:
                continue
            try:
                draw_date = pd.to_datetime(draw["date"], errors="coerce").date()
            except Exception:
                continue
            if pd.isna(pd.to_datetime(draw["date"], errors="coerce")):
                continue
            if draw_date > today_d:
                continue
            if latest_local is not None and draw_date <= latest_local:
                continue
            new_draws.append({
                "date": draw_date.strftime("%Y-%m-%d"),
                "numbers": sorted(int(n) for n in draw["numbers"])[:5],
            })

        new_draws.sort(key=lambda d: d["date"])

        new_count = 0
        for draw in new_draws:
            if self.writer.append(key, cfg, draw["date"], draw["numbers"]):
                new_count += 1

        msg = f"補齊 {new_count} 期" if new_count else f"已抓到 {len(draws_raw)} 期，但沒有比本地更新的資料"
        print(f"  [SYNC] {cfg['name']}：{msg}")
        return {"new_count": new_count, "message": msg, "gap_days": gap_days}


# ============================================================
# SECTION 6 — DATA WRITER
# ============================================================

class DataWriter:
    """以 Supabase upsert 寫入開獎紀錄至雲端 lottery_draws 表。"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir  # 保留供 betlog 匯出等本機操作使用

    def append(self, lottery_key: str, config: Dict,
               date_str: str, numbers: List[int]) -> bool:
        """Upsert 一筆開獎紀錄，衝突時（相同彩種+日期）靜默忽略。"""
        date_obj = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(date_obj):
            return False
        date_fmt = date_obj.strftime("%Y-%m-%d")
        nums = sorted(int(n) for n in numbers)[:5]
        if len(nums) != 5 or len(set(nums)) != 5 or not all(1 <= n <= 39 for n in nums):
            return False
        try:
            r = _requests.post(
                f"{_supa_base()}/lottery_draws?on_conflict=lottery_type,draw_date",
                headers=_supa_whdrs(),
                json={
                    "lottery_type": lottery_key,
                    "draw_date":    date_fmt,
                    "num1": nums[0], "num2": nums[1], "num3": nums[2],
                    "num4": nums[3], "num5": nums[4],
                },
                timeout=30,
            )
            r.raise_for_status()
            print(f"  [SAVE] {config['name']}  {date_fmt}  {nums} → Supabase")
            return True
        except Exception as e:
            print(f"  [ERROR] {config['name']} 寫入失敗：{e}")
            return False


# ============================================================
# SECTION 7 — HTML REPORT GENERATOR  v6.0
# ============================================================

import json as _json

# ── Reverse OE/Color helpers (v10.2) ──────────────────────────

def _lrr(values: List[float], total: int) -> List[int]:
    """Largest remainder rounding: float proportions → integers summing to total."""
    s = sum(values)
    if s == 0:
        n = len(values)
        return [total // n + (1 if i < total % n else 0) for i in range(n)]
    raw    = [v / s * total for v in values]
    floors = [int(r) for r in raw]
    deficit = total - sum(floors)
    order  = sorted(range(len(raw)), key=lambda i: -(raw[i] - floors[i]))
    for i in range(deficit):
        floors[order[i]] += 1
    return floors

def _compute_rev_oe(recent_8: List[Dict]) -> Optional[Dict]:
    """Compute reverse OE/color recommendation from last 3 draws of recent_8."""
    recent_3 = recent_8[:3]
    if not recent_3:
        return None
    t_odd   = sum(d.get("odd",   0) for d in recent_3)
    t_even  = sum(d.get("even",  0) for d in recent_3)
    t_red   = sum(d.get("red",   0) for d in recent_3)
    t_blue  = sum(d.get("blue",  0) for d in recent_3)
    t_green = sum(d.get("green", 0) for d in recent_3)
    oe  = _lrr([float(t_odd), float(t_even)], 5)
    col = _lrr([float(t_red),  float(t_blue),  float(t_green)], 5)
    # Ensure each color ≥ 1
    col = [max(1, x) for x in col]
    excess = sum(col) - 5
    if excess > 0:
        for _ in range(excess):
            idx = col.index(max(col))
            col[idx] -= 1
    def _gen_alts(base: List[int]) -> List[List[int]]:
        alts: List[List[int]] = []
        for i in range(3):
            for j in range(3):
                if i == j:
                    continue
                if base[i] >= 2:
                    alt = base[:]
                    alt[i] -= 1
                    alt[j] += 1
                    if all(x >= 1 for x in alt) and alt not in alts:
                        alts.append(alt)
        return alts[:2]
    alt_cols = _gen_alts(col)
    return {
        "stats":    {"odd": t_odd, "even": t_even, "red": t_red, "blue": t_blue, "green": t_green},
        "main_oe":  {"odd": oe[0],  "even": oe[1]},
        "main_col": {"red": col[0], "blue": col[1], "green": col[2]},
        "alt_cols": [{"red": a[0], "blue": a[1], "green": a[2]} for a in alt_cols],
    }


class HTMLReportGenerator:

    # ── Public ──────────────────────────────────────────────

    def generate(self, results: Dict, output_path: Path, server_mode: bool = False) -> None:
        output_path.write_text(self._build_page(results, server_mode), encoding="utf-8")
        print(f"\n{'='*60}")
        if server_mode:
            print("  報告已更新 → http://localhost:5000")
        else:
            print(f"  報告已產生：{output_path.resolve()}")
        print(f"{'='*60}")

    # ── Page ────────────────────────────────────────────────

    def _build_page(self, results: Dict, server_mode: bool) -> str:
        ts           = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tabs, panels = self._build_tabs_panels(results, server_mode)
        sidebar      = self._build_sidebar(results)
        float_panel  = self._build_float_panel(server_mode)
        js           = self._build_js(server_mode)

        return (
            '<!DOCTYPE html>\n<html lang="zh-TW">\n<head>\n'
            '<meta charset="UTF-8">\n'
            '<meta name="viewport" content="width=device-width,initial-scale=1.0">\n'
            '<title>彩票冷門分析 v10.2</title>\n'
            '<style>' + self._css() + '</style>\n'
            '</head>\n<body>\n'
            + float_panel +
            '<header class="page-header">'
            '<div style="font-size:1.8rem;line-height:1">🎯</div>'
            '<div style="flex:1">'
            '<h1 class="page-title">彩票冷門篩選分析 v10.2</h1>'
            '<div class="page-sub">智能同步｜時光機｜連莊標記｜條件機率回測｜固定側邊選號盤　' + ts + '</div>'
            '</div>'
            '<button class="fp-header-btn" onclick="toggleFloatPanel()">📡 數據管理</button>'
            '</header>\n'
            '<div class="main-layout">'
            '<main class="main-content">'
            '<div class="card">'
            '<div class="tab-bar" id="tab-bar">' + tabs + '</div>'
            '<div id="panels-container">' + panels + '</div>'
            '</div>'
            '</main>'
            + sidebar +
            '</div>'
            '<footer>本報告僅供數據研究，不構成任何投注建議。使用前請遵守當地法規。</footer>\n'
            '<script>' + js + '</script>\n'
            '</body>\n</html>'
        )

    # ── CSS ─────────────────────────────────────────────────

    def _css(self) -> str:
        return """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Segoe UI',system-ui,sans-serif;background:#f1f5f9;color:#1e293b;min-height:100vh}

/* Header */
.page-header{background:linear-gradient(135deg,#1e293b 0%,#312e81 100%);
  padding:.85rem 1.5rem;display:flex;align-items:center;gap:.85rem;
  box-shadow:0 4px 24px rgba(0,0,0,.35);position:sticky;top:0;z-index:200}
.page-title{font-size:1.1rem;font-weight:900;color:#fff}
.page-sub{font-size:.65rem;color:#a5b4fc;margin-top:.15rem}
.fp-header-btn{margin-left:auto;background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.2);
  color:#fff;padding:.45rem .9rem;border-radius:.5rem;cursor:pointer;font-size:.75rem;font-weight:700;
  white-space:nowrap;transition:background .15s}
.fp-header-btn:hover{background:rgba(255,255,255,.22)}

/* CSS variables for sidebar width */
:root{--pw:360px;--pw-wide:440px}
body.sidebar-wide{--pw:var(--pw-wide)}

/* Main layout — flex with sidebar */
.main-layout{display:flex;align-items:flex-start;gap:.85rem;
  max-width:1820px;margin:0 auto;padding:.75rem 1rem 2.5rem}
.main-content{flex:1;min-width:0;overflow:hidden}
.card{background:#fff;border-radius:1rem;box-shadow:0 2px 12px rgba(0,0,0,.1),0 1px 3px rgba(0,0,0,.06);overflow:hidden;margin-bottom:.75rem}
.card-body{padding:1rem 1.25rem}
/* Picker sidebar */
.picker-sidebar{flex:0 0 var(--pw);width:var(--pw);position:sticky;top:58px;
  max-height:calc(100vh - 66px);overflow-y:auto;overflow-x:hidden;
  background:#fff;border-radius:.85rem;
  box-shadow:0 2px 12px rgba(0,0,0,.12),0 1px 4px rgba(0,0,0,.07);
  transition:flex-basis .2s,width .2s}
.sidebar-header{padding:.62rem .9rem;background:linear-gradient(90deg,#1e3a8a,#312e81);
  color:#fff;border-radius:.85rem .85rem 0 0;font-size:.78rem;font-weight:700;
  display:flex;align-items:center;gap:.35rem;position:sticky;top:0;z-index:2}
.sidebar-toggle-btn{margin-left:auto;background:rgba(255,255,255,.18);border:none;
  color:#fff;border-radius:.3rem;cursor:pointer;font-size:.7rem;padding:.15rem .42rem;
  line-height:1.4;font-weight:700;transition:background .15s;white-space:nowrap}
.sidebar-toggle-btn:hover{background:rgba(255,255,255,.32)}
.sidebar-pane{padding:.65rem .95rem}
@media(max-width:1060px){
  .main-layout{flex-direction:column}
  .picker-sidebar{flex:none;width:100%;position:relative;top:0;max-height:none}
}
/* ── Mobile bottom-drawer picker ── */
.mob-drag-pill{display:none;justify-content:center;
  padding:.4rem 0 .15rem;position:absolute;top:0;left:0;right:0;pointer-events:none}
.mob-drag-pill::after{content:'';display:block;width:38px;height:4px;
  border-radius:2px;background:rgba(255,255,255,.5)}
.mob-open-hint{display:none;font-size:.6rem;color:rgba(255,255,255,.7);
  margin-left:auto;white-space:nowrap;pointer-events:none}
@media(max-width:768px){
  .main-layout{padding:.45rem .55rem 0;gap:0}
  .main-content{width:100%!important;padding-bottom:58px}
  .picker-sidebar{
    position:fixed!important;bottom:0;left:0;right:0;
    width:100%!important;flex:none!important;top:auto!important;
    max-height:72vh;overflow-y:auto;overflow-x:hidden;
    border-radius:1rem 1rem 0 0;
    box-shadow:0 -4px 32px rgba(0,0,0,.22);
    z-index:190;
    transform:translateY(calc(100% - 48px));
    transition:transform .28s cubic-bezier(.4,0,.2,1);
  }
  .picker-sidebar.mob-open{transform:translateY(0)}
  .sidebar-header{border-radius:1rem 1rem 0 0!important;cursor:pointer;position:relative}
  #sidebar-wide-btn{display:none!important}
  .mob-drag-pill{display:flex}
  .mob-open-hint{display:inline}
  /* Prevent page-header float-panel button from interfering */
  .fp-header-btn{font-size:.68rem;padding:.38rem .6rem}
}

/* Floating draggable panel */
.float-panel{position:fixed;top:72px;right:20px;width:460px;background:#fff;
  border-radius:1rem;box-shadow:0 8px 36px rgba(0,0,0,.22);z-index:500;
  max-height:calc(100vh - 90px);overflow-y:auto;transition:opacity .2s,transform .2s}
.float-panel.hidden{opacity:0;transform:translateX(calc(100% + 30px));pointer-events:none}
.fp-handle{cursor:move;background:linear-gradient(90deg,#0f172a,#1e3a8a);
  padding:.65rem 1rem;color:#fff;border-radius:1rem 1rem 0 0;
  display:flex;align-items:center;gap:.5rem;user-select:none;position:sticky;top:0;z-index:1}
.fp-handle-icon{font-size:.9rem;opacity:.7}
.fp-handle-title{font-size:.82rem;font-weight:700}
.fp-handle-hint{font-size:.62rem;opacity:.55;margin-left:auto}
.fp-close{background:none;border:none;color:#a5b4fc;cursor:pointer;font-size:1rem;line-height:1;padding:.1rem}
.fp-body{padding:.85rem 1rem}

/* Tabs */
.tab-bar{display:flex;border-bottom:2px solid #e2e8f0;padding:0 .75rem;background:#f8fafc;overflow-x:auto;gap:.1rem}
.tab-btn{padding:.65rem 1.15rem;font-size:.82rem;font-weight:600;color:#64748b;border:none;background:none;cursor:pointer;border-bottom:3px solid transparent;margin-bottom:-2px;white-space:nowrap;transition:color .15s,border-color .15s}
.tab-btn:hover:not(:disabled){color:#4338ca}
.tab-btn.active{color:#4338ca;border-bottom-color:#4338ca;font-weight:800}
.tab-btn:disabled{color:#cbd5e1;cursor:not-allowed}
.tab-dot{display:inline-block;width:.45rem;height:.45rem;border-radius:50%;margin-left:.35rem;vertical-align:middle}
.panel{display:none;padding:1.1rem 1.25rem}
.panel.active{display:block}

/* 8-draw banner */
.draw-banner{border-radius:.85rem;padding:.85rem 1rem;margin-bottom:.75rem}
.draws-grid{display:flex;flex-direction:column;gap:.3rem}
.draw-row{display:flex;align-items:center;gap:.55rem;padding:.45rem .7rem;
  border-radius:.55rem;background:rgba(255,255,255,.6);border:1px solid rgba(0,0,0,.05);
  transition:background .12s}
.draw-row:hover{background:rgba(255,255,255,.9)}
.draw-row.latest{background:#fff!important;
  border-color:rgba(0,0,0,.18);font-weight:700;
  box-shadow:0 2px 6px rgba(0,0,0,.1)}
.draw-date-lbl{font-size:.65rem;color:#64748b;min-width:72px;flex-shrink:0}
.draw-anno{display:flex;gap:.3rem;flex-wrap:wrap;margin-left:auto}
.anno-tag-oe{border-radius:.3rem;padding:.16rem .52rem;font-size:.75rem;font-weight:800;
  white-space:nowrap;background:#fde68a;border:1.5px solid #d97706;color:#0f172a;line-height:1.4;letter-spacing:.01em}
.anno-tag-col{border-radius:.3rem;padding:.16rem .52rem;font-size:.75rem;font-weight:800;
  white-space:nowrap;background:#bae6fd;border:1.5px solid #0284c7;color:#0f172a;line-height:1.4;letter-spacing:.01em}
.hist-badge{display:inline-flex;align-items:center;gap:.35rem;background:#fef9c3;
  border:1px solid #fde047;border-radius:.5rem;padding:.28rem .65rem;
  font-size:.7rem;font-weight:700;color:#713f12;margin-bottom:.5rem}
.banner-meta{font-size:.68rem;color:#64748b}

/* THREE-COLOR BALLS */
.ball,.ball-sm{display:inline-flex;align-items:center;justify-content:center;border-radius:50%;font-weight:800;color:#fff;flex-shrink:0;transition:transform .15s}
.ball{width:2.1rem;height:2.1rem;font-size:.8rem}
.ball-sm{width:1.75rem;height:1.75rem;font-size:.72rem}
.ball:hover,.ball-sm:hover{transform:scale(1.12)}
.b-red  {background:linear-gradient(145deg,#f87171,#ef4444);color:#fff;font-weight:800;
  box-shadow:0 3px 8px rgba(239,68,68,.5),inset 0 1px 0 rgba(255,255,255,.25)}
.b-blue {background:linear-gradient(145deg,#60a5fa,#3b82f6);color:#fff;font-weight:800;
  box-shadow:0 3px 8px rgba(59,130,246,.5),inset 0 1px 0 rgba(255,255,255,.25)}
.b-green{background:linear-gradient(145deg,#4ade80,#22c55e);color:#fff;font-weight:800;
  box-shadow:0 3px 8px rgba(34,197,94,.5),inset 0 1px 0 rgba(255,255,255,.25)}
.ball-legend{display:flex;gap:.8rem;margin-bottom:.8rem;flex-wrap:wrap}
.legend-item{display:flex;align-items:center;gap:.35rem;font-size:.68rem;color:#475569}

/* Time Machine */
.tm-bar{display:flex;flex-wrap:wrap;align-items:center;gap:.55rem;
  background:#f8fafc;border:1px solid #e2e8f0;border-radius:.7rem;
  padding:.6rem .9rem;margin-bottom:1rem}
.tm-bar .tm-label{font-size:.75rem;font-weight:800;color:#475569;white-space:nowrap}
.tm-bar input[type=date]{border:1px solid #e2e8f0;border-radius:.4rem;padding:.28rem .55rem;font-size:.75rem;background:#fff;outline:none;transition:border-color .15s}
.tm-bar input[type=date]:focus{border-color:#6366f1}
.tm-bar input[type=date]:disabled{background:#f1f5f9;color:#94a3b8;cursor:not-allowed}
.btn-tm{padding:.28rem .7rem;border-radius:.4rem;font-size:.73rem;font-weight:700;border:none;cursor:pointer;transition:all .15s;background:#4338ca;color:#fff}
.btn-tm:hover{background:#3730a3}
.btn-tm:disabled{background:#e2e8f0;color:#94a3b8;cursor:not-allowed}
.btn-tm-reset{padding:.28rem .7rem;border-radius:.4rem;font-size:.73rem;font-weight:700;border:1px solid #e2e8f0;cursor:pointer;background:#fff;color:#64748b;transition:all .15s}
.btn-tm-reset:hover{background:#f1f5f9}
.tm-mode-badge{font-size:.68rem;font-weight:700;padding:.18rem .5rem;border-radius:.3rem;white-space:nowrap}
.tm-mode-live{background:#dcfce7;color:#166534}
.tm-mode-hist{background:#fef9c3;color:#713f12}
.tm-mode-loading{background:#dbeafe;color:#1e3a8a}

/* Tail miss panel */
.tail-panel{border-radius:.85rem;padding:.8rem 1rem;margin-bottom:.75rem;
  background:#fff;border:1px solid #e2e8f0}
.tail-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:.4rem;margin-top:.55rem}
@media(max-width:600px){.tail-grid{grid-template-columns:repeat(5,1fr)}}
.tail-card{text-align:center;border-radius:.55rem;padding:.4rem .25rem;
  border:1px solid #e2e8f0;background:#f8fafc;transition:border-color .15s}
.tail-card:hover{border-color:#cbd5e1}
.tail-digit{font-size:.65rem;font-weight:700;color:#64748b}
.tail-miss-val{font-size:1.45rem;font-weight:900;margin-top:.1rem;line-height:1}
.tail-bar{height:4px;border-radius:2px;background:#e2e8f0;margin:.3rem auto;width:80%;overflow:hidden}
.tail-bar-fill{height:100%;border-radius:2px;transition:width .5s}
.tail-nums{font-size:.56rem;color:#94a3b8;margin-top:.15rem;word-break:break-all;line-height:1.3}

/* Two columns */
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:1.25rem}
@media(max-width:700px){.two-col{grid-template-columns:1fr}}
.col-title{display:flex;align-items:center;gap:.45rem;margin-bottom:.3rem}
.col-title .icon{font-size:1.1rem}
.col-title h3{font-size:.9rem;font-weight:900}
.col-desc{font-size:.7rem;color:#64748b;margin-bottom:.7rem;line-height:1.6}

/* Rec cards */
.rec-item{border:1px solid #e2e8f0;border-radius:.7rem;padding:.65rem .85rem;transition:background .15s;margin-bottom:.4rem}
.rec-item:hover{background:#f8fafc;border-color:#cbd5e1}
.rec-top{display:flex;align-items:center;gap:.5rem;margin-bottom:.45rem}
.rec-badge{display:inline-flex;align-items:center;justify-content:center;min-width:2.35rem;height:2.35rem;border-radius:.5rem;font-weight:900;font-size:.73rem;padding:0 .35rem;flex-shrink:0}
.rec-info{flex:1;min-width:0}
.rec-title{font-weight:700;font-size:.8rem;color:#334155}
.rec-sub{font-size:.66rem;color:#94a3b8;margin-top:.1rem}
.rec-pct{font-weight:900;font-size:1rem;font-variant-numeric:tabular-nums;flex-shrink:0}
.balls-row{display:flex;gap:.28rem;flex-wrap:wrap;align-items:center}
.no-balls{font-size:.68rem;color:#94a3b8;font-style:italic}

/* Miss sub-tabs */
.miss-subtabs{display:flex;flex-wrap:wrap;gap:.28rem;margin-bottom:.75rem}
.miss-tab{padding:.22rem .6rem;border-radius:.4rem;font-size:.68rem;font-weight:700;border:1px solid #e2e8f0;background:#f8fafc;color:#64748b;cursor:pointer;transition:all .15s}
.miss-tab:hover{background:#f1f5f9;border-color:#cbd5e1}
.miss-tab.active{background:#b91c1c;color:#fff;border-color:#b91c1c;box-shadow:0 2px 6px rgba(185,28,28,.35)}
.miss-pane{display:none}
.miss-pane.active{display:block}

/* Details */
details{margin-top:.5rem}
details>summary{display:flex;align-items:center;gap:.35rem;cursor:pointer;font-size:.71rem;font-weight:600;padding:.38rem .5rem;border-radius:.4rem;list-style:none;user-select:none;transition:background .15s}
details>summary::-webkit-details-marker{display:none}
details>summary:hover{background:#f1f5f9}
details>summary .caret{display:inline-block;transition:transform .2s;font-size:.6rem}
details[open]>summary .caret{transform:rotate(90deg)}

/* Tables */
.table-wrap{margin-top:.45rem;overflow-x:auto;max-height:240px;overflow-y:auto;border-radius:.5rem;border:1px solid #e2e8f0}
table{border-collapse:collapse;font-size:.72rem;width:100%;min-width:260px}
th,td{padding:4px 9px;border-bottom:1px solid #f1f5f9;text-align:center;white-space:nowrap}
th{background:#f8fafc;font-weight:700;position:sticky;top:0;z-index:1;border-bottom:1px solid #e2e8f0}
tr:last-child td{border-bottom:none}
tr.hl td{background:#fef9c3;font-weight:700}
tr.stripe td{background:#f8fafc}

/* Interactive picker — sidebar edition */
.picker-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:.25rem;margin-top:.45rem}
.pk-cell{display:flex;flex-direction:column;align-items:center;cursor:pointer;
  border-radius:.5rem;padding:.35rem .1rem;border:2px solid #e2e8f0;
  background:#f8fafc;transition:all .13s;user-select:none;gap:.08rem;position:relative;overflow:visible}
.pk-cell:hover{background:#eff6ff;border-color:#93c5fd;transform:scale(1.06);
  box-shadow:0 2px 6px rgba(59,130,246,.2)}
.pk-cell.selected{border-color:#1e3a8a;background:#1e3a8a;
  box-shadow:0 3px 8px rgba(30,58,138,.45)}
.pk-cell.selected .pk-miss{color:#bfdbfe}
.pk-miss{font-size:.72rem;color:#334155;font-weight:600;margin-top:.12rem;line-height:1}

/* Picker cell — 本期中獎號 gold ring (v9.0) */
.pk-cell.pk-latest{border:2.5px solid #f59e0b!important;background:#fffbeb!important;
  box-shadow:0 0 0 2px rgba(245,158,11,.2),0 0 10px rgba(245,158,11,.45)!important}
.pk-cell.pk-latest .pk-miss{color:#92400e}
.pk-badge-cur{position:absolute;top:-6px;right:-6px;
  background:#0f172a;color:#fbbf24;font-size:.42rem;font-weight:900;
  padding:1px 4px;border-radius:3px;line-height:1.3;white-space:nowrap;
  pointer-events:none;z-index:3;letter-spacing:.02em}

/* Picker cell — 鄰號 violet dashed ring (v9.0) */
.pk-cell.pk-neighbor{border:2px dashed #8b5cf6!important;background:#faf5ff!important}
.pk-cell.pk-neighbor .pk-miss{color:#5b21b6}
.pk-badge-nb{position:absolute;top:-6px;right:-6px;
  background:#ede9fe;color:#6d28d9;font-size:.42rem;font-weight:900;
  padding:1px 4px;border-radius:3px;line-height:1.3;white-space:nowrap;
  pointer-events:none;z-index:3;border:1px solid #c4b5fd}

/* pk-latest takes priority if both classes present */
.pk-cell.pk-latest.pk-neighbor{border-style:solid!important;border-color:#f59e0b!important}
.picker-toolbar{display:flex;gap:.4rem;align-items:center;flex-wrap:wrap;
  margin-top:.55rem;padding:.55rem .7rem;background:#f8fafc;border-radius:.55rem;
  border:1px solid #e2e8f0}
.picker-selected-row{display:flex;gap:.22rem;flex-wrap:wrap;flex:1;min-width:0;align-items:center}
.pk-note-input{border:1px solid #e2e8f0;border-radius:.4rem;padding:.28rem .55rem;
  font-size:.7rem;width:120px;outline:none;background:#fff}
.pk-note-input:focus{border-color:#6366f1}
.pk-btn{padding:.3rem .65rem;border-radius:.4rem;font-size:.72rem;font-weight:700;
  border:1px solid #e2e8f0;cursor:pointer;transition:all .13s}
.pk-btn-save{background:#1e3a8a;color:#fff;border-color:#1e3a8a}
.pk-btn-save:hover{background:#1e40af}
.pk-btn-clear{background:#fff;color:#64748b}
.pk-btn-clear:hover{background:#f1f5f9}

/* Betting log */
.bet-log{margin-top:.6rem;max-height:280px;overflow-y:auto}
.bet-time{font-size:.6rem;color:#94a3b8;white-space:nowrap;min-width:62px;padding-top:.1rem}
.bet-note-txt{font-size:.64rem;color:#475569;margin-top:.18rem;font-style:italic}
.bet-hit-badge{font-size:.63rem;font-weight:700;padding:.12rem .38rem;border-radius:.28rem;
  margin-left:auto;white-space:nowrap;flex-shrink:0}
.bet-del-btn{background:none;border:none;cursor:pointer;color:#cbd5e1;font-size:.7rem;
  padding:.1rem;line-height:1;flex-shrink:0;transition:color .12s}
.bet-del-btn:hover{color:#ef4444}
/* drag-ring removed in v8.5 */

/* Streak markers (v9.0) — 連莊視覺標記（高對比，避開紅藍綠球色）*/
.streak-wrap{display:inline-flex;border-radius:50%;cursor:help;flex-shrink:0}
/* 連2: 亮橘/琥珀色 — 白底隔離＋雙層框，與紅藍綠球均高對比 */
.streak-2{box-shadow:0 0 0 2px #fff,0 0 0 4.5px #f59e0b,0 0 8px rgba(245,158,11,.65)}
/* 連3: 霓虹洋紅/紫色 — 視覺最搶眼，與三色球完全不重複 */
.streak-3{box-shadow:0 0 0 2px #fff,0 0 0 4.5px #d946ef,0 0 10px rgba(217,70,239,.75)}
/* 連4+: 強烈紅色脈衝發光 */
.streak-4p{animation:sglow .9s ease-in-out infinite alternate}
@keyframes sglow{
  from{box-shadow:0 0 0 2px #fff,0 0 0 4px #ef4444,0 0 8px rgba(239,68,68,.6)}
  to{box-shadow:0 0 0 2px #fff,0 0 0 5px #dc2626,0 0 18px rgba(239,68,68,.9),0 0 28px rgba(248,113,113,.5)}}

/* Bet log stats bar */
.bet-stats{display:flex;align-items:center;flex-wrap:wrap;gap:.45rem;padding:.38rem .65rem;
  background:#f8fafc;border:1px solid #e2e8f0;border-radius:.45rem;margin-bottom:.4rem;font-size:.7rem;font-weight:600;color:#475569}
.bet-stats .wins{color:#16a34a;font-weight:800}
.bet-stats .losses{color:#dc2626;font-weight:800}
.bet-stats strong{color:#1e293b}

/* Failure analysis panel */
.fail-analysis-panel{margin:.3rem 0 .25rem;border:1px solid #fecaca;border-radius:.5rem;
  overflow:hidden;background:#fff}
.fail-analysis-panel>summary{padding:.22rem .45rem;background:#fff5f5;cursor:pointer;
  border-bottom:1px solid transparent}
.fail-analysis-panel[open]>summary{border-bottom-color:#fecaca}
.fail-analysis-panel>summary:hover{background:#fee2e2}
.fail-analysis-body{padding:.4rem .5rem .45rem;font-size:.68rem}
.fail-section-title{font-size:.63rem;font-weight:800;color:#64748b;
  text-transform:uppercase;letter-spacing:.03em;margin-bottom:.1rem}
.fail-tag{font-size:.6rem;font-weight:700;padding:.1rem .32rem;border-radius:.25rem}
.fail-suggest{font-size:.66rem;color:#1e293b;line-height:1.6;padding:.12rem 0;
  border-bottom:1px solid #f1f5f9}
.fail-suggest:last-child{border-bottom:none}

/* Selection risk summary (v9.6) */
.pk-risk-box{margin:.22rem 0 .18rem;border-radius:.45rem;padding:.3rem .5rem;font-size:.65rem}
/* Picker live stats bar (v9.0) */
.pk-live-bar{padding:.32rem .55rem;background:#f0f9ff;border:1px solid #bae6fd;
  border-radius:.5rem;display:flex;flex-wrap:wrap;gap:.28rem;align-items:center;
  min-height:1.9rem;margin-top:.38rem;transition:background .15s}
.pk-live-chip{display:inline-flex;align-items:center;padding:.1rem .38rem;
  border-radius:.28rem;font-size:.67rem;font-weight:700;line-height:1.4;white-space:nowrap}
/* Today's decision summary (v9.9) */
.pk-daily-panel{margin:.28rem 0 .22rem;min-height:0}
.pk-daily-txt{background:#f0f9ff;border:1px solid #bae6fd;border-radius:.45rem;
  padding:.3rem .55rem;font-size:.65rem;font-weight:800;color:#0369a1;line-height:1.6}
/* Bet log filter bar (v9.9) */
.bet-filter-bar{display:flex;flex-wrap:wrap;gap:.18rem;margin:.22rem 0 .18rem}
.bet-filter-btn{font-size:.58rem;padding:.14rem .32rem;border:1px solid #e2e8f0;
  border-radius:.25rem;background:#f8fafc;color:#475569;cursor:pointer;
  transition:background .12s,color .12s;line-height:1.4;font-weight:500}
.bet-filter-btn.active{background:#4338ca;color:#fff;border-color:#4338ca;font-weight:700}
.bet-filter-btn:hover:not(.active){background:#eef2ff;color:#4338ca}
/* Bet log scrollable section (v9.9) */
.bet-log-section{max-height:52vh;overflow-y:auto;overflow-x:hidden;padding-right:.1rem}
.bet-log-section::-webkit-scrollbar{width:4px}
.bet-log-section::-webkit-scrollbar-thumb{background:#e2e8f0;border-radius:2px}

/* Bet entry wrapper (v9.0) — fixes autoNote squish */
.bet-entry-wrap{margin-bottom:.35rem;border:1px solid #e2e8f0;border-radius:.55rem;
  background:#fff;transition:background .12s;overflow:hidden}
.bet-entry-wrap:hover{background:#f8fafc}
.bet-entry-wrap.bet-hit-wrap{background:#f0fdf4!important;border-color:#86efac!important}
.bet-entry{display:flex;align-items:flex-start;gap:.5rem;padding:.5rem .7rem .35rem;
  border:none;border-radius:0;margin-bottom:0;background:transparent}
.bet-entry-tags{display:flex;gap:.25rem;flex-wrap:wrap;
  padding:.0rem .7rem .3rem;align-items:center}
.bet-autonote{padding:.0rem .65rem .4rem;width:100%;box-sizing:border-box}

/* Consecutive analysis panel (v8.0) */
.consec-panel{border-radius:.85rem;padding:.8rem 1rem;margin-bottom:.75rem;
  background:#fff;border:1px solid #e2e8f0}
.consec-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));
  gap:.38rem;margin-top:.6rem}
.consec-num-card{border:1px solid #e2e8f0;border-radius:.5rem;padding:.3rem .5rem;
  background:#f8fafc;cursor:pointer;transition:all .13s;user-select:none}
.consec-num-card:hover{background:#eff6ff;border-color:#93c5fd}
.consec-num-card.has-streak{border-color:#fde047;background:#fefce8}
.consec-num-card.streak-active{border-color:#f59e0b;background:#fef3c7}
.consec-header{display:flex;align-items:center;gap:.3rem}
.consec-max-badge{font-size:.55rem;color:#94a3b8;margin-left:auto;white-space:nowrap}
.consec-detail{font-size:.62rem;color:#334155;margin-top:.25rem;border-top:1px solid #e2e8f0;padding-top:.25rem;display:none}
.consec-detail.open{display:block}
.consec-row{display:flex;align-items:center;gap:.3rem;padding:.1rem 0}
.consec-k-label{color:#475569;min-width:75px;flex-shrink:0}
.consec-prob{font-weight:800;min-width:42px;text-align:right}
.consec-sample{color:#94a3b8;font-size:.58rem}
.consec-bar-bg{flex:1;height:5px;background:#e2e8f0;border-radius:3px;overflow:hidden;min-width:20px}
.consec-bar-fill{height:100%;border-radius:3px;transition:width .4s}

/* Recent heat probability panel (v9.6) */
.heat-prob-panel{border-radius:.85rem;padding:.7rem 1rem;margin-bottom:.75rem;
  background:#fff;border:1px solid #e2e8f0}
.heat-prob-panel details>summary{padding:.15rem .25rem;border-radius:.4rem}
.heat-prob-panel details>summary:hover{background:#f8fafc}
.heat-prob-num-card{display:inline-flex;flex-direction:column;align-items:center;
  border:1px solid #e2e8f0;border-radius:.45rem;overflow:hidden;
  min-width:4.8rem;margin:.12rem;background:#fafafa;vertical-align:top;cursor:pointer}
.heat-prob-num-card summary{list-style:none;padding:.22rem .3rem;
  background:#f8fafc;display:flex;flex-direction:column;align-items:center;gap:.1rem;cursor:pointer}
.heat-prob-num-card summary:hover{background:#eff6ff}
.heat-prob-matrix-row{display:flex;justify-content:space-between;
  font-size:.59rem;padding:.06rem .25rem;border-top:1px solid #f1f5f9}
.heat-prob-matrix-row:first-child{border-top:none}

/* Update panel (inside float) */
.update-grid{display:grid;grid-template-columns:1fr 1fr;gap:.8rem}
@media(max-width:500px){.update-grid{grid-template-columns:1fr}}
.upd-card{border-radius:.75rem;padding:.85rem}
.upd-title{font-weight:700;font-size:.8rem;margin-bottom:.35rem}
.upd-desc{font-size:.68rem;margin-bottom:.7rem;line-height:1.6}
.btn{display:flex;align-items:center;justify-content:center;padding:.48rem .85rem;border-radius:.5rem;font-weight:700;font-size:.76rem;border:none;cursor:pointer;transition:all .15s;width:100%}
.btn:disabled{background:#e2e8f0!important;color:#94a3b8!important;cursor:not-allowed}
.form-field{width:100%;border:1px solid #e2e8f0;border-radius:.45rem;padding:.42rem .7rem;font-size:.76rem;margin-bottom:.38rem;background:#fff;outline:none;transition:border-color .15s}
.form-field:focus{border-color:#6366f1;box-shadow:0 0 0 2px rgba(99,102,241,.15)}
.num-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:.28rem;margin-bottom:.38rem}
.num-grid input{text-align:center}
.log-area{margin-top:.45rem;font-size:.7rem;min-height:1.2rem;line-height:1.7}
.warn-box{background:#fffbeb;border:1px solid #fcd34d;border-radius:.5rem;padding:.55rem .85rem;font-size:.7rem;color:#92400e;margin-bottom:.8rem}
.warn-box code{background:#fef3c7;padding:1px 5px;border-radius:3px;font-family:monospace}
footer{text-align:center;font-size:.68rem;color:#94a3b8;padding:1.1rem .5rem}

/* OE/Color stats panel (v9.0) */
.oe-color-panel{border-radius:.85rem;padding:.8rem 1rem;margin-bottom:.75rem;
  background:#fff;border:1px solid #e2e8f0}

/* Strategy backtest panel (v9.0) */
.strat-panel{border-radius:.85rem;padding:.8rem 1rem;margin-bottom:.75rem;
  background:#fff;border:1px solid #e2e8f0}
.strat-body{margin-top:.6rem}
.strat-stats-row{display:flex;gap:.4rem;flex-wrap:wrap;margin-bottom:.5rem}
.strat-stat-card{flex:1;min-width:52px;border-radius:.55rem;padding:.45rem .35rem;
  border:1px solid #e2e8f0;text-align:center}
.strat-stat-val{font-size:1.1rem;font-weight:900;line-height:1}
.strat-stat-lbl{font-size:.58rem;color:#64748b;margin-top:.18rem;font-weight:600}

/* Picker mode toggle bar (v9.0) */
.pk-mode-bar{display:flex;align-items:center;flex-wrap:wrap;gap:.22rem;
  margin-bottom:.35rem;padding:.28rem .4rem;background:#f8fafc;
  border:1px solid #e2e8f0;border-radius:.45rem}
.pk-mode-btn{padding:.18rem .5rem;border-radius:.3rem;font-size:.65rem;font-weight:700;
  border:1px solid #e2e8f0;background:#fff;color:#64748b;cursor:pointer;transition:all .13s}
.pk-mode-btn:hover{background:#f1f5f9;border-color:#cbd5e1}
.pk-mode-btn.active{background:#4338ca;color:#fff;border-color:#4338ca}

/* Picker smart recommendation panel (v9.0) */
.pk-smart-panel{margin-top:.38rem;padding:.35rem .5rem;background:#fafafa;
  border:1px solid #e2e8f0;border-radius:.5rem}
.pk-smart-chip{display:inline-flex;flex-direction:column;align-items:center;
  padding:.2rem .35rem;border-radius:.4rem;cursor:pointer;border:1px solid #e2e8f0;
  background:#fff;gap:.05rem;transition:all .13s;user-select:none}
.pk-smart-chip:hover{transform:scale(1.06);border-color:#93c5fd;background:#eff6ff}
.pk-smart-chip .chip-num{font-size:.68rem;font-weight:900;color:#1e293b}
.pk-smart-chip .chip-danger{font-size:.56rem;font-weight:700}

/* Picker cell heatmap + danger overlays (v9.0) */
.pk-cell.mode-heatmap-0{background:#eff6ff!important;border-color:#bfdbfe!important}
.pk-cell.mode-heatmap-1{background:#dbeafe!important;border-color:#93c5fd!important}
.pk-cell.mode-heatmap-2{background:#bfdbfe!important;border-color:#60a5fa!important}
.pk-cell.mode-heatmap-3{background:#93c5fd!important;border-color:#3b82f6!important}
.pk-cell.mode-heatmap-4{background:#60a5fa!important;border-color:#2563eb!important;color:#fff}
.pk-cell.mode-heatmap-5{background:#3b82f6!important;border-color:#1d4ed8!important;color:#fff}
.pk-cell.mode-heatmap-4 .pk-miss,.pk-cell.mode-heatmap-5 .pk-miss{color:#dbeafe}
.pk-cell.mode-danger-0{background:#f0fdf4!important;border-color:#86efac!important}
.pk-cell.mode-danger-25{background:#dcfce7!important;border-color:#4ade80!important}
.pk-cell.mode-danger-50{background:#fef9c3!important;border-color:#fde047!important}
.pk-cell.mode-danger-75{background:#fed7aa!important;border-color:#fb923c!important}
.pk-cell.mode-danger-100{background:#fee2e2!important;border-color:#ef4444!important}
.pk-cell.mode-danger-100 .pk-miss{color:#991b1b!important}
"""

    # ── Static ball helper ───────────────────────────────────

    @staticmethod
    def _ball(n: int, size: str = "ball") -> str:
        cls = ball_cls(n)
        return f'<span class="{size} {cls}">{n:02d}</span>'

    # ── Tabs + Panels ────────────────────────────────────────

    def _build_tabs_panels(self, results: Dict, server_mode: bool):
        tabs = panels = ""
        for key, data in results.items():
            cfg     = LOTTERY_CONFIG[key]
            sname   = cfg["short_name"]
            primary = cfg["theme"]["primary"]
            csvfile = cfg["csv_filename"]
            if data is None:
                tabs += (
                    '<button class="tab-btn" disabled title="無資料：' + csvfile + '">'
                    + sname +
                    '<span class="tab-dot" style="background:#e2e8f0"></span></button>'
                )
            else:
                tabs += (
                    '<button id="tab-' + key + '" class="tab-btn" onclick="switchTab(\'' + key + '\')">'
                    + sname +
                    '<span class="tab-dot" style="background:' + primary + '"></span></button>'
                )
                panels += (
                    '<div id="panel-' + key + '" class="panel">'
                    + self._build_panel(key, data, server_mode)
                    + '</div>'
                )
        return tabs, panels

    # ── Single panel ─────────────────────────────────────────

    # ── Shared legend HTML ───────────────────────────────────
    _LEGEND_HTML = (
        '<div class="ball-legend">'
        '<span class="legend-item"><span class="ball-sm b-red">01</span> ÷3 餘1（紅）</span>'
        '<span class="legend-item"><span class="ball-sm b-blue">02</span> ÷3 餘2（藍）</span>'
        '<span class="legend-item"><span class="ball-sm b-green">03</span> ÷3 整除（綠）</span>'
        '<span class="legend-item" style="margin-left:.5rem;border-left:1px solid #e2e8f0;padding-left:.5rem">'
        '<span class="streak-wrap streak-2"><span class="ball-sm b-blue" style="width:1.45rem;height:1.45rem;font-size:.58rem">02</span></span>'
        ' <span style="color:#d97706;font-weight:700">連2</span>&nbsp;</span>'
        '<span class="legend-item">'
        '<span class="streak-wrap streak-3"><span class="ball-sm b-green" style="width:1.45rem;height:1.45rem;font-size:.58rem">03</span></span>'
        ' <span style="color:#a21caf;font-weight:700">連3</span>&nbsp;</span>'
        '<span class="legend-item">'
        '<span class="streak-wrap streak-4p"><span class="ball-sm b-red" style="width:1.45rem;height:1.45rem;font-size:.58rem">04</span></span>'
        ' <span style="color:#dc2626;font-weight:700">連4+</span></span>'
        '</div>'
    )

    def _build_panel_inner(self, key: str, data: Dict, server_mode: bool = False,
                            is_hist: bool = False, hist_date: str = "") -> str:
        """Build complete panel HTML excluding data_script.
        Used for both initial page render (is_hist=False) and backtest API response.
        Scripts inserted via innerHTML do NOT execute — caller handles JS globals via _btStore.
        """
        tm_bar  = self._build_tm_bar(key, data, server_mode, is_hist=is_hist, hist_date=hist_date)
        _rev_oe = _compute_rev_oe(data["period_result"].get("recent_8", []))
        content = (
            '<div id="content-wrap-' + key + '">'
            + self.build_banner_html(key, data, is_historical=is_hist, hist_date=hist_date)
            + self._build_reverse_oe_panel(key, _rev_oe, data["config"]["theme"])
            + self._build_tail_miss_html(key, data["tail_result"], data["config"]["theme"],
                                          is_historical=is_hist)
            + self._build_consec_html(key, data.get("consec_result", {}), data["config"]["theme"])
            + self.build_analysis_html(key, data)
            + self._build_oe_color_panel(key, data.get("oe_color_stats", {}), data["config"]["theme"])
            + self._build_strat_panel(key, data.get("strategy_bt", {}), data["config"]["theme"],
                                        mbt=data.get("multi_bt"), tun=data.get("excl_tune"))
            + self._build_heat_prob_panel(key, data.get("recent_heat_prob", {}), data["config"]["theme"])
            + '</div>'
        )
        return self._LEGEND_HTML + tm_bar + content

    def _build_panel(self, key: str, data: Dict, server_mode: bool = False) -> str:
        inner = self._build_panel_inner(key, data, server_mode)
        gaps = data.get("data_gaps", [])
        gap_affects_recent = False
        if gaps:
            try:
                max_dt = pd.Timestamp(data.get("max_date", ""))
                min_dt = pd.Timestamp(data.get("min_date", ""))
                rec_n  = data["record_count"]
                if rec_n > 1:
                    avg_days = (max_dt - min_dt).days / rec_n
                    gap_from_dt = pd.Timestamp(gaps[0]["from"])
                    gap_affects_recent = (max_dt - gap_from_dt).days < 300 * avg_days
            except Exception:
                pass
        rev_oe = _compute_rev_oe(data["period_result"].get("recent_8", []))
        data_script = self._build_picker_data_script(
            key, data["miss_result"], data["period_result"],
            nhr=data.get("num_history"),
            oec=data.get("oe_color_stats"),
            sbt=data.get("strategy_bt"),
            rhp=data.get("recent_heat_prob"),
            gaps_in=gaps,
            gap_affects_recent=gap_affects_recent,
            mbt_in=data.get("multi_bt"),
            rev_oe_in=rev_oe,
        )
        return inner + data_script

    # ── Banner — 最近8期（replaceable by time machine）──────────

    def build_banner_html(self, key: str, data: Dict,
                          is_historical: bool = False,
                          hist_date: str = "") -> str:
        cfg      = data["config"]
        pr       = data["period_result"]
        cnt      = data["record_count"]
        p        = cfg["theme"]["primary"]
        light    = cfg["theme"]["light"]
        recent_8 = pr.get("recent_8", [])

        hist_badge = ""
        if is_historical and hist_date:
            hist_badge = (
                '<div class="hist-badge">'
                '⏱️ 歷史回測模式　基準日：' + hist_date + '　（' + str(cnt) + ' 期）'
                '</div>'
            )
        # Data freshness badge (v10.0)
        freshness_badge = ""
        try:
            max_date_dt = pd.Timestamp(data.get("max_date", ""))
            days_old = int((pd.Timestamp.now().normalize() - max_date_dt.normalize()).days)
            max_ds = str(data.get("max_date", ""))
            if days_old <= 1:
                label = "今日" if days_old == 0 else "昨日"
                freshness_badge = (
                    '<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:.38rem;'
                    'padding:.15rem .55rem;font-size:.6rem;color:#15803d;'
                    'margin-bottom:.22rem;display:inline-block">'
                    '✅ 資料最新：' + label + '更新（' + max_ds + '）'
                    '</div>'
                )
            elif days_old <= 5:
                freshness_badge = (
                    '<div style="background:#fffbeb;border:1px solid #fcd34d;border-radius:.38rem;'
                    'padding:.15rem .55rem;font-size:.6rem;color:#92400e;'
                    'margin-bottom:.22rem;display:inline-block">'
                    '⚠️ 資料稍舊：最新期距今 ' + str(days_old) + ' 天（' + max_ds + '），建議更新'
                    '</div>'
                )
            else:
                freshness_badge = (
                    '<div style="background:#fef2f2;border:1px solid #fca5a5;border-radius:.38rem;'
                    'padding:.15rem .55rem;font-size:.6rem;color:#991b1b;'
                    'margin-bottom:.22rem;display:inline-block">'
                    '🔴 資料過舊：最新期距今 ' + str(days_old) + ' 天（' + max_ds + '），請補充 CSV'
                    '</div>'
                )
        except Exception:
            pass

        # Gap warning — collapsible <details> (v9.9)
        gaps  = data.get("data_gaps", [])
        gap_badge = (
            '<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:.45rem;'
            'padding:.2rem .6rem;font-size:.63rem;color:#15803d;margin-bottom:.3rem">'
            '✅ 資料健康：未偵測到明顯缺期'
            '</div>'
        )
        if gaps:
            first = gaps[0]
            n_gaps = len(gaps)
            # Estimate impact
            max_dt  = pd.Timestamp(data.get("max_date", ""))
            min_dt  = pd.Timestamp(data.get("min_date", ""))
            rec_n   = data["record_count"]
            affects_recent = False
            try:
                if rec_n > 1:
                    avg_days = (max_dt - min_dt).days / rec_n
                    gap_from_dt = pd.Timestamp(first["from"])
                    affects_recent = (max_dt - gap_from_dt).days < 300 * avg_days
            except Exception:
                pass
            summary_impact = "可能影響近300期回測 ▼" if affects_recent else "近期300期不受影響 ▼"
            all_gaps_html = "".join(
                '<div style="padding:.1rem 0;border-bottom:1px solid #fde68a;font-size:.62rem">'
                + g["from"] + " → " + g["to"]
                + "（相差 " + str(g["days"]) + " 天）"
                + '</div>'
                for g in gaps
            )
            full_impact = (
                "可能影響近300期冷門回測、時光機與選號紀錄結算，建議補齊資料。"
                if affects_recent else
                "近期300期回測不受影響，僅可能影響全歷史統計與早期時光機。"
            )
            gap_badge = (
                '<details style="background:#fffbeb;border:1px solid #fcd34d;border-radius:.45rem;'
                'padding:.18rem .6rem;font-size:.63rem;color:#92400e;margin-bottom:.3rem">'
                '<summary style="cursor:pointer;list-style:none;user-select:none;font-weight:700">'
                '⚠️ 資料健康：有 ' + str(n_gaps) + ' 處缺期，' + summary_impact
                + '</summary>'
                '<div style="margin-top:.25rem">'
                + all_gaps_html
                + '<div style="margin-top:.2rem;font-size:.62rem;color:#92400e">'
                '<strong>影響範圍</strong>：' + full_impact
                + '</div>'
                '</div>'
                '</details>'
            )

        rows = ""
        for i, draw in enumerate(recent_8):
            is_latest = (i == 0)
            streaks  = draw.get("streaks", {})
            balls_parts = []
            for n in draw["numbers"]:
                b = self._ball(n, "ball-sm")
                streak = streaks.get(n, 1)
                # Streak ring — 正確連莊標記（v8.5）
                if streak >= 4:
                    tip = "連" + str(streak) + "（已連開" + str(streak) + "期）"
                    b = '<span class="streak-wrap streak-4p" title="' + tip + '">' + b + '</span>'
                elif streak == 3:
                    b = '<span class="streak-wrap streak-3" title="連3（已連開3期）">' + b + '</span>'
                elif streak == 2:
                    b = '<span class="streak-wrap streak-2" title="連2（已連開2期）">' + b + '</span>'
                balls_parts.append(b)
            balls  = " ".join(balls_parts)
            prefix = "▶ " if is_latest else "　 "
            odd_s  = str(draw["odd"]) + "單" + str(draw["even"]) + "雙"
            col_s  = "紅" + str(draw["red"]) + "藍" + str(draw["blue"]) + "綠" + str(draw["green"])
            row_cls = "draw-row latest" if is_latest else "draw-row"
            rows += (
                '<div class="' + row_cls + '">'
                '<span class="draw-date-lbl">' + prefix + draw["date"] + '</span>'
                '<div class="balls-row">' + balls + '</div>'
                '<div class="draw-anno">'
                '<span class="anno-tag-oe">' + odd_s + '</span>'
                '<span class="anno-tag-col">' + col_s + '</span>'
                '</div>'
                '</div>'
            )

        if not rows:
            balls = " ".join(self._ball(n) for n in pr["latest_numbers"])
            rows = (
                '<div class="draw-row latest">'
                '<span class="draw-date-lbl">▶ ' + pr["latest_date"] + '</span>'
                '<div class="balls-row">' + balls + '</div>'
                '</div>'
            )

        return (
            '<div id="banner-' + key + '" class="draw-banner" '
            'style="background:' + light + ';border:1px solid ' + p + '33">'
            + hist_badge + gap_badge +
            '<div style="display:flex;justify-content:space-between;align-items:center;'
            'margin-bottom:.5rem">'
            '<div style="font-size:.7rem;font-weight:800;color:' + p + '">'
            + ('歷史回測：近8期' if is_historical else '最近 8 期開獎紀錄') +
            '</div>'
            '<div class="banner-meta">共 <strong>' + str(cnt) + '</strong> 期　'
            '號碼池 1~' + str(cfg["pool_size"]) + '</div>'
            '</div>'
            '<div class="draws-grid">' + rows + '</div>'
            '</div>'
        )

    # ── Reverse OE/Color panel (v10.2) ─────────────────────────

    def _build_reverse_oe_panel(self, key: str, rev_oe: Optional[Dict], theme: Dict) -> str:
        if not rev_oe:
            return ""
        p     = theme["primary"]
        stats = rev_oe["stats"]
        moe   = rev_oe["main_oe"]
        mcol  = rev_oe["main_col"]
        alts  = rev_oe.get("alt_cols", [])
        stats_txt    = (str(stats["odd"]) + "單" + str(stats["even"]) + "雙 / 紅"
                        + str(stats["red"]) + "藍" + str(stats["blue"]) + "綠" + str(stats["green"]))
        main_oe_txt  = str(moe["odd"]) + "單" + str(moe["even"]) + "雙"
        main_col_txt = str(mcol["red"]) + "紅" + str(mcol["blue"]) + "藍" + str(mcol["green"]) + "綠"
        alt_txts = " · ".join(
            str(a["red"]) + "紅" + str(a["blue"]) + "藍" + str(a["green"]) + "綠" for a in alts
        )
        return (
            '<div id="revoe-' + key + '" style="background:#f8f7ff;border:1px solid #ddd6fe;'
            'border-radius:.5rem;padding:.32rem .65rem;margin-bottom:.5rem;font-size:.65rem">'
            '<details open>'
            '<summary style="list-style:none;cursor:pointer;user-select:none;display:flex;'
            'align-items:center;gap:.32rem;padding:.04rem 0">'
            '<span style="font-size:.82rem">🎯</span>'
            '<span style="font-weight:900;color:#5b21b6;font-size:.7rem">今日單雙色球反向推薦</span>'
            '<span class="caret" style="font-size:.6rem;margin-left:auto;color:#94a3b8">▶</span>'
            '</summary>'
            '<div style="margin-top:.28rem;display:grid;gap:.2rem">'
            '<div style="color:#64748b">最近3期統計：<strong style="color:#334155">'
            + stats_txt + '</strong></div>'
            '<div style="display:flex;gap:.7rem;flex-wrap:wrap">'
            '<span>建議單雙：<strong style="color:#5b21b6">' + main_oe_txt + '</strong></span>'
            '<span>主推色球：<strong style="color:#5b21b6">' + main_col_txt + '</strong></span>'
            '</div>'
            + ('<div>備選色球：<span style="color:#7c3aed">' + alt_txts + '</span></div>' if alt_txts else '')
            + '<div style="font-size:.58rem;color:#94a3b8;margin-top:.1rem">'
            '依最近3期高出現類型做五選不中反向配比，高出現→預期降溫→加選該類型。僅作輔助參考。'
            '</div>'
            '</div>'
            '</details>'
            '</div>'
        )

    # ── Time Machine bar ─────────────────────────────────────

    def _build_tm_bar(self, key: str, data: Dict, server_mode: bool,
                      is_hist: bool = False, hist_date: str = "") -> str:
        pr        = data["period_result"]
        min_date  = data.get("min_date", "2000-01-01")
        max_date  = data.get("max_date", pr["latest_date"])
        cur_val   = hist_date if (is_hist and hist_date) else max_date
        rst_style = "display:inline" if is_hist else "display:none"
        if is_hist and hist_date:
            mode_text  = "歷史基準：" + hist_date
            mode_class = "tm-mode-badge tm-mode-hist"
        else:
            mode_text  = "最新狀態"
            mode_class = "tm-mode-badge tm-mode-live"
        return (
            '<div class="tm-bar" id="tm-bar-' + key + '">'
            '<span class="tm-label">⏱️ 時光機</span>'
            '<input type="date" id="tm-date-' + key + '" '
            'min="' + min_date + '" max="' + max_date + '" value="' + cur_val + '">'
            '<button class="btn-tm" onclick="runBacktest(\'' + key + '\')">執行歷史回測</button>'
            '<button class="btn-tm-reset" id="tm-reset-' + key + '" style="' + rst_style + '" '
            'onclick="resetPanel(\'' + key + '\')">↩ 回最新</button>'
            '<span id="tm-mode-' + key + '" class="' + mode_class + '">' + mode_text + '</span>'
            '</div>'
        )

    # ── Two-column analysis (replaceable) ───────────────────

    def build_analysis_html(self, key: str, data: Dict) -> str:
        pr    = data["period_result"]
        mr    = data["miss_result"]
        theme = data["config"]["theme"]
        return (
            '<div id="analysis-' + key + '" class="two-col">'
            + self._build_cold_col(pr, theme)
            + self._build_miss_col(key, mr, theme)
            + '</div>'
        )

    # ── Tail Miss Panel ──────────────────────────────────────

    def _build_tail_miss_html(self, key: str, tr: Dict, theme: Dict,
                              is_historical: bool = False) -> str:
        tail_miss    = tr["tail_miss"]
        tail_numbers = tr["tail_numbers"]
        tail_cond    = tr.get("tail_cond_prob", {})
        p            = theme["primary"]
        max_miss     = max(tail_miss.values()) if tail_miss else 1

        # Always fixed 0尾→9尾 digit order (v9.3 — user requirement)
        sorted_tails = sorted(tail_miss.items(), key=lambda x: x[0])
        cards = ""
        for tail, miss in sorted_tails:
            pct = int(miss / max(max_miss, 1) * 100)
            if miss >= 8:
                bar_c, txt_c  = "#b91c1c", "#b91c1c"
                card_style    = 'border:2px solid #ef4444;background:#fff5f5'
            elif miss >= 5:
                bar_c, txt_c  = "#dc2626", "#dc2626"
                card_style    = 'border:1px solid #fca5a5;background:#fff7f7'
            elif miss >= 3:
                bar_c, txt_c  = "#ef4444", "#ef4444"
                card_style    = 'border:1px solid #fca5a5;background:#fffafa'
            else:
                bar_c, txt_c  = "#22c55e", "#64748b"
                card_style    = 'border:1px solid #e2e8f0;background:#f8fafc'
            nums_str = " ".join(f"{n:02d}" for n in tail_numbers.get(tail, []))

            # Build collapsible conditional probability table
            cond_rows = ""
            cond_data = tail_cond.get(tail, {})
            for m in sorted(cond_data.keys()):
                if m > 10:
                    continue
                d = cond_data[m]
                if d["samples"] < 5:
                    continue
                prob_c = "#dc2626" if d["prob"] >= 50 else ("#d97706" if d["prob"] >= 35 else "#16a34a")
                bar_w = min(int(d["prob"]), 100)
                cond_rows += (
                    '<div class="consec-row">'
                    '<span class="consec-k-label">遺漏' + str(m) + '期：</span>'
                    '<span class="consec-prob" style="color:' + prob_c + '">' + str(d["prob"]) + '%</span>'
                    '<span class="consec-sample">(' + str(d["hit"]) + '/' + str(d["samples"]) + ')</span>'
                    '<div class="consec-bar-bg">'
                    '<div class="consec-bar-fill" style="width:' + str(bar_w) + '%;background:' + prob_c + '"></div>'
                    '</div>'
                    '</div>'
                )

            cond_section = ""
            if cond_rows:
                cond_section = (
                    '<details style="margin-top:.22rem">'
                    '<summary style="font-size:.56rem;color:#6366f1;cursor:pointer;list-style:none;'
                    'padding:.15rem .25rem;border-radius:.25rem;display:flex;align-items:center;gap:.2rem">'
                    '<span style="font-size:.5rem;transition:transform .18s" class="caret">▶</span>'
                    '條件機率</summary>'
                    '<div style="padding:.2rem .1rem 0">' + cond_rows + '</div>'
                    '</details>'
                )

            cards += (
                '<div class="tail-card" style="' + card_style + '">'
                '<div class="tail-digit">' + str(tail) + '尾</div>'
                '<div class="tail-miss-val" style="color:' + txt_c + '">' + str(miss) + '</div>'
                '<div class="tail-bar">'
                '<div class="tail-bar-fill" style="width:' + str(pct) + '%;background:' + bar_c + '"></div>'
                '</div>'
                '<div class="tail-nums">' + nums_str + '</div>'
                + cond_section +
                '</div>'
            )

        hist_hint = ' ｜ ⏱️歷史截止日樣本' if is_historical else ''
        # Build conclusion
        high_miss_tails = [(t, m) for t, m in sorted_tails if m >= 5]
        if high_miss_tails:
            top_tail, top_miss = max(high_miss_tails, key=lambda x: x[1])
            top_nums = tail_numbers.get(top_tail, [])
            nums_str = "、".join(f"{n:02d}" for n in top_nums[:5])
            conclusion = (
                '📌 結論：<strong>' + str(top_tail) + ' 尾</strong>已連續遺漏 '
                '<strong>' + str(top_miss) + ' 期</strong>，號碼為 ' + nums_str
                + '，下期出現機率偏高，建議排除此尾數（五選不中）。'
            )
            tail_conclusion_html = (
                '<div style="font-size:.66rem;color:#7f1d1d;background:#fff5f5;border:1px solid #fecaca;'
                'border-radius:.4rem;padding:.3rem .55rem;margin-top:.35rem;line-height:1.6">'
                + conclusion + '</div>'
            )
        else:
            tail_conclusion_html = ''

        return (
            '<div id="tail-' + key + '" class="tail-panel">'
            '<div style="display:flex;align-items:center;gap:.4rem;margin-bottom:.25rem">'
            '<span style="font-size:1rem">🔢</span>'
            '<h3 style="font-size:.88rem;font-weight:900;color:' + p + '">'
            '尾數遺漏統計（0~9尾 · 當前連續未出期數）</h3>'
            '</div>'
            '<div style="font-size:.66rem;color:#64748b;margin-bottom:.45rem">'
            '0尾=10,20,30 ｜ 固定 0尾→9尾 順序排列' + hist_hint + ' ｜ 點卡片展開條件機率</div>'
            '<div class="tail-grid">' + cards + '</div>'
            + tail_conclusion_html +
            '</div>'
        )

    # ── OE/Color History Stats Panel (v9.0) ──────────────────

    def _build_oe_color_panel(self, key: str, oec: Dict, theme: Dict) -> str:
        if not oec or not oec.get("oe_pcts"):
            return ""
        p          = theme["primary"]
        total      = oec.get("total_draws", 1)
        oe_pcts    = oec.get("oe_pcts", {})
        color_dist = oec.get("color_dist", [])
        pick_count = 5

        oe_bars = ""
        for k in range(pick_count + 1):
            info = oe_pcts.get(k, {"count": 0, "pct": 0})
            pct  = info["pct"]
            cnt  = info["count"]
            bar_w = min(int(pct), 100)
            oe_label = f"{k}單{pick_count - k}雙"
            bar_c = "#4338ca" if abs(k - pick_count // 2) <= 1 else "#94a3b8"
            oe_bars += (
                '<div style="display:flex;align-items:center;gap:.4rem;margin-bottom:.22rem">'
                '<span style="min-width:52px;font-size:.68rem;font-weight:700;color:#334155">'
                + oe_label + '</span>'
                '<div style="flex:1;height:12px;background:#f1f5f9;border-radius:6px;overflow:hidden">'
                '<div style="width:' + str(bar_w) + '%;height:100%;background:' + bar_c + ';border-radius:6px;transition:width .4s"></div>'
                '</div>'
                '<span style="min-width:48px;font-size:.68rem;font-weight:700;color:#334155;text-align:right">'
                + str(pct) + '%</span>'
                '<span style="font-size:.6rem;color:#94a3b8;min-width:36px">(' + str(cnt) + ')</span>'
                '</div>'
            )

        color_rows = ""
        for item in color_dist[:8]:
            parts = item["key"].split(":")
            r_c, b_c, g_c = (int(x) for x in parts) if len(parts) == 3 else (0, 0, 0)
            label = f"紅{r_c}藍{b_c}綠{g_c}"
            bar_w = min(int(item["pct"]), 100)
            color_rows += (
                '<div style="display:flex;align-items:center;gap:.4rem;margin-bottom:.2rem">'
                '<span style="min-width:70px;font-size:.67rem;font-weight:700;color:#334155">'
                + label + '</span>'
                '<div style="flex:1;height:10px;background:#f1f5f9;border-radius:5px;overflow:hidden">'
                '<div style="width:' + str(bar_w) + '%;height:100%;'
                'background:linear-gradient(90deg,#ef4444,#3b82f6,#22c55e);border-radius:5px"></div>'
                '</div>'
                '<span style="min-width:44px;font-size:.68rem;font-weight:700;color:#334155;text-align:right">'
                + str(item["pct"]) + '%</span>'
                '<span style="font-size:.6rem;color:#94a3b8;min-width:36px">(' + str(item["count"]) + ')</span>'
                '</div>'
            )

        return (
            '<div id="oe-color-' + key + '" class="oe-color-panel">'
            '<details>'
            '<summary style="display:flex;align-items:center;gap:.4rem;cursor:pointer;'
            'list-style:none;user-select:none;padding:.1rem .2rem">'
            '<span style="font-size:1rem">🎲</span>'
            '<h3 style="font-size:.88rem;font-weight:900;color:' + p + '">'
            '奇偶與三色球歷史分佈統計（共 ' + str(total) + ' 期）</h3>'
            '<span class="caret" style="font-size:.6rem;transition:transform .2s;margin-left:.3rem">▶</span>'
            '</summary>'
            '<div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-top:.65rem">'
            '<div>'
            '<div style="font-size:.75rem;font-weight:800;color:#475569;margin-bottom:.45rem">奇偶分佈</div>'
            + oe_bars +
            '</div>'
            '<div>'
            '<div style="font-size:.75rem;font-weight:800;color:#475569;margin-bottom:.45rem">色球組合（Top8）</div>'
            + color_rows +
            '</div>'
            '</div>'
            '</details>'
            '</div>'
        )

    # ── Strategy Backtest Panel (v9.5) ───────────────────────

    def _build_strat_panel(self, key: str, sbt: Dict, theme: Dict,
                           mbt: Optional[List] = None,
                           tun: Optional[Dict] = None) -> str:
        if not sbt or sbt.get("total_tested", 0) == 0:
            return ""
        p          = theme["primary"]
        wins       = sbt.get("wins", 0)
        losses     = sbt.get("losses", 0)
        win_rate   = sbt.get("win_rate", 0.0)
        max_win    = sbt.get("max_win_streak", 0)
        max_loss   = sbt.get("max_loss_streak", 0)
        total      = sbt.get("total_tested", 0)
        recent_20  = sbt.get("recent_20", [])

        rate_c = "#16a34a" if win_rate >= 70 else ("#d97706" if win_rate >= 55 else "#dc2626")
        sparkline = ""
        for r in recent_20:
            col = "#16a34a" if r == 1 else "#dc2626"
            sparkline += (
                '<span style="display:inline-block;width:10px;height:18px;'
                'background:' + col + ';border-radius:2px;margin:.5px" title="'
                + ('勝' if r == 1 else '敗') + '"></span>'
            )

        return (
            '<div id="strat-' + key + '" class="strat-panel">'
            '<details>'
            '<summary style="display:flex;align-items:center;gap:.4rem;cursor:pointer;'
            'list-style:none;user-select:none;padding:.1rem .2rem">'
            '<span style="font-size:1rem">📊</span>'
            '<h3 style="font-size:.88rem;font-weight:900;color:' + p + '">'
            '策略批量回測 — 智能推薦評分 Top5 選號歷史驗證</h3>'
            '<span class="caret" style="font-size:.6rem;transition:transform .2s;margin-left:.3rem">▶</span>'
            '<span style="font-size:.62rem;color:#94a3b8;margin-left:auto">共測 ' + str(total) + ' 期</span>'
            '</summary>'
            '<div class="strat-body">'
            '<div class="strat-stats-row">'
            '<div class="strat-stat-card" style="background:#f0fdf4;border-color:#86efac">'
            '<div class="strat-stat-val" style="color:#16a34a">' + str(wins) + '</div>'
            '<div class="strat-stat-lbl">勝（0中）</div>'
            '</div>'
            '<div class="strat-stat-card" style="background:#fff1f2;border-color:#fca5a5">'
            '<div class="strat-stat-val" style="color:#dc2626">' + str(losses) + '</div>'
            '<div class="strat-stat-lbl">敗（有中）</div>'
            '</div>'
            '<div class="strat-stat-card" style="background:#f8fafc;border-color:#e2e8f0">'
            '<div class="strat-stat-val" style="color:' + rate_c + '">' + str(win_rate) + '%</div>'
            '<div class="strat-stat-lbl">勝率</div>'
            '</div>'
            '<div class="strat-stat-card" style="background:#f0fdf4;border-color:#86efac">'
            '<div class="strat-stat-val" style="color:#16a34a">' + str(max_win) + '</div>'
            '<div class="strat-stat-lbl">最長連勝</div>'
            '</div>'
            '<div class="strat-stat-card" style="background:#fff1f2;border-color:#fca5a5">'
            '<div class="strat-stat-val" style="color:#dc2626">' + str(max_loss) + '</div>'
            '<div class="strat-stat-lbl">最長連敗</div>'
            '</div>'
            '</div>'
            '<div style="margin-top:.6rem">'
            '<div style="font-size:.68rem;font-weight:700;color:#64748b;margin-bottom:.3rem">'
            '最近 ' + str(len(recent_20)) + ' 期走勢（綠=勝 紅=敗）</div>'
            '<div style="display:flex;flex-wrap:wrap;gap:0;align-items:flex-end">' + sparkline + '</div>'
            '</div>'
            '<div style="font-size:.62rem;color:#94a3b8;margin-top:.45rem;line-height:1.5">'
            '策略邏輯（v10.0）：每期以智能複合評分（100 − 危險分×0.6 − 近期頻率×8×0.4）選出前5高分號碼，嚴格限用截止期前數據，測試五選不中勝率。僅作歷史統計參考，不保證未來表現。'
            '</div>'
            + self._build_strat_ranking(mbt or [], MultiStrategyBacktester.RANDOM_WIN_RATE)
            + self._build_excl_tune_table(tun or {})
            + '</div>'
            '</details>'
            '</div>'
        )

    def _build_strat_ranking(self, mbt: List, random_wr: float) -> str:
        if not mbt:
            return ""
        medals = ["🥇", "🥈", "🥉", "4.", "5."]
        rows_html = ""
        for idx, s in enumerate(mbt):
            wr    = s["win_rate"]
            delta = s.get("delta_vs_random", round(wr - random_wr, 1))
            stab  = s.get("stability_std", 0)
            r100  = s.get("recent_100_rate")
            spark = "".join(
                '<span style="display:inline-block;width:6px;height:12px;'
                'background:' + ("#16a34a" if r == 1 else "#dc2626") + ';'
                'border-radius:1px;margin:.5px"></span>'
                for r in s.get("recent_10", [])
            )
            wr_c    = "#16a34a" if wr >= 70 else ("#d97706" if wr >= 55 else "#dc2626")
            delta_c = "#16a34a" if delta >= 10 else ("#d97706" if delta >= 0 else "#dc2626")
            # Trend stability: compare near-300 (win_rate) vs near-100 (recent_100_rate)
            if r100 is None:
                trend_lbl = "樣本不足"; trend_c = "#94a3b8"
            else:
                diff100 = round(r100 - wr, 1)
                diff_str = ("+" if diff100 >= 0 else "") + str(diff100) + "%"
                if abs(diff100) <= 5:
                    trend_lbl = "穩定"; trend_c = "#16a34a"
                elif diff100 > 5:
                    trend_lbl = "近期轉強 " + diff_str; trend_c = "#2563eb"
                else:
                    trend_lbl = "近期轉弱 " + diff_str; trend_c = "#d97706"
            # Wilson 95% CI
            n_total = s.get("total", 0)
            p_val   = wr / 100.0
            z       = 1.96
            if n_total >= 3:
                denom  = 1 + z * z / n_total
                center = (p_val + z * z / (2 * n_total)) / denom
                margin = z * (p_val * (1 - p_val) / n_total + z * z / (4 * n_total * n_total)) ** 0.5 / denom
                ci_lo  = round(max(0.0, (center - margin) * 100), 1)
                ci_hi  = round(min(100.0, (center + margin) * 100), 1)
                half   = round((ci_hi - ci_lo) / 2, 1)
                ci_html = '<br><span style="font-size:.58rem;color:#94a3b8">±' + str(half) + '%</span>'
                if n_total < 50:
                    ci_html += '<br><span style="font-size:.55rem;color:#94a3b8">⚠️樣本不足</span>'
            else:
                ci_html = '<br><span style="font-size:.58rem;color:#94a3b8">—</span>'
            rows_html += (
                '<tr style="border-bottom:1px solid #f1f5f9">'
                '<td style="padding:.22rem .35rem;font-size:.7rem;font-weight:700">'
                + medals[idx] + ' ' + s["label"] + '</td>'
                '<td style="padding:.22rem .35rem;font-size:.72rem;font-weight:800;color:' + wr_c + '">'
                + str(wr) + '%' + ci_html + '</td>'
                '<td style="padding:.22rem .35rem;font-size:.68rem;color:' + delta_c + '">'
                + ('+' if delta >= 0 else '') + str(delta) + '%</td>'
                '<td style="padding:.22rem .35rem;font-size:.63rem;color:' + trend_c + ';font-weight:700">'
                + trend_lbl + '</td>'
                '<td style="padding:.22rem .35rem">' + spark + '</td>'
                '</tr>'
            )
        return (
            '<div style="margin-top:.75rem">'
            '<div style="font-size:.68rem;font-weight:800;color:#334155;margin-bottom:.3rem">'
            '📈 策略排行榜（近300期 · 隨機基準 ≈ ' + str(random_wr) + '%）</div>'
            '<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;'
            'font-size:.65rem">'
            '<thead><tr style="background:#f8fafc">'
            '<th style="padding:.18rem .35rem;text-align:left;color:#475569">策略</th>'
            '<th style="padding:.18rem .35rem;text-align:left;color:#475569">近300勝率</th>'
            '<th style="padding:.18rem .35rem;text-align:left;color:#475569">vs隨機</th>'
            '<th style="padding:.18rem .35rem;text-align:left;color:#475569">穩定度</th>'
            '<th style="padding:.18rem .35rem;text-align:left;color:#475569">近10期</th>'
            '</tr></thead>'
            '<tbody>' + rows_html + '</tbody>'
            '</table></div>'
            '<div style="font-size:.57rem;color:#94a3b8;margin-top:.3rem">'
            '穩定度 = 近100期 vs 近300期勝率差距；差距 ≤5% 為穩定，正差為轉強，負差為轉弱。</div>'
            '</div>'
        )

    def _build_excl_tune_table(self, tun: Dict) -> str:
        presets = tun.get("presets", [])
        best_id = tun.get("best_id", "")
        if not presets:
            return ""
        rows_html = ""
        for p in presets:
            wr    = p["win_rate"]
            delta = p.get("delta", 0)
            is_best = p["id"] == best_id
            bg = "background:#f0fdf4;" if is_best else ""
            wr_c = "#16a34a" if wr >= 70 else ("#d97706" if wr >= 55 else "#dc2626")
            delta_c = "#16a34a" if delta >= 10 else ("#d97706" if delta >= 0 else "#dc2626")
            spark = "".join(
                '<span style="display:inline-block;width:5px;height:10px;'
                'background:' + ("#16a34a" if r == 1 else "#dc2626") + ';'
                'border-radius:1px;margin:.3px"></span>'
                for r in p.get("recent_10", [])
            )
            rows_html += (
                '<tr style="border-bottom:1px solid #f1f5f9;' + bg + '">'
                '<td style="padding:.2rem .32rem;font-size:.67rem;font-weight:'
                + ('800' if is_best else '500') + '">'
                + ('★ ' if is_best else '') + p["label"] + '</td>'
                '<td style="padding:.2rem .32rem;font-size:.7rem;font-weight:700;color:' + wr_c + '">'
                + str(wr) + '%</td>'
                '<td style="padding:.2rem .32rem;font-size:.65rem;color:' + delta_c + '">'
                + ('+' if delta >= 0 else '') + str(delta) + '%</td>'
                '<td style="padding:.2rem .32rem">' + spark + '</td>'
                '</tr>'
            )
        return (
            '<div style="margin-top:.6rem">'
            '<div style="font-size:.68rem;font-weight:800;color:#334155;margin-bottom:.3rem">'
            '🎯 排除分權重回測校準（近300期 · 當前最佳：' + tun.get("best_label", "") + '）</div>'
            '<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse">'
            '<thead><tr style="background:#f8fafc">'
            '<th style="padding:.15rem .32rem;text-align:left;font-size:.63rem;color:#475569">預設組合</th>'
            '<th style="padding:.15rem .32rem;text-align:left;font-size:.63rem;color:#475569">勝率</th>'
            '<th style="padding:.15rem .32rem;text-align:left;font-size:.63rem;color:#475569">vs隨機</th>'
            '<th style="padding:.15rem .32rem;text-align:left;font-size:.63rem;color:#475569">近10期</th>'
            '</tr></thead>'
            '<tbody>' + rows_html + '</tbody>'
            '</table></div>'
            '<div style="font-size:.6rem;color:#94a3b8;margin-top:.22rem;line-height:1.5">'
            '各預設以排除分最低的5個號碼作為選號組合進行五不中回測，勝率越高表示該組合越能避開開獎號碼。'
            '</div></div>'
        )

    # ── Recent Heat Probability Panel (v9.6) ─────────────────

    def _build_heat_prob_panel(self, key: str, rhp: Dict, theme: Dict) -> str:
        if not rhp or not rhp.get("numbers"):
            return ""
        p       = theme["primary"]
        numbers = rhp["numbers"]
        matrix  = rhp.get("matrix", {})
        window  = rhp.get("window", 20)
        samples = rhp.get("backtest_samples", 0)

        cards = ""
        for num in range(1, 40):
            nd = numbers.get(num) or numbers.get(str(num))
            if nd is None:
                continue
            rc    = nd.get("recent_count", 0)
            rate  = nd.get("next_hit_rate")
            hit   = nd.get("hit_count", 0)
            total = nd.get("sample_count", 0)
            cls   = ball_cls(num)

            if total < 10:
                rate_str   = "樣本少"
                rate_color = "#94a3b8"
            elif rate is None:
                rate_str   = "--"
                rate_color = "#94a3b8"
            else:
                rate_str   = f"{rate}%"
                rate_color = "#16a34a" if rate < 15 else ("#d97706" if rate < 25 else "#dc2626")

            nm = matrix.get(num) or matrix.get(str(num)) or {}
            mat_rows = ""
            for k in sorted(nm.keys()):
                mv  = nm[k]
                mr2 = mv.get("rate")
                mt  = mv.get("total", 0)
                if mt == 0:
                    continue
                mr_str = (f"{mr2}%" if mr2 is not None else "--")
                active = " style=\"background:#eff6ff;font-weight:700\"" if k == rc else ""
                mat_rows += (
                    '<div class="heat-prob-matrix-row"' + active + '>'
                    '<span>' + str(k) + '次</span>'
                    '<span style="color:#475569">' + mr_str
                    + '<span style="color:#94a3b8;font-weight:400"> '
                    + str(mv.get("hit", 0)) + '/' + str(mt)
                    + '</span></span></div>'
                )
            if not mat_rows:
                mat_rows = '<div style="color:#94a3b8;font-size:.58rem;padding:.1rem .2rem">無樣本</div>'

            cards += (
                '<details class="heat-prob-num-card">'
                '<summary>'
                '<span class="ball-sm ' + cls + '" '
                'style="width:1.5rem;height:1.5rem;font-size:.62rem">' + f'{num:02d}' + '</span>'
                '<span style="font-size:.62rem;color:#475569">近' + str(rc) + '次</span>'
                '<span style="font-size:.65rem;font-weight:700;color:' + rate_color + '">'
                + rate_str + '</span>'
                '<span style="font-size:.55rem;color:#94a3b8">'
                + str(hit) + '/' + str(total) + '</span>'
                '</summary>'
                '<div>' + mat_rows + '</div>'
                '</details>'
            )

        # Build conclusion: find numbers with high recent count AND high next_hit_rate
        high_risk = []
        for num in range(1, 40):
            nd = numbers.get(num) or numbers.get(str(num))
            if nd is None:
                continue
            rc   = nd.get("recent_count", 0)
            rate = nd.get("next_hit_rate")
            samp = nd.get("sample_count", 0)
            if rc >= 3 and rate is not None and rate >= 25 and samp >= 10:
                high_risk.append((num, rc, rate))
        high_risk.sort(key=lambda x: (-x[2], -x[1]))
        if high_risk:
            top_num, top_rc, top_rate = high_risk[0]
            extra = f"，共 {len(high_risk)} 個號碼符合條件" if len(high_risk) > 1 else ""
            heat_conclusion = (
                '📌 結論：<strong>' + f'{top_num:02d}' + ' 號</strong>'
                '近 ' + str(window) + ' 期出現 ' + str(top_rc) + ' 次，'
                '歷史上再次出現機率 <strong>' + str(top_rate) + '%</strong>（樣本充足）'
                + extra + '，建議排除熱門號。'
            )
            heat_concl_html = (
                '<div style="font-size:.66rem;color:#7c2d12;background:#fff7ed;border:1px solid #fde68a;'
                'border-radius:.4rem;padding:.3rem .55rem;margin-top:.35rem;line-height:1.6">'
                + heat_conclusion + '</div>'
            )
        else:
            heat_concl_html = (
                '<div style="font-size:.66rem;color:#166534;background:#f0fdf4;border:1px solid #bbf7d0;'
                'border-radius:.4rem;padding:.3rem .55rem;margin-top:.35rem;line-height:1.6">'
                '✅ 目前無號碼同時滿足「高近期頻率＋高歷史重複率」，整體熱力風險偏低。'
                '</div>'
            )

        return (
            '<div id="heat-prob-' + key + '" class="heat-prob-panel">'
            '<details>'
            '<summary style="display:flex;align-items:center;gap:.4rem;cursor:pointer;'
            'list-style:none;user-select:none;padding:.1rem .2rem">'
            '<span style="font-size:1rem">🔥</span>'
            '<h3 style="font-size:.88rem;font-weight:900;color:' + p + '">'
            '近' + str(window) + '期熱度條件機率總攬</h3>'
            '<span class="caret" style="font-size:.6rem;transition:transform .2s;margin-left:.3rem">▶</span>'
            '<span style="font-size:.62rem;color:#94a3b8;margin-left:auto">'
            '滾動回測 ' + str(samples) + ' 期樣本</span>'
            '</summary>'
            '<div style="padding:.4rem .2rem">'
            '<div style="font-size:.63rem;color:#64748b;margin-bottom:.4rem;line-height:1.5">'
            '統計方式：取每期前 ' + str(window) + ' 期為視窗，統計號碼在窗口出現 k 次後下一期再開機率。'
            '當前數字 = 號碼在最近 ' + str(window) + ' 期的出現次數（藍底列為當前 k）。'
            '</div>'
            '<div style="display:flex;flex-wrap:wrap;align-items:flex-start">'
            + cards +
            '</div>'
            + heat_concl_html +
            '</div>'
            '</details>'
            '</div>'
        )

    # ── Picker data scripts (embedded in each panel) ───────────

    def _build_picker_data_script(self, key: str, mr: Dict, pr: Dict,
                                   nhr: Optional[Dict] = None,
                                   oec: Optional[Dict] = None,
                                   sbt: Optional[Dict] = None,
                                   rhp: Optional[Dict] = None,
                                   gaps_in: Optional[List] = None,
                                   gap_affects_recent: bool = False,
                                   mbt_in: Optional[List] = None,
                                   rev_oe_in: Optional[Dict] = None) -> str:
        """Embed JS globals for all analysis data."""
        current_misses = mr["current_misses"]
        miss_json   = _json.dumps({str(k): v for k, v in current_misses.items()})
        recent_json = _json.dumps(pr.get("recent_8", []))
        draw_json   = _json.dumps(pr.get("recent_match", pr.get("recent_8", [])))
        period_json = _json.dumps(pr.get("top8_lowest", []))
        nhr_json    = _json.dumps({str(k): v for k, v in (nhr or {}).items()})
        oec_json    = _json.dumps(oec or {})
        sbt_json    = _json.dumps(sbt or {})
        # Serialize rhp.numbers with string keys for JS lookup
        rhp_nums    = {str(k): v for k, v in (rhp or {}).get("numbers", {}).items()}
        rhp_json    = _json.dumps(rhp_nums)
        gap_json    = _json.dumps({
            "count": len(gaps_in or []),
            "affectsRecent": gap_affects_recent,
        })
        mbt_json    = _json.dumps(mbt_in or [])
        rev_oe_json = _json.dumps(rev_oe_in or {})
        return (
            '<script>'
            'window._MISS_DATA=window._MISS_DATA||{};'
            'window._MISS_DATA["' + key + '"]=' + miss_json + ';'
            'window._RECENT_DATA=window._RECENT_DATA||{};'
            'window._RECENT_DATA["' + key + '"]=' + recent_json + ';'
            'window._DRAW_DATA=window._DRAW_DATA||{};'
            'window._DRAW_DATA["' + key + '"]=' + draw_json + ';'
            'window._PERIOD_DATA=window._PERIOD_DATA||{};'
            'window._PERIOD_DATA["' + key + '"]=' + period_json + ';'
            'window._NUM_HIST_DATA=window._NUM_HIST_DATA||{};'
            'window._NUM_HIST_DATA["' + key + '"]=' + nhr_json + ';'
            'window._OE_COLOR_DATA=window._OE_COLOR_DATA||{};'
            'window._OE_COLOR_DATA["' + key + '"]=' + oec_json + ';'
            'window._STRAT_DATA=window._STRAT_DATA||{};'
            'window._STRAT_DATA["' + key + '"]=' + sbt_json + ';'
            'window._HEAT_PROB_DATA=window._HEAT_PROB_DATA||{};'
            'window._HEAT_PROB_DATA["' + key + '"]=' + rhp_json + ';'
            'window._GAP_DATA=window._GAP_DATA||{};'
            'window._GAP_DATA["' + key + '"]=' + gap_json + ';'
            'window._MULTI_BT_DATA=window._MULTI_BT_DATA||{};'
            'window._MULTI_BT_DATA["' + key + '"]=' + mbt_json + ';'
            'window._REV_OE_DATA=window._REV_OE_DATA||{};'
            'window._REV_OE_DATA["' + key + '"]=' + rev_oe_json + ';'
            '</script>'
        )

    # ── Picker UI (used in sidebar) ──────────────────────────

    def _build_picker_ui(self, key: str, mr: Dict, pr: Dict) -> str:
        current_misses = mr["current_misses"]
        cells = ""
        for n in range(1, 40):
            cls  = ball_cls(n)
            miss = current_misses.get(n, 0)
            cells += (
                '<div class="pk-cell" data-num="' + str(n) + '" '
                'onclick="togglePickerNum(\'' + key + '\',' + str(n) + ')">'
                '<span class="ball-sm ' + cls + '">' + f'{n:02d}' + '</span>'
                '<span class="pk-miss">遺漏' + str(miss) + '</span>'
                '</div>'
            )
        return (
            # ── Miss distribution histogram (collapsible)
            '<details style="margin-bottom:.4rem">'
            '<summary style="color:#6366f1;font-size:.68rem;font-weight:600;cursor:pointer;list-style:none;display:flex;align-items:center;gap:.22rem;padding:.22rem .3rem;border-radius:.3rem;transition:background .12s" onmouseover="this.style.background=\'#eff6ff\'" onmouseout="this.style.background=\'\'">'
            '<span class="caret" style="font-size:.5rem">▶</span>遺漏分佈直方圖</summary>'
            '<div id="miss-dist-' + key + '" style="padding:.35rem .2rem .1rem;font-size:.62rem"></div>'
            '</details>'
            # ── Picker mode toggle
            '<div class="pk-mode-bar">'
            '<span style="font-size:.65rem;font-weight:700;color:#64748b;margin-right:.3rem">顯示模式：</span>'
            '<button class="pk-mode-btn active" id="pkm-def-' + key + '" '
            'onclick="setPickerMode(\'' + key + '\',\'default\')">遺漏期</button>'
            '<button class="pk-mode-btn" id="pkm-heat-' + key + '" '
            'onclick="setPickerMode(\'' + key + '\',\'heatmap\')">近期熱力</button>'
            '<button class="pk-mode-btn" id="pkm-danger-' + key + '" '
            'onclick="setPickerMode(\'' + key + '\',\'danger\')">危險度</button>'
            '</div>'
            # ── Picker grid
            '<div id="pk-grid-' + key + '" class="picker-grid">' + cells + '</div>'
            # ── Live stats bar
            '<div id="pk-live-' + key + '" class="pk-live-bar">'
            '<span style="color:#94a3b8;font-size:.65rem">點選號碼查看即時單雙與色球統計</span>'
            '</div>'
            # ── Smart recommendation
            '<div class="pk-smart-panel">'
            '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:.3rem">'
            '<span style="font-size:.68rem;font-weight:800;color:#334155">🎯 智能推薦（最低危險度前5）</span>'
            '<button class="pk-mode-btn" style="font-size:.6rem" '
            'onclick="_updateSmartRec(\'' + key + '\')">刷新</button>'
            '</div>'
            '<div id="pk-smart-' + key + '" style="display:flex;flex-wrap:wrap;gap:.22rem">'
            '<span style="color:#94a3b8;font-size:.63rem">載入中…</span>'
            '</div>'
            '<div style="display:flex;gap:.28rem;margin-top:.28rem">'
            '<button class="pk-mode-btn" style="font-size:.57rem;flex:1;'
            'background:#059669;color:#fff;padding:.28rem .1rem;" '
            'onclick="_adoptRec(\'' + key + '\',\'conservative\')">🛡 保守推薦</button>'
            '<button class="pk-mode-btn" style="font-size:.57rem;flex:1;'
            'background:#4338ca;color:#fff;padding:.28rem .1rem;" '
            'onclick="_adoptRec(\'' + key + '\',\'balanced\')">⚖ 均衡推薦</button>'
            '<button class="pk-mode-btn" style="font-size:.57rem;flex:1;'
            'background:#9333ea;color:#fff;padding:.28rem .1rem;" '
            'onclick="_adoptRec(\'' + key + '\',\'cold\')">❄ 冷門推薦</button>'
            '</div>'
            '</div>'
            # ── Today's Decision Summary placeholder
            '<div id="pk-daily-' + key + '" class="pk-daily-panel"></div>'
            # ── Selected row + toolbar
            '<div class="picker-toolbar" style="margin-top:.38rem">'
            '<div id="pk-selected-' + key + '" class="picker-selected-row">'
            '<span style="color:#94a3b8;font-size:.68rem">點號碼選號（最多5個）</span>'
            '</div>'
            '</div>'
            # ── Selection risk summary placeholder
            '<div id="pk-risk-' + key + '"></div>'
            # ── OE/Color guide placeholder (v10.2)
            '<div id="pk-oe-guide-' + key + '" style="margin:.1rem 0 .18rem"></div>'
            # ── A/B Compare + Combo Backtest (v10.0 / v10.1)
            '<div style="display:flex;gap:.22rem;margin-top:.22rem;flex-wrap:wrap">'
            '<button class="pk-mode-btn" style="font-size:.6rem;padding:.18rem .38rem" '
            'onclick="savePickerCompare(\'' + key + '\',\'a\')">📌 存為A組</button>'
            '<button class="pk-mode-btn" style="font-size:.6rem;padding:.18rem .38rem" '
            'onclick="savePickerCompare(\'' + key + '\',\'b\')">📌 存為B組</button>'
            '<button class="pk-mode-btn" style="font-size:.6rem;padding:.18rem .38rem;color:#94a3b8" '
            'onclick="clearPickerCompare(\'' + key + '\')">✕ 清除</button>'
            '<button class="pk-mode-btn" style="font-size:.6rem;padding:.18rem .38rem;color:#6366f1" '
            'onclick="backtestCurrentCombo(\'' + key + '\')">📊 回測此組合</button>'
            '</div>'
            '<div id="pk-compare-' + key + '"></div>'
            '<div id="pk-combo-bt-' + key + '"></div>'
            '<div style="display:flex;gap:.3rem;align-items:center;margin-top:.3rem">'
            '<input id="pk-note-' + key + '" class="pk-note-input" style="flex:1" type="text" '
            'placeholder="備註（可空）">'
            '<button class="pk-btn pk-btn-save" '
            'onclick="savePickerEntry(\'' + key + '\')">💾</button>'
            '<button id="pk-lock-btn-' + key + '" class="pk-btn" '
            'style="background:#e0f2fe;color:#0369a1;font-size:.68rem" '
            'onclick="lockPickerSel(\'' + key + '\')">🔒</button>'
            '<button class="pk-btn pk-btn-clear" '
            'onclick="clearPickerSel(\'' + key + '\')">✕</button>'
            '</div>'
            # ── Bet log section
            '<details style="margin-top:.45rem">'
            '<summary style="color:#6366f1;font-size:.7rem;list-style:none;cursor:pointer;'
            'display:flex;align-items:center;gap:.28rem;padding:.3rem .4rem;'
            'border-radius:.35rem;transition:background .12s" '
            'onmouseover="this.style.background=\'#f1f5f9\'" '
            'onmouseout="this.style.background=\'\'">'
            '<span style="font-size:.55rem;transition:transform .2s" class="caret">▶</span>'
            ' 選號紀錄本</summary>'
            '<div style="display:flex;justify-content:flex-end;margin-top:.3rem">'
            '<button class="pk-mode-btn" style="font-size:.62rem;color:#6366f1" '
            'onclick="exportBetLogCSV(\'' + key + '\')">📥 匯出CSV</button>'
            '</div>'
            # ── Personal win rate dashboard (v10.1)
            '<div id="pk-wr-' + key + '"></div>'
            # ── Filter bar
            '<div id="bet-filter-bar-' + key + '" class="bet-filter-bar">'
            '<button class="bet-filter-btn active" data-f="all" '
            'onclick="_setBetFilter(\'' + key + '\',\'all\')">全部</button>'
            '<button class="bet-filter-btn" data-f="pending" '
            'onclick="_setBetFilter(\'' + key + '\',\'pending\')">未結算</button>'
            '<button class="bet-filter-btn" data-f="win" '
            'onclick="_setBetFilter(\'' + key + '\',\'win\')">勝</button>'
            '<button class="bet-filter-btn" data-f="loss" '
            'onclick="_setBetFilter(\'' + key + '\',\'loss\')">敗</button>'
            '<button class="bet-filter-btn" data-f="manual" '
            'onclick="_setBetFilter(\'' + key + '\',\'manual\')">手動</button>'
            '<button class="bet-filter-btn" data-f="conservative" '
            'onclick="_setBetFilter(\'' + key + '\',\'conservative\')">保守</button>'
            '<button class="bet-filter-btn" data-f="balanced" '
            'onclick="_setBetFilter(\'' + key + '\',\'balanced\')">均衡</button>'
            '<button class="bet-filter-btn" data-f="cold" '
            'onclick="_setBetFilter(\'' + key + '\',\'cold\')">冷門</button>'
            '</div>'
            # ── Scrollable section
            '<div class="bet-log-section">'
            '<div id="bet-stats-' + key + '" class="bet-stats" style="display:none;margin-top:.25rem"></div>'
            '<div id="win-trend-' + key + '"></div>'
            '<div id="fail-analysis-' + key + '"></div>'
            '<div id="bet-log-' + key + '" class="bet-log" style="margin-top:.25rem"></div>'
            '</div>'
            '</details>'
        )

    # ── Fixed Picker Sidebar ─────────────────────────────────

    def _build_sidebar(self, results: Dict) -> str:
        panes = ""
        first_key = None
        for key, data in results.items():
            if data is None:
                continue
            if first_key is None:
                first_key = key
            picker_ui = self._build_picker_ui(key, data["miss_result"], data["period_result"])
            display = "" if key == first_key else "display:none"
            panes += (
                '<div id="sp-' + key + '" class="sidebar-pane" style="' + display + '">'
                + picker_ui + '</div>'
            )

        if not panes:
            return ''

        return (
            '<aside class="picker-sidebar" id="picker-sidebar">'
            '<div class="mob-drag-pill"></div>'
            '<div class="sidebar-header" onclick="toggleMobilePicker()">'
            '<span>🎮</span>'
            '<span id="sidebar-title">互動選號盤</span>'
            '<span class="mob-open-hint" id="mob-open-hint">▲ 展開</span>'
            '<button class="sidebar-toggle-btn" id="sidebar-wide-btn" '
            'onclick="event.stopPropagation();toggleSidebarWide()" title="展開/縮窄選號盤">⇔ 展寬</button>'
            '</div>'
            + panes +
            '</aside>'
        )

    # ── Floating Data Management Panel ───────────────────────

    def _build_float_panel(self, server_mode: bool) -> str:
        today = datetime.now().strftime("%Y-%m-%d")
        num_inputs = "".join(
            '<input id="m-n' + str(i+1) + '" type="number" min="1" max="39" placeholder="' + str(i+1) + '" '
            'class="form-field" style="margin-bottom:0;padding:.38rem .18rem;text-align:center">'
            for i in range(5)
        )
        return (
            '<div id="float-panel" class="float-panel hidden">'
            '<div id="fp-drag-handle" class="fp-handle">'
            '<span class="fp-handle-icon">⣿</span>'
            '<span class="fp-handle-title">📡 數據管理面板</span>'
            '<span class="fp-handle-hint">拖拽移動</span>'
            '<button id="fp-close" class="fp-close" title="關閉">✕</button>'
            '</div>'
            '<div class="fp-body" id="upd-body">'
            '<div class="update-grid">'
            '<div class="upd-card" style="background:#f0fdf4;border:1px solid #bbf7d0">'
            '<div class="upd-title" style="color:#166534">🤖 智能自動同步</div>'
            '<div class="upd-desc" style="color:#15803d">'
            '偵測本地 CSV 缺少的期數，自動批量抓取補齊</div>'
            '<button id="btn-scrape" class="btn" '
            'style="background:#16a34a;color:#fff" onclick="runScrape()">一鍵智能補齊</button>'
            '<div id="scrape-log" class="log-area"></div>'
            '</div>'
            '<div class="upd-card" style="background:#eff6ff;border:1px solid #bfdbfe">'
            '<div class="upd-title" style="color:#1e40af">✏️ 手動輸入</div>'
            '<div class="upd-desc" style="color:#1d4ed8">'
            '選彩票 → 日期 → 5個號碼 → 儲存</div>'
            '<select id="m-lottery" class="form-field">'
            '<option value="taiwan_539">台灣今彩539</option>'
            '<option value="michigan_fantasy5">密西根天天樂</option>'
            '<option value="california_fantasy5">加州天天樂</option>'
            '<option value="newyork_take5">紐約天天樂</option>'
            '</select>'
            '<input id="m-date" type="date" value="' + today + '" class="form-field">'
            '<div class="num-grid">' + num_inputs + '</div>'
            '<button id="btn-manual" class="btn" '
            'style="background:#2563eb;color:#fff" onclick="submitManual()">儲存並重新分析</button>'
            '<div id="manual-log" class="log-area"></div>'
            '</div>'
            '</div>'
            '</div>'
            '</div>'
        )

    # ── Cold Period Column ───────────────────────────────────

    def _build_cold_col(self, pr: Dict, theme: Dict) -> str:
        cb = theme["cold_bg"]
        cf = theme["cold_fg"]
        p  = theme["primary"]

        items = ""
        for item in pr["top8_lowest"]:
            t_val    = item["t"]
            ref_date = item["ref_date"]
            prob     = item["overlap_prob"]
            n_sample = item["sample_size"]
            avg      = item["avg_overlap"]
            balls    = (
                " ".join(self._ball(n, "ball-sm") for n in item["ref_numbers"])
                if item["ref_numbers"] else '<span class="no-balls">資料不足</span>'
            )
            items += (
                '<div class="rec-item">'
                '<div class="rec-top">'
                '<span class="rec-badge" style="background:' + cb + ';color:' + cf + '">t' + str(t_val) + '</span>'
                '<div class="rec-info">'
                '<div class="rec-title">往前第 ' + str(t_val) + ' 期　'
                '<span style="font-weight:400;font-size:.7rem;color:#94a3b8">(' + ref_date + ')</span></div>'
                '<div class="rec-sub">樣本 ' + str(n_sample) + ' 期　平均重複 ' + str(avg) + '</div>'
                '</div>'
                '<span class="rec-pct" style="color:' + p + '">' + str(prob) + '%</span>'
                '</div>'
                '<div class="balls-row">' + balls + '</div>'
                '</div>'
            )

        top_ts = {x["t"] for x in pr["top8_lowest"]}
        rows = ""
        for j, s in enumerate(sorted(pr["t_stats"], key=lambda x: x["t"])):
            is_top = s["t"] in top_ts
            tr_cls = "hl" if is_top else ("stripe" if j % 2 == 0 else "")
            star   = " ★" if is_top else ""
            rows += (
                '<tr class="' + tr_cls + '"><td>t' + str(s["t"]) + star + '</td>'
                '<td>' + str(s["overlap_prob"]) + '%</td>'
                '<td>' + str(s["avg_overlap"]) + '</td>'
                '<td>' + str(s["sample_size"]) + '</td></tr>'
            )

        # Build conclusion sentence
        if pr["top8_lowest"]:
            best = pr["top8_lowest"][0]
            conclusion = (
                '📌 結論：近 ' + str(PERIOD_BACKTEST_WINDOW) + ' 期回測中，'
                '往前第 <strong>t' + str(best["t"]) + '</strong> 期（' + best["ref_date"] + '）'
                '的號碼重複率最低（' + str(best["overlap_prob"]) + '%），'
                '建議優先排除其 ' + str(len(best["ref_numbers"])) + ' 個號碼。'
            )
        else:
            conclusion = ''

        return (
            '<div>'
            '<div class="col-title"><span class="icon">🧊</span>'
            '<h3 style="color:' + cb + '">冷門期數 Top ' + str(TOP_N) + '（近' + str(PERIOD_BACKTEST_WINDOW) + '期回測）</h3></div>'
            '<p class="col-desc">往前第 t 期開出的號碼，在最新一期<strong>最不容易再出現</strong>（重複率低→高）。<br>'
            '僅使用目前基準日前最近 ' + str(PERIOD_BACKTEST_WINDOW) + ' 期作為回測樣本。<br>'
            '球形數字 ＝ 該期實際開獎號碼。'
            '<span style="background:#fef9c3;color:#713f12;border-radius:.25rem;padding:.05rem .35rem;font-size:.65rem;font-weight:700;margin-left:.3rem">⛔ 建議避開</span>'
            '這些號碼不要選入你的組合。</p>'
            + items +
            ('<div style="font-size:.66rem;color:#1e3a8a;background:#eff6ff;border:1px solid #bfdbfe;'
             'border-radius:.4rem;padding:.3rem .55rem;margin:.3rem 0 .4rem;line-height:1.6">'
             + conclusion + '</div>' if conclusion else '')
            + '<details><summary style="color:' + p + '">'
            '<span class="caret">▶</span>展開 t1～t' + str(MAX_T) + ' 近' + str(PERIOD_BACKTEST_WINDOW) + '期完整機率表（★ 為 Top ' + str(TOP_N) + '）'
            '</summary>'
            '<div class="table-wrap"><table>'
            '<thead><tr><th>期距</th><th>重複率</th><th>平均重複數</th><th>樣本</th></tr></thead>'
            '<tbody>' + rows + '</tbody>'
            '</table></div></details>'
            '</div>'
        )

    # ── Miss Value Column ────────────────────────────────────

    def _build_miss_col(self, key: str, mr: Dict, theme: Dict) -> str:
        window_data = mr["window_data"]

        sub_tabs = ""
        for wkey in MISS_WINDOWS:
            label  = MISS_WIN_LABELS[wkey]
            active = " active" if wkey == DEFAULT_MISS_WINDOW else ""
            sub_tabs += (
                '<button class="miss-tab' + active + '" data-win="' + wkey + '" '
                'onclick="setMissWin(\'' + key + '\',\'' + wkey + '\')">' + label + '</button>'
            )

        panes = ""
        for wkey in MISS_WINDOWS:
            wd     = window_data[wkey]
            active = " active" if wkey == DEFAULT_MISS_WINDOW else ""
            panes += (
                '<div id="miss-pane-' + key + '-' + wkey + '" class="miss-pane' + active + '">'
                + self._build_miss_pane(key, wkey, wd)
                + '</div>'
            )

        return (
            '<div id="miss-col-' + key + '">'
            '<div class="col-title"><span class="icon">❄️</span>'
            '<h3 style="color:#7f1d1d">五不出推薦 Top ' + str(TOP_N) + '</h3></div>'
            '<p class="col-desc">遺漏值 X 期時，號碼下期<strong>不出現（不中）</strong>的歷史勝率。<br>'
            '<span style="background:#fef9c3;color:#713f12;border-radius:.25rem;padding:.05rem .35rem;font-size:.65rem;font-weight:700">⛔ 建議避開</span>'
            ' 粉色球 = 目前遺漏高且勝率佳的號碼，不要選它們。　'
            '<span style="background:#eff6ff;color:#1e40af;border-radius:.25rem;padding:.05rem .35rem;font-size:.65rem;font-weight:700">👀 可觀察</span>'
            ' 勝率中等（50~65%）。　'
            '<span style="background:#f0fdf4;color:#166534;border-radius:.25rem;padding:.05rem .35rem;font-size:.65rem;font-weight:700">✓ 相對安全</span>'
            ' 勝率低或樣本少，可納入。切換視窗查看不同期數統計。</p>'
            '<div class="miss-subtabs">' + sub_tabs + '</div>'
            + panes +
            '</div>'
        )

    def _build_miss_pane(self, key: str, wkey: str, wd: Dict) -> str:
        top8      = wd["top8_highest_no_show"]
        all_probs = wd["all_number_probs"]
        mstats    = wd["miss_stats"]
        top8_mvs  = {x["miss_value"] for x in top8}
        win_label = MISS_WIN_LABELS[wkey]
        min_s     = WIN_MIN_SAMPLE.get(wkey, 10)

        miss_shades = [
            ("#7f1d1d", "#fef2f2"), ("#991b1b", "#fef2f2"), ("#b91c1c", "#fff1f2"),
            ("#dc2626", "#fff1f2"), ("#ef4444", "#fff"),     ("#f87171", "#fff"),
            ("#fca5a5", "#7f1d1d"), ("#fecaca", "#7f1d1d"),
        ]

        items = ""
        if not top8:
            items = '<div style="font-size:.75rem;color:#94a3b8;padding:.5rem 0">此視窗期數不足</div>'
        else:
            for i, item in enumerate(top8):
                bg, fg   = miss_shades[min(i, len(miss_shades) - 1)]
                matching = item.get("matching_numbers", [])
                mv_val   = item["miss_value"]
                rate     = item["no_show_rate"]
                nshow_c  = item["no_show_count"]
                total_c  = item["total_count"]
                balls = (
                    " ".join(self._ball(m["number"], "ball-sm") for m in matching)
                    if matching else '<span class="no-balls">目前無符合號碼</span>'
                )
                items += (
                    '<div class="rec-item">'
                    '<div class="rec-top">'
                    '<span class="rec-badge" style="background:' + bg + ';color:' + fg + '">'
                    + str(mv_val) + '期</span>'
                    '<div class="rec-info">'
                    '<div class="rec-title">遺漏值 ' + str(mv_val) + ' 期</div>'
                    '<div class="rec-sub">不出 ' + str(nshow_c) + ' / 觀測 ' + str(total_c) + '</div>'
                    '</div>'
                    '<span class="rec-pct" style="color:#b91c1c">' + str(rate) + '%</span>'
                    '</div>'
                    '<div class="balls-row">' + balls + '</div>'
                    '</div>'
                )

        miss_rows = ""
        for j, mv in enumerate(sorted(mstats.keys())):
            s = mstats[mv]
            if s["total_count"] < min_s:
                continue
            is_top = mv in top8_mvs
            tr_cls = "hl" if is_top else ("stripe" if j % 2 == 0 else "")
            star   = " ★" if is_top else ""
            miss_rows += (
                '<tr class="' + tr_cls + '"><td>' + str(mv) + star + '</td>'
                '<td>' + str(s["no_show_rate"]) + '%</td>'
                '<td>' + str(s["no_show_count"]) + '</td>'
                '<td>' + str(s["total_count"]) + '</td></tr>'
            )
        if not miss_rows:
            miss_rows = '<tr><td colspan="4" style="color:#94a3b8">樣本不足</td></tr>'

        curr_rows = ""
        for j, it in enumerate(sorted(all_probs, key=lambda x: x["number"])):
            is_top = it["current_miss"] in top8_mvs
            tr_cls = "hl" if is_top else ("stripe" if j % 2 == 0 else "")
            star   = " ★" if is_top else ""
            ball_h = self._ball(it["number"], "ball-sm")
            curr_rows += (
                '<tr class="' + tr_cls + '"><td>' + ball_h + star + '</td>'
                '<td>' + str(it["current_miss"]) + '</td>'
                '<td>' + str(it["no_show_rate"]) + '%</td>'
                '<td>' + str(it["no_show_count"]) + '</td>'
                '<td>' + str(it["total_count"]) + '</td></tr>'
            )

        return (
            items
            + '<details><summary style="color:#ef4444">'
            '<span class="caret">▶</span>展開遺漏值完整勝率表（' + win_label + '，★ Top ' + str(TOP_N) + '）'
            '</summary>'
            '<div class="table-wrap"><table>'
            '<thead><tr><th>遺漏值</th><th>不出勝率</th><th>不出次數</th><th>總樣本</th></tr></thead>'
            '<tbody>' + miss_rows + '</tbody>'
            '</table></div></details>'
            '<details><summary style="color:#ef4444">'
            '<span class="caret">▶</span>展開全部號碼當前遺漏狀態（' + win_label + ' 勝率，★ 推薦排除）'
            '</summary>'
            '<div class="table-wrap"><table>'
            '<thead><tr><th>號碼</th><th>當前遺漏</th><th>不出勝率</th><th>不出次數</th><th>總樣本</th></tr></thead>'
            '<tbody>' + curr_rows + '</tbody>'
            '</table></div></details>'
        )

    # ── Consecutive Draw Analysis Panel (v8.0) ──────────────────

    def _build_consec_html(self, key: str, consec_result: Dict, theme: Dict) -> str:
        p = theme["primary"]
        cards = ""
        for num in range(1, 40):
            data     = consec_result.get(num, {})
            stats    = data.get("stats", {})
            max_s    = data.get("max_streak", 0)
            cur_s    = data.get("cur_streak", 0)

            # Only show k≥2 in the detail
            rows = ""
            for k in sorted(stats.keys()):
                if k < 2:
                    continue
                d = stats[k]
                if d["samples"] < 3:
                    continue
                prob_c = "#dc2626" if d["prob"] >= 30 else ("#d97706" if d["prob"] >= 20 else "#16a34a")
                bar_w  = min(int(d["prob"]), 100)
                rows += (
                    '<div class="consec-row">'
                    '<span class="consec-k-label">連' + str(k) + '→連' + str(k+1) + '：</span>'
                    '<span class="consec-prob" style="color:' + prob_c + '">' + str(d["prob"]) + '%</span>'
                    '<span class="consec-sample">(' + str(d["hit"]) + '/' + str(d["samples"]) + ')</span>'
                    '<div class="consec-bar-bg">'
                    '<div class="consec-bar-fill" style="width:' + str(bar_w) + '%;background:' + prob_c + '"></div>'
                    '</div>'
                    '</div>'
                )

            # Card styling based on current streak
            if cur_s >= 4:
                card_extra = ' streak-active'
                cur_badge  = '<span style="font-size:.55rem;font-weight:800;color:#dc2626;background:#fee2e2;border-radius:.2rem;padding:1px 4px;margin-left:.2rem">連' + str(cur_s) + '🔥</span>'
            elif cur_s == 3:
                card_extra = ' streak-active'
                cur_badge  = '<span style="font-size:.55rem;font-weight:800;color:#d97706;background:#fef3c7;border-radius:.2rem;padding:1px 4px;margin-left:.2rem">連3</span>'
            elif cur_s == 2:
                card_extra = ' has-streak'
                cur_badge  = '<span style="font-size:.55rem;font-weight:700;color:#2563eb;background:#dbeafe;border-radius:.2rem;padding:1px 4px;margin-left:.2rem">連2</span>'
            else:
                card_extra = ' has-streak' if max_s >= 3 else ''
                cur_badge  = ''

            detail_html = ''
            if rows:
                detail_html = (
                    '<div id="consec-d-' + key + '-' + str(num) + '" class="consec-detail">'
                    + rows + '</div>'
                )

            cards += (
                '<div class="consec-num-card' + card_extra + '" '
                'onclick="toggleConsec(\'' + key + '\',' + str(num) + ')">'
                '<div class="consec-header">'
                + self._ball(num, "ball-sm") +
                cur_badge +
                '<span class="consec-max-badge">最高連' + str(max_s) + '</span>'
                '</div>'
                + detail_html +
                '</div>'
            )

        return (
            '<div id="consec-' + key + '" class="consec-panel">'
            '<details>'
            '<summary style="display:flex;align-items:center;gap:.4rem;cursor:pointer;'
            'list-style:none;user-select:none;padding:.1rem .2rem">'
            '<span style="font-size:1rem">🔄</span>'
            '<h3 style="font-size:.88rem;font-weight:900;color:' + p + '">'
            '01~39 歷史連開深度條件機率回測</h3>'
            '<span class="caret" style="font-size:.6rem;transition:transform .2s;margin-left:.3rem">▶</span>'
            '<span style="font-size:.62rem;color:#94a3b8;margin-left:auto">'
            '預設收起｜點號碼卡查看連k→連k+1歷史機率</span>'
            '</summary>'
            '<div class="consec-grid">' + cards + '</div>'
            '</details>'
            '</div>'
        )

    # _build_update_panel is superseded by _build_float_panel in v6.0
    # kept as no-op to avoid breaking any external callers
    def _build_update_panel(self, server_mode: bool) -> str:
        return ""

    # ── JavaScript ───────────────────────────────────────────

    def _build_js(self, server_mode: bool) -> str:
        if server_mode:
            mode_decl = "const IS_SERVER_MODE=true;"
        else:
            mode_decl = (
                "const IS_SERVER_MODE=(function(){"
                "var h=(window.location.hostname||'').toLowerCase();"
                "var p=window.location.protocol;"
                "return p!=='file:'||h==='localhost'||h==='127.0.0.1';"
                "})();"
            )

        return mode_decl + r"""

/* ── Tab switching ── */
function switchTab(key){
  document.querySelectorAll('.tab-btn').forEach(function(b){b.classList.remove('active');});
  document.querySelectorAll('.panel').forEach(function(p){p.classList.remove('active');});
  var btn=document.getElementById('tab-'+key);
  var pnl=document.getElementById('panel-'+key);
  if(btn)btn.classList.add('active');
  if(pnl)pnl.classList.add('active');
  try{localStorage.setItem('lotteryTabV85',key);}catch(e){}
  _currentPickerKey=key;
  // Switch sidebar pane
  document.querySelectorAll('.sidebar-pane').forEach(function(p){p.style.display='none';});
  var sp=document.getElementById('sp-'+key);
  if(sp)sp.style.display='block';
  // Update sidebar title
  var titles={'taiwan_539':'539','michigan_fantasy5':'密西根','california_fantasy5':'加州','newyork_take5':'紐約'};
  var st=document.getElementById('sidebar-title');
  if(st)st.textContent='互動選號盤 — '+(titles[key]||key);
  _applyPickerMarks(key);
  _applyPickerModeColors(key);
  _updateSmartRec(key);
  _renderMissDist(key);
  renderBetLog(key);
  renderDailyDecisionSummary(key);
  _restoreLock(key);
}
function setMissWin(key,win){
  var col=document.getElementById('miss-col-'+key);
  if(!col)return;
  col.querySelectorAll('.miss-tab').forEach(function(b){b.classList.remove('active');});
  col.querySelectorAll('.miss-pane').forEach(function(p){p.classList.remove('active');});
  var btn=col.querySelector('[data-win="'+win+'"]');
  var pane=document.getElementById('miss-pane-'+key+'-'+win);
  if(btn)btn.classList.add('active');
  if(pane)pane.classList.add('active');
}

/* ── Floating panel drag ── */
function toggleFloatPanel(){
  var fp=document.getElementById('float-panel');
  if(fp)fp.classList.toggle('hidden');
}
(function(){
  var fp=document.getElementById('float-panel');
  if(!fp)return;
  var handle=document.getElementById('fp-drag-handle');
  var closeBtn=document.getElementById('fp-close');
  if(closeBtn)closeBtn.addEventListener('click',function(e){
    fp.classList.add('hidden');e.stopPropagation();
  });
  if(!handle)return;
  var dragging=false,startX,startY,initL,initT;
  handle.addEventListener('mousedown',function(e){
    if(e.target===closeBtn)return;
    dragging=true;
    var r=fp.getBoundingClientRect();
    startX=e.clientX;startY=e.clientY;
    initL=r.left;initT=r.top;
    fp.style.transition='none';
    document.body.style.userSelect='none';
    e.preventDefault();
  });
  document.addEventListener('mousemove',function(e){
    if(!dragging)return;
    var nx=initL+e.clientX-startX;
    var ny=initT+e.clientY-startY;
    nx=Math.max(0,Math.min(nx,window.innerWidth-fp.offsetWidth));
    ny=Math.max(0,Math.min(ny,window.innerHeight-fp.offsetHeight));
    fp.style.left=nx+'px';fp.style.top=ny+'px';
    fp.style.right='auto';fp.style.bottom='auto';
  });
  document.addEventListener('mouseup',function(){
    dragging=false;
    fp.style.transition='';
    document.body.style.userSelect='';
  });
  // touch support
  handle.addEventListener('touchstart',function(e){
    if(e.target===closeBtn)return;
    var t=e.touches[0];
    var r=fp.getBoundingClientRect();
    dragging=true;startX=t.clientX;startY=t.clientY;initL=r.left;initT=r.top;
    fp.style.transition='none';
  },{passive:true});
  document.addEventListener('touchmove',function(e){
    if(!dragging)return;
    var t=e.touches[0];
    var nx=initL+t.clientX-startX;
    var ny=initT+t.clientY-startY;
    nx=Math.max(0,Math.min(nx,window.innerWidth-fp.offsetWidth));
    ny=Math.max(0,Math.min(ny,window.innerHeight-fp.offsetHeight));
    fp.style.left=nx+'px';fp.style.top=ny+'px';
    fp.style.right='auto';fp.style.bottom='auto';
    e.preventDefault();
  },{passive:false});
  document.addEventListener('touchend',function(){dragging=false;fp.style.transition='';});
})();

/* ── Server API ── */
async function runScrape(){
  if(!IS_SERVER_MODE){alert('需要伺服器模式：\npython lottery_analyzer.py --serve');return;}
  var btn=document.getElementById('btn-scrape');
  var log=document.getElementById('scrape-log');
  btn.disabled=true;btn.textContent='同步中...';
  log.innerHTML='<span style="color:#94a3b8">正在偵測並補齊缺漏期數...</span>';
  try{
    var r=await fetch('/api/scrape',{method:'POST'});
    var d=await r.json();
    log.innerHTML=(d.details||[]).map(function(m){
      return '<div style="color:'+(m.ok?'#166534':'#b91c1c')+'">'+(m.ok?'✓':'✗')+' '+m.msg+'</div>';
    }).join('');
    if(d.rebuilt){
      log.innerHTML+='<div style="color:#2563eb;font-weight:700;margin-top:4px">✓ 已更新，3秒後重新整理...</div>';
      setTimeout(function(){location.reload();},3000);
    }
  }catch(e){log.textContent='錯誤：'+e.message;}
  finally{btn.disabled=false;btn.textContent='一鍵智能補齊';}
}
async function submitManual(){
  if(!IS_SERVER_MODE){alert('需要伺服器模式：\npython lottery_analyzer.py --serve');return;}
  var log=document.getElementById('manual-log');
  var key=document.getElementById('m-lottery').value;
  var date=document.getElementById('m-date').value;
  var nums=[1,2,3,4,5].map(function(i){return +document.getElementById('m-n'+i).value;});
  if(!date){log.textContent='請選擇日期';return;}
  if(nums.some(function(n){return isNaN(n)||n<1||n>39;})){log.textContent='號碼需在 1~39 之間';return;}
  if(new Set(nums).size!==5){log.textContent='5 個號碼不得重複';return;}
  log.textContent='儲存中...';
  try{
    var r=await fetch('/api/manual',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({lottery:key,date:date,numbers:nums})
    });
    var d=await r.json();
    log.innerHTML='<span style="color:'+(d.success?'#166534':'#b91c1c')+'">'+d.message+'</span>';
    if(d.success){
      log.innerHTML+=' <span style="color:#2563eb;font-weight:700">3秒後重新整理...</span>';
      setTimeout(function(){location.reload();},3000);
    }
  }catch(e){log.textContent='錯誤：'+e.message;}
}
/* ── v9.3 全模組時空動態回測 ── */
/* Backtest data store: window._BT[key] = last received API response */
window._BT=window._BT||{};

/* ── _btStore: 把 API 回傳的所有數據寫入 JS 全域變數 ── */
function _btStore(key,d){
  window._BT[key]=d;
  window._MISS_DATA=window._MISS_DATA||{};
  window._RECENT_DATA=window._RECENT_DATA||{};
  window._DRAW_DATA=window._DRAW_DATA||{};
  if(d.miss_data)       window._MISS_DATA[key]=d.miss_data;
  if(d.recent_data)     window._RECENT_DATA[key]=d.recent_data;
  if(d.draw_data)       window._DRAW_DATA[key]=d.draw_data;
  if(d.period_data){    window._PERIOD_DATA=window._PERIOD_DATA||{};    window._PERIOD_DATA[key]=d.period_data; }
  if(d.num_hist_data){  window._NUM_HIST_DATA=window._NUM_HIST_DATA||{};  window._NUM_HIST_DATA[key]=d.num_hist_data; }
  if(d.oe_color_data){  window._OE_COLOR_DATA=window._OE_COLOR_DATA||{};  window._OE_COLOR_DATA[key]=d.oe_color_data; }
  if(d.strat_data){     window._STRAT_DATA=window._STRAT_DATA||{};     window._STRAT_DATA[key]=d.strat_data; }
  if(d.heat_prob_data){ window._HEAT_PROB_DATA=window._HEAT_PROB_DATA||{}; window._HEAT_PROB_DATA[key]=d.heat_prob_data; }
  if(d.multi_bt_data){  window._MULTI_BT_DATA=window._MULTI_BT_DATA||{};  window._MULTI_BT_DATA[key]=d.multi_bt_data; }
  if(d.rev_oe_data){    window._REV_OE_DATA=window._REV_OE_DATA||{};      window._REV_OE_DATA[key]=d.rev_oe_data; }
}

/* ── updateSelectionBoard: 右側選號盤（側邊欄）完整同步 ── */
function updateSelectionBoard(key){
  var bt=window._BT&&window._BT[key];
  if(!bt)return;
  var missMap=bt.miss_data||{};
  var grid=document.getElementById('pk-grid-'+key);
  if(grid){
    grid.querySelectorAll('.pk-cell').forEach(function(cell){
      var n=parseInt(cell.dataset.num);
      var miss=(missMap[''+n]!==undefined)?missMap[''+n]:0;
      var el=cell.querySelector('.pk-miss');
      if(el)el.textContent='遺漏'+miss;
    });
  }
  _applyPickerMarks(key);        // 本期金框 + 鄰號紫框（依歷史 latest draw）
  _applyPickerModeColors(key);   // 熱力圖 / 危險度色彩
  _renderMissDist(key);          // 遺漏分佈直方圖
  renderBetLog(key);             // 投注紀錄命中對比
  renderOEColorGuide(key);       // 今日配比推薦（v10.2）
}

/* ── updateTailOmissions: （panel_inner_html 已包含，此為獨立觸發入口）── */
function updateTailOmissions(key){
  /* Tail panel is embedded inside panel_inner_html; calling _applyBacktestResult covers it.
     This stub exists for future manual invocation hooks. */
}

/* ── updateRecommendations: 智能推薦依歷史危險度重算 ── */
function updateRecommendations(key){
  _updateSmartRec(key);
}

/* ── _applyBacktestResult: 時光機全面重繪調度鏈（v9.3 最終架構）── */
function _applyBacktestResult(key,d){
  /* Step 1 — write all data globals BEFORE any DOM work */
  _btStore(key,d);

  /* Step 2 — replace entire panel-{key} innerHTML atomically.
     panel_inner_html = legend + tm-bar (with hist badge pre-set) + content-wrap
     (data_script excluded: <script> tags injected via innerHTML do NOT execute;
      JS globals are already restored in Step 1 via _btStore.)                   */
  var panel=document.getElementById('panel-'+key);
  if(panel&&d.panel_inner_html){
    panel.innerHTML=d.panel_inner_html;
  }

  /* Step 3 — update sidebar picker (separate from main panel DOM) */
  updateSelectionBoard(key);
  /* Step 3b — restore locked selection if any (v10.1) */
  _restoreLock(key);

  /* Step 4 — update smart recommendations */
  updateRecommendations(key);

  /* Step 5 — refresh selection risk summary (data globals updated in Step 1) */
  renderSelectionRiskSummary(key);
  /* Step 6 — refresh daily decision summary */
  renderDailyDecisionSummary(key);
}

/* ── runBacktest: 執行歷史回測 ── */
async function runBacktest(key){
  if(!IS_SERVER_MODE){alert('需要伺服器模式：\npython lottery_analyzer.py --serve');return;}
  var dateEl=document.getElementById('tm-date-'+key);
  if(!dateEl||!dateEl.value){alert('請選擇歷史日期');return;}
  var dateVal=dateEl.value;
  /* Show loading state on the badge (element is in current DOM before replacement) */
  var modeEl=document.getElementById('tm-mode-'+key);
  if(modeEl){modeEl.textContent='計算中...';modeEl.className='tm-mode-badge tm-mode-loading';}
  try{
    var r=await fetch('/api/backtest',{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({lottery:key,date:dateVal})
    });
    var d=await r.json();
    if(d.success){
      /* panel.innerHTML is replaced; new tm-bar already contains correct badge/date.
         No extra badge-text update needed — it is baked into panel_inner_html.     */
      _applyBacktestResult(key,d);
    }else{
      /* panel was NOT replaced; safe to update the existing badge element */
      if(modeEl){modeEl.textContent='錯誤：'+d.message;modeEl.className='tm-mode-badge';
        modeEl.style.background='#fee2e2';modeEl.style.color='#b91c1c';}
    }
  }catch(e){
    if(modeEl){modeEl.textContent='連線錯誤：'+e.message;modeEl.className='tm-mode-badge';}
  }
}

/* ── resetPanel: 回最新狀態 ── */
function resetPanel(key){
  if(!IS_SERVER_MODE)return;
  /* Show loading on badge before fetch */
  var modeEl=document.getElementById('tm-mode-'+key);
  if(modeEl){modeEl.textContent='載入中...';modeEl.className='tm-mode-badge tm-mode-loading';}
  fetch('/api/backtest',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({lottery:key,date:'latest'})
  }).then(function(r){return r.json();}).then(function(d){
    if(d.success){
      /* panel.innerHTML is replaced; new tm-bar shows "最新狀態" and latest date. */
      _applyBacktestResult(key,d);
    }
  }).catch(function(){location.reload();});
}

/* ── Number picker ── */
var _currentPickerKey=null;
var _pickerSel={};
var _pickerStrategySource={};
var _betLogFilter={};
var _betLogSyncTimer={};
var _betLogSyncHash={};
var _pickerCompare={};
var _pickerLock={};
window._MISS_DATA=window._MISS_DATA||{};
window._RECENT_DATA=window._RECENT_DATA||{};

function _clsBall(n){return n%3===1?'b-red':n%3===2?'b-blue':'b-green';}

/* ── Picker latest-draw & neighbor marks (v9.0) ── */
function _applyPickerMarks(key){
  var grid=document.getElementById('pk-grid-'+key);
  if(!grid)return;
  var recentDraws=(window._RECENT_DATA&&window._RECENT_DATA[key])||[];
  var latestNums=recentDraws.length>0?(recentDraws[0].numbers||[]):[];

  // Build neighbor set: ±1 of each drawn number, clamped 1-39, excluding drawn numbers
  var neighborSet={};
  latestNums.forEach(function(n){
    if(n>1) neighborSet[n-1]=true;
    if(n<39) neighborSet[n+1]=true;
  });
  latestNums.forEach(function(n){delete neighborSet[n];});

  grid.querySelectorAll('.pk-cell').forEach(function(cell){
    var n=parseInt(cell.dataset.num);
    // Clear old marks
    cell.classList.remove('pk-latest','pk-neighbor');
    cell.querySelectorAll('.pk-badge-cur,.pk-badge-nb').forEach(function(b){b.remove();});
    if(latestNums.indexOf(n)!==-1){
      cell.classList.add('pk-latest');
      var b=document.createElement('span');b.className='pk-badge-cur';b.textContent='本期';
      cell.appendChild(b);
    }else if(neighborSet[n]){
      cell.classList.add('pk-neighbor');
      var b=document.createElement('span');b.className='pk-badge-nb';b.textContent='鄰';
      cell.appendChild(b);
    }
  });
  // Score trend labels (v10.1): ↑ rising / ↓ falling
  var nhd_t=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  grid.querySelectorAll('.pk-cell').forEach(function(cell){
    var n=parseInt(cell.dataset.num);
    var nd=nhd_t[n]||nhd_t[''+n]||{};
    var trend=nd.score_trend||'stable';
    var missEl=cell.querySelector('.pk-miss');
    if(missEl&&trend!=='stable'){
      var base=missEl.textContent.replace(/\s*[↑↓]$/,'');
      if(trend==='up'){
        missEl.innerHTML=base+'<span style="color:#d97706;font-size:.5rem"> ↑</span>';
      }else if(trend==='down'){
        missEl.innerHTML=base+'<span style="color:#0d9488;font-size:.5rem"> ↓</span>';
      }
    }
  });
}

/* ── Live picker stats (v9.0) ── */
function _calcPickerStats(sel){
  var odd=0,even=0,red=0,blue=0,green=0;
  sel.forEach(function(n){
    if(n%2===1)odd++;else even++;
    if(n%3===1)red++;else if(n%3===2)blue++;else green++;
  });
  return{odd:odd,even:even,red:red,blue:blue,green:green};
}

function togglePickerNum(key,n){
  var sel=_pickerSel[key]||(_pickerSel[key]=[]);
  var idx=sel.indexOf(n);
  if(idx!==-1){sel.splice(idx,1);}
  else{if(sel.length>=5)return;sel.push(n);}
  _pickerStrategySource[key]='';
  _refreshPickerUI(key);
  renderSelectionRiskSummary(key);
}
function clearPickerSel(key){
  _pickerSel[key]=[];
  _refreshPickerUI(key);
  renderSelectionRiskSummary(key);
}
function _refreshPickerUI(key){
  var sel=_pickerSel[key]||[];
  var grid=document.getElementById('pk-grid-'+key);
  if(grid){
    grid.querySelectorAll('.pk-cell').forEach(function(cell){
      var n=parseInt(cell.dataset.num);
      cell.classList.toggle('selected',sel.indexOf(n)!==-1);
    });
  }
  var row=document.getElementById('pk-selected-'+key);
  if(row){
    if(sel.length===0){
      row.innerHTML='<span style="color:#94a3b8;font-size:.7rem">點擊號碼選號（最多 5 個）</span>';
    }else{
      row.innerHTML=sel.slice().sort(function(a,b){return a-b;}).map(function(n){
        var s=n<10?'0'+n:''+n;
        return '<span class="ball-sm '+_clsBall(n)+'">'+s+'</span>';
      }).join('');
    }
  }
  // Update live stats bar
  var liveBar=document.getElementById('pk-live-'+key);
  if(liveBar){
    if(sel.length===0){
      liveBar.innerHTML='<span style="color:#94a3b8;font-size:.65rem">點選號碼查看即時單雙與色球統計</span>';
      liveBar.style.background='#f0f9ff';
    }else{
      var st=_calcPickerStats(sel);
      liveBar.style.background='#fffbeb';
      liveBar.innerHTML=
        '<span class="pk-live-chip" style="background:#fef9c3;color:#92400e;border:1px solid #fcd34d">'
        +'已選 '+sel.length+' / 5</span>'
        +'<span class="pk-live-chip" style="background:#f1f5f9;color:#1e293b;border:1px solid #cbd5e1">'
        +st.odd+'單 '+st.even+'雙</span>'
        +'<span class="pk-live-chip" style="background:#fee2e2;color:#991b1b;border:1px solid #fca5a5">'
        +'紅 '+st.red+'</span>'
        +'<span class="pk-live-chip" style="background:#dbeafe;color:#1e3a8a;border:1px solid #93c5fd">'
        +'藍 '+st.blue+'</span>'
        +'<span class="pk-live-chip" style="background:#dcfce7;color:#166534;border:1px solid #86efac">'
        +'綠 '+st.green+'</span>';
    }
  }
}
function _addDaysISO(dateStr,days){
  if(!dateStr)return '';
  var parts=String(dateStr).slice(0,10).split('-').map(function(x){return parseInt(x,10);});
  if(parts.length!==3||parts.some(function(x){return isNaN(x);})){return '';}
  var d=new Date(Date.UTC(parts[0],parts[1]-1,parts[2]+days));
  return d.toISOString().slice(0,10);
}
function _pickerBaseDate(key){
  var recent=(window._RECENT_DATA&&window._RECENT_DATA[key])||[];
  if(recent.length&&recent[0].date)return String(recent[0].date).slice(0,10);
  var dateEl=document.getElementById('tm-date-'+key);
  return dateEl&&dateEl.value?dateEl.value:'';
}
function _pickerTargetDate(key){
  var base=_pickerBaseDate(key);
  if(base){
    var draws=_drawDataFor(key).filter(function(d){
      return String(d.date).slice(0,10)>base;
    }).sort(function(a,b){
      return String(a.date).localeCompare(String(b.date));
    });
    if(draws.length)return String(draws[0].date).slice(0,10);
  }
  return base?_addDaysISO(base,1):new Date().toISOString().slice(0,10);
}
function _entryTargetDate(entry){
  if(entry.targetDate)return String(entry.targetDate).slice(0,10);
  if(entry.date)return String(entry.date).slice(0,10);
  return '';
}
function _entryBaseDate(entry){
  if(entry.baseDate)return String(entry.baseDate).slice(0,10);
  return '';
}
function _drawDataFor(key){
  var draws=(window._DRAW_DATA&&window._DRAW_DATA[key])||[];
  if(!draws.length)draws=(window._RECENT_DATA&&window._RECENT_DATA[key])||[];
  return (draws||[]).filter(function(d){
    return d&&d.date&&Array.isArray(d.numbers);
  });
}
function _findDrawForEntry(key,entry,drawList){
  var draws=(drawList||_drawDataFor(key)).slice();
  var target=_entryTargetDate(entry);
  var base=_entryBaseDate(entry);
  var byDate={};
  draws.forEach(function(d){byDate[String(d.date).slice(0,10)]=d;});
  if(target&&byDate[target]){
    return {draw:byDate[target],targetDate:target,exact:true,adjusted:false};
  }
  if(base){
    var future=draws.filter(function(d){
      return String(d.date).slice(0,10)>base;
    }).sort(function(a,b){
      return String(a.date).localeCompare(String(b.date));
    });
    if(future.length){
      var nextDate=String(future[0].date).slice(0,10);
      return {draw:future[0],targetDate:nextDate,exact:false,adjusted:target!==nextDate};
    }
  }
  return {draw:null,targetDate:target||'',exact:false,adjusted:false};
}
function _settleBetLogTargets(key,log,drawList){
  var changed=false;
  log.forEach(function(entry){
    var found=_findDrawForEntry(key,entry,drawList);
    if(found.draw&&found.adjusted){
      entry.targetDate=found.targetDate;
      entry.date=found.targetDate;
      changed=true;
      if(entry.snapshot){
        entry.snapshot.targetDate=found.targetDate;
      }
    }
  });
  if(changed){
    try{localStorage.setItem('betLog_'+key,JSON.stringify(log));}catch(e){}
    _scheduleBetLogAutoSync(key);
  }
  return changed;
}
function _setBetLogSyncStatus(key,msg,kind){
  var logEl=document.getElementById('bet-log-'+key);
  var el=document.getElementById('bet-sync-status-'+key);
  if(!el&&logEl){
    var host=logEl.closest('.bet-log-section')||logEl.parentNode;
    if(host){
      el=document.createElement('div');
      el.id='bet-sync-status-'+key;
      host.insertBefore(el,host.firstChild);
    }
  }
  if(!el)return;
  var bg=kind==='ok'?'#ecfdf5':(kind==='err'?'#fef2f2':'#eff6ff');
  var fg=kind==='ok'?'#047857':(kind==='err'?'#b91c1c':'#1d4ed8');
  var bd=kind==='ok'?'#a7f3d0':(kind==='err'?'#fecaca':'#bfdbfe');
  el.style.cssText='font-size:.64rem;margin:.22rem 0;padding:.22rem .42rem;border-radius:.38rem;'
    +'background:'+bg+';color:'+fg+';border:1px solid '+bd+';font-weight:700';
  el.textContent='CSV自動同步：'+msg;
}
function _buildBetLogSyncEntries(key,log,drawList){
  var draws=drawList||_drawDataFor(key);
  return (log||[]).map(function(entry){
    var found=_findDrawForEntry(key,entry,draws);
    var matchDraw=found.draw;
    var hitCnt=null;
    var status='pending';
    if(matchDraw){
      hitCnt=(entry.nums||[]).filter(function(n){return matchDraw.numbers.indexOf(n)!==-1;}).length;
      status=hitCnt===0?'win':'loss';
    }
    return {
      nums:(entry.nums||[]).slice(),
      note:entry.note||'',
      autoNote:entry.autoNote||'',
      baseDate:_entryBaseDate(entry),
      targetDate:found.targetDate||_entryTargetDate(entry),
      createdAt:entry.createdAt||'',
      strategySource:entry.strategySource||'',
      odd:entry.oe?entry.oe.odd:'',
      even:entry.oe?entry.oe.even:'',
      red:entry.col?entry.col.red:'',
      blue:entry.col?entry.col.blue:'',
      green:entry.col?entry.col.green:'',
      status:status,
      hitCount:hitCnt,
      drawNumbers:matchDraw?(matchDraw.numbers||[]).join(' '):'',
      raw:entry
    };
  });
}
function _scheduleBetLogAutoSync(key){
  if(!IS_SERVER_MODE||!window.fetch)return;
  clearTimeout(_betLogSyncTimer[key]);
  _setBetLogSyncStatus(key,'等待同步','pending');
  _betLogSyncTimer[key]=setTimeout(function(){_syncBetLogNow(key);},450);
}
function _syncBetLogNow(key){
  if(!IS_SERVER_MODE||!window.fetch)return;
  var sk='betLog_'+key;
  var log=[];
  try{log=JSON.parse(localStorage.getItem(sk)||'[]');}catch(e){log=[];}
  var entries=_buildBetLogSyncEntries(key,log,_drawDataFor(key));
  var payload=JSON.stringify({lottery:key,entries:entries});
  if(_betLogSyncHash[key]===payload){
    _setBetLogSyncStatus(key,'已是最新（'+entries.length+'筆）','ok');
    return;
  }
  _setBetLogSyncStatus(key,'同步中...','pending');
  fetch('/api/betlog/sync',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:payload
  }).then(function(r){return r.json();}).then(function(d){
    if(d&&d.success){
      _betLogSyncHash[key]=payload;
      _setBetLogSyncStatus(key,'已同步 '+d.count+' 筆','ok');
    }else{
      _setBetLogSyncStatus(key,(d&&d.message)||'同步失敗','err');
    }
  }).catch(function(){
    _setBetLogSyncStatus(key,'同步失敗，稍後會再試','err');
  });
}
function savePickerEntry(key){
  var sel=(_pickerSel[key]||[]).slice().sort(function(a,b){return a-b;});
  if(sel.length===0){alert('請先選擇號碼');return;}
  var noteEl=document.getElementById('pk-note-'+key);
  var note=noteEl?noteEl.value.trim():'';
  var missData=window._MISS_DATA[key]||{};
  var periodData=window._PERIOD_DATA[key]||[];
  var autoNote=sel.map(function(n){
    var miss=(missData[n]!==undefined?missData[n]:(missData[''+n]||0));
    var coldParts=[];
    periodData.forEach(function(p){
      if(p.ref_numbers&&p.ref_numbers.indexOf(n)!==-1){
        coldParts.push('屬 t'+p.t+' 冷門期（'+p.ref_date+'，重複率 '+p.overlap_prob+'%）');
      }
    });
    var s=(n<10?'0'+n:''+n)+'號：當前遺漏 '+miss+' 期';
    if(coldParts.length>0)s+='；'+coldParts.join('；');
    return s;
  }).join('\n');
  var st=_calcPickerStats(sel);
  var baseDate=_pickerBaseDate(key);
  var targetDate=_pickerTargetDate(key);
  // Build strategy snapshot at save time
  var nhd=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  var hpd=window._HEAT_PROB_DATA&&window._HEAT_PROB_DATA[key]||{};
  var pd=window._PERIOD_DATA&&window._PERIOD_DATA[key]||[];
  var snapNums={};
  sel.forEach(function(n){
    var nd=nhd[n]||nhd[''+n]||{};
    var hpn=hpd[n]||hpd[''+n]||{};
    var coldPeriods=[];
    pd.forEach(function(p){if(p.ref_numbers&&p.ref_numbers.indexOf(n)!==-1)coldPeriods.push(p.t);});
    snapNums[n]={
      miss:nd.current_miss!==undefined?nd.current_miss:(missData[n]!==undefined?missData[n]:(missData[''+n]||0)),
      recentFreq:nd.recent_freq||0,
      heatProb:hpn.next_hit_rate!==undefined?hpn.next_hit_rate:null,
      dangerPct:nd.danger_pct||0,
      isLatestDraw:nd.in_latest_draw||false,
      isNeighbor:nd.is_neighbor||false,
      coldPeriods:coldPeriods,
      color:(n%3===1?'red':n%3===2?'blue':'green'),
      oddEven:(n%2===1?'odd':'even')
    };
  });
  var entry={nums:sel,note:note,autoNote:autoNote,
    oe:{odd:st.odd,even:st.even},
    col:{red:st.red,blue:st.blue,green:st.green},
    date:targetDate,
    targetDate:targetDate,
    baseDate:baseDate,
    createdAt:new Date().toISOString(),
    strategySource:_pickerStrategySource[key]||'',
    snapshot:{baseDate:baseDate,targetDate:targetDate,numbers:snapNums,
      groupStats:{odd:st.odd,even:st.even,red:st.red,blue:st.blue,green:st.green}}};
  var sk='betLog_'+key;
  var log=[];
  try{log=JSON.parse(localStorage.getItem(sk)||'[]');}catch(e){}
  log.unshift(entry);if(log.length>200)log=log.slice(0,200);
  localStorage.setItem(sk,JSON.stringify(log));
  _scheduleBetLogAutoSync(key);
  _pickerSel[key]=[];
  if(noteEl)noteEl.value='';
  _refreshPickerUI(key);
  renderBetLog(key);
}
function deletePickerEntry(key,idx){
  var sk='betLog_'+key;
  var log=[];
  try{log=JSON.parse(localStorage.getItem(sk)||'[]');}catch(e){}
  log.splice(idx,1);
  localStorage.setItem(sk,JSON.stringify(log));
  _scheduleBetLogAutoSync(key);
  renderBetLog(key);
}
function renderBetLog(key){
  var container=document.getElementById('bet-log-'+key);
  if(!container)return;
  var sk='betLog_'+key;
  var log=[];
  try{log=JSON.parse(localStorage.getItem(sk)||'[]');}catch(e){}
  if(log.length===0){
    container.innerHTML='<div style="font-size:.7rem;color:#94a3b8;padding:.4rem 0">尚無選號紀錄</div>';
    var statsEl0=document.getElementById('bet-stats-'+key);
    if(statsEl0)statsEl0.style.display='none';
    return;
  }
  var drawData=_drawDataFor(key);
  _settleBetLogTargets(key,log,drawData);
  renderPersonalWinRate(key);

  // ── Build helper: render one bet card ──
  function _renderBetCard(entry,idx){
    var nums=entry.nums;
    var baseTs=_entryBaseDate(entry);
    var found=_findDrawForEntry(key,entry,drawData);
    var ts=found.targetDate||_entryTargetDate(entry);
    var matchDraw=found.draw;
    var bestHit=-1;
    if(matchDraw){
      bestHit=nums.filter(function(n){return matchDraw.numbers.indexOf(n)!==-1;}).length;
    }
    var balls=nums.map(function(n){
      var s=n<10?'0'+n:''+n;
      return '<span class="ball-sm '+_clsBall(n)+'">'+s+'</span>';
    }).join('');
    var badgeHtml='';
    if(bestHit<0){
      badgeHtml='<span class="bet-hit-badge" style="background:#f1f5f9;color:#94a3b8">等待開獎</span>';
    }else if(bestHit===0){
      badgeHtml='<span class="bet-hit-badge" style="background:#dcfce7;color:#166534;font-weight:800">✓ 勝（0中）</span>';
    }else if(bestHit>=4){
      badgeHtml='<span class="bet-hit-badge" style="background:#fee2e2;color:#991b1b">敗 '+bestHit+'中 ★</span>';
    }else if(bestHit>=3){
      badgeHtml='<span class="bet-hit-badge" style="background:#fee2e2;color:#b91c1c">敗 '+bestHit+'中</span>';
    }else{
      badgeHtml='<span class="bet-hit-badge" style="background:#fff7ed;color:#c2410c">敗 '+bestHit+'中</span>';
    }
    var wrapCls=(bestHit===0)?'bet-entry-wrap bet-hit-wrap':'bet-entry-wrap';
    var oeTags='';
    if(entry.oe){
      oeTags+='<span class="anno-tag-oe" style="font-size:.63rem;padding:.1rem .38rem">'
        +entry.oe.odd+'單'+entry.oe.even+'雙</span>';
    }
    if(entry.col){
      oeTags+='<span class="anno-tag-col" style="font-size:.63rem;padding:.1rem .38rem">'
        +'紅'+entry.col.red+'藍'+entry.col.blue+'綠'+entry.col.green+'</span>';
    }
    if(entry.strategySource){
      oeTags+='<span style="font-size:.6rem;padding:.1rem .38rem;border-radius:.25rem;'
        +'background:#ede9fe;color:#6d28d9;border:1px solid #c4b5fd">策略：'
        +entry.strategySource+'</span>';
    }
    var autoNoteHtml='';
    if(entry.autoNote){
      var lines=entry.autoNote.split('\n').filter(function(l){return l.trim();});
      var listItems=lines.map(function(l){
        return '<li style="padding:.12rem 0;color:#334155;font-size:.7rem;'
          +'line-height:1.65;overflow-wrap:break-word;word-break:break-word">• '+l+'</li>';
      }).join('');
      autoNoteHtml='<div class="bet-autonote">'
        +'<details>'
        +'<summary style="color:#6366f1;font-size:.68rem;font-weight:600;cursor:pointer;list-style:none;'
        +'display:flex;align-items:center;gap:.25rem;padding:.18rem .28rem;border-radius:.3rem;'
        +'transition:background .12s;user-select:none" '
        +'onmouseover="this.style.background=\'#eff6ff\'" onmouseout="this.style.background=\'\'">'
        +'<span style="font-size:.5rem;display:inline-block;transition:transform .18s" class="caret">▶</span>'
        +'查看選號條件詳情（'+lines.length+' 項）</summary>'
        +'<ul style="list-style:none;padding:.3rem .5rem .15rem;margin:.2rem 0 0;'
        +'background:#f8fafc;border:1px solid #e2e8f0;border-radius:.4rem">'
        +listItems
        +'</ul>'
        +'</details>'
        +'</div>';
    }
    return '<div class="'+wrapCls+'">'
      +'<div class="bet-entry">'
      +'<div style="flex:1;min-width:0">'
      +'<div style="display:flex;flex-wrap:wrap;gap:.2rem;align-items:center">'+balls+'</div>'
      +(entry.note?'<div class="bet-note-txt" style="margin-top:.12rem">📝 '+entry.note+'</div>':'')
      +(baseTs?'<div class="bet-note-txt" style="font-style:normal;color:#64748b">基準 '+baseTs+' → 對應 '+ts+'</div>':'')
      +'</div>'
      +'<span class="bet-time">'+ts+'</span>'
      +badgeHtml
      +'<button class="bet-del-btn" onclick="deletePickerEntry(\''+key+'\','+idx+')">✕</button>'
      +'</div>'
      +(oeTags?'<div class="bet-entry-tags">'+oeTags+'</div>':'')
      +autoNoteHtml
      +'</div>';
  }

  // ── Split log into pending / settled using original indices ──
  var pendingItems=[],settledItems=[];
  for(var i=0;i<log.length;i++){
    var e=log[i];
    var found2=_findDrawForEntry(key,e,drawData);
    var md=found2.draw;
    var bh=-1;
    if(md){bh=e.nums.filter(function(n){return md.numbers.indexOf(n)!==-1;}).length;}
    if(bh<0){pendingItems.push({entry:e,idx:i});}
    else{settledItems.push({entry:e,idx:i,bestHit:bh});}
  }

  // ── Apply filter (v9.9) ──
  var filter=_betLogFilter[key]||'all';
  function _matchStrat(it,src){return it.entry.strategySource===src;}
  function _isManual(it){var s=it.entry.strategySource||'';return s===''||s==='手動';}
  var filtPending=pendingItems,filtSettled=settledItems;
  if(filter==='pending'){filtSettled=[];}
  else if(filter==='win'){filtPending=[];filtSettled=filtSettled.filter(function(it){return it.bestHit===0;});}
  else if(filter==='loss'){filtPending=[];filtSettled=filtSettled.filter(function(it){return it.bestHit>0;});}
  else if(filter==='manual'){filtPending=filtPending.filter(_isManual);filtSettled=filtSettled.filter(_isManual);}
  else if(filter==='conservative'){filtPending=filtPending.filter(function(it){return _matchStrat(it,'保守');});filtSettled=filtSettled.filter(function(it){return _matchStrat(it,'保守');});}
  else if(filter==='balanced'){filtPending=filtPending.filter(function(it){return _matchStrat(it,'均衡');});filtSettled=filtSettled.filter(function(it){return _matchStrat(it,'均衡');});}
  else if(filter==='cold'){filtPending=filtPending.filter(function(it){return _matchStrat(it,'冷門');});filtSettled=filtSettled.filter(function(it){return _matchStrat(it,'冷門');});}

  // Stats count only filtered settled
  var wins=0,losses=0;
  filtSettled.forEach(function(it){
    if(it.bestHit===0)wins++;else losses++;
  });

  // ── Section label helper ──
  function _sectionLabel(icon,label,count,color){
    return '<div style="font-size:.65rem;font-weight:800;color:'+color+';'
      +'margin:.28rem 0 .12rem;display:flex;align-items:center;gap:.22rem">'
      +icon+' '+label+'（'+count+' 筆）</div>';
  }
  var html='';

  // No results at all under filter
  if(filtPending.length===0&&filtSettled.length===0&&filter!=='all'){
    html='<div style="font-size:.68rem;color:#9ca3af;padding:.35rem .5rem;'
      +'background:#f9fafb;border-radius:.4rem;text-align:center;margin:.2rem 0">'
      +'目前沒有符合條件的紀錄</div>';
  }else{
    // Pending section
    html+=_sectionLabel('⏳','未結算',filtPending.length,'#6b7280');
    if(filtPending.length===0){
      html+='<div style="font-size:.68rem;color:#9ca3af;padding:.22rem .3rem;'
        +'background:#f9fafb;border-radius:.35rem;margin-bottom:.18rem">目前沒有待結算紀錄</div>';
    }else{
      filtPending.forEach(function(it){html+=_renderBetCard(it.entry,it.idx);});
    }
    // Settled section
    html+=_sectionLabel('✅','已結算',filtSettled.length,'#374151');
    if(filtSettled.length===0){
      html+='<div style="font-size:.68rem;color:#9ca3af;padding:.22rem .3rem;'
        +'background:#f9fafb;border-radius:.35rem">目前沒有已結算紀錄</div>';
    }else{
      filtSettled.forEach(function(it){html+=_renderBetCard(it.entry,it.idx);});
    }
  }

  container.innerHTML=html;
  // Stats bar (settled only)
  var statsEl=document.getElementById('bet-stats-'+key);
  if(statsEl){
    var total=wins+losses;
    var rate=total>0?(wins/total*100).toFixed(1):'0.0';
    statsEl.style.display='block';
    statsEl.innerHTML='五選不中：<span class="wins">'+wins+'勝</span> <span class="losses">'+losses+'敗</span>'
      +'　總勝率：<strong>'+rate+'%</strong>'
      +'<span style="color:#94a3b8;font-size:.62rem;margin-left:.3rem">（等待開獎不計）</span>';
  }
  renderWinRateTrend(key,log,drawData);
  renderFailureAnalysis(key,log,drawData);
  _scheduleBetLogAutoSync(key);
}

/* ── Sliding win rate trend (v9.7) ── */
function renderWinRateTrend(key,log,drawData){
  var el=document.getElementById('win-trend-'+key);
  if(!el)return;
  if(!log||log.length===0){el.innerHTML='';return;}

  // Build settled results (newest-first): 1=win, 0=loss, skip pending
  var settled=[];
  for(var i=0;i<log.length;i++){
    var entry=log[i];
    var found=_findDrawForEntry(key,entry,drawData);
    if(!found.draw)continue;
    var hits=entry.nums.filter(function(n){return found.draw.numbers.indexOf(n)!==-1;}).length;
    settled.push(hits===0?1:0);
  }

  if(settled.length===0){el.innerHTML='';return;}

  // Less than 3 settled → show minimal placeholder (v9.9)
  if(settled.length<3){
    el.innerHTML='<div style="font-size:.63rem;color:#9ca3af;padding:.18rem 0;font-style:italic">'
      +'累積 3 筆已結算紀錄後顯示滑動勝率趨勢</div>';
    return;
  }

  // Returns {wins,losses,total,rate} for chunk [from, from+count), or null if < 3
  function _chunk(from,count){
    var c=settled.slice(from,from+count);
    if(c.length<3)return null;
    var w=c.filter(function(x){return x===1;}).length;
    return{wins:w,losses:c.length-w,total:c.length,rate:w/c.length*100};
  }

  var windows=[10,20,50];
  var rows='';
  windows.forEach(function(n){
    var cur=_chunk(0,n);
    var prev=_chunk(n,n);
    var rowInner='';
    if(!cur){
      rowInner='<span style="font-size:.62rem;color:#94a3b8">樣本不足</span>';
    }else{
      var rStr=cur.rate.toFixed(1)+'%';
      var rC=cur.rate>=65?'#16a34a':cur.rate>=50?'#d97706':'#dc2626';
      var detailStr='（'+cur.wins+'勝'+cur.losses+'敗）';
      var trendPart='';
      if(!prev){
        trendPart='<span style="font-size:.59rem;color:#94a3b8;margin-left:.15rem">無前段比較</span>';
      }else{
        var diff=cur.rate-prev.rate;
        var arrow,label,tC;
        if(diff>5){arrow='↗';label='進步';tC='#16a34a';}
        else if(diff<-5){arrow='↘';label='退步';tC='#dc2626';}
        else{arrow='→';label='持平';tC='#64748b';}
        var diffStr=(diff>=0?'+':'')+diff.toFixed(1)+'%';
        trendPart='<span style="font-size:.63rem;font-weight:800;color:'+tC+';margin-left:.2rem">'
          +arrow+' '+label+(label!=='持平'?' '+diffStr:'')+'</span>';
      }
      rowInner='<span style="font-size:.68rem;font-weight:800;color:'+rC+'">'+rStr+'</span>'
        +'<span style="font-size:.6rem;color:#64748b;margin-left:.15rem">'+detailStr+'</span>'
        +trendPart;
    }
    rows+='<div style="display:flex;align-items:center;flex-wrap:wrap;gap:.2rem;padding:.17rem 0;'
      +'border-bottom:1px solid #f1f5f9">'
      +'<span style="font-size:.62rem;color:#475569;min-width:3.2rem;flex-shrink:0">近'+n+'筆：</span>'
      +rowInner+'</div>';
  });

  el.innerHTML='<div style="margin:.22rem 0 .18rem;background:#f8fafc;border:1px solid #e2e8f0;'
    +'border-radius:.45rem;padding:.3rem .55rem">'
    +'<div style="font-size:.65rem;font-weight:800;color:#334155;margin-bottom:.15rem">📈 滑動勝率趨勢</div>'
    +rows
    +'<div style="font-size:.57rem;color:#94a3b8;margin-top:.15rem;line-height:1.4">'
    +'僅計已開獎紀錄；趨勢 = 當段 vs 前一段同長度比較，差距 &gt;5% 才標記升降</div>'
    +'</div>';
}

/* ── Failure analysis ── */
function renderFailureAnalysis(key,log,recentDraws){
  var el=document.getElementById('fail-analysis-'+key);
  if(!el)return;

  // Collect failed entries (已開獎 + 命中 ≥ 1)
  var failures=[];
  log.forEach(function(entry){
    var matchDraw=_findDrawForEntry(key,entry,recentDraws).draw;
    if(!matchDraw)return;
    var hits=entry.nums.filter(function(n){return matchDraw.numbers.indexOf(n)!==-1;});
    if(hits.length===0)return;
    failures.push({entry:entry,hits:hits});
  });

  var totalFail=failures.length;
  var summaryLabel='🧠 失敗原因分析（'+totalFail+' 敗）';

  if(totalFail===0){
    el.innerHTML='<details class="fail-analysis-panel">'
      +'<summary style="display:flex;align-items:center;gap:.35rem;cursor:pointer;list-style:none;padding:.22rem .45rem;user-select:none">'
      +'<span class="caret" style="font-size:.5rem;transition:transform .18s">▶</span>'
      +'<span style="font-size:.7rem;font-weight:700;color:#1e293b">'+summaryLabel+'</span>'
      +'</summary>'
      +'<div class="fail-analysis-body" style="color:#94a3b8">目前沒有可分析的失敗場次</div>'
      +'</details>';
    return;
  }

  // A. 最常撞到的號碼
  var hitCnt={};
  failures.forEach(function(f){
    f.hits.forEach(function(n){hitCnt[n]=(hitCnt[n]||0)+1;});
  });
  var topHits=Object.keys(hitCnt).sort(function(a,b){return hitCnt[b]-hitCnt[a];}).slice(0,5);

  // B. 遺漏值風險（優先使用 snapshot 記錄的當時遺漏值）
  var missData=(window._MISS_DATA&&window._MISS_DATA[key])||{};
  var lowM=0,midM=0,highM=0,totalHN=0;
  failures.forEach(function(f){
    var snapNums=f.entry.snapshot&&f.entry.snapshot.numbers||{};
    f.hits.forEach(function(n){
      totalHN++;
      var m;
      if(snapNums[n]&&snapNums[n].miss!==undefined){m=snapNums[n].miss;}
      else{m=missData[n]!==undefined?missData[n]:(missData[''+n]||0);}
      if(m<=2)lowM++;else if(m<=9)midM++;else highM++;
    });
  });

  // C. 近期熱力（優先使用 snapshot 記錄的當時頻率）
  var nhd=(window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key])||{};
  var hotHits=0;
  failures.forEach(function(f){
    var snapNums=f.entry.snapshot&&f.entry.snapshot.numbers||{};
    f.hits.forEach(function(n){
      var freq;
      if(snapNums[n]&&snapNums[n].recentFreq!==undefined){freq=snapNums[n].recentFreq;}
      else{freq=(nhd[n]||nhd[''+n]||{}).recent_freq||0;}
      if(freq>=3)hotHits++;
    });
  });

  // D. 色球與單雙偏態
  var oeCnt={},colCnt={};
  failures.forEach(function(f){
    var e=f.entry;
    if(e.oe){var k=e.oe.odd+'單'+e.oe.even+'雙';oeCnt[k]=(oeCnt[k]||0)+1;}
    if(e.col){var ck='紅'+e.col.red+'藍'+e.col.blue+'綠'+e.col.green;colCnt[ck]=(colCnt[ck]||0)+1;}
  });
  var topOE=Object.keys(oeCnt).sort(function(a,b){return oeCnt[b]-oeCnt[a];})[0];
  var topCol=Object.keys(colCnt).sort(function(a,b){return colCnt[b]-colCnt[a];})[0];

  // E. 冷門期條件衝突
  var coldConflict=0,coldTotal=0;
  failures.forEach(function(f){
    var note=f.entry.autoNote||'';
    f.hits.forEach(function(n){
      coldTotal++;
      var numStr=(n<10?'0'+n:''+n)+'號';
      if(note.indexOf(numStr)!==-1&&note.indexOf('冷門期')!==-1){
        // Check if this number line mentions 冷門期
        var lines=note.split('\n');
        lines.forEach(function(l){if(l.indexOf(numStr)!==-1&&l.indexOf('冷門期')!==-1)coldConflict++;});
      }
    });
  });

  // Build suggestions
  var sugs=[];
  if(totalHN>0){
    var lp=Math.round(lowM/totalHN*100);
    var hp2=Math.round(hotHits/totalHN*100);
    if(lp>=50)sugs.push('⚠️ 低遺漏（0~2期）號碼近期活躍，五選不中應降低其權重（命中佔 '+lp+'%）');
    else if(highM>0&&highM>=Math.round(totalHN*0.4))sugs.push('⚠️ 高遺漏（10期+）號碼仍有命中風險，選號時搭配近期熱力二次確認');
    if(hp2>=50)sugs.push('🔥 近20期高頻號不要一次選太多（命中佔 '+hp2+'%）');
  }
  var minRepeat=Math.max(2,Math.round(totalFail*0.4));
  if(topOE&&oeCnt[topOE]>=minRepeat)sugs.push('📊「'+topOE+'」比例在你的紀錄中失敗偏多（'+oeCnt[topOE]+'/'+totalFail+'），建議降低權重');
  if(topCol&&colCnt[topCol]>=minRepeat)sugs.push('🎨 色球「'+topCol+'」失敗偏多（'+colCnt[topCol]+'/'+totalFail+'），建議調整選號結構');
  if(coldTotal>0&&coldConflict>=Math.max(1,Math.round(coldTotal*0.3)))sugs.push('🧊 冷門期條件近期可能失效，建議搭配遺漏/熱力二次過濾（衝突 '+coldConflict+'/'+coldTotal+'）');
  if(sugs.length===0)sugs.push('✅ 暫無明顯規律，繼續累積紀錄後分析更準確');

  // Render hit balls
  var hitBalls=topHits.map(function(n){
    var ni=parseInt(n);
    var cls=_clsBall(ni);
    var ns=ni<10?'0'+n:''+n;
    return '<span style="display:inline-flex;align-items:center;gap:.15rem;margin:.08rem">'
      +'<span class="ball-sm '+cls+'" style="width:1.3rem;height:1.3rem;font-size:.58rem">'+ns+'</span>'
      +'<span style="font-size:.62rem;color:#475569">×'+hitCnt[n]+'</span>'
      +'</span>';
  }).join('');

  // Render miss distribution bar
  var missBar='';
  if(totalHN>0){
    var lPct=Math.round(lowM/totalHN*100);
    var mPct=Math.round(midM/totalHN*100);
    var hPct=Math.round(highM/totalHN*100);
    missBar='<div style="display:flex;gap:.22rem;flex-wrap:wrap;margin:.15rem 0">'
      +'<span class="fail-tag" style="background:#fef9c3;color:#713f12">低遺漏 '+lPct+'%</span>'
      +'<span class="fail-tag" style="background:#eff6ff;color:#1e40af">中遺漏 '+mPct+'%</span>'
      +'<span class="fail-tag" style="background:#f0fdf4;color:#166534">高遺漏 '+hPct+'%</span>'
      +'</div>';
  }
  var heatLine=totalHN>0
    ?'<div style="font-size:.63rem;color:#475569;margin:.1rem 0">近20期高頻命中：<strong>'
      +Math.round(hotHits/totalHN*100)+'%</strong>（'+hotHits+'/'+totalHN+' 個）</div>':'' ;

  var sugHtml=sugs.map(function(s){return '<div class="fail-suggest">'+s+'</div>';}).join('');

  el.innerHTML='<details class="fail-analysis-panel">'
    +'<summary style="display:flex;align-items:center;gap:.35rem;cursor:pointer;list-style:none;padding:.22rem .45rem;user-select:none">'
    +'<span class="caret" style="font-size:.5rem;transition:transform .18s">▶</span>'
    +'<span style="font-size:.7rem;font-weight:700;color:#7f1d1d">'+summaryLabel+'</span>'
    +'</summary>'
    +'<div class="fail-analysis-body">'
    +'<div class="fail-section-title">常撞號碼 Top '+Math.min(5,topHits.length)+'</div>'
    +'<div style="display:flex;flex-wrap:wrap;align-items:center;margin:.1rem 0 .05rem">'+hitBalls+'</div>'
    +'<div class="fail-section-title" style="margin-top:.38rem">遺漏值 &amp; 熱力分布</div>'
    +missBar+heatLine
    +'<div class="fail-section-title" style="margin-top:.38rem">建議</div>'
    +sugHtml
    +'</div>'
    +'</details>';
}

/* ── calcExcludeScore: 排除分 0-100（越高越危險，越應排除）── */
function calcExcludeScore(key,n){
  var nhd=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  var pd=window._PERIOD_DATA&&window._PERIOD_DATA[key]||[];
  var nd=nhd[n]||nhd[''+n]||{};
  var dp=nd.danger_pct||0;
  var freq=nd.recent_freq||0;
  var miss=nd.current_miss||0;
  var isLatest=nd.in_latest_draw||false;
  var isNeighbor=nd.is_neighbor||false;
  var hasCold=pd.some(function(p){return p.ref_numbers&&p.ref_numbers.indexOf(n)!==-1;});
  var score=0;
  // Low miss (+20 risk): miss ≤ 3 means this number appeared recently
  if(miss<=3)score+=20;
  else if(miss<=8)score+=10;
  // Recent frequency (+0~25)
  score+=Math.min(freq*5,25);
  // Danger percent contribution
  score+=Math.round(dp*0.35);
  // Latest draw (+25)
  if(isLatest)score+=25;
  // Neighbor of latest draw (+12)
  if(isNeighbor)score+=12;
  // Cold period match (+10)
  if(hasCold)score+=10;
  return Math.min(Math.round(score),100);
}

/* ── calcComboScore: 號碼組合危險指數 0-100（v10.0）── */
function calcComboScore(key,sel){
  if(!sel||sel.length===0)return 0;
  var scores=sel.map(function(n){return calcExcludeScore(key,n);});
  var avgEx=scores.reduce(function(a,b){return a+b;},0)/scores.length;
  var maxEx=Math.max.apply(null,scores);
  var drawData=_drawDataFor(key);
  var check50=Math.min(drawData.length,50);
  var anyHit=0;
  for(var i=0;i<check50;i++){
    var d=drawData[i];
    if(!d||!d.numbers)continue;
    if(sel.some(function(n){return d.numbers.indexOf(n)!==-1;}))anyHit++;
  }
  var anyHitRate=check50>0?(anyHit/check50)*100:50;
  return Math.min(Math.round(0.6*(avgEx*0.5+maxEx*0.5)+0.4*anyHitRate),100);
}

/* ── renderSelectionRiskSummary: 即時風險摘要（v10.0）── */
function renderSelectionRiskSummary(key){
  var el=document.getElementById('pk-risk-'+key);
  if(!el)return;
  var sel=_pickerSel[key]||[];
  if(sel.length===0){el.innerHTML='';return;}
  var nhd=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  // Feature 1: combo score bar
  var comboScore=calcComboScore(key,sel);
  var comboBg=comboScore>=70?'#fee2e2':comboScore>=45?'#fff7ed':'#f0fdf4';
  var comboBorder=comboScore>=70?'#fca5a5':comboScore>=45?'#fcd34d':'#86efac';
  var comboC=comboScore>=70?'#dc2626':comboScore>=45?'#d97706':'#16a34a';
  var comboLabel=comboScore>=70?'高危':comboScore>=45?'中等':'安全';
  var comboHtml='<div style="margin-bottom:.22rem;padding:.15rem .3rem;border-radius:.3rem;'
    +'background:'+comboBg+';border:1px solid '+comboBorder+'">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.1rem">'
    +'<span style="font-size:.6rem;font-weight:800;color:#475569">組合危險指數</span>'
    +'<span style="font-size:.7rem;font-weight:900;color:'+comboC+'">'+comboScore
    +' <span style="font-size:.55rem">('+comboLabel+')</span></span>'
    +'</div>'
    +'<div style="height:4px;background:#e2e8f0;border-radius:2px;overflow:hidden">'
    +'<div style="height:100%;width:'+comboScore+'%;background:'+comboC+';border-radius:2px"></div>'
    +'</div></div>';
  // Feature 2: range balance (1-13 / 14-26 / 27-39)
  var seg=[0,0,0];
  sel.forEach(function(n){if(n<=13)seg[0]++;else if(n<=26)seg[1]++;else seg[2]++;});
  var maxSeg=Math.max.apply(null,seg);
  var segBg=maxSeg>=5?'#fef2f2':maxSeg>=4?'#fff7ed':'#f8fafc';
  var segBd=maxSeg>=5?'#fca5a5':maxSeg>=4?'#fcd34d':'#e2e8f0';
  var segWarnHtml=maxSeg>=5
    ?'<span style="font-weight:700;color:#dc2626"> ⚠️高度集中！</span>'
    :maxSeg>=4?'<span style="font-weight:700;color:#d97706"> ⚠️偏集中</span>':'';
  var segHtml='<div style="font-size:.59rem;color:#475569;padding:.1rem .18rem;'
    +'border-radius:.25rem;background:'+segBg+';border:1px solid '+segBd+';margin-top:.18rem">'
    +'段位分布：低(1-13)×'+seg[0]+' · 中(14-26)×'+seg[1]+' · 高(27-39)×'+seg[2]+segWarnHtml
    +'</div>';
  // Per-number rows with miss anomaly (Feature 6)
  var items=sel.map(function(n){
    var nd=nhd[n]||nhd[''+n]||{};
    var dp=nd.danger_pct||0;
    var freq=nd.recent_freq||0;
    var miss=nd.current_miss||0;
    var avgG=nd.avg_gap||0;
    var isLatest=nd.in_latest_draw||false;
    var exScore=calcExcludeScore(key,n);
    var ns=n<10?'0'+n:''+n;
    var cls=_clsBall(n);
    var label=exScore>=70?'⛔建議避開':exScore>=45?'👀可觀察':'✓相對安全';
    var labelC=exScore>=70?'#dc2626':exScore>=45?'#d97706':'#16a34a';
    var warns=[];
    if(isLatest)warns.push('最新期');
    if(dp>=80)warns.push('危'+dp+'%');
    if(freq>=4)warns.push('熱'+freq+'次');
    var pressHtml='';
    if(avgG>0&&miss>0){
      var ratio=miss/avgG;
      if(ratio>=2.0){
        pressHtml='<span style="font-size:.53rem;font-weight:700;color:#b45309;'
          +'background:#fef3c7;border-radius:.2rem;padding:.02rem .2rem;margin-left:.1rem">'
          +'強回歸壓力×'+ratio.toFixed(1)+'</span>';
      }else if(ratio>=1.5){
        pressHtml='<span style="font-size:.53rem;font-weight:700;color:#c2410c;'
          +'background:#fff7ed;border-radius:.2rem;padding:.02rem .2rem;margin-left:.1rem">'
          +'回歸壓力×'+ratio.toFixed(1)+'</span>';
      }
    }
    var missDisplay=avgG>0?'漏'+miss+'(均'+avgG+')':'漏'+miss;
    return '<div style="display:flex;align-items:center;gap:.22rem;padding:.15rem 0;'
      +'border-bottom:1px solid #f1f5f9">'
      +'<span class="ball-sm '+cls+'" style="width:1.4rem;height:1.4rem;font-size:.58rem;flex-shrink:0">'+ns+'</span>'
      +'<span style="font-size:.59rem;color:#475569;flex:1">'+missDisplay+'｜危'+dp+'%｜近'+freq+'次</span>'
      +pressHtml
      +(warns.length?'<span style="font-size:.56rem;color:#64748b;background:#f1f5f9;border-radius:.2rem;padding:.05rem .22rem">'+warns.join(' · ')+'</span>':'')
      +'<span style="font-size:.58rem;font-weight:800;color:'+labelC+';white-space:nowrap">'+label+'</span>'
      +'</div>';
  }).join('');
  // Co-occurrence check from recent draws
  var coOccHtml='';
  if(sel.length>=2){
    var drawData=_drawDataFor(key);
    var checkN=Math.min(drawData.length,100);
    var matchCount=0;
    for(var ci=0;ci<checkN;ci++){
      var d=drawData[ci];
      if(!d||!d.numbers)continue;
      var overlap=sel.filter(function(n){return d.numbers.indexOf(n)!==-1;}).length;
      if(overlap>=2)matchCount++;
    }
    if(checkN>=20){
      var coRate=Math.round(matchCount/checkN*100);
      var coC=coRate>=20?'#dc2626':coRate>=10?'#d97706':'#16a34a';
      var coLabel=coRate>=20?'⚠️ 高':'';
      coOccHtml='<div style="font-size:.59rem;color:'+coC+';margin-top:.1rem;padding:.1rem 0;border-top:1px solid #e2e8f0">'
        +'組合共現率（近'+checkN+'期中 ≥2 個同開）：<strong>'+coRate+'%</strong> '+coLabel
        +(coRate>=20?' — 此組合在近期出現同開次數偏多，整體風險升高':'')
        +'</div>';
    }
  }
  var maxScore=sel.reduce(function(mx,n){return Math.max(mx,calcExcludeScore(key,n));},0);
  var avgScore=Math.round(sel.reduce(function(s,n){return s+calcExcludeScore(key,n);},0)/sel.length);
  var levelC=maxScore>=70?'#fee2e2':maxScore>=45?'#fff7ed':'#f0fdf4';
  var levelBorder=maxScore>=70?'#fca5a5':maxScore>=45?'#fcd34d':'#86efac';
  var levelText=maxScore>=70?'⚠️ 高風險':maxScore>=45?'👀 注意觀察':'✓ 相對安全';
  var levelTC=maxScore>=70?'#dc2626':maxScore>=45?'#d97706':'#16a34a';
  var highDanger=sel.filter(function(n){return calcExcludeScore(key,n)>=65;}).length;
  var lowMiss=sel.filter(function(n){
    var nd2=nhd[n]||nhd[''+n]||{};return (nd2.current_miss||0)<=2;
  }).length;
  var coRateSummary=0;
  if(coOccHtml){
    var m=coOccHtml.match(/<strong>(\d+)%<\/strong>/);
    if(m)coRateSummary=parseInt(m[1],10);
  }
  var summaryTxt,summaryC;
  if(highDanger>=2||coRateSummary>=35){
    summaryTxt='建議重選，整體風險偏高';summaryC='#dc2626';
  }else if(highDanger===1||lowMiss>=2){
    summaryTxt='建議調整其中 1~2 個號碼';summaryC='#d97706';
  }else{
    summaryTxt='組合可觀察，整體排除分偏低';summaryC='#16a34a';
  }
  var summaryHtml='<div style="font-size:.62rem;font-weight:700;color:'+summaryC+';'
    +'margin-top:.15rem;padding:.12rem .3rem;border-radius:.28rem;background:rgba(255,255,255,.55)">'
    +'📋 本次選號總結：'+summaryTxt+'</div>';
  // Pair co-occurrence warning (v10.1)
  var pairWarnHtml='';
  if(sel.length>=2){
    var drawData3=_drawDataFor(key);
    var checkPN=Math.min(drawData3.length,100);
    var highPairs=[];
    for(var pi=0;pi<sel.length;pi++){
      for(var qi=pi+1;qi<sel.length;qi++){
        var na=sel[pi],nb=sel[qi];
        var pairCnt=0;
        for(var ri=0;ri<checkPN;ri++){
          var pd3=drawData3[ri];
          if(!pd3||!pd3.numbers)continue;
          if(pd3.numbers.indexOf(na)!==-1&&pd3.numbers.indexOf(nb)!==-1)pairCnt++;
        }
        var pr2=checkPN>0?Math.round(pairCnt/checkPN*100):0;
        if(pr2>=20)highPairs.push((na<10?'0'+na:''+na)+'+'+(nb<10?'0'+nb:''+nb)+'('+pr2+'%)');
      }
    }
    if(highPairs.length>0){
      pairWarnHtml='<div style="font-size:.58rem;color:#dc2626;margin-top:.12rem;padding:.08rem .18rem;'
        +'background:#fef2f2;border-radius:.25rem;border:1px solid #fca5a5">'
        +'⚠️ 高共現配對：'+highPairs.join('、')+'（近'+checkPN+'期）</div>';
    }
  }
  el.innerHTML='<div style="margin:.25rem 0 .2rem;border:1px solid '+levelBorder+';border-radius:.45rem;'
    +'background:'+levelC+';padding:.3rem .5rem;font-size:.65rem">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.15rem">'
    +'<span style="font-weight:800;color:#1e293b">選號風險分析</span>'
    +'<span style="font-size:.6rem;font-weight:800;color:'+levelTC+'">'+levelText+'（均排'+avgScore+'）</span>'
    +'</div>'
    +comboHtml
    +items
    +segHtml
    +pairWarnHtml
    +coOccHtml
    +summaryHtml
    +'</div>';
  renderOEColorGuide(key);
}

/* ── A/B Compare (v10.0) ── */
function savePickerCompare(key,slot){
  var sel=(_pickerSel[key]||[]).slice();
  if(sel.length===0)return;
  _pickerCompare[key]=_pickerCompare[key]||{a:null,b:null};
  _pickerCompare[key][slot]={sel:sel,score:calcComboScore(key,sel),stratSrc:_pickerStrategySource[key]||''};
  renderPickerCompare(key);
}
function clearPickerCompare(key){
  _pickerCompare[key]={a:null,b:null};
  renderPickerCompare(key);
}
function renderPickerCompare(key){
  var el=document.getElementById('pk-compare-'+key);
  if(!el)return;
  var cmp=_pickerCompare[key]||{};
  if(!cmp.a&&!cmp.b){el.innerHTML='';return;}
  var nhd=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  function _slotHtml(slot,label){
    var s=cmp[slot];
    if(!s)return '<div style="flex:1;padding:.28rem .35rem;border-radius:.3rem;background:#f8fafc;'
      +'border:1px dashed #cbd5e1;text-align:center;font-size:.6rem;color:#94a3b8">'+label+'尚未存入</div>';
    var scores=s.sel.map(function(n){return calcExcludeScore(key,n);});
    var avgEx=Math.round(scores.reduce(function(a,b){return a+b;},0)/scores.length);
    var maxEx=Math.max.apply(null,scores);
    var sc=s.score;
    var cc=sc>=70?'#dc2626':sc>=45?'#d97706':'#16a34a';
    var balls=s.sel.map(function(n){
      var ns=n<10?'0'+n:''+n;
      return '<span class="ball-sm '+_clsBall(n)+'" style="width:1.2rem;height:1.2rem;font-size:.52rem">'+ns+'</span>';
    }).join('');
    var seg=[0,0,0];
    s.sel.forEach(function(n){if(n<=13)seg[0]++;else if(n<=26)seg[1]++;else seg[2]++;});
    return '<div style="flex:1;padding:.28rem .35rem;border-radius:.3rem;background:#f8fafc;border:1px solid #e2e8f0">'
      +'<div style="font-size:.6rem;font-weight:800;color:#475569;margin-bottom:.1rem">'+label
      +(s.stratSrc?'<span style="font-size:.52rem;color:#94a3b8;margin-left:.2rem">['+s.stratSrc+']</span>':'')+'</div>'
      +'<div style="display:flex;flex-wrap:wrap;gap:.1rem;margin-bottom:.12rem">'+balls+'</div>'
      +'<div style="font-size:.58rem;color:#475569">危險指數：<strong style="color:'+cc+'">'+sc+'</strong>'
      +' · 均排：'+avgEx+' · 最高：'+maxEx+'</div>'
      +'<div style="font-size:.56rem;color:#94a3b8;margin-top:.06rem">段：低'+seg[0]+'/中'+seg[1]+'/高'+seg[2]+'</div>'
      +'</div>';
  }
  var aHtml=_slotHtml('a','A 組');
  var bHtml=_slotHtml('b','B 組');
  var diffHtml='';
  if(cmp.a&&cmp.b){
    var diff=cmp.a.score-cmp.b.score;
    var better=diff>0?'B 組較安全':'A 組較安全';
    if(Math.abs(diff)<=3)better='兩組相近';
    diffHtml='<div style="font-size:.59rem;font-weight:700;color:#6366f1;text-align:center;margin-top:.1rem">⟺ '+better+'（差 '+Math.abs(diff)+'）</div>';
  }
  el.innerHTML='<div style="margin-top:.22rem;padding:.22rem .28rem;border:1px solid #e0e7ff;'
    +'border-radius:.4rem;background:#f8f7ff">'
    +'<div style="font-size:.62rem;font-weight:800;color:#6366f1;margin-bottom:.15rem">A/B 組合比較</div>'
    +'<div style="display:flex;gap:.22rem">'+aHtml+bHtml+'</div>'
    +diffHtml
    +'</div>';
}

/* ── backtestCurrentCombo: 自訂組合歷史回測（v10.1）── */
function backtestCurrentCombo(key){
  var sel=_pickerSel[key]||[];
  if(sel.length<5){alert('請先選滿5個號碼');return;}
  var drawData=_drawDataFor(key);
  var checkN=Math.min(drawData.length,300);
  if(checkN<10){alert('歷史資料不足');return;}
  var wins=0;var resultArr=[];
  for(var i=0;i<checkN;i++){
    var d=drawData[i];
    if(!d||!d.numbers)continue;
    var hit=sel.some(function(n){return d.numbers.indexOf(n)!==-1;});
    resultArr.push(hit?0:1);
    if(!hit)wins++;
  }
  var total=resultArr.length;
  var wr=total>0?Math.round(wins/total*1000)/10:0;
  var rnd=48.3;
  var diff=Math.round((wr-rnd)*10)/10;
  var recent10=resultArr.slice(0,10);
  var spark=recent10.map(function(r){
    return '<span style="display:inline-block;width:6px;height:12px;background:'
      +(r?'#16a34a':'#dc2626')+';border-radius:1px;margin:.5px"></span>';
  }).join('');
  // Wilson CI
  var p=wr/100;var z=1.96;var ci_html='';
  if(total>=3){
    var denom=1+z*z/total;
    var center=(p+z*z/(2*total))/denom;
    var margin=z*Math.sqrt(p*(1-p)/total+z*z/(4*total*total))/denom;
    var half=Math.round((Math.min(1,center+margin)-Math.max(0,center-margin))*50*10)/10;
    ci_html=' <span style="font-size:.56rem;color:#94a3b8">±'+half+'%</span>';
  }
  var wr_c=wr>=55?'#16a34a':wr>=45?'#d97706':'#dc2626';
  var diff_c=diff>=5?'#16a34a':diff>=-2?'#64748b':'#dc2626';
  var el=document.getElementById('pk-combo-bt-'+key);
  if(!el)return;
  el.innerHTML='<div style="margin-top:.22rem;padding:.22rem .32rem;border:1px solid #ddd6fe;'
    +'border-radius:.4rem;background:#faf5ff">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:.1rem">'
    +'<span style="font-size:.6rem;font-weight:800;color:#5b21b6">📊 組合歷史回測（近'+checkN+'期）</span>'
    +'<button onclick="document.getElementById(\'pk-combo-bt-'+key+'\').innerHTML=\'\'" '
    +'style="font-size:.55rem;color:#94a3b8;background:none;border:none;cursor:pointer">✕</button>'
    +'</div>'
    +'<div style="display:flex;gap:.5rem;align-items:baseline;flex-wrap:wrap">'
    +'<span style="font-size:.78rem;font-weight:900;color:'+wr_c+'">'+wr+'%</span>'+ci_html
    +'<span style="font-size:.6rem;color:'+diff_c+'">vs隨機 '+(diff>=0?'+':'')+diff+'%</span>'
    +'<span style="font-size:.6rem;color:#64748b">'+wins+'/'+total+'</span>'
    +'</div>'
    +'<div style="display:flex;gap:0;align-items:flex-end;margin-top:.12rem">'+spark+'</div>'
    +'</div>';
}

/* ── renderPersonalWinRate: 個人勝率儀表板（v10.1）── */
function renderPersonalWinRate(key){
  var el=document.getElementById('pk-wr-'+key);
  if(!el)return;
  var sk='betLog_'+key;
  var log=[];
  try{log=JSON.parse(localStorage.getItem(sk)||'[]');}catch(e){}
  if(log.length===0){el.innerHTML='';return;}
  var drawData=_drawDataFor(key);
  var total=log.length,settled=0,wins=0;
  log.forEach(function(entry){
    var found=_findDrawForEntry(key,entry,drawData);
    if(found.draw){
      settled++;
      var hits=entry.nums.filter(function(n){return found.draw.numbers.indexOf(n)!==-1;}).length;
      if(hits===0)wins++;
    }
  });
  if(settled===0){el.innerHTML='';return;}
  var wr=Math.round(wins/settled*1000)/10;
  var mbtList=window._MULTI_BT_DATA&&window._MULTI_BT_DATA[key]||[];
  var btRef=48.3;
  for(var i=0;i<mbtList.length;i++){if(mbtList[i].id==='smart'){btRef=mbtList[i].win_rate;break;}}
  var diff=Math.round((wr-btRef)*10)/10;
  var wr_c=wr>=60?'#16a34a':wr>=45?'#d97706':'#dc2626';
  var diff_c=diff>=5?'#16a34a':diff>=-5?'#64748b':'#dc2626';
  var pending=total-settled;
  el.innerHTML='<details style="margin-bottom:.18rem"><summary style="list-style:none;cursor:pointer;'
    +'font-size:.62rem;font-weight:800;color:#6366f1;padding:.12rem .32rem;border-radius:.3rem;'
    +'background:#f5f3ff">📈 個人勝率儀表板</summary>'
    +'<div style="display:flex;flex-wrap:wrap;gap:.22rem .55rem;padding:.22rem .15rem;'
    +'font-size:.62rem;color:#475569;border-top:1px solid #ede9fe;margin-top:.05rem">'
    +'<span>總計 <strong>'+total+'</strong> 筆</span>'
    +'<span>已結算 <strong>'+settled+'</strong> 筆</span>'
    +(pending>0?'<span>待結 <strong>'+pending+'</strong></span>':'')
    +'<span>個人勝率 <strong style="color:'+wr_c+'">'+wr+'%</strong></span>'
    +'<span>vs回測 <strong>'+btRef+'%</strong></span>'
    +'<span style="color:'+diff_c+'">差距 '+(diff>=0?'+':'')+diff+'%</span>'
    +'</div></details>';
}

/* ── lockPickerSel / unlockPickerSel / _restoreLock（v10.1）── */
function lockPickerSel(key){
  var sel=_pickerSel[key]||[];
  if(sel.length===0){alert('請先選號再鎖定');return;}
  _pickerLock[key]={sel:sel.slice(),stratSrc:_pickerStrategySource[key]||''};
  try{localStorage.setItem('pickerLock_'+key,JSON.stringify(_pickerLock[key]));}catch(e){}
  savePickerCompare(key,'a');
  var btn=document.getElementById('pk-lock-btn-'+key);
  if(btn){btn.textContent='🔓';btn.style.color='#dc2626';btn.title='已鎖定－點擊解鎖';
    btn.setAttribute('onclick','unlockPickerSel(\''+key+'\')');
    btn.style.background='#fee2e2';}
}
function unlockPickerSel(key){
  delete _pickerLock[key];
  try{localStorage.removeItem('pickerLock_'+key);}catch(e){}
  var btn=document.getElementById('pk-lock-btn-'+key);
  if(btn){btn.textContent='🔒';btn.style.color='#0369a1';btn.title='鎖定選號';
    btn.setAttribute('onclick','lockPickerSel(\''+key+'\')');
    btn.style.background='#e0f2fe';}
}
function _restoreLock(key){
  if(_pickerLock[key])return;
  try{
    var saved=localStorage.getItem('pickerLock_'+key);
    if(saved){_pickerLock[key]=JSON.parse(saved);}
  }catch(e){}
  if(!_pickerLock[key])return;
  _pickerSel[key]=_pickerLock[key].sel.slice();
  _pickerStrategySource[key]=_pickerLock[key].stratSrc||'';
  _refreshPickerUI(key);
  renderSelectionRiskSummary(key);
  var btn=document.getElementById('pk-lock-btn-'+key);
  if(btn){btn.textContent='🔓';btn.style.color='#dc2626';btn.title='已鎖定－點擊解鎖';
    btn.setAttribute('onclick','unlockPickerSel(\''+key+'\')');
    btn.style.background='#fee2e2';}
}

/* ── renderOEColorGuide: 今日配比推薦側欄比對（v10.2）── */
function renderOEColorGuide(key){
  var el=document.getElementById('pk-oe-guide-'+key);
  if(!el)return;
  var rev=window._REV_OE_DATA&&window._REV_OE_DATA[key];
  if(!rev||!rev.main_oe){el.innerHTML='';return;}
  var moe=rev.main_oe,mcol=rev.main_col,alts=rev.alt_cols||[],stats=rev.stats||{};
  var sel=_pickerSel[key]||[];
  var matchHtml='';
  if(sel.length>0){
    var curOdd=0,curEven=0,curRed=0,curBlue=0,curGreen=0;
    sel.forEach(function(n){
      if(n%2===1)curOdd++;else curEven++;
      if(n%3===1)curRed++;else if(n%3===2)curBlue++;else curGreen++;
    });
    var oeDiff=Math.abs(curOdd-moe.odd);
    var oeStatus=oeDiff===0?'✓ 符合':oeDiff<=1?'~ 接近':'✗ 不符';
    var oeC=oeDiff===0?'#16a34a':oeDiff<=1?'#d97706':'#dc2626';
    var colDiff=Math.abs(curRed-mcol.red)+Math.abs(curBlue-mcol.blue)+Math.abs(curGreen-mcol.green);
    var colStatus=colDiff===0?'✓ 符合':colDiff<=2?'~ 接近':'✗ 不符';
    var colC=colDiff===0?'#16a34a':colDiff<=2?'#d97706':'#dc2626';
    matchHtml='<div style="margin-top:.18rem;padding:.15rem .22rem;background:#f8fafc;border-radius:.3rem;border:1px solid #e2e8f0;font-size:.59rem">'
      +'<div>目前：<strong>'+curOdd+'單'+curEven+'雙 / 紅'+curRed+'藍'+curBlue+'綠'+curGreen+'</strong></div>'
      +'<div style="display:flex;gap:.5rem;margin-top:.06rem">'
      +'<span style="color:'+oeC+'">單雙 '+oeStatus+'</span>'
      +'<span style="color:'+colC+'">色球 '+colStatus+'</span>'
      +'</div></div>';
  }
  var altTxt=alts.map(function(a){return a.red+'紅'+a.blue+'藍'+a.green+'綠';}).join(' · ');
  el.innerHTML='<details style="font-size:.63rem" id="pk-oe-det-'+key+'">'
    +'<summary style="list-style:none;cursor:pointer;padding:.12rem .32rem;border-radius:.3rem;'
    +'background:#f5f3ff;font-weight:800;color:#5b21b6;display:flex;align-items:center;gap:.2rem">'
    +'<span>🎯 今日配比推薦</span>'
    +'<span style="font-size:.55rem;color:#94a3b8;margin-left:auto">▶</span>'
    +'</summary>'
    +'<div style="padding:.18rem .12rem .1rem;font-size:.6rem;color:#475569">'
    +'<div>近3期：<strong style="color:#334155">'+stats.odd+'單'+stats.even+'雙 / 紅'+stats.red+'藍'+stats.blue+'綠'+stats.green+'</strong></div>'
    +'<div style="margin-top:.07rem">建議：<strong style="color:#5b21b6">'+moe.odd+'單'+moe.even+'雙 / '+mcol.red+'紅'+mcol.blue+'藍'+mcol.green+'綠</strong></div>'
    +(altTxt?'<div style="color:#7c3aed;margin-top:.04rem">備選：'+altTxt+'</div>':'')
    +matchHtml
    +'<div style="margin-top:.15rem">'
    +'<button class="pk-mode-btn" style="font-size:.58rem;padding:.14rem .35rem;color:#5b21b6" '
    +'onclick="_applyOERatioSel(\''+key+'\')">⚙️ 套用配比挑號</button>'
    +'</div></div></details>';
}

/* ── _applyOERatioSel: 套用建議配比選號（v10.2）── */
function _applyOERatioSel(key){
  var rev=window._REV_OE_DATA&&window._REV_OE_DATA[key];
  if(!rev||!rev.main_oe){alert('建議配比資料未載入');return;}
  var oddReq=rev.main_oe.odd,evenReq=rev.main_oe.even;
  var redReq=rev.main_col.red,blueReq=rev.main_col.blue,greenReq=rev.main_col.green;
  // Build sorted pools for 6 OE×Color categories
  var pools={or:[],ob:[],og:[],er:[],eb:[],eg:[]};
  for(var n=1;n<=39;n++){
    var isOdd=n%2===1;
    var col=n%3===1?'r':n%3===2?'b':'g';
    var cat=(isOdd?'o':'e')+col;
    pools[cat].push({n:n,score:calcExcludeScore(key,n)});
  }
  ['or','ob','og','er','eb','eg'].forEach(function(c){pools[c].sort(function(a,b){return a.score-b.score;});});
  var bestSel=null,bestScore=Infinity;
  for(var nor=0;nor<=Math.min(oddReq,redReq,pools.or.length);nor++){
    var ner=redReq-nor;
    if(ner<0||ner>pools.er.length)continue;
    for(var nob=0;nob<=Math.min(oddReq-nor,blueReq,pools.ob.length);nob++){
      var neb=blueReq-nob;
      if(neb<0||neb>pools.eb.length)continue;
      var nog=oddReq-nor-nob;
      var neg=greenReq-nog;
      if(nog<0||nog>pools.og.length)continue;
      if(neg<0||neg>pools.eg.length)continue;
      if(ner+neb+neg!==evenReq)continue;
      var sel2=[],sc=0;
      [['or',nor],['ob',nob],['og',nog],['er',ner],['eb',neb],['eg',neg]].forEach(function(p){
        pools[p[0]].slice(0,p[1]).forEach(function(item){sel2.push(item.n);sc+=item.score;});
      });
      if(sel2.length===5&&sc<bestScore){bestScore=sc;bestSel=sel2.slice();}
    }
  }
  if(!bestSel){alert('找不到符合配比的5個號碼，請嘗試備選配比');return;}
  _pickerSel[key]=bestSel.sort(function(a,b){return a-b;});
  _pickerStrategySource[key]='配比';
  _refreshPickerUI(key);
  renderSelectionRiskSummary(key);
  renderDailyDecisionSummary(key);
}

/* ── toggleMobilePicker: 手機底部抽屜開關 ── */
function toggleMobilePicker(){
  if(window.innerWidth>768) return;
  var sb=document.getElementById('picker-sidebar');
  if(!sb) return;
  var open=sb.classList.toggle('mob-open');
  var hint=document.getElementById('mob-open-hint');
  if(hint) hint.textContent=open?'▼ 收起':'▲ 展開';
}
/* 手機下滑主內容時自動收起選號盤 */
(function(){
  var lastY=0;
  window.addEventListener('scroll',function(){
    if(window.innerWidth>768) return;
    var y=window.scrollY||window.pageYOffset;
    if(y>lastY+30){
      var sb=document.getElementById('picker-sidebar');
      if(sb&&sb.classList.contains('mob-open')){
        sb.classList.remove('mob-open');
        var hint=document.getElementById('mob-open-hint');
        if(hint) hint.textContent='▲ 展開';
      }
    }
    lastY=y;
  },{passive:true});
})();

/* ── toggleSidebarWide: 展寬/縮窄選號盤 ── */
function toggleSidebarWide(){
  var isWide=document.body.classList.toggle('sidebar-wide');
  var btn=document.getElementById('sidebar-wide-btn');
  if(btn)btn.textContent=isWide?'⇔ 縮窄':'⇔ 展寬';
  try{localStorage.setItem('pickerSidebarWide',isWide?'1':'0');}catch(e){}
}

/* ── Consecutive analysis toggle ── */
function toggleConsec(key,num){
  var detailEl=document.getElementById('consec-d-'+key+'-'+num);
  if(!detailEl)return;
  detailEl.classList.toggle('open');
}

/* ── Picker mode (heatmap / danger / default) ── */
var _pickerMode={};
function setPickerMode(key,mode){
  _pickerMode[key]=mode;
  // Update toggle buttons
  ['default','heatmap','danger'].forEach(function(m){
    var btn=document.getElementById('pkm-'+
      (m==='default'?'def':m==='heatmap'?'heat':'danger')+'-'+key);
    if(btn)btn.classList.toggle('active',m===mode);
  });
  _applyPickerModeColors(key);
}
function _applyPickerModeColors(key){
  var mode=_pickerMode[key]||'default';
  var grid=document.getElementById('pk-grid-'+key);
  if(!grid)return;
  var nhd=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  grid.querySelectorAll('.pk-cell').forEach(function(cell){
    var n=parseInt(cell.dataset.num);
    // Remove existing mode classes
    cell.className=cell.className.replace(/\bmode-\S+/g,'').trim();
    cell.querySelectorAll('.pk-mode-label').forEach(function(el){el.remove();});
    if(mode==='default'){
      // Restore pk-miss text
      var missEl=cell.querySelector('.pk-miss');
      if(missEl){
        var md=window._MISS_DATA&&window._MISS_DATA[key];
        var mv=md?(md[n]!==undefined?md[n]:(md[''+n]||0)):0;
        missEl.textContent='遺漏'+mv;
        missEl.style.color='';
      }
    }else if(mode==='heatmap'){
      var freq=(nhd[n]||nhd[''+n]||{}).recent_freq||0;
      freq=Math.min(freq,5);
      cell.classList.add('mode-heatmap-'+freq);
      var missEl=cell.querySelector('.pk-miss');
      if(missEl){
        var hpd=window._HEAT_PROB_DATA&&window._HEAT_PROB_DATA[key];
        var hpn=hpd?(hpd[n]||hpd[''+n]):null;
        var rateStr='';
        if(hpn){
          var sc=hpn.sample_count||0;
          if(sc<10){rateStr='｜樣少';}
          else if(hpn.next_hit_rate!==null&&hpn.next_hit_rate!==undefined){
            rateStr='｜'+hpn.next_hit_rate+'%';
          }else{rateStr='｜--';}
        }
        missEl.textContent='近'+freq+'次'+rateStr;
        missEl.style.color='';
      }
    }else if(mode==='danger'){
      var dp=(nhd[n]||nhd[''+n]||{}).danger_pct||0;
      var exScore=calcExcludeScore(key,n);
      var cls='mode-danger-0';
      if(exScore>=85)cls='mode-danger-100';
      else if(exScore>=65)cls='mode-danger-75';
      else if(exScore>=45)cls='mode-danger-50';
      else if(exScore>=20)cls='mode-danger-25';
      cell.classList.add(cls);
      cell.title='危險分 '+exScore+'/100；遺漏危險 '+dp+'%';
      var missEl=cell.querySelector('.pk-miss');
      if(missEl){
        missEl.textContent='危'+exScore;
        missEl.style.color=exScore>=70?'#991b1b':exScore>=45?'#c2410c':'#475569';
      }
    }
  });
}

/* ── Smart recommendation (v9.6 — explainable chips) ── */
function _updateSmartRec(key){
  var el=document.getElementById('pk-smart-'+key);
  if(!el)return;
  var nhd=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  if(!Object.keys(nhd).length){
    el.innerHTML='<span style="color:#94a3b8;font-size:.63rem">數據載入中…</span>';
    return;
  }
  var scored=[];
  for(var n=1;n<=39;n++){
    var nd=nhd[n]||nhd[''+n]||{};
    var dp=nd.danger_pct||0;
    var freq=nd.recent_freq||0;
    var safe=100-dp*0.6-freq*8*0.4;
    scored.push({n:n,safe:safe,dp:dp,freq:freq,
      miss:nd.current_miss||0,
      isLatest:nd.in_latest_draw||false,
      isNeighbor:nd.is_neighbor||false});
  }
  scored.sort(function(a,b){return b.safe-a.safe;});
  var top5=scored.slice(0,5);
  el.innerHTML=top5.map(function(s){
    var cls=_clsBall(s.n);
    var ns=s.n<10?'0'+s.n:''+s.n;
    var dpC=s.dp<30?'#16a34a':s.dp<60?'#d97706':'#dc2626';
    var badges='';
    if(s.isLatest)badges+='<span style="font-size:.52rem;background:#fee2e2;color:#991b1b;border-radius:.2rem;padding:.05rem .22rem;font-weight:700">最新</span>';
    if(s.isNeighbor)badges+='<span style="font-size:.52rem;background:#fef9c3;color:#713f12;border-radius:.2rem;padding:.05rem .22rem;font-weight:700">鄰</span>';
    return '<div class="pk-smart-chip" onclick="togglePickerNum(\''+key+'\','+s.n+')" '
      +'style="flex-direction:column;align-items:center;gap:.08rem;padding:.18rem .28rem;min-width:3.2rem">'
      +'<span class="ball-sm '+cls+'" style="width:1.5rem;height:1.5rem;font-size:.62rem">'+ns+'</span>'
      +'<span class="chip-danger" style="color:'+dpC+';font-size:.58rem">危'+s.dp+'%</span>'
      +'<span style="font-size:.55rem;color:#475569">漏'+s.miss+'｜近'+s.freq+'次</span>'
      +(badges?'<div style="display:flex;gap:.1rem;flex-wrap:wrap;justify-content:center">'+badges+'</div>':'')
      +'</div>';
  }).join('');
  renderDailyDecisionSummary(key);
}

/* ── Today's Decision Summary (v9.9) ── */
function renderDailyDecisionSummary(key){
  var el=document.getElementById('pk-daily-'+key);
  if(!el)return;
  var nhd=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  if(!Object.keys(nhd).length){
    el.innerHTML='<div class="pk-daily-txt">今日建議：資料不足，請先更新或切換彩票。</div>';
    return;
  }
  var pd2=window._PERIOD_DATA&&window._PERIOD_DATA[key]||[];
  // High-danger numbers (排除分 >= 65)
  var dangerNums=[];
  for(var n=1;n<=39;n++){var sc=calcExcludeScore(key,n);if(sc>=65)dangerNums.push({n:n,sc:sc});}
  dangerNums.sort(function(a,b){return b.sc-a.sc;});
  var top3=dangerNums.slice(0,3);
  // Smart-5 avg danger pct
  var sc0=[];
  for(var n2=1;n2<=39;n2++){
    var nd2=nhd[n2]||nhd[''+n2]||{};
    sc0.push({n:n2,safe:100-(nd2.danger_pct||0)*0.6-(nd2.recent_freq||0)*8*0.4,dp:nd2.danger_pct||0});
  }
  sc0.sort(function(a,b){return b.safe-a.safe;});
  var smart5=sc0.slice(0,5);
  var avgDp=smart5.length?Math.round(smart5.reduce(function(s,x){return s+x.dp;},0)/smart5.length):50;
  // Best strategy: lowest avg exclude score
  function _stratAvg(strat){
    var pool=[];
    if(strat==='conservative'){
      for(var n3=1;n3<=39;n3++)pool.push({sc:calcExcludeScore(key,n3)});
      pool.sort(function(a,b){return a.sc-b.sc;});return pool.slice(0,5).reduce(function(s,x){return s+x.sc;},0)/5;
    }else if(strat==='balanced'){
      for(var n4=1;n4<=39;n4++){
        var nd4=nhd[n4]||nhd[''+n4]||{};
        if(nd4.in_latest_draw||(nd4.recent_freq||0)>=4)continue;
        pool.push({sc:calcExcludeScore(key,n4)});
      }
      pool.sort(function(a,b){return a.sc-b.sc;});
      return pool.length?pool.slice(0,5).reduce(function(s,x){return s+x.sc;},0)/Math.min(pool.length,5):999;
    }else{
      var seen={};
      pd2.forEach(function(p){if(p.ref_numbers)p.ref_numbers.forEach(function(n5){
        if(!seen[n5]){seen[n5]=true;var s2=calcExcludeScore(key,n5);if(s2<=65)pool.push({sc:s2});}
      });});
      pool.sort(function(a,b){return a.sc-b.sc;});
      return pool.length?pool.slice(0,5).reduce(function(s,x){return s+x.sc;},0)/Math.min(pool.length,5):999;
    }
  }
  var stratScores=[
    {label:'保守',avg:_stratAvg('conservative')},
    {label:'均衡',avg:_stratAvg('balanced')},
    {label:'冷門',avg:_stratAvg('cold')}
  ];
  stratScores.sort(function(a,b){return a.avg-b.avg;});
  var bestStrat=stratScores[0].label;
  // Gap info
  var gapData=window._GAP_DATA&&window._GAP_DATA[key];
  var gapTxt='資料完整度正常';
  if(gapData&&gapData.count>0){
    gapTxt='有缺期'+(gapData.affectsRecent?'（影響近300期）':'（不影響近300期）');
  }
  // Risk label
  var riskTxt=avgDp<35?'整體風險低':avgDp<55?'整體風險中低':avgDp<70?'整體風險中高':'建議保守觀望';
  var dangerStr=top3.length?'，避免 '+top3.map(function(x){return x.n<10?'0'+x.n:''+x.n;}).join('/'):'';
  var msg='今日建議：'+bestStrat+'推薦較佳'+dangerStr+'，'+gapTxt+'，'+riskTxt+'。';
  el.innerHTML='<div class="pk-daily-txt">📌 今日決策總覽<br>'
    +'<span style="font-size:.62rem;font-weight:500;color:#0c4a6e">'+msg+'</span></div>';
}

/* ── Bet log filter (v9.9) ── */
function _setBetFilter(key,f){
  _betLogFilter[key]=f;
  var bar=document.getElementById('bet-filter-bar-'+key);
  if(bar){
    bar.querySelectorAll('.bet-filter-btn').forEach(function(btn){
      btn.classList.toggle('active',btn.dataset.f===f);
    });
  }
  renderBetLog(key);
}

/* ── Adopt smart recommendations by strategy (v9.8) ── */
function _adoptRec(key,strategy){
  var nhd=window._NUM_HIST_DATA&&window._NUM_HIST_DATA[key]||{};
  if(!Object.keys(nhd).length){alert('智能推薦數據未載入，請等待或刷新');return;}
  var pd2=window._PERIOD_DATA&&window._PERIOD_DATA[key]||[];
  var selected=[];
  // Feature 7: detect if 'smart' strategy is weakening (近期趨勢修正)
  var mbtList=window._MULTI_BT_DATA&&window._MULTI_BT_DATA[key]||[];
  var smartEntry=null;
  for(var mi=0;mi<mbtList.length;mi++){if(mbtList[mi].id==='smart'){smartEntry=mbtList[mi];break;}}
  var isWeakening=smartEntry&&smartEntry.recent_100_rate!=null
    &&(smartEntry.recent_100_rate<smartEntry.win_rate-5);
  function _fillConservative(exclude){
    var all=[];
    for(var n=1;n<=39;n++){
      if(exclude&&exclude.indexOf(n)!==-1)continue;
      all.push({n:n,score:calcExcludeScore(key,n)});
    }
    all.sort(function(a,b){return a.score-b.score;});
    return all;
  }
  if(strategy==='conservative'){
    var sc=_fillConservative(null);
    if(isWeakening){
      // Also exclude neighbor-of-latest numbers when weakening
      sc=sc.filter(function(s){var nd3=nhd[s.n]||nhd[''+s.n]||{};return !nd3.is_neighbor;});
    }
    selected=sc.slice(0,5).map(function(s){return s.n;});
    _pickerStrategySource[key]=isWeakening?'保守↓':'保守';
  }else if(strategy==='balanced'){
    var sc2=[];
    for(var n2=1;n2<=39;n2++){
      var nd2=nhd[n2]||nhd[''+n2]||{};
      if(nd2.in_latest_draw)continue;
      if((nd2.recent_freq||0)>=4)continue;
      if(isWeakening&&(nd2.current_miss||0)<=5)continue;
      sc2.push({n:n2,score:calcExcludeScore(key,n2)});
    }
    sc2.sort(function(a,b){return a.score-b.score;});
    selected=sc2.slice(0,5).map(function(s){return s.n;});
    if(selected.length<5){
      var fallback=_fillConservative(selected);
      while(selected.length<5&&fallback.length){selected.push(fallback.shift().n);}
    }
    _pickerStrategySource[key]=isWeakening?'均衡↓':'均衡';
  }else if(strategy==='cold'){
    var seen={};
    var coldPool=[];
    pd2.forEach(function(p){
      if(p.ref_numbers){
        p.ref_numbers.forEach(function(n3){
          if(!seen[n3]){
            seen[n3]=true;
            var sc3=calcExcludeScore(key,n3);
            if(sc3<=65)coldPool.push({n:n3,score:sc3});
          }
        });
      }
    });
    coldPool.sort(function(a,b){return a.score-b.score;});
    selected=coldPool.slice(0,5).map(function(s){return s.n;});
    if(selected.length<5){
      var fallback2=_fillConservative(selected);
      while(selected.length<5&&fallback2.length){selected.push(fallback2.shift().n);}
    }
    _pickerStrategySource[key]='冷門';
  }
  _pickerSel[key]=selected;
  _refreshPickerUI(key);
  renderSelectionRiskSummary(key);
  renderDailyDecisionSummary(key);
}

/* ── CSV export (v9.0) ── */
function exportBetLogCSV(key){
  var sk='betLog_'+key;
  var log=[];
  try{log=JSON.parse(localStorage.getItem(sk)||'[]');}catch(e){}
  if(!log.length){alert('目前無選號紀錄可匯出');return;}
  var recentDraws=(window._RECENT_DATA&&window._RECENT_DATA[key])||[];
  var drawData=_drawDataFor(key);
  var rows=['對應日期,基準日期,號碼,單雙,色球,策略來源,備註,勝負,中獎數'];
  log.forEach(function(entry){
    var found=_findDrawForEntry(key,entry,drawData);
    var ts=found.targetDate||_entryTargetDate(entry);
    var baseTs=_entryBaseDate(entry);
    var nums=entry.nums.join(' ');
    var oeStr=entry.oe?(entry.oe.odd+'單'+entry.oe.even+'雙'):'';
    var colStr=entry.col?('紅'+entry.col.red+'藍'+entry.col.blue+'綠'+entry.col.green):'';
    var stratSrc=entry.strategySource||'手動';
    var note=(entry.note||'').replace(/,/g,'，');
    var matchDraw=found.draw;
    var result='等待開獎',hitCnt='';
    if(matchDraw){
      var hits=entry.nums.filter(function(n){return matchDraw.numbers.indexOf(n)!==-1;}).length;
      result=hits===0?'勝':'敗';
      hitCnt=String(hits);
    }
    rows.push([ts,baseTs,nums,oeStr,colStr,stratSrc,note,result,hitCnt].join(','));
  });
  var csv='﻿'+rows.join('\n');
  var blob=new Blob([csv],{type:'text/csv;charset=utf-8'});
  var url=URL.createObjectURL(blob);
  var a=document.createElement('a');
  a.href=url;a.download='betlog_'+key+'_'+new Date().toISOString().slice(0,10)+'.csv';
  document.body.appendChild(a);a.click();document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

/* ── Miss distribution histogram (v9.0) ── */
function _renderMissDist(key){
  var missData=window._MISS_DATA&&window._MISS_DATA[key];
  if(!missData)return;
  // Build frequency distribution of current miss values across all 39 numbers
  var buckets={};
  for(var n=1;n<=39;n++){
    var mv=missData[n]!==undefined?missData[n]:(missData[''+n]||0);
    var bucket=Math.floor(mv/5)*5; // group by 5s: 0-4, 5-9, 10-14...
    buckets[bucket]=(buckets[bucket]||0)+1;
  }
  var el=document.getElementById('miss-dist-'+key);
  if(!el)return;
  var keys=Object.keys(buckets).map(Number).sort(function(a,b){return a-b;});
  var maxCount=Math.max.apply(null,Object.values(buckets));
  el.innerHTML=keys.map(function(k){
    var cnt=buckets[k];
    var pct=Math.round(cnt/maxCount*100);
    var label=k+'~'+(k+4);
    return '<div style="display:flex;align-items:center;gap:.3rem;margin-bottom:.15rem">'
      +'<span style="min-width:40px;font-size:.62rem;color:#475569;text-align:right">'+label+'期</span>'
      +'<div style="flex:1;height:10px;background:#f1f5f9;border-radius:5px;overflow:hidden">'
      +'<div style="width:'+pct+'%;height:100%;background:#4338ca;border-radius:5px;transition:width .4s"></div>'
      +'</div>'
      +'<span style="min-width:18px;font-size:.62rem;font-weight:700;color:#334155">'+cnt+'</span>'
      +'</div>';
  }).join('');
}

/* ── Init ── */
(function(){
  // Restore sidebar wide state
  try{
    if(localStorage.getItem('pickerSidebarWide')==='1'){
      document.body.classList.add('sidebar-wide');
      var btn=document.getElementById('sidebar-wide-btn');
      if(btn)btn.textContent='⇔ 縮窄';
    }
  }catch(e){}
  if(!IS_SERVER_MODE){
    var s=document.getElementById('btn-scrape');if(s)s.disabled=true;
    var m=document.getElementById('btn-manual');if(m)m.disabled=true;
    var body=document.getElementById('upd-body');
    if(body){
      var w=document.createElement('div');w.className='warn-box';
      w.innerHTML='⚠️ 靜態模式：更新功能需啟動伺服器。請執行 <code>python lottery_analyzer.py --serve</code> 後訪問 <strong>http://localhost:5000</strong>';
      body.insertBefore(w,body.firstChild);
    }
    document.querySelectorAll('.tm-bar input[type="date"]').forEach(function(i){i.disabled=true;});
    document.querySelectorAll('.tm-bar .btn-tm').forEach(function(b){b.disabled=true;});
    document.querySelectorAll('.tm-mode-badge').forEach(function(el){
      el.textContent='需 --serve 模式';
      el.style.cssText='background:#fee2e2;color:#b91c1c;border-radius:.3rem;padding:.15rem .5rem;font-size:.68rem;font-weight:700';
    });
  }
  var saved=null;
  try{saved=localStorage.getItem('lotteryTabV85');}catch(e){}
  var btns=Array.from(document.querySelectorAll('.tab-btn:not([disabled])'));
  if(!btns.length)return;
  var target=saved?btns.find(function(b){return b.id==='tab-'+saved;}):null;
  (target||btns[0]).click();
})();
"""


# ============================================================
# SECTION 8 — LOTTERY ANALYZER  (Orchestrator)
# ============================================================

class LotteryAnalyzer:

    def __init__(self, data_dir: Path = Path(".")):
        self.data_dir  = data_dir
        self.loader    = DataLoader(data_dir)
        self.period_an = PeriodRepetitionAnalyzer()
        self.miss_an   = MissValueAnalyzer()
        self.tail_an   = TailMissAnalyzer()
        self.consec_an  = ConsecutiveDrawAnalyzer()
        self.num_hist_an  = NumberHistoryAnalyzer()
        self.oe_color_an  = OEColorStatsAnalyzer()
        self.strategy_bt   = StrategyBacktester()
        self.multi_bt      = MultiStrategyBacktester()
        self.excl_tuner    = ExcludeScoreTuner()
        self.heat_prob_an  = RecentHeatProbabilityAnalyzer()
        self.reporter      = HTMLReportGenerator()

    def analyze_all(self, verbose: bool = True) -> Dict:
        results: Dict[str, Any] = {}
        min_rec = MAX_T + 10
        for key, cfg in LOTTERY_CONFIG.items():
            if verbose:
                print(f"\n▶ {cfg['name']}")
            df = self.loader.load(key, cfg)
            if df is None or len(df) < min_rec:
                if df is not None and verbose:
                    print(f"  [SKIP] 資料不足（{len(df)} 筆，需 ≥ {min_rec} 筆）")
                results[key] = None
                continue
            pr  = self.period_an.analyze(df, MAX_T)
            mr  = self.miss_an.analyze(df, cfg["pool_size"])
            tr  = self.tail_an.analyze(df)
            cr  = self.consec_an.analyze(df, cfg["pool_size"])
            nhr = self.num_hist_an.analyze(df, cfg["pool_size"])
            oec = self.oe_color_an.analyze(df, cfg["pick_count"])
            sbt = self.strategy_bt.backtest(df, cfg["pool_size"], lookback=500)
            mbt = self.multi_bt.backtest_all(df, cfg["pool_size"], lookback=300)
            tun = self.excl_tuner.tune(df, cfg["pool_size"], lookback=300)
            rhp = self.heat_prob_an.analyze(df, cfg["pool_size"])
            gaps = self.loader.detect_gaps(df, cfg.get("draws_per_day", 1))
            results[key] = {
                "config":            cfg,
                "record_count":      len(df),
                "period_result":     pr,
                "miss_result":       mr,
                "tail_result":       tr,
                "consec_result":     cr,
                "num_history":       nhr,
                "oe_color_stats":    oec,
                "strategy_bt":       sbt,
                "multi_bt":          mbt,
                "excl_tune":         tun,
                "recent_heat_prob":  rhp,
                "data_gaps":         gaps,
                "min_date":          df["date"].min().strftime("%Y-%m-%d"),
                "max_date":          df["date"].max().strftime("%Y-%m-%d"),
            }
            if verbose:
                print(f"  [OK] 完成（{len(MISS_WINDOWS)} 個回測視窗）")
        return results

    def analyze_for_date(self, key: str, cutoff_date: pd.Timestamp) -> Optional[Dict]:
        """時光機：以 cutoff_date 為截止點重新計算"""
        cfg = LOTTERY_CONFIG[key]
        df  = self.loader.load_silent(key, cfg, cutoff_date=cutoff_date)
        if df is None or len(df) < MAX_T + 10:
            return None
        pr  = self.period_an.analyze(df, MAX_T)
        mr  = self.miss_an.analyze(df, cfg["pool_size"])
        tr  = self.tail_an.analyze(df)
        cr  = self.consec_an.analyze(df, cfg["pool_size"])
        nhr = self.num_hist_an.analyze(df, cfg["pool_size"])
        oec = self.oe_color_an.analyze(df, cfg["pick_count"])
        sbt = self.strategy_bt.backtest(df, cfg["pool_size"])
        mbt = self.multi_bt.backtest_all(df, cfg["pool_size"], lookback=300)
        tun = self.excl_tuner.tune(df, cfg["pool_size"], lookback=300)
        rhp = self.heat_prob_an.analyze(df, cfg["pool_size"])
        gaps = self.loader.detect_gaps(df, cfg.get("draws_per_day", 1))
        return {
            "config":            cfg,
            "record_count":      len(df),
            "period_result":     pr,
            "miss_result":       mr,
            "tail_result":       tr,
            "consec_result":     cr,
            "num_history":       nhr,
            "oe_color_stats":    oec,
            "strategy_bt":       sbt,
            "multi_bt":          mbt,
            "excl_tune":         tun,
            "recent_heat_prob":  rhp,
            "data_gaps":         gaps,
            "min_date":          df["date"].min().strftime("%Y-%m-%d"),
            "max_date":          df["date"].max().strftime("%Y-%m-%d"),
        }

    def run(self, output_path: Path = Path("index.html"), server_mode: bool = False) -> Dict:
        print("=" * 60)
        print("  彩票冷門篩選工具 v10.2")
        print(f"  資料目錄：{self.data_dir.resolve()}")
        print("=" * 60)
        results = self.analyze_all()
        valid = sum(1 for v in results.values() if v is not None)
        print(f"\n完成：{valid}/{len(LOTTERY_CONFIG)} 種彩票")
        if valid == 0:
            print("[ERROR] 找不到任何資料，請確認 CSV 存在或用 --demo 生成示範資料")
        else:
            self.reporter.generate(results, output_path, server_mode)
        return results


# ============================================================
# SECTION 9 — FLASK SERVER
# ============================================================

def _create_flask_app(data_dir: Path, output_path: Path, run_init: bool = True):
    try:
        from flask import Flask, jsonify, request, send_file
    except ImportError:
        print("[ERROR] pip install flask")
        sys.exit(1)

    app      = Flask(__name__)
    analyzer = LotteryAnalyzer(data_dir)
    writer   = DataWriter(data_dir)
    syncer   = AutoSyncManager(data_dir, writer)

    if run_init:
        # ── Auto-sync on startup ──────────────────────────────────
        print("\n[AUTO-SYNC] 自動偵測並補齊缺漏期數...")
        sync_report = syncer.sync_all()
        total_new = sum(v["new_count"] for v in sync_report.values())
        if total_new:
            print(f"[AUTO-SYNC] 共補齊 {total_new} 期資料")
        else:
            print("[AUTO-SYNC] 所有彩票資料已是最新")

        print("\n[INIT] 建立初始報告...")
        analyzer.run(output_path, server_mode=True)

    betlog_dir = data_dir.resolve() / "betlog_exports"

    def _write_betlog_files(key: str, entries: list) -> Tuple[Path, Path, int]:
        betlog_dir.mkdir(parents=True, exist_ok=True)
        csv_path = betlog_dir / f"betlog_{key}.csv"
        json_path = betlog_dir / f"betlog_{key}.json"
        saved_at = datetime.now().isoformat(timespec="seconds")

        rows = []
        for idx, raw in enumerate(entries if isinstance(entries, list) else []):
            if not isinstance(raw, dict):
                continue
            nums = raw.get("nums") or raw.get("numbers") or []
            clean_nums = []
            for n in nums[:5]:
                try:
                    clean_nums.append(int(n))
                except (TypeError, ValueError):
                    pass
            while len(clean_nums) < 5:
                clean_nums.append("")
            rows.append({
                "lottery": key,
                "index": idx + 1,
                "target_date": raw.get("targetDate", ""),
                "base_date": raw.get("baseDate", ""),
                "n1": clean_nums[0],
                "n2": clean_nums[1],
                "n3": clean_nums[2],
                "n4": clean_nums[3],
                "n5": clean_nums[4],
                "numbers": " ".join(str(n) for n in clean_nums if n != ""),
                "status": raw.get("status", ""),
                "hit_count": raw.get("hitCount", ""),
                "draw_numbers": raw.get("drawNumbers", ""),
                "strategy_source": raw.get("strategySource", ""),
                "odd": raw.get("odd", ""),
                "even": raw.get("even", ""),
                "red": raw.get("red", ""),
                "blue": raw.get("blue", ""),
                "green": raw.get("green", ""),
                "note": raw.get("note", ""),
                "auto_note": raw.get("autoNote", ""),
                "created_at": raw.get("createdAt", ""),
                "saved_at": saved_at,
            })

        payload = {
            "lottery": key,
            "count": len(rows),
            "saved_at": saved_at,
            "entries": entries if isinstance(entries, list) else [],
        }
        json_path.write_text(_json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        fields = [
            "lottery", "index", "target_date", "base_date",
            "n1", "n2", "n3", "n4", "n5", "numbers",
            "status", "hit_count", "draw_numbers", "strategy_source",
            "odd", "even", "red", "blue", "green",
            "note", "auto_note", "created_at", "saved_at",
        ]
        with csv_path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer_csv = _csv.DictWriter(fh, fieldnames=fields)
            writer_csv.writeheader()
            writer_csv.writerows(rows)
        return csv_path, json_path, len(rows)

    @app.route("/")
    def index():
        # 若報告檔不存在（雲端冷啟動），只做分析，不自動爬蟲
        if not output_path.exists():
            try:
                analyzer.run(output_path, server_mode=True)
            except Exception as e:
                print(f"[ERROR] init failed: {e}")
        response = send_file(str(output_path.resolve()))
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.route("/api/betlog/sync", methods=["POST"])
    def api_betlog_sync():
        data = request.get_json(force=True) or {}
        key = data.get("lottery", "")
        if key not in LOTTERY_CONFIG:
            return jsonify({"success": False, "message": "invalid lottery"}), 400
        entries = data.get("entries", [])
        if not isinstance(entries, list):
            return jsonify({"success": False, "message": "entries must be a list"}), 400
        csv_path, json_path, count = _write_betlog_files(key, entries)
        return jsonify({
            "success": True,
            "count": count,
            "csv": str(csv_path),
            "json": str(json_path),
        })

    @app.route("/api/scrape", methods=["POST"])
    def api_scrape():
        """手動觸發智能同步（同 auto-sync 邏輯）"""
        try:
            sync_rpt = syncer.sync_all()
            details  = []
            rebuilt  = False
            for key, info in sync_rpt.items():
                cfg = LOTTERY_CONFIG[key]
                ok  = "爬蟲無回應" not in info["message"] and "封鎖" not in info["message"]
                if info["new_count"] > 0:
                    rebuilt = True
                details.append({
                    "ok":  ok,
                    "msg": f"{cfg['short_name']}：{info['message']}（落後 {info['gap_days']} 天）",
                })
            try:
                analyzer.run(output_path, server_mode=True)
                rebuilt = True
            except Exception as e:
                print(f"[ERROR] analyzer.run after sync: {e}")
            return jsonify({"rebuilt": rebuilt, "details": details})
        except Exception as e:
            import traceback
            print(f"[ERROR] api_scrape: {traceback.format_exc()}")
            return jsonify({
                "status":  "error",
                "message": str(e),
                "rebuilt": False,
                "details": [{"ok": False, "msg": f"同步失敗：{e}"}],
            }), 500

    @app.route("/api/manual", methods=["POST"])
    def api_manual():
        data    = request.get_json(force=True)
        key     = data.get("lottery", "")
        date_s  = data.get("date", "")
        numbers = data.get("numbers", [])
        if key not in LOTTERY_CONFIG:
            return jsonify({"success": False, "message": "無效的彩票類型"})
        if len(numbers) != 5:
            return jsonify({"success": False, "message": "必須提供 5 個號碼"})
        cfg = LOTTERY_CONFIG[key]
        ok  = writer.append(key, cfg, date_s, [int(n) for n in numbers])
        if ok:
            analyzer.run(output_path, server_mode=True)
            return jsonify({"success": True, "message": "已儲存並更新報告"})
        return jsonify({"success": False, "message": "儲存失敗（日期可能重複）"})

    @app.route("/api/backtest", methods=["POST"])
    def api_backtest():
        """時光機：以任意歷史日期為截止點重新計算"""
        req_data = request.get_json(force=True)
        key      = req_data.get("lottery", "")
        date_str = req_data.get("date", "")

        if key not in LOTTERY_CONFIG:
            return jsonify({"success": False, "message": "無效的彩票類型"})

        def _make_backtest_response(result_data, is_hist=False, hist_date=""):
            rg  = HTMLReportGenerator()
            # Full panel inner HTML — replaces panel-{key}.innerHTML atomically.
            # data_script is intentionally excluded: scripts injected via innerHTML
            # do not execute; JS globals are restored via the data fields below.
            panel_inner_html = rg._build_panel_inner(
                key, result_data, server_mode=True,
                is_hist=is_hist, hist_date=hist_date
            )
            nhr         = result_data.get("num_history", {})
            oec         = result_data.get("oe_color_stats", {})
            sbt         = result_data.get("strategy_bt", {})
            rhp         = result_data.get("recent_heat_prob", {})
            miss_data   = {str(k): v
                           for k, v in result_data["miss_result"]["current_misses"].items()}
            panel_recent_data = result_data["period_result"].get("recent_8", [])
            draw_data   = result_data["period_result"].get("recent_match", panel_recent_data)
            recent_data = draw_data
            period_data = result_data["period_result"].get("top8_lowest", [])
            # Serialize rhp numbers with str keys for JS
            rhp_nums    = {str(k): v for k, v in (rhp or {}).get("numbers", {}).items()}
            rev_oe_resp = _compute_rev_oe(result_data["period_result"].get("recent_8", []))
            resp = {
                "success":          True,
                "panel_inner_html": panel_inner_html,  # replaces #panel-{key} innerHTML
                "miss_data":        miss_data,
                "recent_data":      recent_data,
                "draw_data":        draw_data,
                "period_data":      period_data,
                "num_hist_data":    {str(k): v for k, v in nhr.items()},
                "oe_color_data":    oec,
                "strat_data":       sbt,
                "heat_prob_data":   rhp_nums,
                "multi_bt_data":    result_data.get("multi_bt", []),
                "rev_oe_data":      rev_oe_resp or {},
            }
            if not is_hist:
                resp["max_date"] = result_data["max_date"]
            return jsonify(resp)

        # "latest" → 用最新資料
        if date_str == "latest":
            result_data = analyzer.analyze_all(verbose=False).get(key)
            if not result_data:
                return jsonify({"success": False, "message": "無法載入資料"})
            return _make_backtest_response(result_data, is_hist=False)

        cutoff = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(cutoff):
            return jsonify({"success": False, "message": "無效日期格式（請用 YYYY-MM-DD）"})

        result_data = analyzer.analyze_for_date(key, cutoff)
        if result_data is None:
            return jsonify({"success": False,
                            "message": f"截止日 {date_str} 前的資料不足（需至少 {MAX_T+10} 期）"})

        return _make_backtest_response(result_data, is_hist=True, hist_date=date_str)

    return app


def start_server(data_dir: Path, output_path: Path, port: int = 5000) -> None:
    flask_app = _create_flask_app(data_dir, output_path, run_init=True)
    PORT = int(os.environ.get("PORT", port))
    print(f"\n伺服器啟動：http://0.0.0.0:{PORT}")
    print("按 Ctrl+C 停止\n")
    flask_app.run(host="0.0.0.0", port=PORT, debug=False)


# ============================================================
# SECTION 10 — DEMO DATA GENERATOR
# ============================================================

def generate_demo_data(data_dir: Path) -> None:
    """產生示範資料並批量 upsert 至 Supabase lottery_draws 表（每批 1000 筆）。"""
    rng  = np.random.default_rng(42)
    base = pd.Timestamp("2010-01-01")
    specs = [
        ("taiwan_539",          3000),
        ("michigan_fantasy5",   2000),
        ("california_fantasy5", 2500),
        ("newyork_take5",       2000),
    ]
    api_url = f"{_supa_base()}/lottery_draws?on_conflict=lottery_type,draw_date"
    hdrs    = _supa_whdrs()
    for key, rows in specs:
        records = []
        for i in range(rows):
            dt   = (base + pd.Timedelta(days=i // 2)).strftime("%Y-%m-%d")
            nums = sorted((rng.choice(39, 5, replace=False) + 1).tolist())
            records.append({
                "lottery_type": key,
                "draw_date":    dt,
                "num1": nums[0], "num2": nums[1], "num3": nums[2],
                "num4": nums[3], "num5": nums[4],
            })
        for start in range(0, len(records), 1000):
            r = _requests.post(api_url, headers=hdrs, json=records[start:start + 1000], timeout=60)
            r.raise_for_status()
        print(f"  [DEMO] {key}（{rows} 筆）→ Supabase")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="彩票冷門篩選工具 v10.2")
    parser.add_argument("--data-dir", "-d", default=".", help="資料目錄（預設：當前目錄）")
    parser.add_argument("--output",   "-o", default="index.html", help="輸出 HTML 路徑")
    parser.add_argument("--serve",    action="store_true", help="啟動 Flask 伺服器")
    parser.add_argument("--port",     type=int, default=5000, help="伺服器埠號（預設 5000）")
    parser.add_argument("--demo",     action="store_true", help="生成示範資料後執行分析")
    parser.add_argument("--sync",     action="store_true", help="僅執行自動同步（不啟動伺服器）")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.is_dir():
        print(f"[ERROR] 目錄不存在：{data_dir}")
        sys.exit(1)

    if args.demo:
        print("▶ 生成示範資料...")
        generate_demo_data(data_dir)

    if args.sync:
        writer = DataWriter(data_dir)
        syncer = AutoSyncManager(data_dir, writer)
        syncer.sync_all()
        sys.exit(0)

    if args.serve:
        start_server(data_dir, Path(args.output), args.port)
    else:
        LotteryAnalyzer(data_dir).run(Path(args.output), server_mode=False)

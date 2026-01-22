# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v3.0.1 (Sponsor 即時Tick版)"
UPDATE_LOG = """
- v3.0.0: 架構升級為 FinMind Sponsor。
- v3.0.1: 修正 API 端點錯誤。
  1. 改用 `taiwan_stock_tick_snapshot` (成交快照) 取代 daily_short。
  2. 這是 Sponsor 專用的即時報價 API，確保盤中能抓到最新價格 (deal_price)。
  3. 解決「現價為 0」的問題。
"""

# ==========================================
# 參數與 Token (Sponsor)
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷 (Sponsor Tick)", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_current_status():
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    # 08:45 ~ 13:30 視為盤中
    is_intraday = time(8, 45) <= current_time < time(13, 30)
    return tw_now, is_intraday

def get_trading_days(api):
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if df.empty: return []
        dates = sorted(df['date'].unique().tolist())
    except:
        return []
    
    tw_now, is_intraday = get_current_status()
    today_str = tw_now.strftime("%Y-%m-%d")
    
    if 0 <= tw_now.weekday() <= 4 and tw_now.time() >= time(8, 45):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
    return dates

def smart_get_column(df, candidates):
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

def fetch_finmind_snapshot(api, date_str):
    """ 
    [Sponsor 專用 - 修正版] 
    使用 taiwan_stock_tick_snapshot 抓取最新一筆成交 (Tick)。
    這才是盤中真正的即時價。
    """
    try:
        # stock_id="" 代表抓全市場最新一筆 Tick
        df = api.taiwan_stock_tick_snapshot(stock_id="")
        
        if df.empty: 
            return {}, None
        
        # 建立快速查詢表 {stock_id: deal_price}
        code_col = smart_get_column(df, ['stock_id', 'code'])
        # Tick API 的價格欄位通常是 'deal_price'
        price_col = smart_get_column(df, ['deal_price', 'price', 'close'])
        
        if code_col is None or price_col is None:
            return {}, None
            
        snapshot_map = dict(zip(code_col, price_col))
        
        # 取得資料時間
        time_col = smart_get_column(df, ['time', 'date'])
        last_time = "即時"
        if time_col is not None:
            last_time = time_col.iloc[-1]
            
        return snapshot_map, last_time
    except Exception as e:
        st.error(f"Snapshot Error: {e}")
        return {}, None

def get_rank_list(api, date_str, backup_date=None):
    try:
        df_rank = api.taiwan_stock_daily(stock_id="", start_date=date_str)
        if df_rank.empty and backup_date:
            df_rank = api.taiwan_stock_daily(stock_id="", start_date=backup_date)
        if df_rank.empty: return []

        df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
        df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
        
        df_rank['ID'] = df_rank['ID'].astype(str)
        df_rank = df_rank[df_rank['ID'].str.len() == 4]
        df_rank = df_rank[df_rank['ID'].str.isdigit()]
        for prefix in EXCLUDE_PREFIXES:
            df_rank = df_rank[~df_rank['ID'].str.startswith(prefix)]
            
        df_candidates = df_rank.sort_values('Money', ascending=False).head(TOP_N)
        return df_candidates['ID'].tolist()
    except:
        return []

# === 運算邏輯 ===

def calc_stats_finmind_only(_api, target_date, rank_codes, use_realtime=False):
    hits = 0
    valid = 0
    details = []
    
    # 若需即時，抓全市場 Tick 快照
    snapshot_map = {}
    last_t = None
    if use_realtime:
        snapshot_map, last_t = fetch_finmind_snapshot(_api, target_date)
    
    for i, code in enumerate(rank_codes):
        rank = i + 1
        current_price = 0
        status = "未知"
        price_src = "歷史"
        
        # 1. 決定價格
        if use_realtime:
            current_price = snapshot_map.get(code, 0)
            if current_price > 0:
                price_src = "FinMind即時"
            else:
                status = "⚠️ 無即時價"
        
        try:
            # 2. 抓歷史 K 線 (抓到 D-1)
            # 這裡我們只抓歷史，不含今日，今日的資料用拼的
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            # 確保只保留 target_date 之前的資料 (不含 target_date)
            # 因為 target_date 是今天，我們要用歷史+即時價來算
            stock_df = stock_df[stock_df['date'] < target_date]
            
            # 3. 資料合成
            if use_realtime and current_price > 0:
                # 拼上今日即時價
                new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                stock_df = pd.concat([stock_df, new_row], ignore_index=True)
            elif not use_realtime:
                # 算 D-1 歷史模式 (其實這段應該用不到了，因為我們用另一組日期，但保留邏輯)
                # 這裡要確保包含 target_date (如果 target_date 是 yesterday)
                # 但上面的 filter 是 < target_date，這會導致 D-1 模式少一天
                # 修正：
                stock_df = _api.taiwan_stock_daily(
                    stock_id=code, 
                    start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                )
                stock_df = stock_df[stock_df['date'] <= target_date]

            # 4. 指標計算
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr = stock_df.iloc[-1]
                
                final_price = float(curr['close'])
                ma5 = float(curr['MA5'])
                
                is_ok = final_price > ma5
                
                if is_ok:
                    hits += 1
                    status = "✅ 通過"
                else:
                    status = f"📉 未通過 (MA5:{ma5:.1f})"
                
                valid += 1
                
                # 若是算歷史模式，current_price 要更新為歷史收盤價以便顯示
                if not use_realtime: current_price = final_price
                
            else:
                if status == "未知": status = "🚫 資料不足"
                
        except Exception as e:
            status = "❌ 錯誤"
        
        details.append({
            '排名': rank,
            '代號': code,
            '現價': current_price,
            '來源': price_src if use_realtime else "歷史收盤",
            '狀態': status
        })
        
    return hits, valid, details, last_t

@st.cache_data(ttl=60)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2: return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    # 1. 取得昨日排行 (基準)
    prev_rank_codes = get_rank_list(_api, d_prev_str, backup_date=all_days[-3])
    
    if not prev_rank_codes:
        st.error("無法取得排行資料")
        return None

    # 2. 計算昨日廣度 (固定不變)
    progress_bar = st.progress(0, text="計算昨日數據 (歷史鎖定)...")
    hit_prev, valid_prev, _, _ = calc_stats_finmind_only(_api, d_prev_str, prev_rank_codes, use_realtime=False)
    
    # 3. 決定今日名單 & 計算今日廣度
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中極速模式 (Sponsor Tick)"
    else:
        curr_rank_codes = get_rank_list(_api, d_curr_str)
        if curr_rank_codes:
            mode_msg = "🐢 盤後精準模式 (今日排行)"
        else:
            curr_rank_codes = prev_rank_codes
            mode_msg = "⚠️ 盤後模式 (排行未更新，沿用昨日)"
            
    progress_bar.progress(50, text=f"計算今日數據 ({mode_msg})...")
    hit_curr, valid_curr, details, last_time = calc_stats_finmind_only(_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
    progress_bar.empty()
    
    detail_df = pd.DataFrame(details)
    
    # 4. 斜率
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        if is_intraday:
            # 嘗試抓大盤 Tick
            twii_snap = _api.taiwan_stock_tick_snapshot(stock_id="TAIEX")
            if not twii_snap.empty:
                twii_price = float(twii_snap['deal_price'].iloc[-1])
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': twii_price}])
                twii_df = pd.concat([twii_df, new_row], ignore_index=True)
                 
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except: pass
    
    br_prev = hit_prev / valid_prev if valid_prev > 0 else 0
    br_curr = hit_curr / valid_curr if valid_curr > 0 else 0

    return {
        "d_curr": d_curr_str,
        "d_prev": d_prev_str,
        "br_curr": br_curr, "br_prev": br_prev,
        "hit_curr": hit_curr, "valid_curr": valid_curr,
        "hit_prev": hit_prev, "valid_prev": valid_prev,
        "slope": slope,
        "detail_df": detail_df,
        "mode_msg": mode_msg,
        "last_time": last_time
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v3.0.1 Sponsor)")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("Sponsor Token 已啟用")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    api = DataLoader()
    api.login_by_token(API_TOKEN)

    if st.button("🔄 立即重新整理"):
        st.cache_data.clear()

    try:
        data = fetch_data(api)
            
        if data is None:
            st.warning("⚠️ 暫無有效數據")
        else:
            cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
            cond2 = data['slope'] > 0
            final_decision = cond1 and cond2
            
            t_str = str(data['last_time']) if data['last_time'] else "未知"

            st.subheader(f"📅 基準日：{data['d_curr']}")
            st.caption(f"昨日基準: {data['d_prev']}")
            st.success(f"📌 {data['mode_msg']}")
            
            c1, c2, c3 = st.columns(3)
            c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid_curr']}")
            c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid_prev']}")
            c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

            st.divider()
            if final_decision:
                st.success(f"✅ 結論：可進場")
            else:
                st.error(f"⛔ 結論：不可進場")
            
            st.caption(f"FinMind Tick 時間: {t_str}")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

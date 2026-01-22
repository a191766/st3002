# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import time as time_module  # 引入時間模組做延遲控制

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v3.1.0 (Sponsor 穩定防爆版)"
UPDATE_LOG = """
- v3.0.1: Sponsor 即時 Tick 版。
- v3.1.0: 解決大量請求導致的後段報錯問題。
  1. 新增「失敗重試 (Retry)」機制：若抓取失敗，自動冷靜 1 秒後重試。
  2. 新增「微量延遲」：每檔間隔 0.02 秒，避免瞬間流量過大被伺服器阻擋。
  3. 恢復顯示詳細錯誤訊息，方便除錯。
"""

# ==========================================
# 參數與 Token (Sponsor)
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷 (Sponsor)", layout="wide")

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
    """ [Sponsor] 全市場即時成交快照 """
    try:
        df = api.taiwan_stock_tick_snapshot(stock_id="")
        if df.empty: return {}, None
        
        code_col = smart_get_column(df, ['stock_id', 'code'])
        price_col = smart_get_column(df, ['deal_price', 'price', 'close'])
        
        if code_col is None or price_col is None: return {}, None
            
        snapshot_map = dict(zip(code_col, price_col))
        
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

# === 運算邏輯 (含重試機制) ===

def get_history_with_retry(_api, code, start_date, max_retries=1):
    """ 
    包裝過的歷史資料抓取函式
    如果失敗，會等待 1 秒後重試一次
    """
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=start_date
            )
            return df, None # 成功
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                time_module.sleep(1.0) # 冷靜 1 秒
            else:
                return None, last_err

def calc_stats_finmind_only(_api, target_date, rank_codes, use_realtime=False):
    hits = 0
    valid = 0
    details = []
    
    # 抓全市場 Tick 快照
    snapshot_map = {}
    last_t = None
    if use_realtime:
        snapshot_map, last_t = fetch_finmind_snapshot(_api, target_date)
    
    start_date_query = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    # === 進度條 ===
    prog_bar = st.progress(0, text="逐檔分析中...")
    total = len(rank_codes)

    for i, code in enumerate(rank_codes):
        # 1. 微量延遲 (Pacing)：防止瞬間請求過多被 Ban
        time_module.sleep(0.02) 
        
        # 更新進度條 (每10檔更新一次，減少介面重繪負擔)
        if i % 10 == 0:
            prog_bar.progress((i / total), text=f"分析進度: {i+1}/{total}")

        rank = i + 1
        current_price = 0
        status = "未知"
        price_src = "歷史"
        
        # 取得即時價
        if use_realtime:
            current_price = snapshot_map.get(code, 0)
            if current_price > 0:
                price_src = "FinMind即時"
            else:
                status = "⚠️ 無即時價"
        
        # 抓取歷史 (含重試機制)
        stock_df, err = get_history_with_retry(_api, code, start_date_query)
        
        if stock_df is None:
            # 即使重試後還是失敗
            status = f"❌ 錯誤 ({str(err)})"
        else:
            try:
                # 處理資料
                stock_df = stock_df[stock_df['date'] < target_date]
                
                # 合成
                if use_realtime and current_price > 0:
                    new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                    stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                elif not use_realtime:
                    # 補抓一次確保包含今日(若為計算歷史) - 其實上面已經抓夠了，這裡只是切割
                    pass

                # 計算 MA5
                if len(stock_df) >= 6:
                    stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                    curr = stock_df.iloc[-1]
                    
                    final_price = float(curr['close'])
                    ma5 = float(curr['MA5'])
                    
                    if final_price > ma5:
                        hits += 1
                        status = "✅ 通過"
                    else:
                        status = f"📉 未通過 (MA5:{ma5:.1f})"
                    
                    valid += 1
                    if not use_realtime: current_price = final_price
                else:
                    if status == "未知": status = "🚫 資料不足"
                    
            except Exception as inner_e:
                status = f"❌ 運算錯 ({str(inner_e)})"
        
        details.append({
            '排名': rank,
            '代號': code,
            '現價': current_price,
            '來源': price_src if use_realtime else "歷史收盤",
            '狀態': status
        })
    
    prog_bar.empty()
    return hits, valid, details, last_t

@st.cache_data(ttl=60)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2: return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    prev_rank_codes = get_rank_list(_api, d_prev_str, backup_date=all_days[-3])
    if not prev_rank_codes:
        st.error("無法取得排行資料")
        return None

    # 計算昨日 (歷史)
    hit_prev, valid_prev, _, _ = calc_stats_finmind_only(_api, d_prev_str, prev_rank_codes, use_realtime=False)
    
    # 計算今日
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
            
    hit_curr, valid_curr, details, last_time = calc_stats_finmind_only(_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
    detail_df = pd.DataFrame(details)
    
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        if is_intraday:
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
    st.title("📈 盤中權證進場判斷 (v3.1.0 穩定版)")

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

# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import yfinance as yf

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v3.4.0 (邏輯修正+極致快取)"
UPDATE_LOG = """
- v3.3.0: 智慧快取名單。
- v3.4.0: 修復日期切割邏輯 & 全面快取歷史運算。
  1. 【Bug修復】修正歷史回測邏輯。昨日廣度現在會正確包含「昨日」K線 (<= date)，不再誤切導致數據錯誤。
  2. 【極致快取】將「昨日廣度」的**運算結果**也納入 24H 快取。重新整理時，昨日數據直接秒出，完全不消耗 FinMind 額度。
  3. 【效能優化】現在只有「今日即時盤」會真正去呼叫 API，效率最大化。
"""

# ==========================================
# 參數與 Token (Sponsor)
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷 (極致快取)", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_current_status():
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    # 08:45 ~ 13:30 視為盤中
    is_intraday = time(8, 45) <= current_time < time(13, 30)
    return tw_now, is_intraday

def get_trading_days_robust(api):
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if not df.empty:
            return sorted(df['date'].unique().tolist())
    except:
        pass 
    
    dates = []
    tw_now, _ = get_current_status()
    check_day = tw_now
    while len(dates) < 5:
        if check_day.weekday() <= 4:
            dates.append(check_day.strftime("%Y-%m-%d"))
        check_day -= timedelta(days=1)
    return sorted(dates)

def smart_get_column(df, candidates):
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

# === 快取函式 1: 取得排行榜名單 (24H) ===
@st.cache_data(ttl=86400, show_spinner=False)
def get_cached_rank_list(token, date_str, backup_date=None):
    local_api = DataLoader()
    local_api.login_by_token(token)
    try:
        df_rank = local_api.taiwan_stock_daily(stock_id="", start_date=date_str)
        if df_rank.empty and backup_date:
            df_rank = local_api.taiwan_stock_daily(stock_id="", start_date=backup_date)

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

# === 報價源擷取 ===
def fetch_finmind_snapshot(api):
    try:
        df = api.taiwan_stock_tick_snapshot(stock_id="")
        if df.empty: return {}, None
        
        code_col = smart_get_column(df, ['stock_id', 'code'])
        price_col = smart_get_column(df, ['deal_price', 'price', 'close'])
        
        if code_col is None or price_col is None: return {}, None
        
        # 轉成 dict
        snapshot_map = dict(zip(code_col, price_col))
        
        time_col = smart_get_column(df, ['time', 'date'])
        last_time = "FinMind即時"
        if time_col is not None:
            last_time = time_col.iloc[-1]
            
        return snapshot_map, last_time
    except:
        return {}, None

def fetch_yahoo_realtime_batch(codes):
    if not codes: return {}
    all_tickers = [f"{c}.TW" for c in codes] + [f"{c}.TWO" for c in codes]
    try:
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
        realtime_map = {}
        
        valid_tickers = []
        if isinstance(data.columns, pd.MultiIndex):
            valid_tickers = data.columns.levels[0]
        elif not data.empty:
            valid_tickers = [data.name] if hasattr(data, 'name') else []
            if len(all_tickers) == 1: valid_tickers = all_tickers

        if len(valid_tickers) == 0 and not data.empty and len(all_tickers) == 1:
             df = data
             if not df.empty and not df['Close'].isna().all():
                 realtime_map[codes[0]] = float(df['Close'].iloc[-1])
        else:
            for t in valid_tickers:
                try:
                    df = data[t] if isinstance(data.columns, pd.MultiIndex) else data
                    if df.empty or df['Close'].isna().all(): continue
                    realtime_map[t.split('.')[0]] = float(df['Close'].iloc[-1])
                except: continue
        return realtime_map
    except:
        return {}

# === 快取函式 2: 計算「歷史」廣度 (昨日) ===
# 這裡使用 @st.cache_data 鎖定 24H，因為昨日的歷史資料不會變
@st.cache_data(ttl=86400, show_spinner=False)
def calc_historical_stats_cached(token, target_date, rank_codes):
    """
    專門計算「歷史日期」的廣度。
    這部分完全不涉及即時價，只抓歷史 K 線。
    快取後，重新整理不會再消耗 API。
    """
    local_api = DataLoader()
    local_api.login_by_token(token)
    
    hits = 0
    valid = 0
    start_date_query = (datetime.now() - timedelta(days=35)).strftime("%Y-%m-%d") # 多抓幾天保險
    
    # 這裡不需要 progress bar，因為如果有 cache 瞬間就跑完，沒 cache 就讓它跑
    for i, code in enumerate(rank_codes):
        try:
            # 直接全速抓
            stock_df = local_api.taiwan_stock_daily(stock_id=code, start_date=start_date_query)
            
            if not stock_df.empty:
                # 【關鍵修正】歷史回測：要「包含」當天，所以用 <=
                stock_df = stock_df[stock_df['date'] <= target_date]
                
                if len(stock_df) >= 6:
                    stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                    curr = stock_df.iloc[-1]
                    # 再次確認日期是否對應 (防止 FinMind 資料缺失)
                    # 寬容度 3 天
                    last_dt = pd.to_datetime(curr['date']).strftime("%Y-%m-%d")
                    days_diff = (pd.to_datetime(target_date) - pd.to_datetime(last_dt)).days
                    
                    if 0 <= days_diff <= 3:
                        if curr['close'] > curr['MA5']:
                            hits += 1
                        valid += 1
        except:
            pass
            
    return hits, valid

# === 即時函式: 計算「今日」廣度 (不快取 or 短快取) ===
def calc_realtime_stats(_api, target_date, rank_codes):
    """
    計算「今日」廣度。
    需要：歷史資料 (< Today) + 即時資料 (Today)
    """
    hits = 0
    valid = 0
    details = []
    
    # 1. 準備即時價 (雙源)
    fm_map, fm_time = fetch_finmind_snapshot(_api)
    need_yahoo = False
    if not fm_map: need_yahoo = True
    
    yahoo_map = {}
    last_t = None
    if need_yahoo:
        yahoo_map = fetch_yahoo_realtime_batch(rank_codes)
        last_t = "Yahoo備援"
    else:
        last_t = fm_time

    # 2. 準備歷史資料
    start_date_query = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    prog_bar = st.progress(0, text="即時運算中...")
    total = len(rank_codes)

    for i, code in enumerate(rank_codes):
        if i % 20 == 0:
            prog_bar.progress((i / total), text=f"進度: {i+1}/{total}")
        
        rank = i + 1
        current_price = 0
        status = "未知"
        price_src = "歷史"
        
        # 取價
        if code in fm_map and fm_map[code] > 0:
            current_price = fm_map[code]
            price_src = "FinMind"
        elif need_yahoo and code in yahoo_map and yahoo_map[code] > 0:
            current_price = yahoo_map[code]
            price_src = "Yahoo"
            
        if current_price == 0:
            status = "⚠️ 無報價"

        try:
            stock_df = _api.taiwan_stock_daily(stock_id=code, start_date=start_date_query)
            
            if stock_df.empty:
                 status = "❌ 歷史無資料"
            else:
                # 【關鍵修正】即時盤：要「不含」當天 (騰出位子給即時價)，所以用 <
                stock_df = stock_df[stock_df['date'] < target_date]
                
                # 合成
                if current_price > 0:
                    new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                    stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                
                # 計算
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
                else:
                    if status == "未知": status = "🚫 資料不足"

        except Exception:
            status = "❌ 錯誤"
        
        details.append({
            '排名': rank,
            '代號': code,
            '現價': current_price,
            '來源': price_src,
            '狀態': status
        })
    
    prog_bar.empty()
    return hits, valid, details, last_t

# === 主流程 fetch_data (TTL=60s 只為了即時盤更新) ===
@st.cache_data(ttl=60)
def fetch_data(_api):
    all_days = get_trading_days_robust(_api)
    if len(all_days) < 2: 
        st.error("日期資料異常")
        return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    # 1. 取得昨日名單 (Cache 24H)
    prev_rank_codes = get_cached_rank_list(API_TOKEN, d_prev_str, backup_date=all_days[-3])
    if not prev_rank_codes:
        st.error("無法取得排行")
        return None

    # 2. 計算昨日廣度 (Cache 24H) - 【極致省流】
    # 這裡會直接讀 Cache，不會真的跑迴圈
    hit_prev, valid_prev = calc_historical_stats_cached(API_TOKEN, d_prev_str, prev_rank_codes)
    
    # 3. 計算今日廣度 (Realtime)
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中極速 (智慧快取啟動)"
    else:
        # 盤後抓今日排行 (Cache 24H)
        curr_rank_codes = get_cached_rank_list(API_TOKEN, d_curr_str)
        if curr_rank_codes:
            mode_msg = "🐢 盤後精準 (今日排行)"
        else:
            curr_rank_codes = prev_rank_codes
            mode_msg = "⚠️ 盤後 (沿用昨日)"
            
    hit_curr, valid_curr, details, last_time = calc_realtime_stats(_api, d_curr_str, curr_rank_codes)
    
    detail_df = pd.DataFrame(details)
    
    # 4. 斜率
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        if is_intraday:
            twii_p = 0
            try:
                twii_snap = _api.taiwan_stock_tick_snapshot(stock_id="TAIEX")
                if not twii_snap.empty: twii_p = float(twii_snap['deal_price'].iloc[-1])
            except: pass
            
            if twii_p == 0:
                try: 
                    t = yf.Ticker("^TWII")
                    hist = t.history(period="1d")
                    if not hist.empty: twii_p = float(hist['Close'].iloc[-1])
                except: pass
            
            if twii_p > 0:
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': twii_p}])
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
    st.title("📈 盤中權證進場判斷 (v3.4.0 極致快取)")

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
            st.warning("⚠️ 初始化失敗，請稍後再試")
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
            
            st.caption(f"報價來源時間: {t_str} (若 FinMind 無資料則自動切換 Yahoo)")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

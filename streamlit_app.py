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
APP_VERSION = "v2.5.1 (詳細狀態版)"
UPDATE_LOG = """
- v2.5.0: 邏輯回歸穩定。
- v2.5.1: 優化狀態顯示。
  現在表格會明確區分：
  1. 「📉 未通過」：資料有效，但股價跌破 MA5 (附註 MA5 價格)。
  2. 「🚫 剔除」：資料不足或無報價 (不計入分母)。
  3. 「✅ 通過」：股價站上 MA5。
"""

# ==========================================
# 參數與 Token
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_current_status():
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
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

def fetch_yahoo_realtime_batch(codes):
    if not codes: return {}
    all_tickers = [f"{c}.TW" for c in codes] + [f"{c}.TWO" for c in codes]
    try:
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=False)
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

def calc_yesterday_stats(_api, date_prev, rank_codes):
    """ 計算昨日 (D-1) """
    hits = 0
    valid = 0
    for code in rank_codes:
        try:
            stock_df = _api.taiwan_stock_daily(stock_id=code, start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
            stock_df = stock_df[stock_df['date'] <= date_prev]
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr = stock_df.iloc[-1]
                if curr['close'] > curr['MA5']: hits += 1
                valid += 1
        except: pass
    return hits, valid

def calc_today_stats(_api, date_curr, rank_codes):
    """ 計算今日 (D) 並回傳詳細原因 """
    hits = 0
    valid = 0
    details = []
    
    rt_map = fetch_yahoo_realtime_batch(rank_codes)
    
    for i, code in enumerate(rank_codes):
        current_price = rt_map.get(code, 0)
        rank = i + 1
        status = "未知"
        
        try:
            stock_df = _api.taiwan_stock_daily(stock_id=code, start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
            stock_df = stock_df[stock_df['date'] < date_curr]
            
            if current_price > 0:
                new_row = pd.DataFrame([{'date': date_curr, 'close': current_price}])
                stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                
                if len(stock_df) >= 6:
                    stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                    ma5_val = stock_df['MA5'].iloc[-1]
                    curr_close = stock_df['close'].iloc[-1]
                    
                    if curr_close > ma5_val:
                        hits += 1
                        valid += 1
                        status = "✅ 通過"
                    else:
                        valid += 1 # 雖未通過但資料有效，計入分母
                        status = f"📉 未通過 (MA5:{ma5_val:.1f})"
                else:
                    status = f"🚫 剔除 (資料不足K={len(stock_df)})"
            else:
                status = "⚠️ 剔除 (無即時價)"
                
        except Exception as e:
            status = f"❌ 錯誤 ({str(e)})"
            
        details.append({
            '排名': rank,
            '代號': code,
            '現價': current_price,
            '狀態': status
        })
        
    return hits, valid, details

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2: return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    # 1. 取得昨日排行 (基準)
    prev_rank_codes = get_rank_list(_api, d_prev_str, backup_date=all_days[-3])
    
    # 2. 計算昨日廣度
    if prev_rank_codes:
        hit_prev, valid_prev = calc_yesterday_stats(_api, d_prev_str, prev_rank_codes)
    else:
        hit_prev, valid_prev = 0, 0
        
    # 3. 決定今日名單 & 計算今日廣度
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中模式 (母體:昨日排行)"
    else:
        curr_rank_codes = get_rank_list(_api, d_curr_str)
        if curr_rank_codes:
            mode_msg = "🐢 盤後模式 (母體:今日排行)"
        else:
            curr_rank_codes = prev_rank_codes
            mode_msg = "⚠️ 盤後模式 (FinMind 未更新，沿用昨日排行)"
            
    progress_bar = st.progress(0, text=f"分析中 ({mode_msg})...")
    hit_curr, valid_curr, details = calc_today_stats(_api, d_curr_str, curr_rank_codes)
    progress_bar.empty()
    
    detail_df = pd.DataFrame(details)
    
    # 4. 斜率
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
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
        "mode_msg": mode_msg
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v2.5.1 詳細狀態)")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("API Token 已載入")
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
                
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

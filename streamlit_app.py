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
APP_VERSION = "v2.4.0 (盤後保底版)"
UPDATE_LOG = """
- v2.3.1: 空值防護。
- v2.4.0: 修正盤後 0% 問題。
  1. 新增「排行榜收盤價」作為備援：當 Yahoo 盤後抓不到即時價時，直接使用 FinMind 排行榜內的收盤價。
  2. 確保只要排行榜有資料，今日廣度就絕對算得出來，不再依賴不穩定的 Yahoo。
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
    if not codes: return {}, None
    tw_tickers = [f"{c}.TW" for c in codes]
    all_tickers = tw_tickers + [f"{c}.TWO" for c in codes]
    
    try:
        # 使用 threads=False 增加穩定性，避免盤後多執行緒被擋
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=False)
        realtime_map = {}
        latest_time = None
        
        valid_tickers = []
        if isinstance(data.columns, pd.MultiIndex):
            valid_tickers = data.columns.levels[0]
        elif not data.empty:
            valid_tickers = [data.name] if hasattr(data, 'name') else []
            if len(all_tickers) == 1: valid_tickers = all_tickers

        if len(valid_tickers) == 0 and not data.empty and len(all_tickers) == 1:
             df = data
             if not df.empty and not df['Close'].isna().all():
                 c = float(df['Close'].iloc[-1])
                 realtime_map[codes[0]] = c
                 latest_time = df.index[-1]
        else:
            for t in valid_tickers:
                try:
                    df = data[t] if isinstance(data.columns, pd.MultiIndex) else data
                    if df.empty or df['Close'].isna().all(): continue
                    last_price = float(df['Close'].iloc[-1])
                    last_ts = df.index[-1]
                    if latest_time is None or last_ts > latest_time:
                        latest_time = last_ts
                    stock_id = t.split('.')[0]
                    realtime_map[stock_id] = last_price
                except: continue
                
        return realtime_map, latest_time
    except:
        return {}, None

def get_rank_list(api, date_str, backup_date=None):
    try:
        df_rank = api.taiwan_stock_daily(stock_id="", start_date=date_str)
        if df_rank.empty and backup_date:
            df_rank = api.taiwan_stock_daily(stock_id="", start_date=backup_date)
        if df_rank.empty: return []

        df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
        df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
        df_rank['Close'] = smart_get_column(df_rank, ['close', 'Close', 'price'])
        
        df_rank['ID'] = df_rank['ID'].astype(str)
        df_rank = df_rank[df_rank['ID'].str.len() == 4]
        df_rank = df_rank[df_rank['ID'].str.isdigit()]
        for prefix in EXCLUDE_PREFIXES:
            df_rank = df_rank[~df_rank['ID'].str.startswith(prefix)]
            
        df_candidates = df_rank.sort_values('Money', ascending=False).head(TOP_N)
        target_list = []
        for _, row in df_candidates.iterrows():
            target_list.append({
                'code': row['ID'],
                'hist_close': float(row['Close']) if pd.notnull(row['Close']) else 0.0
            })
        return target_list
    except:
        return []

def calc_breadth_score(_api, target_list, check_date, use_realtime, rank_source_date):
    hits = 0
    valid = 0
    detail_res = []
    
    rt_map = {}
    last_t = None
    if use_realtime:
        codes = [x['code'] for x in target_list]
        rt_map, last_t = fetch_yahoo_realtime_batch(codes)
        
    for i, item in enumerate(target_list):
        code = item['code']
        
        # === 核心修正：決定使用的價格 ===
        price_to_use = 0
        source_type = "None"
        
        if use_realtime:
            # 1. 優先用 Yahoo
            yahoo_p = rt_map.get(code, 0)
            if yahoo_p > 0:
                price_to_use = yahoo_p
                source_type = "Yahoo"
            # 2. 備援：如果 Yahoo 沒資料，但排行榜是「今天」的，直接用排行榜收盤價
            elif rank_source_date == check_date and item['hist_close'] > 0:
                price_to_use = item['hist_close']
                source_type = "FinMind(收盤)"
        
        try:
            # 抓歷史
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            # 手動合成 K 棒邏輯
            if use_realtime:
                if price_to_use > 0:
                    # 有抓到價格 (Yahoo 或 FinMind榜單)，強制合成今日
                    stock_df = stock_df[stock_df['date'] < check_date] # 刪舊
                    new_row = pd.DataFrame([{'date': check_date, 'close': price_to_use}])
                    stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                else:
                    # 真的完全沒價格，保留原樣 (可能有 FinMind 自己更新的今日 K 線)
                    pass
            else:
                # 算 D-1：切除未來數據
                stock_df = stock_df[stock_df['date'] <= check_date]
            
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr = stock_df.iloc[-1]
                
                # 日期檢查
                last_dt = pd.to_datetime(curr['date']).strftime("%Y-%m-%d")
                days_diff = (pd.to_datetime(check_date) - pd.to_datetime(last_dt)).days
                
                is_valid_date = False
                if last_dt == check_date:
                    is_valid_date = True
                elif not use_realtime and 0 < days_diff <= 3: 
                    # D-1 寬容模式
                    is_valid_date = True
                
                if is_valid_date:
                    is_ok = curr['close'] > curr['MA5']
                    if is_ok: hits += 1
                    valid += 1
                    detail_res.append({
                        'code': code, 
                        'price': curr['close'],
                        'ok': is_ok,
                        'rank': i+1,
                        'src': source_type if use_realtime else '歷史'
                    })
        except:
            pass
            
    return hits, valid, detail_res, last_t

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2: return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    # 步驟 1: 取得排行
    # D-1 排行
    prev_rank_list = get_rank_list(_api, d_prev_str, backup_date=all_days[-3])
    
    if is_intraday:
        curr_rank_list = prev_rank_list
        rank_source_date = d_prev_str
        mode_msg = "🚀 盤中模式 (母體:昨日排行)"
    else:
        # 盤後嘗試抓 D
        curr_rank_list = get_rank_list(_api, d_curr_str)
        if curr_rank_list:
            rank_source_date = d_curr_str
            mode_msg = "🐢 盤後模式 (母體:今日排行)"
        else:
            curr_rank_list = prev_rank_list
            rank_source_date = d_prev_str
            mode_msg = "⚠️ 盤後模式 (FinMind 未更新，沿用昨日排行)"

    if not prev_rank_list:
        st.error("無法取得排行資料")
        return None

    progress_bar = st.progress(0, text="計算昨日數據...")
    # D-1 計算：傳入 d_prev_str 作為 rank_source_date (雖然這裡沒用到即時價，但保持一致)
    hit_prev, valid_prev, _, _ = calc_breadth_score(_api, prev_rank_list, d_prev_str, use_realtime=False, rank_source_date=d_prev_str)
    
    progress_bar.progress(50, text=f"計算今日數據 ({mode_msg})...")
    # D 計算：傳入 rank_source_date 讓函數知道能否用榜單價當作今日價
    hit_curr, valid_curr, details, last_time = calc_breadth_score(_api, curr_rank_list, d_curr_str, use_realtime=True, rank_source_date=rank_source_date)
    
    progress_bar.empty()
    
    detail_df = pd.DataFrame(details)
    if not detail_df.empty:
        detail_df['狀態'] = detail_df['ok'].apply(lambda x: '✅ 納入' if x else '❌ 剔除')
        detail_df = detail_df[['排名', 'code', 'price', 'src', '狀態']]
        detail_df.columns = ['排名', '代號', '現價', '來源', '狀態']

    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        if not twii_df.empty:
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
    st.title("📈 盤中權證進場判斷 (v2.4.0 盤後保底)")

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
            time_str = data['last_time'].strftime("%H:%M:%S") if data['last_time'] else "無Yahoo數據"

            st.subheader(f"📅 基準日：{data['d_curr']}")
            st.caption(f"D-1: {data['d_prev']} | D: {data['d_curr']}")
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
                
            st.caption(f"即時報價時間: {time_str}")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

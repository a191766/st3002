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
APP_VERSION = "v2.3.1 (空值防護版)"
UPDATE_LOG = """
- v2.3.0: 歷史鎖定。
- v2.3.1: 
  1. 修復 Yahoo 抓取失敗時導致今日數據全空的問題 (改為：若無 Yahoo 價則保留 FinMind 原資料)。
  2. 新增 D-1 資料寬容度：若個股尚未更新昨日 K 線，允許沿用前一筆收盤價 (解決 FinMind 更新延遲問題)。
  3. 介面新增顯示實際使用的日期，方便除錯。
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
    
    # 強制加入今天
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
        # 增加 headers 減少被擋機率
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
        realtime_map = {}
        latest_time = None
        
        valid_tickers = []
        if isinstance(data.columns, pd.MultiIndex):
            valid_tickers = data.columns.levels[0]
        elif not data.empty:
            valid_tickers = [data.name] if hasattr(data, 'name') else []
            if len(all_tickers) == 1: valid_tickers = all_tickers

        # 單檔處理
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
            target_list.append({'code': row['ID'], 'hist_close': row['Close']})
        return target_list
    except:
        return []

def calc_breadth_score(_api, target_list, check_date, use_realtime=False):
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
        # 這裡很關鍵：如果有 Yahoo 價就用，沒有就設為 0 (後面會判斷)
        yahoo_price = rt_map.get(code, 0) if use_realtime else 0
        
        try:
            # 抓歷史
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            if use_realtime:
                # === D (今日) 邏輯 ===
                if yahoo_price > 0:
                    # 如果有抓到 Yahoo 價，就用 Yahoo 覆蓋 FinMind 的今日資料
                    stock_df = stock_df[stock_df['date'] < check_date]
                    new_row = pd.DataFrame([{'date': check_date, 'close': yahoo_price}])
                    stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                else:
                    # 如果沒抓到 Yahoo 價，就保留 FinMind 原本的資料 (不做任何刪除)
                    # 這樣即使 Yahoo 掛了，只要 FinMind 有更新，就不會全黑
                    pass
            else:
                # === D-1 (昨日) 邏輯 ===
                # 確保不含未來資料
                stock_df = stock_df[stock_df['date'] <= check_date]
            
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr = stock_df.iloc[-1]
                
                # 日期檢查 (寬容模式)
                # 理想情況：curr['date'] == check_date
                # 實際情況：FinMind 可能還沒更新到 check_date，curr['date'] 可能是前一天
                # 我們允許 3 天內的落差 (例如週一查，資料只到週五)
                last_dt = pd.to_datetime(curr['date']).strftime("%Y-%m-%d")
                
                # 只有當日期完全吻合，或者 (是D-1計算 且 日期差距小) 才算有效
                days_diff = (pd.to_datetime(check_date) - pd.to_datetime(last_dt)).days
                
                is_valid_date = False
                if last_dt == check_date:
                    is_valid_date = True
                elif not use_realtime and 0 < days_diff <= 3: 
                    # D-1 模式下允許資料延遲 (視為當天沒開盤或資料未更，沿用上筆狀態)
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
                        'src': 'Yahoo' if (use_realtime and yahoo_price > 0) else 'FinMind'
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
    # 這裡邏輯：D-1 永遠用 D-1 的排行
    prev_rank_list = get_rank_list(_api, d_prev_str, backup_date=all_days[-3])
    
    if is_intraday:
        curr_rank_list = prev_rank_list
        mode_msg = "🚀 盤中模式 (母體:昨日排行)"
    else:
        curr_rank_list = get_rank_list(_api, d_curr_str)
        if curr_rank_list:
            mode_msg = "🐢 盤後模式 (母體:今日排行)"
        else:
            curr_rank_list = prev_rank_list
            mode_msg = "⚠️ 盤後模式 (FinMind 未更新，沿用昨日排行)"

    if not prev_rank_list:
        st.error("無法取得排行資料")
        return None

    progress_bar = st.progress(0, text="計算昨日數據...")
    hit_prev, valid_prev, _, _ = calc_breadth_score(_api, prev_rank_list, d_prev_str, use_realtime=False)
    
    progress_bar.progress(50, text=f"計算今日數據 ({mode_msg})...")
    hit_curr, valid_curr, details, last_time = calc_breadth_score(_api, curr_rank_list, d_curr_str, use_realtime=True)
    
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
    st.title("📈 盤中權證進場判斷 (v2.3.1)")

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
            time_str = data['last_time'].strftime("%H:%M:%S") if data['last_time'] else "未知"

            st.subheader(f"📅 基準日：{data['d_curr']}")
            st.caption(f"D-1: {data['d_prev']} | D: {data['d_curr']}") # 除錯顯示日期
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

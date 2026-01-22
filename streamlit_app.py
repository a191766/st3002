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
APP_VERSION = "v3.5.0 (完整報價並列版)"
UPDATE_LOG = """
- v3.4.0: 嚴格日期核實。
- v3.5.0: 表格欄位擴充。
  1. 【新增欄位】表格現在同時顯示「昨日收盤」與「現價(即時)」，方便對照漲跌。
  2. 【資料透明】若「昨日收盤」數值異常，可立即判斷是否為 FinMind 日線未更新。
  3. 維持雙源極速抓取邏輯。
"""

# ==========================================
# 參數與 Token (Sponsor)
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷 (報價並列)", layout="wide")

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

def fetch_finmind_snapshot(api):
    try:
        df = api.taiwan_stock_tick_snapshot(stock_id="")
        if df.empty: return {}, None
        
        code_col = smart_get_column(df, ['stock_id', 'code'])
        price_col = smart_get_column(df, ['deal_price', 'price', 'close'])
        
        if code_col is None or price_col is None: return {}, None
            
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

def calc_stats_hybrid(_api, target_date, rank_codes, use_realtime=False):
    hits = 0
    valid = 0
    details = []
    
    # 1. 準備外部價格源
    price_map = {}
    source_map = {}
    last_t = None
    
    if use_realtime:
        fm_map, fm_time = fetch_finmind_snapshot(_api)
        need_yahoo = False
        if not fm_map: need_yahoo = True
        yahoo_map = {}
        if need_yahoo:
            yahoo_map = fetch_yahoo_realtime_batch(rank_codes)
            last_t = "Yahoo備援"
        else:
            last_t = fm_time
            
        for code in rank_codes:
            p = 0
            src = "無"
            if code in fm_map and fm_map[code] > 0:
                p = fm_map[code]
                src = "FinMind"
            elif need_yahoo and code in yahoo_map and yahoo_map[code] > 0:
                p = yahoo_map[code]
                src = "Yahoo"
            price_map[code] = p
            source_map[code] = src
    
    # 2. 準備歷史資料
    start_date_query = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    prog_bar = st.progress(0, text="極速運算中...")
    total = len(rank_codes)

    for i, code in enumerate(rank_codes):
        if i % 20 == 0:
            prog_bar.progress((i / total), text=f"進度: {i+1}/{total}")

        rank = i + 1
        current_price = 0
        prev_close = 0 # 新增：昨日收盤價變數
        status = "未知"
        price_src = "歷史"
        
        if use_realtime:
            current_price = price_map.get(code, 0)
            price_src = source_map.get(code, "無")
            if current_price == 0: status = "⚠️ 無報價"

        try:
            stock_df = _api.taiwan_stock_daily(stock_id=code, start_date=start_date_query)
            
            if stock_df.empty:
                 status = "❌ 歷史無資料"
            else:
                if use_realtime:
                    # 算今日 (D)：歷史 < D
                    stock_df = stock_df[stock_df['date'] < target_date]
                    
                    # 【關鍵新增】在這裡抓出真正的「昨日收盤價」
                    if not stock_df.empty:
                        prev_close = float(stock_df.iloc[-1]['close'])
                        
                    # 拼上 D 的即時價
                    if current_price > 0:
                        new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                        stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                    
                    if len(stock_df) > 0:
                        last_dt = stock_df.iloc[-1]['date']
                        if last_dt != target_date:
                            status = "🚫 缺今日價"
                            stock_df = pd.DataFrame() 

                else:
                    # 算昨日 (D-1)：歷史 <= D-1
                    stock_df = stock_df[stock_df['date'] <= target_date]
                    
                    if len(stock_df) > 0:
                        last_dt = stock_df.iloc[-1]['date']
                        if isinstance(last_dt, pd.Timestamp):
                            last_dt = last_dt.strftime("%Y-%m-%d")
                        
                        if last_dt != target_date:
                            status = f"🚫 日線未更({last_dt})"
                            stock_df = pd.DataFrame()
                        else:
                            # 如果是算昨日廣度，那「昨日收盤」就是今天的「現價」
                            prev_close = float(stock_df.iloc[-1]['close']) # 其實這是昨天
                
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
                    if "未更" not in status: status = "🚫 資料不足"

        except Exception as e:
            status = "❌ 錯誤"
        
        details.append({
            'rank': rank,
            'code': code,
            'prev_close': prev_close,   # 新增
            'price': current_price,
            'src': price_src if use_realtime else "歷史收盤",
            'status': status
        })
    
    prog_bar.empty()
    return hits, valid, details, last_t

@st.cache_data(ttl=60)
def fetch_data(_api):
    all_days = get_trading_days_robust(_api)
    if len(all_days) < 2: 
        st.error("日期資料異常")
        return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    prev_rank_codes = get_cached_rank_list(API_TOKEN, d_prev_str, backup_date=all_days[-3])
    
    if not prev_rank_codes:
        st.error("無法取得排行")
        return None

    hit_prev, valid_prev, _, _ = calc_stats_hybrid(_api, d_prev_str, prev_rank_codes, use_realtime=False)
    
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中極速 (雙源+智慧快取)"
    else:
        curr_rank_codes = get_cached_rank_list(API_TOKEN, d_curr_str)
        if curr_rank_codes:
            mode_msg = "🐢 盤後精準 (今日排行)"
        else:
            curr_rank_codes = prev_rank_codes
            mode_msg = "⚠️ 盤後 (沿用昨日)"
            
    hit_curr, valid_curr, details, last_time = calc_stats_hybrid(_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
    # 整理 DataFrame
    detail_df = pd.DataFrame(details)
    # 重新命名欄位，讓使用者看懂
    detail_df = detail_df.rename(columns={
        'rank': '排名', 
        'code': '代號', 
        'prev_close': '昨日收盤', 
        'price': '現價', 
        'src': '來源', 
        'status': '狀態'
    })
    
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
    st.title("📈 盤中權證進場判斷 (v3.5.0 完整並列)")

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
            # 這裡顯示的表格已經包含「昨日收盤」和「現價」
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

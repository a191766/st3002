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
APP_VERSION = "v3.6.0 (雙日數據並列版)"
UPDATE_LOG = """
- v3.5.0: 完整報價並列。
- v3.6.0: 表格欄位大升級，詳細列出雙日指標。
  1. 【昨日專區】新增「昨MA5」、「昨狀態」，明確顯示昨日是否站上均線。
  2. 【今日專區】新增「今MA5」、「今狀態」，明確顯示今日即時表現。
  3. 透過並列顯示，可直接觀察股價與 MA5 的動態變化 (例如：昨天沒過但今天過了)。
"""

# ==========================================
# 參數與 Token (Sponsor)
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷 (雙日詳情)", layout="wide")

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
    """
    修改回傳格式：除了 hits/valid，多回傳一個 dict 包含詳細資訊
    """
    hits = 0
    valid = 0
    # 用 dict 儲存結果，方便後續合併 {code: {data}}
    stats_map = {} 
    
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
    
    # 如果是即時運算才顯示進度條，不然背景算昨天的時候安靜一點
    if use_realtime:
        prog_bar = st.progress(0, text="極速運算中...")
    total = len(rank_codes)

    for i, code in enumerate(rank_codes):
        if use_realtime and i % 20 == 0:
            prog_bar.progress((i / total), text=f"進度: {i+1}/{total}")

        current_price = 0
        status = "未知"
        price_src = "歷史"
        ma5_val = 0
        is_pass = False
        
        if use_realtime:
            current_price = price_map.get(code, 0)
            price_src = source_map.get(code, "無")
            if current_price == 0: status = "⚠️ 無報價"

        try:
            stock_df = _api.taiwan_stock_daily(stock_id=code, start_date=start_date_query)
            
            if stock_df.empty:
                 status = "❌ 無資料"
            else:
                if use_realtime:
                    # 今日 (D): 歷史 < D + 即時
                    stock_df = stock_df[stock_df['date'] < target_date]
                    if current_price > 0:
                        new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                        stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                    
                    if len(stock_df) > 0 and stock_df.iloc[-1]['date'] != target_date:
                         status = "🚫 缺今日價"
                         stock_df = pd.DataFrame() # 無效化

                else:
                    # 昨日 (D-1): 歷史 <= D-1
                    stock_df = stock_df[stock_df['date'] <= target_date]
                    
                    if len(stock_df) > 0:
                        last_dt = stock_df.iloc[-1]['date']
                        if isinstance(last_dt, pd.Timestamp): last_dt = last_dt.strftime("%Y-%m-%d")
                        if last_dt != target_date:
                            status = f"🚫 未更"
                            stock_df = pd.DataFrame() # 無效化
                        else:
                            # 昨天的收盤價就是 current_price (為了顯示用)
                            if not use_realtime:
                                current_price = float(stock_df.iloc[-1]['close'])
                
                # 計算 MA5
                if len(stock_df) >= 6:
                    stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                    curr = stock_df.iloc[-1]
                    
                    final_price = float(curr['close'])
                    ma5_val = float(curr['MA5'])
                    
                    if final_price > ma5_val:
                        hits += 1
                        is_pass = True
                        status = "✅ 通過"
                    else:
                        is_pass = False
                        status = f"📉 未過"
                    
                    valid += 1
                else:
                    if "未更" not in status and "缺" not in status: status = "🚫 資料不足"

        except Exception:
            status = "❌ 錯誤"
        
        # 儲存該檔股票的完整數據
        stats_map[code] = {
            'price': current_price,
            'ma5': ma5_val,
            'status': status,
            'is_pass': is_pass,
            'src': price_src
        }
    
    if use_realtime: prog_bar.empty()
    return hits, valid, stats_map, last_t

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

    # 計算昨日 (取得詳細數據 map)
    hit_prev, valid_prev, map_prev, _ = calc_stats_hybrid(_api, d_prev_str, prev_rank_codes, use_realtime=False)
    
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
            
    # 計算今日 (取得詳細數據 map)
    hit_curr, valid_curr, map_curr, last_time = calc_stats_hybrid(_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
    # === 合併報表 ===
    # 以「今日名單」為主表
    final_details = []
    for i, code in enumerate(curr_rank_codes):
        # 取得昨天的數據 (如果名單不同，可能昨天沒這檔，就留空)
        prev_data = map_prev.get(code, {})
        # 取得今天的數據
        curr_data = map_curr.get(code, {})
        
        # 格式化顯示 (若無數據顯示 -)
        p_price = prev_data.get('price', 0)
        p_ma5 = prev_data.get('ma5', 0)
        p_status = "✅" if prev_data.get('is_pass') else "📉"
        if not prev_data.get('status') or "🚫" in prev_data.get('status', ''): p_status = "🚫"
        
        c_price = curr_data.get('price', 0)
        c_ma5 = curr_data.get('ma5', 0)
        c_status = "✅" if curr_data.get('is_pass') else "📉"
        if not curr_data.get('status') or "🚫" in curr_data.get('status', ''): c_status = "🚫"
        if "⚠️" in curr_data.get('status', ''): c_status = "⚠️"

        final_details.append({
            "排名": i+1,
            "代號": code,
            "昨收": p_price,
            "昨MA5": round(p_ma5, 2) if p_ma5 else 0,
            "昨狀態": p_status,
            "現價": c_price,
            "今MA5": round(c_ma5, 2) if c_ma5 else 0,
            "今狀態": c_status,
            "來源": curr_data.get('src', '-')
        })

    detail_df = pd.DataFrame(final_details)
    
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
    st.title("📈 盤中權證進場判斷 (v3.6.0 雙日詳情)")

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
            
            st.caption(f"報價來源時間: {t_str}")
            # 顯示升級後的雙日表格
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

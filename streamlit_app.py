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
APP_VERSION = "v2.3.0 (歷史廣度鎖定版)"
UPDATE_LOG = """
- v2.2.1: 修復變數錯誤。
- v2.3.0: 邏輯修正！
  1. 「昨日廣度 (D-1)」：強制永遠使用「昨日排行」計算，確保該數值為固定歷史事實，不再隨今日排行變動。
  2. 「今日廣度 (D)」：盤中沿用昨日排行，盤後使用今日排行。
  3. 解決盤後切換榜單時，導致昨日數據跳動的混淆問題。
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
    """ 判斷目前是盤中還是盤後 """
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    # 08:45 ~ 13:30 視為盤中
    is_intraday = time(8, 45) <= current_time < time(13, 30)
    return tw_now, is_intraday

def get_trading_days(api):
    """ 取得交易日 """
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
    """ 智慧欄位搜尋 """
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

def fetch_yahoo_realtime_batch(codes):
    """ Yahoo 批次下載 """
    if not codes: return {}, None
    
    tw_tickers = [f"{c}.TW" for c in codes]
    all_tickers = tw_tickers + [f"{c}.TWO" for c in codes]
    
    try:
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
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
             if not df.empty:
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
    """ 取得指定日期的排行榜清單 (回傳 list of dict) """
    try:
        df_rank = api.taiwan_stock_daily(stock_id="", start_date=date_str)
        
        if df_rank.empty and backup_date:
            df_rank = api.taiwan_stock_daily(stock_id="", start_date=backup_date)
            
        if df_rank.empty: return []

        df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
        df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
        df_rank['Close'] = smart_get_column(df_rank, ['close', 'Close', 'price'])
        
        # 篩選
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
                'hist_close': row['Close']
            })
        return target_list
    except:
        return []

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2:
        st.error("歷史資料不足。")
        return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    # === 步驟 1: 取得排行清單 ===
    # A. 取得「昨日排行」 (永遠用於計算昨日廣度)
    prev_rank_list = get_rank_list(_api, d_prev_str, backup_date=all_days[-3])
    
    # B. 決定「今日目標排行」 (盤中=昨日排行, 盤後=今日排行)
    if is_intraday:
        curr_rank_list = prev_rank_list # 盤中直接沿用
        mode_msg = "🚀 盤中模式 (母體:昨日排行)"
        # 標記是否為同一份名單 (若是，運算可優化)
        same_list = True
    else:
        # 盤後嘗試抓今日排行
        curr_rank_list = get_rank_list(_api, d_curr_str)
        if curr_rank_list:
            mode_msg = "🐢 盤後模式 (母體:今日排行)"
            same_list = False
        else:
            curr_rank_list = prev_rank_list
            mode_msg = "⚠️ 盤後模式 (FinMind 未更新，沿用昨日排行)"
            same_list = True

    if not prev_rank_list:
        st.error("無法取得排行資料")
        return None

    # === 步驟 2: 計算「昨日廣度」 (固定使用 prev_rank_list) ===
    # 為了確保「昨日數據」恆定，我們單獨計算它
    # 這一步只算 D-1 狀態，不需要 Yahoo 即時價
    br_prev_hits = 0
    br_prev_valid = 0
    
    # 為了加速，如果是 same_list，我們可以合併在後面算
    # 但為了邏輯清晰且徹底解決問題，我們分開處理 D-1 的狀態
    
    progress_bar = st.progress(0, text="正在鎖定昨日歷史廣度...")
    
    # 只需要 D-1 以前的資料
    # 我們可以偷懶：如果是 same_list，在後面一次算
    # 如果不是 same_list (盤後)，我們必須多跑一次 loop 來算 D-1 的正確廣度
    
    # 策略：建立一個 function 來算單一 list 在特定日期的廣度
    def calc_breadth_score(target_list, check_date, use_realtime=False):
        hits = 0
        valid = 0
        detail_res = []
        
        # 取得 Yahoo 即時價 (僅當需要 realtime 時)
        rt_map = {}
        last_t = None
        if use_realtime:
            codes = [x['code'] for x in target_list]
            rt_map, last_t = fetch_yahoo_realtime_batch(codes)
            
        for i, item in enumerate(target_list):
            code = item['code']
            c_price = rt_map.get(code, item['hist_close']) if use_realtime else 0
            
            try:
                # 抓歷史
                stock_df = _api.taiwan_stock_daily(
                    stock_id=code,
                    start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
                )
                
                # 裁切到 check_date (含)
                # 如果是算 D-1，我們只需要 D-1 及其之前的資料
                # 如果是算 D (且 use_realtime)，我們拿 D-1 之前的資料 + 即時 D
                
                if use_realtime:
                    # 排除 D (以防 FinMind 偷跑)
                    stock_df = stock_df[stock_df['date'] < check_date]
                    if c_price > 0:
                        new_row = pd.DataFrame([{'date': check_date, 'close': c_price}])
                        stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                else:
                    # 純歷史模式 (算 D-1)
                    # 確保包含 check_date
                    stock_df = stock_df[stock_df['date'] <= check_date]
                
                if len(stock_df) >= 6:
                    stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                    curr = stock_df.iloc[-1]
                    # 確認日期對不對
                    if curr['date'].strftime("%Y-%m-%d") == check_date:
                        is_ok = curr['close'] > curr['MA5']
                        if is_ok: hits += 1
                        valid += 1
                        
                        detail_res.append({
                            'code': code, 
                            'price': curr['close'],
                            'ok': is_ok,
                            'rank': i+1
                        })
            except:
                pass
        return hits, valid, detail_res, last_t

    # A. 計算固定不變的「昨日廣度」 (使用 prev_rank_list, 檢查日 d_prev_str)
    # 這樣無論盤中盤後，這個數字永遠是用「昨日排行」算的「昨日廣度」
    hit_prev, valid_prev, _, _ = calc_breadth_score(prev_rank_list, d_prev_str, use_realtime=False)
    
    progress_bar.progress(50, text=f"分析今日數據 ({mode_msg})...")

    # B. 計算「今日廣度」 (使用 curr_rank_list, 檢查日 d_curr_str, 開啟即時)
    hit_curr, valid_curr, details, last_time = calc_breadth_score(curr_rank_list, d_curr_str, use_realtime=True)
    
    progress_bar.empty()
    
    # 整理顯示資料
    detail_df = pd.DataFrame(details)
    if not detail_df.empty:
        detail_df['狀態'] = detail_df['ok'].apply(lambda x: '✅ 納入' if x else '❌ 剔除')
        detail_df = detail_df.rename(columns={'rank': '排名', 'code': '代號', 'price': '現價'})
        detail_df = detail_df[['排名', '代號', '現價', '狀態']]

    # === 大盤斜率 ===
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        twii_df = twii_df[twii_df['date'] < d_curr_str]
        try:
            twii_rt = yf.download("^TWII", period="1d", progress=False)
            if not twii_rt.empty:
                last_twii = float(twii_rt['Close'].iloc[-1])
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': last_twii}])
                twii_df = pd.concat([twii_df, new_row], ignore_index=True)
        except: pass
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except: pass
    
    # 計算比率
    br_prev = hit_prev / valid_prev if valid_prev > 0 else 0
    br_curr = hit_curr / valid_curr if valid_curr > 0 else 0

    return {
        "d_curr": d_curr_str,
        "br_curr": br_curr,
        "br_prev": br_prev,
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
    st.title("📈 盤中權證進場判斷 (v2.3 歷史鎖定)")

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
            st.success(f"📌 {data['mode_msg']}")
            
            c1, c2, c3 = st.columns(3)
            c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid_curr']}")
            # 這裡顯示的是固定後的歷史數據
            c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid_prev']}")
            c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

            st.divider()
            if final_decision:
                st.success(f"✅ 結論：可進場")
            else:
                st.error(f"⛔ 結論：不可進場")
                
            st.caption(f"即時報價時間: {time_str} | 篩選條件：4碼純數字個股 (排除權證/ETF)")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

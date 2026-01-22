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
APP_VERSION = "v3.0.0 (Sponsor 極速版)"
UPDATE_LOG = """
- v3.0.0: 架構全面升級為 FinMind Sponsor 專用版。
  1. 移除 yfinance，所有資料源統一為 FinMind。
  2. 使用 `taiwan_stock_daily_short` (全市場快照) 抓取即時價，速度極快且無延遲。
  3. 維持「昨日數據鎖定」邏輯，確保監控指標穩定。
"""

# ==========================================
# 參數與 Token (Sponsor)
# ==========================================
# 您提供的 Sponsor Token
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
    """ 
    [Sponsor 專用] 
    一次抓取全市場即時快照 (Realtime Snapshot)。
    因為有付費，這支 API 不會被擋，且速度極快。
    """
    try:
        # stock_id="" 代表抓全市場
        df = api.taiwan_stock_daily_short(stock_id="", start_date=date_str)
        if df.empty: return {}, None
        
        # 建立快速查詢表 {stock_id: price}
        # 注意欄位: FinMind snapshot 通常是 close, open, high, low, volume
        # 智慧欄位對應
        code_col = smart_get_column(df, ['stock_id', 'code'])
        price_col = smart_get_column(df, ['close', 'price', 'deal_price'])
        
        if code_col is None or price_col is None:
            return {}, None
            
        # 轉換為 dict
        snapshot_map = dict(zip(code_col, price_col))
        
        # 取得資料時間 (取最後一筆的時間作為參考)
        # 欄位可能是 date 或 timestamp
        time_col = smart_get_column(df, ['date', 'time'])
        last_time = None
        if time_col is not None:
            last_time = time_col.iloc[-1]
            
        return snapshot_map, last_time
    except Exception as e:
        print(f"FinMind Snapshot Error: {e}")
        return {}, None

def get_rank_list(api, date_str, backup_date=None):
    """ 取得排行榜清單 """
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
    """ 
    統一運算函式 (Sponsor 版)
    use_realtime=True: 會去呼叫 taiwan_stock_daily_short 取得即時價
    use_realtime=False: 只用 taiwan_stock_daily (算昨日歷史)
    """
    hits = 0
    valid = 0
    details = []
    
    # 若需即時，先抓全市場快照 (Sponsor 優勢：一次到位，不用迴圈)
    snapshot_map = {}
    last_t = None
    if use_realtime:
        snapshot_map, last_t = fetch_finmind_snapshot(_api, target_date)
    
    # 批次抓取 300 檔歷史資料 (Sponsor 流量大，可以直接抓)
    # 不過 FinMind API 設計通常還是單檔抓歷史比較穩，或者我們用迴圈
    # 這裡維持迴圈抓歷史 (因為歷史資料不變，且 FinMind 速度夠快)
    
    for i, code in enumerate(rank_codes):
        rank = i + 1
        current_price = 0
        status = "未知"
        price_src = "歷史"
        
        # 1. 決定價格
        if use_realtime:
            # 從快照 Map 裡找
            current_price = snapshot_map.get(code, 0)
            if current_price > 0:
                price_src = "FinMind即時"
            else:
                # 沒抓到即時價 (可能未開盤或撮合中)
                status = "⚠️ 無即時價"
        
        try:
            # 2. 抓歷史 K 線 (用來算 MA5)
            # 抓過去 30 天
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            # 3. 資料合成
            if use_realtime:
                # 確保不含今日 (避免重複)
                stock_df = stock_df[stock_df['date'] < target_date]
                
                if current_price > 0:
                    # 拼上今日即時價
                    new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                    stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                else:
                    # 如果沒抓到即時價，就不拼湊，這樣 K 線會少一天，自然被下面的 len 檢查踢掉
                    pass
            else:
                # 算 D-1：切除未來數據
                stock_df = stock_df[stock_df['date'] <= target_date]

            # 4. 指標計算
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr = stock_df.iloc[-1]
                
                # 取得當下的收盤價 (歷史或即時)
                final_price = curr['close']
                ma5 = curr['MA5']
                
                is_ok = final_price > ma5
                
                if is_ok:
                    hits += 1
                    status = "✅ 通過"
                else:
                    status = f"📉 未通過 (MA5:{ma5:.1f})"
                
                valid += 1
                # 更新顯示價格
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

@st.cache_data(ttl=60) # Sponsor 版可以設短一點，例如 60秒更新一次
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
        mode_msg = "🚀 盤中極速模式 (Sponsor 直連)"
    else:
        # 盤後嘗試抓今日排行
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
    
    # 4. 斜率 (也改用 FinMind)
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        # 若盤中，嘗試抓大盤即時
        if is_intraday:
            twii_snap, _ = fetch_finmind_snapshot(_api, d_curr_str) # 其實這裡會抓到全市場，稍微有點浪費但沒差
            twii_price = twii_snap.get('TAIEX', 0)
            if twii_price > 0:
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
    st.title("📈 盤中權證進場判斷 (v3.0 Sponsor)")

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

            # 時間顯示處理
            t_str = "未知"
            if data['last_time']:
                # FinMind 有時回傳字串，有時回傳 datetime
                t_str = str(data['last_time'])

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
                
            st.caption(f"FinMind 快照時間: {t_str}")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

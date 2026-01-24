# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import shioaji as sj
import os
import altair as alt
import time as time_module
import yfinance as yf

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v5.3.0 (數據透明化版)"
UPDATE_LOG = """
- v5.2.0: 假日修復。
- v5.3.0: 圖表重構與數據驗證。
  1. 【簡化縱軸】回歸單純顯示廣度 0%~100%，移除易出錯的雙重標籤。
  2. 【數據透明】大盤 Tooltip 新增「計算現價」與「基準昨收」，方便驗證漲跌幅來源。
  3. 【防呆補強】當即時價為 0 時，強制使用 Yahoo Finance 的最後收盤價，避免假日歸零。
"""

# ==========================================
# 參數與 Token
# ==========================================
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]
HISTORY_FILE = "breadth_history_v3.csv" # 改名 v3 以更新欄位結構
AUTO_REFRESH_SECONDS = 180 

st.set_page_config(page_title="盤中權證進場判斷 (v5.3)", layout="wide")

# ==========================================
# 🔐 Secrets
# ==========================================
def get_finmind_token():
    try: return st.secrets["finmind"]["token"]
    except: return None

# ==========================================
# API 初始化
# ==========================================
@st.cache_resource
def get_shioaji_api():
    api = sj.Shioaji(simulation=False)
    try:
        api_key = st.secrets["shioaji"]["api_key"]
        secret_key = st.secrets["shioaji"]["secret_key"]
        api.login(api_key=api_key, secret_key=secret_key)
    except: return None
    return api

# ==========================================
# 靜態資料快取
# ==========================================
def smart_get_column(df, candidates):
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

@st.cache_data(ttl=3600, show_spinner=False)
def get_cached_trading_days(token):
    api = DataLoader()
    api.login_by_token(token)
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if not df.empty: return sorted(df['date'].unique().tolist())
    except: pass
    return []

@st.cache_data(ttl=86400, show_spinner=False, persist="disk")
def get_cached_rank_list(token, date_str, backup_date=None):
    local_api = DataLoader()
    local_api.login_by_token(token)
    df_rank = pd.DataFrame()
    try: df_rank = local_api.taiwan_stock_daily(stock_id="", start_date=date_str)
    except: pass
    if df_rank.empty and backup_date:
        try: df_rank = local_api.taiwan_stock_daily(stock_id="", start_date=backup_date)
        except: pass
    if df_rank.empty: raise RuntimeError("API_FETCH_FAILED") 

    df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
    df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
    if df_rank['ID'] is None or df_rank['Money'] is None: raise RuntimeError("DATA_FORMAT_ERROR")

    df_rank['ID'] = df_rank['ID'].astype(str)
    df_rank = df_rank[df_rank['ID'].str.len() == 4]
    df_rank = df_rank[df_rank['ID'].str.isdigit()]
    for prefix in EXCLUDE_PREFIXES: df_rank = df_rank[~df_rank['ID'].str.startswith(prefix)]
        
    df_candidates = df_rank.sort_values('Money', ascending=False).head(TOP_N)
    return df_candidates['ID'].tolist()

@st.cache_data(ttl=21600, show_spinner=False)
def get_cached_stock_history(token, code, start_date):
    api = DataLoader()
    api.login_by_token(token)
    try: return api.taiwan_stock_daily(stock_id=code, start_date=start_date)
    except: return pd.DataFrame()

# ==========================================
# 廣度記錄與繪圖 (新增價格欄位)
# ==========================================
def save_breadth_record(current_date, current_time, breadth_value, taiex_change, taiex_curr, taiex_prev, is_intraday):
    # 防呆：如果現價是 0，絕對不存
    if taiex_curr == 0:
        return

    new_data = pd.DataFrame([{
        'Date': current_date,
        'Time': current_time,
        'Breadth': breadth_value,
        'Taiex_Change': taiex_change,
        'Taiex_Current': taiex_curr,    # 新增：紀錄當下價格
        'Taiex_Prev_Close': taiex_prev  # 新增：紀錄昨收
    }])
    
    if not os.path.exists(HISTORY_FILE):
        new_data.to_csv(HISTORY_FILE, index=False)
    else:
        try:
            df = pd.read_csv(HISTORY_FILE)
            if not df.empty:
                last_date = str(df.iloc[-1]['Date'])
                if last_date != str(current_date):
                    new_data.to_csv(HISTORY_FILE, index=False)
                else:
                    if not is_intraday:
                        # 盤後：覆蓋最後一筆
                        df = df[:-1]
                        df = pd.concat([df, new_data], ignore_index=True)
                        df.to_csv(HISTORY_FILE, index=False)
                    else:
                        # 盤中：Append
                        last_time = str(df.iloc[-1]['Time'])
                        if last_time != str(current_time):
                            new_data.to_csv(HISTORY_FILE, mode='a', header=False, index=False)
            else:
                new_data.to_csv(HISTORY_FILE, index=False)
        except:
            new_data.to_csv(HISTORY_FILE, index=False)

def plot_breadth_chart():
    if not os.path.exists(HISTORY_FILE): return None
    try:
        df = pd.read_csv(HISTORY_FILE)
        if df.empty: return None
        
        df['Breadth_Pct'] = df['Breadth']
        df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
        
        # 換算大盤位置： (漲跌幅% * 10) + 0.5
        # 0% -> 0.5 (50%)
        # 1% -> 0.6 (60%)
        df['Taiex_Scaled'] = (df['Taiex_Change'] * 10) + 0.5
        
        base_date = df.iloc[0]['Date']
        start_bound = pd.to_datetime(f"{base_date} 09:00:00")
        end_bound = pd.to_datetime(f"{base_date} 14:30:00")

        # 簡單明瞭的 10% 刻度
        tick_values = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]

        base = alt.Chart(df).encode(
            x=alt.X('Datetime', 
                    title='時間', 
                    axis=alt.Axis(format='%H:%M'), 
                    scale=alt.Scale(domain=[start_bound, end_bound])
            )
        )

        # 1. 廣度 (藍色)
        line_breadth = base.mark_line(color='#007bff', clip=False).encode(
            y=alt.Y('Breadth_Pct', 
                    title=None, # 不顯示標題，只顯示 %
                    scale=alt.Scale(domain=[0, 1]),
                    axis=alt.Axis(
                        format='%', 
                        values=tick_values,
                        tickCount=11,
                        labelOverlap=False
                    )
            )
        )
        
        point_breadth = base.mark_circle(color='#007bff', size=60, clip=False).encode(
            y='Breadth_Pct',
            tooltip=[
                alt.Tooltip('Datetime', format='%H:%M'), 
                alt.Tooltip('Breadth_Pct', title='廣度', format='.1%')
            ]
        )

        # 2. 大盤 (黃色) - Tooltip 增加詳細價格資訊
        line_taiex = base.mark_line(color='#ffc107', strokeDash=[4,4], clip=False).encode(
            y=alt.Y('Taiex_Scaled', scale=alt.Scale(domain=[0, 1]), axis=None)
        )
        
        point_taiex = base.mark_circle(color='#ffc107', size=60, clip=False).encode(
            y='Taiex_Scaled',
            tooltip=[
                alt.Tooltip('Datetime', format='%H:%M'), 
                alt.Tooltip('Taiex_Change', title='大盤漲跌', format='.2%'),
                alt.Tooltip('Taiex_Current', title='計算現價', format='.2f'),
                alt.Tooltip('Taiex_Prev_Close', title='基準昨收', format='.2f')
            ]
        )
        
        rule = alt.Chart(pd.DataFrame({'y': [BREADTH_THRESHOLD]})).mark_rule(color='red', strokeDash=[5, 5]).encode(y='y')

        return (line_breadth + point_breadth + line_taiex + point_taiex + rule).properties(
            title=f"走勢對照 (藍:廣度 / 黃:大盤) - {base_date}",
            height=400
        )
    except: return None

# ==========================================
# 動態資料區
# ==========================================
def get_current_status():
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    valid_time = time(8, 45) <= current_time < time(13, 30)
    valid_day = 0 <= tw_now.weekday() <= 4
    is_intraday = valid_time and valid_day
    return tw_now, is_intraday

def get_trading_days_robust(token):
    dates = get_cached_trading_days(token)
    tw_now, _ = get_current_status()
    
    if not dates:
        check_day = tw_now
        while len(dates) < 5:
            if check_day.weekday() <= 4:
                dates.append(check_day.strftime("%Y-%m-%d"))
            check_day -= timedelta(days=1)
        dates = sorted(dates)

    today_str = tw_now.strftime("%Y-%m-%d")
    if 0 <= tw_now.weekday() <= 4 and tw_now.time() >= time(8, 45):
        if not dates or today_str > dates[-1]: dates.append(today_str)
            
    # 週末補救
    if tw_now.weekday() > 4:
        days_to_fri = tw_now.weekday() - 4
        last_friday = (tw_now - timedelta(days=days_to_fri)).strftime("%Y-%m-%d")
        if not dates or last_friday > dates[-1]: dates.append(last_friday)
            
    return dates

def fetch_shioaji_snapshots(sj_api, codes):
    if not sj_api or not codes: return {}, None
    contracts = []
    for code in codes:
        try:
            contract = sj_api.Contracts.Stocks[code]
            if contract: contracts.append(contract)
        except: pass
    if not contracts: return {}, None
    try:
        snapshots = sj_api.snapshots(contracts)
        price_map = {}
        ts = datetime.now()
        for snap in snapshots:
            if snap.close > 0:
                price_map[snap.code] = float(snap.close)
                if snap.ts: ts = datetime.fromtimestamp(snap.ts / 1000000000)
        return price_map, ts.strftime("%H:%M:%S")
    except: return {}, None

def calc_stats_hybrid(sj_api, target_date, rank_codes, use_realtime=False):
    fm_token = get_finmind_token()
    if not fm_token: raise ValueError("Token Error")

    hits = 0; valid = 0; stats_map = {}; price_map = {}; last_t = None
    
    if use_realtime:
        if sj_api: price_map, last_t = fetch_shioaji_snapshots(sj_api, rank_codes)
        if not price_map: last_t = "無即時資料"
    
    start_date_query = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if use_realtime: prog_bar = st.progress(0, text="運算中...")
    total = len(rank_codes)

    for i, code in enumerate(rank_codes):
        if use_realtime and i % 50 == 0: prog_bar.progress((i / total), text=f"進度: {i+1}/{total}")
        current_price = 0; status = "未知"; price_src = "歷史"; ma5_val = 0; is_pass = False
        
        if use_realtime:
            current_price = price_map.get(code, 0)
            price_src = "永豐API"
            if current_price == 0: status = "⚠️ 無報價"

        try:
            stock_df = get_cached_stock_history(fm_token, code, start_date_query)
            if stock_df.empty: status = "❌ 無資料"
            else:
                if use_realtime:
                    stock_df = stock_df[stock_df['date'] < target_date]
                    if current_price > 0:
                        new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                        stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                    if len(stock_df) > 0 and stock_df.iloc[-1]['date'] != target_date:
                         status = "🚫 缺今日價"; stock_df = pd.DataFrame() 
                else:
                    stock_df = stock_df[stock_df['date'] <= target_date]
                    if len(stock_df) > 0:
                        last_dt = stock_df.iloc[-1]['date']
                        if isinstance(last_dt, pd.Timestamp): last_dt = last_dt.strftime("%Y-%m-%d")
                        if last_dt != target_date: status = f"🚫 未更"; stock_df = pd.DataFrame()
                        else: 
                            if not use_realtime: current_price = float(stock_df.iloc[-1]['close'])
                
                if len(stock_df) >= 6:
                    stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                    curr = stock_df.iloc[-1]
                    final_price = float(curr['close'])
                    ma5_val = float(curr['MA5'])
                    if final_price > ma5_val: hits += 1; is_pass = True; status = "✅ 通過"
                    else: is_pass = False; status = f"📉 未過"
                    valid += 1
                else:
                    if "未更" not in status: status = "🚫 資料不足"
        except: status = "❌ 錯誤"
        
        stats_map[code] = {'price': current_price, 'ma5': ma5_val, 'status': status, 'is_pass': is_pass, 'src': price_src}
    
    if use_realtime: prog_bar.empty()
    return hits, valid, stats_map, last_t

def fetch_data():
    fm_token = get_finmind_token()
    sj_api = get_shioaji_api()
    if not fm_token or not sj_api: st.error("Token Error"); return None

    all_days = get_trading_days_robust(fm_token)
    if len(all_days) < 2: return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    tw_now, is_intraday = get_current_status()
    
    try: prev_rank_codes = get_cached_rank_list(fm_token, d_prev_str, backup_date=all_days[-3])
    except: return None
    
    hit_prev, valid_prev, map_prev, _ = calc_stats_hybrid(None, d_prev_str, prev_rank_codes, use_realtime=False)
    
    rank_source_msg = ""
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中"
        rank_source_msg = f"名單：{d_prev_str} (昨日)"
    else:
        try: curr_rank_codes = get_cached_rank_list(fm_token, d_curr_str)
        except: curr_rank_codes = []
        if curr_rank_codes:
            mode_msg = "🐢 盤後"
            rank_source_msg = f"名單：{d_curr_str} (今日)"
        else:
            curr_rank_codes = prev_rank_codes
            mode_msg = "⚠️ 盤後"
            rank_source_msg = f"名單：{d_prev_str} (昨日)"
            
    hit_curr, valid_curr, map_curr, last_time = calc_stats_hybrid(sj_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
    # === 大盤數據修復 (週末強制修正) ===
    taiex_change = 0; slope = 0
    prev_close_price = 0; curr_taiex_price = 0
    
    try:
        twii_df = get_cached_stock_history(fm_token, "TAIEX", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        
        # 1. 找昨收
        if not twii_df.empty:
            prev_row = twii_df[twii_df['date'] == d_prev_str]
            if not prev_row.empty: prev_close_price = float(prev_row.iloc[0]['close'])
        
        # 2. 找現價
        # A. 永豐
        if sj_api:
             try:
                 snap = sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0]
                 if snap.close > 0: curr_taiex_price = float(snap.close)
             except: pass
        
        # B. FinMind 歷史
        if curr_taiex_price == 0:
            curr_row = twii_df[twii_df['date'] == d_curr_str]
            if not curr_row.empty: curr_taiex_price = float(curr_row.iloc[0]['close'])
                
        # C. Yahoo (終極備援)
        if curr_taiex_price == 0:
            try:
                yf_data = yf.Ticker("^TWII").history(period="5d")
                # 簡單暴力：直接抓最後一筆 Close
                if not yf_data.empty: curr_taiex_price = float(yf_data.iloc[-1]['Close'])
            except: pass

        if curr_taiex_price > 0:
            if twii_df.empty or twii_df.iloc[-1]['date'] != d_curr_str:
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': curr_taiex_price}])
                twii_df = pd.concat([twii_df, new_row], ignore_index=True)
        
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
        
        if prev_close_price > 0 and curr_taiex_price > 0:
            taiex_change = (curr_taiex_price - prev_close_price) / prev_close_price
            
    except: pass
    
    br_curr = hit_curr / valid_curr if valid_curr > 0 else 0
    record_time = "13:30:00" if not is_intraday else (last_time if last_time and "無" not in str(last_time) else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S"))
    
    # 將現價與昨收也存入
    save_breadth_record(d_curr_str, record_time, br_curr, taiex_change, curr_taiex_price, prev_close_price, is_intraday)
    
    final_details = []
    for i, code in enumerate(curr_rank_codes):
        c_d = map_curr.get(code, {}); p_d = map_prev.get(code, {})
        final_details.append({
            "排名": i+1, "代號": code,
            "昨收": p_d.get('price', 0), "昨MA5": round(p_d.get('ma5', 0), 2), "昨狀態": "✅" if p_d.get('is_pass') else "📉",
            "現價": c_d.get('price', 0), "今MA5": round(c_d.get('ma5', 0), 2), "今狀態": "✅" if c_d.get('is_pass') else "📉",
            "來源": c_d.get('src', '-')
        })

    return {
        "d_curr": d_curr_str, "d_prev": d_prev_str,
        "br_curr": br_curr, "br_prev": hit_prev / valid_prev if valid_prev else 0,
        "hit_curr": hit_curr, "valid_curr": valid_curr,
        "hit_prev": hit_prev, "valid_prev": valid_prev,
        "slope": slope, "detail_df": pd.DataFrame(final_details),
        "mode_msg": mode_msg, "rank_source_msg": rank_source_msg, "last_time": last_time
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v5.3.0)")
    with st.sidebar:
        auto_refresh = st.checkbox("啟用自動更新 (每3分鐘)", value=False)
        st.markdown(UPDATE_LOG)

    if st.button("🔄 立即重新整理"): pass 

    try:
        data = fetch_data()
        if data:
            cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
            cond2 = data['slope'] > 0
            
            st.subheader(f"📅 基準日：{data['d_curr']}")
            st.caption(f"昨日基準: {data['d_prev']}")
            st.info(f"ℹ️ {data['rank_source_msg']}") 
            
            chart = plot_breadth_chart()
            if chart: st.altair_chart(chart, use_container_width=True)
            
            c1, c2, c3 = st.columns(3)
            c1.metric("今日廣度", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid_curr']}")
            c2.metric("昨日廣度", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid_prev']}")
            c3.metric("大盤MA5斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

            if cond1 and cond2: st.success("✅ 結論：可進場")
            else: st.error("⛔ 結論：不可進場")
            
            st.caption(f"報價時間: {data['last_time']}")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)
            
    except Exception as e: st.error(f"Error: {e}")

    if auto_refresh:
        tw_now, is_intraday = get_current_status()
        if is_intraday:
            time_module.sleep(AUTO_REFRESH_SECONDS)
            st.rerun()
        else:
            with st.sidebar: st.warning("⏸ 非盤中，暫停更新")

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

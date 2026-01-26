# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import shioaji as sj
import os
import altair as alt
import time as time_module
import yfinance as yf
import requests

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v8.2.3 (穩定運作版)"
UPDATE_LOG = """
- v8.2.2: 處理縮排問題。
- v8.2.3: 綜合修復。
  1. 【防黑屏機制】若資料讀取失敗，顯示提示訊息而非空白畫面。
  2. 【格式修正】移除可能導致 IndentationError 的寫法。
  3. 【快取優化】移除 persist="disk" 以消除 Log 警告。
"""

# ==========================================
# 參數與 Token
# ==========================================
TOP_N = 300              
BREADTH_THRESHOLD = 0.65 
BREADTH_LOWER_REF = 0.55 
RAPID_CHANGE_THRESHOLD = 0.02 
EXCLUDE_PREFIXES = ["00", "91"]
HISTORY_FILE = "breadth_history_v3.csv"

st.set_page_config(page_title="盤中權證進場判斷 (v8.2.3)", layout="wide")

# ==========================================
# 🔐 Secrets
# ==========================================
def get_finmind_token():
    try:
        return st.secrets["finmind"]["token"]
    except:
        return None

# ==========================================
# 📨 Telegram 通知功能
# ==========================================
def send_telegram_notify(token, chat_id, msg):
    if not token or not chat_id:
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
    try:
        r = requests.post(url, json=payload)
        return r.status_code == 200
    except Exception as e:
        print(f"Telegram Error: {e}")
        return False

# ==========================================
# ⚡ 急速變動檢查邏輯
# ==========================================
def check_rapid_change(current_row):
    if not os.path.exists(HISTORY_FILE):
        return None, None
        
    try:
        df = pd.read_csv(HISTORY_FILE)
        if len(df) < 2:
            return None, None
        
        curr_dt_str = f"{current_row['Date']} {current_row['Time']}"
        curr_dt = datetime.strptime(curr_dt_str, "%Y-%m-%d %H:%M:%S")
        curr_val = float(current_row['Breadth'])
        
        target_row = None
        # 尋找 3 分鐘前的紀錄 (誤差容許範圍 170s ~ 190s)
        for i in range(2, min(10, len(df) + 1)): 
            row = df.iloc[-i]
            row_dt_str = f"{row['Date']} {row['Time']}"
            row_dt = datetime.strptime(row_dt_str, "%Y-%m-%d %H:%M:%S")
            diff_seconds = (curr_dt - row_dt).total_seconds()
            
            if 170 <= diff_seconds <= 190:
                target_row = row
                break
        
        if target_row is not None:
            past_val = float(target_row['Breadth'])
            diff = curr_val - past_val
            
            if abs(diff) >= RAPID_CHANGE_THRESHOLD:
                direction = "上漲" if diff > 0 else "下跌"
                p_time = target_row['Time'][:5] 
                c_time = current_row['Time'][:5]
                msg = f"⚡ <b>【廣度急變警報】</b>\n{p_time}廣度{past_val:.0%}，{c_time}廣度{curr_val:.0%}，{direction}{abs(diff):.0%}"
                return msg, curr_dt_str
                
    except Exception as e:
        print(f"Rapid Check Error: {e}")
        
    return None, None

# ==========================================
# API 初始化
# ==========================================
@st.cache_resource(ttl=3600) 
def get_shioaji_api():
    api = sj.Shioaji(simulation=False)
    try:
        api_key = st.secrets["shioaji"]["api_key"]
        secret_key = st.secrets["shioaji"]["secret_key"]
        api.login(api_key=api_key, secret_key=secret_key)
    except:
        return None
    return api

# ==========================================
# 靜態資料快取
# ==========================================
def smart_get_column(df, candidates):
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols:
            return df[name]
        if name.lower() in lower_map:
            return df[lower_map[name.lower()]]
    return None

@st.cache_data(ttl=3600, show_spinner=False)
def get_cached_trading_days(token):
    api = DataLoader()
    api.login_by_token(token)
    try:
        start_date = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
        df = api.taiwan_stock_daily(stock_id="0050", start_date=start_date)
        if not df.empty:
            return sorted(df['date'].unique().tolist())
    except:
        pass
    return []

# 移除 persist="disk" 避免 Log 警告
@st.cache_data(ttl=86400, show_spinner=False)
def get_cached_rank_list(token, date_str, backup_date=None):
    local_api = DataLoader()
    local_api.login_by_token(token)
    df_rank = pd.DataFrame()
    
    try:
        df_rank = local_api.taiwan_stock_daily(stock_id="", start_date=date_str)
    except:
        pass
        
    if df_rank.empty and backup_date:
        try:
            df_rank = local_api.taiwan_stock_daily(stock_id="", start_date=backup_date)
        except:
            pass
            
    if df_rank.empty:
        raise RuntimeError("API_FETCH_FAILED") 

    df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
    df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
    
    if df_rank['ID'] is None or df_rank['Money'] is None:
        raise RuntimeError("DATA_FORMAT_ERROR")

    df_rank['ID'] = df_rank['ID'].astype(str)
    df_rank = df_rank[df_rank['ID'].str.len() == 4]
    df_rank = df_rank[df_rank['ID'].str.isdigit()]
    
    for prefix in EXCLUDE_PREFIXES:
        df_rank = df_rank[~df_rank['ID'].str.startswith(prefix)]
        
    df_candidates = df_rank.sort_values('Money', ascending=False).head(TOP_N)
    return df_candidates['ID'].tolist()

@st.cache_data(ttl=21600, show_spinner=False)
def get_cached_stock_history(token, code, start_date):
    api = DataLoader()
    api.login_by_token(token)
    try:
        return api.taiwan_stock_daily(stock_id=code, start_date=start_date)
    except:
        return pd.DataFrame()

# ==========================================
# 廣度記錄與繪圖
# ==========================================
def save_breadth_record(current_date, current_time, breadth_value, taiex_change, taiex_curr, taiex_prev, is_intraday):
    if taiex_curr == 0:
        return 

    new_data = pd.DataFrame([{
        'Date': current_date,
        'Time': current_time,
        'Breadth': breadth_value,
        'Taiex_Change': taiex_change,
        'Taiex_Current': taiex_curr,
        'Taiex_Prev_Close': taiex_prev
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
                        df = df[:-1]
                        df = pd.concat([df, new_data], ignore_index=True)
                        df.to_csv(HISTORY_FILE, index=False)
                    else:
                        last_time = str(df.iloc[-1]['Time'])
                        if last_time != str(current_time):
                            new_data.to_csv(HISTORY_FILE, mode='a', header=False, index=False)
            else:
                new_data.to_csv(HISTORY_FILE, index=False)
        except:
            new_data.to_csv(HISTORY_FILE, index=False)

def plot_breadth_chart():
    if not os.path.exists(HISTORY_FILE):
        return None
    try:
        df = pd.read_csv(HISTORY_FILE)
        if df.empty:
            return None
        
        df['Breadth_Pct'] = df['Breadth']
        df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
        df['Taiex_Scaled'] = (df['Taiex_Change'] * 10) + 0.5
        
        base_date = df.iloc[0]['Date']
        start_bound = pd.to_datetime(f"{base_date} 09:00:00")
        end_bound = pd.to_datetime(f"{base_date} 14:30:00")

        base = alt.Chart(df).encode(
            x=alt.X('Datetime', 
                    title='時間', 
                    axis=alt.Axis(format='%H:%M'), 
                    scale=alt.Scale(domain=[start_bound, end_bound])
            )
        )

        tick_values = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        y_axis_config = alt.Axis(format='%', values=tick_values, tickCount=11, labelOverlap=False)

        line_breadth = base.mark_line(color='#007bff', clip=False).encode(
            y=alt.Y('Breadth_Pct', title=None, scale=alt.Scale(domain=[0, 1], nice=False), axis=y_axis_config)
        )
        point_breadth = base.mark_circle(color='#007bff', size=30, clip=False).encode(
            y='Breadth_Pct', tooltip=[alt.Tooltip('Datetime', format='%H:%M'), alt.Tooltip('Breadth_Pct', title='廣度', format='.1%')]
        )

        line_taiex = base.mark_line(color='#ffc107', strokeDash=[4,4], clip=False).encode(
            y=alt.Y('Taiex_Scaled', scale=alt.Scale(domain=[0, 1])) 
        )
        point_taiex = base.mark_circle(color='#ffc107', size=30, clip=False).encode(
            y='Taiex_Scaled', tooltip=[
                alt.Tooltip('Datetime', format='%H:%M'), alt.Tooltip('Taiex_Change', title='大盤漲跌', format='.2%'),
                alt.Tooltip('Taiex_Current', title='計算現價', format='.2f'), alt.Tooltip('Taiex_Prev_Close', title='基準昨收', format='.2f')
            ]
        )
        
        rule_red = alt.Chart(pd.DataFrame({'y': [BREADTH_THRESHOLD]})).mark_rule(color='red', strokeDash=[5, 5]).encode(y='y')
        rule_green = alt.Chart(pd.DataFrame({'y': [BREADTH_LOWER_REF]})).mark_rule(color='green', strokeDash=[5, 5]).encode(y='y')

        return (line_breadth + point_breadth + line_taiex + point_taiex + rule_red + rule_green).properties(
            title=f"走勢對照 (藍:廣度 / 黃:大盤) - {base_date}", height=400
        ).resolve_scale(y='shared')
    except:
        return None

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
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
    if tw_now.weekday() > 4:
        days_to_fri = tw_now.weekday() - 4
        last_friday = (tw_now - timedelta(days=days_to_fri)).strftime("%Y-%m-%d")
        if not dates or last_friday > dates[-1]:
            dates.append(last_friday)
    return dates

def fetch_shioaji_snapshots(sj_api, codes):
    if not sj_api or not codes:
        return {}, None
    contracts = []
    for code in codes:
        try:
            contract = sj_api.Contracts.Stocks[code]
            if contract:
                contracts.append(contract)
        except:
            pass
    if not contracts:
        return {}, None
    try:
        snapshots = sj_api.snapshots(contracts)
        price_map = {}
        ts = datetime.now()
        for snap in snapshots:
            if snap.close > 0:
                price_map[snap.code] = float(snap.close)
                if snap.ts:
                    ts = datetime.fromtimestamp(snap.ts / 1000000000)
        return price_map, ts.strftime("%H:%M:%S")
    except:
        return {}, None

def calc_stats_hybrid(sj_api, target_date, rank_codes, use_realtime=False):
    fm_token = get_finmind_token()
    if not fm_token:
        raise ValueError("Token Error")

    hits = 0; valid = 0; stats_map = {}; price_map = {}; last_t = None
    
    if use_realtime:
        if sj_api:
            price_map, last_t = fetch_shioaji_snapshots(sj_api, rank_codes)
        if not price_map:
            last_t = "無即時資料"
    
    start_date_query = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if use_realtime:
        prog_bar = st.progress(0, text="運算中...")
        
    total = len(rank_codes)

    for i, code in enumerate(rank_codes):
        if use_realtime and i % 50 == 0:
            prog_bar.progress((i / total), text=f"進度: {i+1}/{total}")
            
        current_price = 0; status = "未知"; price_src = "歷史"; ma5_val = 0; is_pass = False
        
        if use_realtime:
            current_price = price_map.get(code, 0)
            price_src = "永豐API"
            if current_price == 0:
                status = "⚠️ 無報價"

        try:
            stock_df = get_cached_stock_history(fm_token, code, start_date_query)
            if stock_df.empty:
                status = "❌ 無資料"
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
                        if isinstance(last_dt, pd.Timestamp):
                            last_dt = last_dt.strftime("%Y-%m-%d")
                        if last_dt != target_date:
                            status = f"🚫 未更"; stock_df = pd.DataFrame()
                        else: 
                            if not use_realtime:
                                current_price = float(stock_df.iloc[-1]['close'])
                
                if len(stock_df) >= 6:
                    stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                    curr = stock_df.iloc[-1]
                    final_price = float(curr['close'])
                    ma5_val = float(curr['MA5'])
                    if final_price > ma5_val:
                        hits += 1; is_pass = True; status = "✅ 通過"
                    else:
                        is_pass = False; status = f"📉 未過"
                    valid += 1
                else:
                    if "未更" not in status:
                        status = "🚫 資料不足"
        except:
            status = "❌ 錯誤"
        
        stats_map[code] = {'price': current_price, 'ma5': ma5_val, 'status': status, 'is_pass': is_pass, 'src': price_src}
    
    if use_realtime:
        prog_bar.empty()
    return hits, valid, stats_map, last_t

def fetch_data():
    fm_token = get_finmind_token()
    sj_api = get_shioaji_api()
    if not fm_token or not sj_api:
        return None

    all_days = get_trading_days_robust(fm_token)
    if len(all_days) < 2:
        return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    tw_now, is_intraday = get_current_status()
    
    try:
        prev_rank_codes = get_cached_rank_list(fm_token, d_prev_str, backup_date=all_days[-3])
    except:
        return None
    
    hit_prev, valid_prev, map_prev, _ = calc_stats_hybrid(None, d_prev_str, prev_rank_codes, use_realtime=False)
    
    rank_source_msg = ""
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中模式"
        rank_source_msg = f"名單依據：{d_prev_str} (昨日排行)"
    else:
        try:
            curr_rank_codes = get_cached_rank_list(fm_token, d_curr_str)
        except:
            curr_rank_codes = []
            
        if curr_rank_codes:
            mode_msg = "🐢 盤後模式 (資料已更新)"
            rank_source_msg = f"名單依據：{d_curr_str} (✅ 今日新排行)"
        else:
            curr_rank_codes = prev_rank_codes
            mode_msg = "⚠️ 盤後模式 (資料未更新)"
            rank_source_msg = f"名單依據：{d_prev_str} (⏳ 沿用昨日排行)"
            
    hit_curr, valid_curr, map_curr, last_time = calc_stats_hybrid(sj_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
    taiex_change = 0; slope = 0
    prev_close_price = 0; curr_taiex_price = 0
    
    try:
        twii_df = get_cached_stock_history(fm_token, "TAIEX", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        if not twii_df.empty:
            prev_row = twii_df[twii_df['date'] == d_prev_str]
            if not prev_row.empty:
                prev_close_price = float(prev_row.iloc[0]['close'])
        
        if sj_api:
             try:
                 snap = sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0]
                 if snap.close > 0:
                     curr_taiex_price = float(snap.close)
             except:
                 pass
        
        if curr_taiex_price == 0:
            curr_row = twii_df[twii_df['date'] == d_curr_str]
            if not curr_row.empty:
                curr_taiex_price = float(curr_row.iloc[0]['close'])
                
        if curr_taiex_price == 0:
            try:
                yf_data = yf.Ticker("^TWII").history(period="5d")
                if not yf_data.empty:
                    curr_taiex_price = float(yf_data.iloc[-1]['Close'])
            except:
                pass

        if curr_taiex_price > 0:
            if twii_df.empty or twii_df.iloc[-1]['date'] != d_curr_str:
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': curr_taiex_price}])
                twii_df = pd.concat([twii_df, new_row], ignore_index=True)
        
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
        
        if prev_close_price > 0 and curr_taiex_price > 0:
            taiex_change = (curr_taiex_price - prev_close_price) / prev_close_price
    except:
        pass
    
    br_curr = hit_curr / valid_curr if valid_curr > 0 else 0
    
    if is_intraday:
        if last_time and "無" not in str(last_time):
             record_time = last_time
        else:
             record_time = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
    else:
        record_time = "14:30:00"
    
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
        "mode_msg": mode_msg, "rank_source_msg": rank_source_msg, "last_time": last_time,
        "raw_record": {'Date': d_curr_str, 'Time': record_time, 'Breadth': br_curr}
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v8.2.3)")
    
    if 'last_alert_s
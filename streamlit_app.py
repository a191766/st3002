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
# 設定區
# ==========================================
APP_VERSION = "v8.2.4 (穩定瘦身版)"
# 參數
TOP_N = 300              
BREADTH_THRESHOLD = 0.65 
BREADTH_LOWER_REF = 0.55 
RAPID_CHANGE_THRESHOLD = 0.02 
EXCLUDE_PREFIXES = ["00", "91"]
HISTORY_FILE = "breadth_history_v3.csv"

st.set_page_config(page_title="盤中權證進場判斷 (v8.2.4)", layout="wide")

# ==========================================
# 核心功能函式
# ==========================================
def get_finmind_token():
    try: return st.secrets["finmind"]["token"]
    except: return None

def send_telegram_notify(token, chat_id, msg):
    if not token or not chat_id: return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}
        r = requests.post(url, json=payload)
        return r.status_code == 200
    except: return False

def check_rapid_change(current_row):
    if not os.path.exists(HISTORY_FILE): return None, None
    try:
        df = pd.read_csv(HISTORY_FILE)
        if len(df) < 2: return None, None
        
        curr_dt = datetime.strptime(f"{current_row['Date']} {current_row['Time']}", "%Y-%m-%d %H:%M:%S")
        curr_val = float(current_row['Breadth'])
        
        target_row = None
        # 找3分鐘前的資料 (誤差 170s~190s)
        for i in range(2, min(10, len(df) + 1)): 
            row = df.iloc[-i]
            row_dt = datetime.strptime(f"{row['Date']} {row['Time']}", "%Y-%m-%d %H:%M:%S")
            if 170 <= (curr_dt - row_dt).total_seconds() <= 190:
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
                return msg, str(curr_dt)
    except: pass
    return None, None

@st.cache_resource(ttl=3600) 
def get_shioaji_api():
    api = sj.Shioaji(simulation=False)
    try:
        api.login(api_key=st.secrets["shioaji"]["api_key"], secret_key=st.secrets["shioaji"]["secret_key"])
    except: return None
    return api

# ==========================================
# 資料處理與繪圖
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
    api = DataLoader(); api.login_by_token(token)
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now()-timedelta(days=20)).strftime("%Y-%m-%d"))
        if not df.empty: return sorted(df['date'].unique().tolist())
    except: pass
    return []

@st.cache_data(ttl=86400, show_spinner=False)
def get_cached_rank_list(token, date_str, backup_date=None):
    api = DataLoader(); api.login_by_token(token)
    df = pd.DataFrame()
    try: df = api.taiwan_stock_daily(stock_id="", start_date=date_str)
    except: pass
    if df.empty and backup_date:
        try: df = api.taiwan_stock_daily(stock_id="", start_date=backup_date)
        except: pass
    if df.empty: return []

    df['ID'] = smart_get_column(df, ['stock_id', 'code'])
    df['Money'] = smart_get_column(df, ['Trading_money', 'Trading_Money', 'turnover'])
    if df['ID'] is None or df['Money'] is None: return []

    df['ID'] = df['ID'].astype(str)
    df = df[df['ID'].str.len() == 4]
    df = df[df['ID'].str.isdigit()]
    for p in EXCLUDE_PREFIXES: df = df[~df['ID'].str.startswith(p)]
    return df.sort_values('Money', ascending=False).head(TOP_N)['ID'].tolist()

@st.cache_data(ttl=21600, show_spinner=False)
def get_cached_stock_history(token, code, start_date):
    api = DataLoader(); api.login_by_token(token)
    try: return api.taiwan_stock_daily(stock_id=code, start_date=start_date)
    except: return pd.DataFrame()

def save_breadth_record(date, time_str, breadth, taiex_chg, taiex_cur, taiex_prev, intraday):
    if taiex_cur == 0: return 
    row = pd.DataFrame([{'Date': date, 'Time': time_str, 'Breadth': breadth, 'Taiex_Change': taiex_chg, 'Taiex_Current': taiex_cur, 'Taiex_Prev_Close': taiex_prev}])
    if not os.path.exists(HISTORY_FILE):
        row.to_csv(HISTORY_FILE, index=False)
    else:
        try:
            df = pd.read_csv(HISTORY_FILE)
            if not df.empty and str(df.iloc[-1]['Date']) == str(date):
                if not intraday:
                    df = df[:-1]
                    pd.concat([df, row], ignore_index=True).to_csv(HISTORY_FILE, index=False)
                elif str(df.iloc[-1]['Time']) != str(time_str):
                    row.to_csv(HISTORY_FILE, mode='a', header=False, index=False)
            else:
                row.to_csv(HISTORY_FILE, index=False) # 新的一天或新檔
        except: row.to_csv(HISTORY_FILE, index=False)

def plot_breadth_chart():
    if not os.path.exists(HISTORY_FILE): return None
    try:
        df = pd.read_csv(HISTORY_FILE)
        if df.empty: return None
        df['Datetime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
        df['Taiex_Scaled'] = (df['Taiex_Change'] * 10) + 0.5
        
        base_date = df.iloc[0]['Date']
        base = alt.Chart(df).encode(x=alt.X('Datetime', title='時間', axis=alt.Axis(format='%H:%M'), scale=alt.Scale(domain=[pd.to_datetime(f"{base_date} 09:00:00"), pd.to_datetime(f"{base_date} 14:30:00")])))
        
        y_axis = alt.Axis(format='%', values=[i/10 for i in range(11)], tickCount=11, labelOverlap=False)
        line_b = base.mark_line(color='#007bff').encode(y=alt.Y('Breadth', title=None, scale=alt.Scale(domain=[0,1], nice=False), axis=y_axis))
        point_b = base.mark_circle(color='#007bff', size=30).encode(y='Breadth', tooltip=['Datetime', alt.Tooltip('Breadth', format='.1%')])
        
        line_t = base.mark_line(color='#ffc107', strokeDash=[4,4]).encode(y=alt.Y('Taiex_Scaled', scale=alt.Scale(domain=[0,1])))
        point_t = base.mark_circle(color='#ffc107', size=30).encode(y='Taiex_Scaled', tooltip=['Datetime', alt.Tooltip('Taiex_Change', format='.2%'), 'Taiex_Current'])
        
        rule_r = alt.Chart(pd.DataFrame({'y': [BREADTH_THRESHOLD]})).mark_rule(color='red', strokeDash=[5,5]).encode(y='y')
        rule_g = alt.Chart(pd.DataFrame({'y': [BREADTH_LOWER_REF]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y')
        
        return (line_b + point_b + line_t + point_t + rule_r + rule_g).properties(height=400, title=f"走勢對照 - {base_date}").resolve_scale(y='shared')
    except: return None

def get_current_status():
    now = datetime.now(timezone(timedelta(hours=8)))
    is_intraday = (time(8,45) <= now.time() < time(13,30)) and (0 <= now.weekday() <= 4)
    return now, is_intraday

def get_trading_days_robust(token):
    dates = get_cached_trading_days(token)
    now, _ = get_current_status()
    if not dates:
        d = now
        while len(dates) < 5:
            if d.weekday() <= 4: dates.append(d.strftime("%Y-%m-%d"))
            d -= timedelta(days=1)
        dates = sorted(dates)
    
    today = now.strftime("%Y-%m-%d")
    if 0 <= now.weekday() <= 4 and now.time() >= time(8,45):
        if not dates or today > dates[-1]: dates.append(today)
    return dates

def fetch_data():
    fm_token = get_finmind_token()
    sj_api = get_shioaji_api()
    if not fm_token or not sj_api: return None # Token Error

    days = get_trading_days_robust(fm_token)
    if len(days) < 2: return None
    
    d_curr, d_prev = days[-1], days[-2]
    now, is_intraday = get_current_status()
    
    # 取得名單
    prev_codes = get_cached_rank_list(fm_token, d_prev, days[-3])
    if not prev_codes: return None
    
    curr_codes = []
    if not is_intraday: curr_codes = get_cached_rank_list(fm_token, d_curr)
    
    final_codes = curr_codes if curr_codes else prev_codes
    mode_msg = f"名單: {d_curr if curr_codes else d_prev}"

    # 取得即時報價
    price_map = {}
    last_time = "無即時資料"
    if sj_api:
        try:
            contracts = [sj_api.Contracts.Stocks[c] for c in final_codes if c in sj_api.Contracts.Stocks]
            snaps = sj_api.snapshots(contracts)
            ts = datetime.now()
            for s in snaps:
                if s.close > 0: 
                    price_map[s.code] = float(s.close)
                    ts = datetime.fromtimestamp(s.ts / 1e9)
            last_time = ts.strftime("%H:%M:%S")
        except: pass

    # 計算廣度
    hits, valid = 0, 0
    start_dt = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    details = []
    
    for code in final_codes:
        curr_p = price_map.get(code, 0)
        df = get_cached_stock_history(fm_token, code, start_date=start_dt)
        
        status, ma5 = "無資料", 0
        if not df.empty:
            # 簡單 MA5 計算
            if is_intraday and curr_p > 0:
                new_row = pd.DataFrame([{'date': d_curr, 'close': curr_p}])
                df = pd.concat([df[df['date'] < d_curr], new_row], ignore_index=True)
            
            if len(df) >= 5:
                df['MA5'] = df['close'].rolling(5).mean()
                ma5 = df.iloc[-1]['MA5']
                final_p = float(df.iloc[-1]['close'])
                if final_p > ma5: hits += 1
                valid += 1
                status = "✅" if final_p > ma5 else "📉"
        
        details.append({"代號": code, "現價": curr_p, "MA5": round(ma5, 2), "狀態": status})

    br_curr = hits / valid if valid > 0 else 0
    
    # 大盤資料
    taiex_cur, taiex_prev_close = 0, 0
    try:
        twii = get_cached_stock_history(fm_token, "TAIEX", start_dt)
        if not twii.empty:
            taiex_prev_close = float(twii[twii['date'] == d_prev].iloc[0]['close']) if not twii[twii['date'] == d_prev].empty else 0
            
        if sj_api:
            s = sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0]
            if s.close > 0: taiex_cur = float(s.close)
            
        if taiex_cur == 0 and not twii.empty: # Fallback
             r = twii[twii['date'] == d_curr]
             if not r.empty: taiex_cur = float(r.iloc[0]['close'])
    except: pass
    
    taiex_chg = (taiex_cur - taiex_prev_close) / taiex_prev_close if taiex_prev_close > 0 else 0
    
    # 存檔
    rec_time = last_time if is_intraday and "無" not in str(last_time) else ("14:30:00" if not is_intraday else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S"))
    save_breadth_record(d_curr, rec_time, br_curr, taiex_chg, taiex_cur, taiex_prev_close, is_intraday)

    return {
        "d_curr": d_curr, "br_curr": br_curr, "hit": hits, "valid": valid,
        "detail": pd.DataFrame(details), "msg": mode_msg, "last_time": last_time,
        "taiex_chg": taiex_chg,
        "raw": {'Date': d_curr, 'Time': rec_time, 'Breadth': br_curr}
    }

# ==========================================
# 主程式
# ==========================================
def run_streamlit():
    st.title(f"📈 盤中權證進場判斷 ({APP_VERSION})")
    
    if 'last_alert' not in st.session_state: st.session_state['last_alert'] = 'normal'
    if 'last_rapid' not in st.session_state: st.session_state['last_rapid'] = ""

    with st.sidebar:
        st.subheader("設定")
        auto_ref = st.checkbox("啟用自動更新", value=False)
        tg_token = st.text_input("TG Token", value=st.secrets.get("telegram", {}).get("token", ""), type="password")
        tg_id = st.text_input("Chat ID", value=st.secrets.get("telegram", {}).get("chat_id", ""))
        if tg_token and tg_id: st.success("TG Ready")

    if st.button("🔄 重新整理"): st.rerun()

    try:
        data = fetch_data()
        if data:
            br = data['br_curr']
            # Telegram Alert
            if tg_token and tg_id:
                status = 'normal'
                if br >= BREADTH_THRESHOLD: status = 'hot'
                elif br <= BREADTH_LOWER_REF: status = 'cold'
                
                if status != st.session_state['last_alert']:
                    if status == 'hot': send_telegram_notify(tg_token, tg_id, f"🔥 過熱警報: {br:.1%}")
                    if status == 'cold': send_telegram_notify(tg_token, tg_id, f"❄️ 冰點警報: {br:.1%}")
                    st.session_state['last_alert'] = status
                
                rapid_msg, rid = check_rapid_change(data['raw'])
                if rapid_msg and rid != st.session_state['last_rapid']:
                    send_telegram_notify(tg_token, tg_id, rapid_msg)
                    st.session_state['last_rapid'] = rid

            st.subheader(f"📅 {data['d_curr']}")
            st.caption(data['msg'])
            
            chart = plot_breadth_chart()
            if chart: st.altair_chart(chart, use_container_width=True)
            
            c1, c2, c3 = st.columns(3)
            c1.metric("今日廣度", f"{br:.1%}", f"{data['hit']}/{data['valid']}")
            c2.metric("大盤漲跌", f"{data['taiex_chg']:.2%}")
            c3.metric("狀態", "🔥" if br>=0.65 else ("❄️" if br<=0.55 else "---"))
            
            st.caption(f"Update: {data['last_time']}")
            st.dataframe(data['detail'], use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ 目前無資料，正在連線或非交易日...")
            
    except Exception as e: st.error(f"Error: {e}")

    if auto_ref:
        now, intraday = get_current_status()
        if intraday:
            sec = 60 if (time(9,0)<=now.time()<time(10,0) or time(12,30)<=now.time()<time(13,30)) else 180
            with st.sidebar:
                t = st.empty()
                for i in range(sec, 0, -1):
                    t.info(f"⏳ 更新: {i}s")
                    time_module.sleep(1)
            st.rerun()
        else: st.sidebar.warning("⏸ 休市中")

if __name__ == "__main__":
    if 'streamlit' in sys.modules: run_streamlit()

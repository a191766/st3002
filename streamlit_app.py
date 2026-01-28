# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import shioaji as sj
import os, sys, requests, json, subprocess, traceback
import altair as alt
import time as time_module
import random

# ==========================================
# 設定區 v9.37.1 (Key值與連線修復版)
# ==========================================
APP_VER = "v9.37.1 (Key值與連線修復版)"
TOP_N = 300              
BREADTH_THR = 0.65 
BREADTH_LOW = 0.55 
RAPID_THR = 0.03 
OPEN_DEV_THR = 0.05 
OPEN_COUNT_THR = 295 

EXCL_PFX = ["00", "91"]
HIST_FILE = "breadth_history_v3.csv"
RANK_FILE = "ranking_cache.json"
NOTIFY_FILE = "notify_state.json" 

# ==========================================
# 基礎函式
# ==========================================
def get_finmind_token():
    try:
        return st.secrets["finmind"]["token"]
    except:
        return None

def send_tg(token, chat_id, msg):
    if not token or not chat_id: return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})
        return r.status_code == 200
    except:
        return False

def load_notify_state(today_str):
    default_state = {
        "date": today_str,
        "last_stt": "normal",
        "last_rap": "",
        "was_dev_high": False,
        "was_dev_low": False,
        "notified_drop_high": False,
        "notified_rise_low": False,
        "intraday_trend": None  
    }
    if not os.path.exists(NOTIFY_FILE): return default_state
    try:
        with open(NOTIFY_FILE, 'r') as f:
            state = json.load(f)
            if state.get("date") != today_str: return default_state
            if "intraday_trend" not in state: state["intraday_trend"] = None
            return state
    except: return default_state

def save_notify_state(state):
    try:
        with open(NOTIFY_FILE, 'w') as f:
            json.dump(state, f)
    except: pass

def check_rapid(row):
    if not os.path.exists(HIST_FILE): return None, None
    try:
        df = pd.read_csv(HIST_FILE)
        if len(df) < 2: return None, None
        curr_dt = datetime.strptime(f"{row['Date']} {row['Time']}", "%Y-%m-%d %H:%M")
        curr_v = float(row['Breadth'])
        target = None
        for i in range(2, min(15, len(df)+1)):
            r = df.iloc[-i]
            try: 
                r_t = r['Time'] if len(str(r['Time']))==5 else r['Time'][:5]
            except: continue
            r_dt = datetime.strptime(f"{r['Date']} {r_t}", "%Y-%m-%d %H:%M")
            seconds_diff = (curr_dt - r_dt).total_seconds()
            if 230 <= seconds_diff <= 250:
                target = r; break
        if target is not None:
            prev_v = float(target['Breadth'])
            diff = curr_v - prev_v
            if abs(diff) >= RAPID_THR:
                d_str = "上漲" if diff>0 else "下跌"
                msg = f"⚡ <b>【廣度急變】</b>\n{target['Time'][:5]}廣度{prev_v:.0%}，{row['Time']}廣度{curr_v:.0%}，{d_str}{abs(diff):.0%}"
                return msg, str(curr_dt)
    except: pass
    return None, None

def get_opening_breadth(d_cur):
    if not os.path.exists(HIST_FILE): return None
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: return None
        if 'Total' not in df.columns: df['Total'] = 0
        df['Date'] = df['Date'].astype(str)
        df_today = df[df['Date'] == str(d_cur)].copy()
        if df_today.empty: return None
        df_today = df_today[df_today['Time'] >= "09:00"]
        df_valid = df_today[df_today['Total'] >= OPEN_COUNT_THR].sort_values('Time')
        if not df_valid.empty: return float(df_valid.iloc[0]['Breadth'])
    except: pass
    return None

def get_intraday_extremes(d_cur):
    if not os.path.exists(HIST_FILE): return None, None
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: return None, None
        df['Date'] = df['Date'].astype(str)
        df_today = df[df['Date'] == str(d_cur)]
        if df_today.empty: return None, None
        return df_today['Breadth'].max(), df_today['Breadth'].min()
    except: return None, None

@st.cache_resource(ttl=3600) 
def get_api():
    api = sj.Shioaji(simulation=False)
    try: 
        api.login(api_key=st.secrets["shioaji"]["api_key"], secret_key=st.secrets["shioaji"]["secret_key"])
        api.fetch_contracts(contract_download=True)
        return api, None
    except Exception as e:
        return None, str(e)

# ==========================================
# 資料處理
# ==========================================
def get_col(df, names):
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n in df.columns: return df[n]
        if n.lower() in cols: return df[cols[n.lower()]]
    return None

@st.cache_data(ttl=600)
def get_days(token):
    api = DataLoader()
    if token: api.login_by_token(token)
    dates = []
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now()-timedelta(days=20)).strftime("%Y-%m-%d"))
        if not df.empty: dates = sorted(df['date'].unique().tolist())
    except: pass
    now = datetime.now(timezone(timedelta(hours=8)))
    today_str = now.strftime("%Y-%m-%d")
    if 0 <= now.weekday() <= 4 and now.time() >= time(8,0):
        if not dates or today_str > dates[-1]: dates.append(today_str)
    return dates

@st.cache_data(ttl=86400)
def get_stock_info_map(token):
    api = DataLoader()
    if token: api.login_by_token(token)
    try:
        df = api.taiwan_stock_info()
        if df.empty: return {}
        df['stock_id'] = df['stock_id'].astype(str)
        return dict(zip(df['stock_id'], df['type']))
    except: return {}

def get_ranks_strict(token, target_date_str):
    if os.path.exists(RANK_FILE):
        try:
            with open(RANK_FILE, 'r') as f:
                data = json.load(f)
                if data.get("date") == target_date_str and data.get("ranks"):
                    return data["ranks"], True
        except: pass
    api = DataLoader()
    if token: api.login_by_token(token)
    df = pd.DataFrame()
    try: df = api.taiwan_stock_daily(stock_id="", start_date=target_date_str)
    except: pass
    if df.empty: return [], False
    df['ID'] = get_col(df, ['stock_id','code'])
    df['Money'] = get_col(df, ['Trading_money','turnover'])
    if df['ID'] is None or df['Money'] is None: return [], False
    df['ID'] = df['ID'].astype(str)
    df = df[df['ID'].str.len()==4]
    df = df[df['ID'].str.isdigit()]
    for p in EXCL_PFX: df = df[~df['ID'].str.startswith(p)]
    ranks = df.sort_values('Money', ascending=False).head(TOP_N)['ID'].tolist()
    if ranks:
        try:
            with open(RANK_FILE, 'w') as f:
                json.dump({"date": target_date_str, "ranks": ranks}, f)
        except: pass
    return ranks, False

@st.cache_data(ttl=43200)
def get_hist(token, code, start):
    api = DataLoader()
    if token: api.login_by_token(token)
    try: return api.taiwan_stock_daily(stock_id=code, start_date=start)
    except: return pd.DataFrame()

def get_prices_twse_mis(codes, info_map):
    """
    強化版：統一 Key 值為 price 與 y_close
    """
    if not codes: return {}
    if 'mis_session' not in st.session_state:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw",
            "X-Requested-With": "XMLHttpRequest"
        })
        try:
            session.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", timeout=10)
        except: pass
        st.session_state['mis_session'] = session
    
    session = st.session_state['mis_session']
    results = {}
    chunk_size = 20 # 稍微放大以提高效率，但若失敗可改回 5
    
    for i in range(0, len(codes), chunk_size):
        chunk = codes[i:i+chunk_size]
        q_list = []
        for c in chunk:
            if c == "t00": q_list.append("tse_t00.tw")
            else:
                m_type = info_map.get(c, "")
                if m_type == "tpex": q_list.append(f"otc_{c}.tw")
                elif m_type == "twse": q_list.append(f"tse_{c}.tw")
                else:
                    q_list.append(f"tse_{c}.tw")
                    q_list.append(f"otc_{c}.tw")
        
        q_str = "|".join(q_list)
        ts = int(time_module.time() * 1000)
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&delay=0&_={ts}&ex_ch={q_str}"
        
        try:
            time_module.sleep(random.uniform(0.2, 0.5))
            r = session.get(url, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if 'msgArray' in data:
                    for item in data['msgArray']:
                        c = item.get('c', '')
                        z = item.get('z', '-') # 當前成交價
                        y = item.get('y', '-') # 昨收
                        
                        val = {}
                        try:
                            if y != '-' and y != '': val['y_close'] = float(y)
                            
                            price = 0
                            if z != '-' and z != '': price = float(z)
                            if price == 0:
                                b_str = item.get('b', '-')
                                if b_str != '-' and b_str != '': price = float(b_str.split('_')[0])
                            
                            if price > 0: 
                                val['price'] = price
                                results[c] = val
                        except: continue
            elif r.status_code in [403, 401]:
                del st.session_state['mis_session']
                break
        except: pass
            
    return results

def save_rec(d, t, b, tc, t_cur, t_prev, intra, total_v):
    if t_cur == 0: return 
    t_short = t[:5] 
    row = pd.DataFrame([{
        'Date':d, 'Time':t_short, 'Breadth':b, 
        'Taiex_Change':tc, 'Taiex_Current':t_cur, 'Taiex_Prev_Close':t_prev,
        'Total': total_v
    }])
    if not os.path.exists(HIST_FILE): 
        row.to_csv(HIST_FILE, index=False); return
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: row.to_csv(HIST_FILE, index=False); return
        if 'Total' not in df.columns: df['Total'] = 0
        df['Date'] = df['Date'].astype(str); df['Time'] = df['Time'].astype(str)
        last_d = str(df.iloc[-1]['Date']); last_t = str(df.iloc[-1]['Time'])[:5]
        if last_d != str(d): 
            pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
        else:
            if not intra: 
                df = df[df['Date'] != str(d)]
                pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
            elif last_t != str(t_short): 
                row.to_csv(HIST_FILE, mode='a', header=False, index=False)
    except: row.to_csv(HIST_FILE, index=False)

def display_strategy_panel(slope, open_br, br, n_state):
    st.subheader("♟️ 戰略指揮所")
    strategies = []
    if slope > 0: strategies.append({"sig": "MA5斜率為正 ➜ 大盤偏多", "act": "只做多單", "type": "success"})
    elif slope < 0: strategies.append({"sig": "MA5斜率為負 ➜ 大盤偏空", "act": "只做空單", "type": "error"})
    else: strategies.append({"sig": "MA5斜率持平", "act": "", "type": "info"})
        
    trend_status = n_state.get('intraday_trend')
    if trend_status == 'up': strategies.append({"sig": "🔒 已觸發【開盤+5%】", "act": "今日偏多確認", "type": "success"})
    elif trend_status == 'down': strategies.append({"sig": "🔒 已觸發【開盤-5%】", "act": "今日偏空確認", "type": "error"})

    if slope > 0 and trend_status == 'up' and n_state['notified_drop_high']:
        strategies.append({"sig": "多頭回檔 (高點落 5%)", "act": "🎯 尋找進場多點", "type": "success"})
    elif slope < 0 and trend_status == 'down' and n_state['notified_rise_low']:
        strategies.append({"sig": "空頭反彈 (低點彈 5%)", "act": "🎯 尋找放空點位", "type": "error"})

    cols = st.columns(len(strategies))
    for i, s in enumerate(strategies):
        with cols[i]:
            if s["type"] == "success": st.success(f"**{s['sig']}**\n\n{s['act']}")
            elif s["type"] == "error": st.error(f"**{s['sig']}**\n\n{s['act']}")
            else: st.info(f"**{s['sig']}**\n\n{s['act']}")

def plot_chart():
    if not os.path.exists(HIST_FILE): return None
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: return None
        df['Date'] = df['Date'].astype(str); df['Time'] = df['Time'].astype(str).apply(lambda x: x[:5])
        df['DT'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')
        df = df.dropna(subset=['DT'])
        df['T_S'] = (df['Taiex_Change']*10)+0.5
        base_d = df.iloc[-1]['Date']
        chart_data = df[df['Date'] == base_d].copy()
        if chart_data.empty: return None
        start_t = pd.to_datetime(f"{base_d} 09:00:00"); end_t = pd.to_datetime(f"{base_d} 13:30:00")
        base = alt.Chart(chart_data).encode(x=alt.X('DT:T', axis=alt.Axis(format='%H:%M'), scale=alt.Scale(domain=[start_t, end_t])))
        l_b = base.mark_line(color='#007bff').encode(y=alt.Y('Breadth', scale=alt.Scale(domain=[0,1])))
        l_t = base.mark_line(color='#ffc107', strokeDash=[4,4]).encode(y=alt.Y('T_S', scale=alt.Scale(domain=[0,1])))
        return (l_b+l_t).properties(height=400, title=f"走勢對照 - {base_d}")
    except: return None

def fetch_all():
    ft = get_finmind_token()
    sj_api, sj_err = get_api() 
    days = get_days(ft)
    now = datetime.now(timezone(timedelta(hours=8)))
    today_str = now.strftime("%Y-%m-%d")
    if not days: days = [today_str]
    info_map = get_stock_info_map(ft)
    d_cur = days[-1]
    is_intra = (time(8,45)<=now.time()<time(13,30)) and (0<=now.weekday()<=4)
    allow_live_fetch = (0<=now.weekday()<=4) and (now.time() >= time(8,45))
    
    target_date_for_ranks = days[-2] if (len(days)>1 and now.time() < time(14, 0) and d_cur == today_str) else d_cur
    final_codes, from_disk = get_ranks_strict(ft, target_date_for_ranks)
    
    pmap = {}
    data_source = "歷史"
    last_t = "無即時資料"
    api_status_code = 0 
    
    if allow_live_fetch:
        # 1. Shioaji
        if sj_api:
            try:
                contracts = [sj_api.Contracts.Stocks[c] for c in final_codes if c in sj_api.Contracts.Stocks]
                if contracts:
                    for i in range(0, len(contracts), 50):
                        snaps = sj_api.snapshots(contracts[i:i+50])
                        for s in snaps:
                            if s.close > 0:
                                pmap[s.code] = {'price': float(s.close), 'y_close': float(s.reference_price)}
                    if pmap: data_source = "永豐API"; api_status_code = 2
            except: pass
        
        # 2. MIS
        missing_codes = [c for c in final_codes if c not in pmap]
        if missing_codes:
            mis_data = get_prices_twse_mis(missing_codes, info_map)
            for c, val in mis_data.items(): pmap[c] = val
            if mis_data and data_source == "歷史": data_source = "證交所MIS"; api_status_code = 2

    s_dt = (datetime.now()-timedelta(days=40)).strftime("%Y-%m-%d")
    h_c, v_c, h_p, v_p = 0, 0, 0, 0
    dtls = []
    
    for c in final_codes:
        df = get_hist(ft, c, s_dt)
        m_display = {"twse":"上市", "tpex":"上櫃"}.get(info_map.get(c, ""), "未知")
        info = pmap.get(c, {})
        curr_p = info.get('price', 0)
        real_y = info.get('y_close', 0)
        
        p_price = real_y if real_y > 0 else (float(df.iloc[-1]['close']) if not df.empty else 0)
        p_ma5, p_stt = 0, "-"
        if not df.empty and p_price > 0:
            closes = df['close'].tail(5).tolist()
            if len(closes) >= 5:
                p_ma5 = sum(closes) / 5
                p_stt = "✅" if p_price > p_ma5 else "📉"
                h_p += (1 if p_price > p_ma5 else 0); v_p += 1

        c_ma5, c_stt = 0, "-"
        if curr_p > 0 and p_price > 0 and not df.empty:
            hist_closes = df['close'].tail(4).tolist()
            hist_closes.append(curr_p)
            c_ma5 = sum(hist_closes) / 5
            c_stt = "✅" if curr_p > c_ma5 else "📉"
            h_c += (1 if curr_p > c_ma5 else 0); v_c += 1
        elif curr_p == 0: c_stt = "⚠️無報價"
        
        dtls.append({"代號":c, "市場": m_display, "昨收":p_price, "昨MA5":round(p_ma5,2), "昨狀態":p_stt, "現價":curr_p, "今MA5":round(c_ma5,2), "今狀態":c_stt})

    br_c = h_c/v_c if v_c>0 else 0
    br_p = h_p/v_p if v_p>0 else 0
    
    # 大盤處理
    t_cur, t_pre, slope = 0, 0, 0
    try:
        tw = get_hist(ft, "TAIEX", s_dt)
        mis_tw = get_prices_twse_mis(["t00"], {"t00":"twse"})
        t_cur = mis_tw.get("t00", {}).get("price", 0)
        t_pre = mis_tw.get("t00", {}).get("y_close", (float(tw.iloc[-1]['close']) if not tw.empty else 0))
        if t_cur > 0 and not tw.empty:
            h_tw = tw['close'].tail(4).tolist()
            ma5_y = sum(tw['close'].tail(5)) / 5
            ma5_c = (sum(h_tw) + t_cur) / 5
            slope = ma5_c - ma5_y
    except: pass
    
    t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    rec_t = now.strftime("%H:%M")
    save_rec(d_cur, rec_t, br_c, t_chg, t_cur, t_pre, is_intra, v_c)
    
    return {
        "d":d_cur, "br":br_c, "br_p":br_p, "h":h_c, "v":v_c, "df":pd.DataFrame(dtls), 
        "t":rec_t, "tc":t_chg, "slope":slope, "src_type": data_source, "api_status": api_status_code
    }

def run_app():
    st.title(f"📈 {APP_VER}")
    data = fetch_all()
    if data:
        st.sidebar.info(f"報價來源: {data['src_type']}")
        br = data['br']
        open_br = get_opening_breadth(data['d'])
        n_state = load_notify_state(data['d']) 
        display_strategy_panel(data['slope'], open_br, br, n_state)
        
        c1,c2,c3 = st.columns(3)
        c1.metric("今日廣度", f"{br:.1%}", f"{data['h']}/{data['v']}")
        c2.metric("大盤漲跌", f"{data['tc']:.2%}")
        c3.metric("大盤MA5斜率", f"{data['slope']:.2f}")
        
        st.dataframe(data['df'], use_container_width=True, hide_index=True)
    else: st.warning("⚠️ 無資料")

if __name__ == "__main__":
    if 'streamlit' in sys.modules and any('streamlit' in arg for arg in sys.argv):
        run_app()
    else:
        subprocess.call(["streamlit", "run", __file__])
        input("\n程式執行結束，請按 Enter 鍵離開...")

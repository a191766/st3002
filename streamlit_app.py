# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import shioaji as sj
import os, sys, requests, json
import altair as alt
import yfinance as yf
import time as time_module
import random

# ==========================================
# 設定區 v9.22.0 (策略訊號戰情版)
# ==========================================
APP_VER = "v9.22.0 (策略訊號戰情版)"
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

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 基礎函式
# ==========================================
def get_finmind_token():
    try: return st.secrets["finmind"]["token"]
    except: return None

def send_tg(token, chat_id, msg):
    if not token or not chat_id: return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"})
        return r.status_code == 200
    except: return False

def load_notify_state(today_str):
    default_state = {
        "date": today_str,
        "last_stt": "normal",
        "last_rap": "",
        "was_dev_high": False,
        "was_dev_low": False,
        "notified_drop_high": False,
        "notified_rise_low": False
    }
    
    if not os.path.exists(NOTIFY_FILE): return default_state
    
    try:
        with open(NOTIFY_FILE, 'r') as f:
            state = json.load(f)
            if state.get("date") != today_str:
                return default_state
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
            try: r_t = r['Time'] if len(str(r['Time']))==5 else r['Time'][:5]
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
        df['Time'] = df['Time'].astype(str)
        
        df_today = df[df['Date'] == str(d_cur)].copy()
        if df_today.empty: return None
        
        df_today = df_today[df_today['Time'] >= "09:00"]
        
        df_valid = df_today[df_today['Total'] >= OPEN_COUNT_THR].sort_values('Time')
        
        if not df_valid.empty:
            return float(df_valid.iloc[0]['Breadth'])
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
    api = DataLoader(); api.login_by_token(token)
    dates = []
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now()-timedelta(days=20)).strftime("%Y-%m-%d"))
        if not df.empty: dates = sorted(df['date'].unique().tolist())
    except: pass
    
    now = datetime.now(timezone(timedelta(hours=8)))
    today_str = now.strftime("%Y-%m-%d")
    if 0 <= now.weekday() <= 4 and now.time() >= time(8,45):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
    return dates

@st.cache_data(ttl=86400)
def get_stock_info_map(token):
    api = DataLoader(); api.login_by_token(token)
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

    api = DataLoader(); api.login_by_token(token)
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
    api = DataLoader(); api.login_by_token(token)
    try: return api.taiwan_stock_daily(stock_id=code, start_date=start)
    except: return pd.DataFrame()

def get_prices_twse_mis(codes, info_map):
    if not codes: return {}
    req_strs = []
    chunk_size = 50 
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw",
    }

    for i in range(0, len(codes), chunk_size):
        chunk = codes[i:i+chunk_size]
        q_list = []
        for c in chunk:
            m_type = info_map.get(c, "twse")
            prefix = "otc" if m_type == "tpex" else "tse"
            q_list.append(f"{prefix}_{c}.tw")
        req_strs.append("|".join(q_list))
    
    results = {}
    ts = int(time_module.time() * 1000)
    base_url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?json=1&_={ts}&ex_ch="
    
    for q_str in req_strs:
        try:
            url = base_url + q_str
            time_module.sleep(random.uniform(0.1, 0.3)) 
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                data = r.json()
                if 'msgArray' in data:
                    for item in data['msgArray']:
                        c = item.get('c', '') 
                        z = item.get('z', '-') 
                        y = item.get('y', '-') 
                        if c and z != '-':
                            try: results[c] = float(z)
                            except: pass
                        elif c and y != '-':
                            try: results[c] = float(y)
                            except: pass
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
        
        df['Date'] = df['Date'].astype(str)
        df['Time'] = df['Time'].astype(str)
        last_d = str(df.iloc[-1]['Date'])
        last_t_raw = str(df.iloc[-1]['Time'])
        last_t = last_t_raw[:5]
        if last_d != str(d): 
            pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
        else:
            if not intra: 
                df = df[df['Date'] != str(d)]
                pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
            elif last_t != str(t_short): 
                row.to_csv(HIST_FILE, mode='a', header=False, index=False)
    except: row.to_csv(HIST_FILE, index=False)

def plot_chart():
    if not os.path.exists(HIST_FILE): return None
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: return None
        df['Date'] = df['Date'].astype(str)
        df['Time'] = df['Time'].astype(str)
        df['Time'] = df['Time'].apply(lambda x: x[:5])
        df['DT'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')
        df = df.dropna(subset=['DT'])
        df['T_S'] = (df['Taiex_Change']*10)+0.5
        base_d = df.iloc[-1]['Date']
        chart_data = df[df['Date'] == base_d].copy()
        if chart_data.empty: return None
        
        start_t = pd.to_datetime(f"{base_d} 09:00:00")
        end_t = pd.to_datetime(f"{base_d} 13:30:00")
        
        base = alt.Chart(chart_data).encode(x=alt.X('DT:T', title='時間', axis=alt.Axis(format='%H:%M'), scale=alt.Scale(domain=[start_t, end_t])))
        y_ax = alt.Axis(format='%', values=[i/10 for i in range(11)], tickCount=11, labelOverlap=False)
        l_b = base.mark_line(color='#007bff').encode(y=alt.Y('Breadth', title=None, scale=alt.Scale(domain=[0,1], nice=False), axis=y_ax))
        p_b = base.mark_circle(color='#007bff', size=15).encode(y='Breadth', tooltip=['DT', alt.Tooltip('Breadth', format='.1%')])
        l_t = base.mark_line(color='#ffc107', strokeDash=[4,4]).encode(y=alt.Y('T_S', scale=alt.Scale(domain=[0,1])))
        p_t = base.mark_circle(color='#ffc107', size=15).encode(y='T_S', tooltip=['DT', alt.Tooltip('Taiex_Change', format='.2%')])
        rule_r = alt.Chart(pd.DataFrame({'y':[BREADTH_THR]})).mark_rule(color='red', strokeDash=[5,5]).encode(y='y')
        rule_g = alt.Chart(pd.DataFrame({'y':[BREADTH_LOW]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y')
        return (l_b+p_b+l_t+p_t+rule_r+rule_g).properties(height=400, title=f"走勢對照 - {base_d}").resolve_scale(y='shared')
    except: return None

def fetch_all():
    ft = get_finmind_token()
    if not ft: return "FinMind Token Error"
    
    sj_api, sj_err = get_api() 
    days = get_days(ft)
    if len(days)<2: return "日期資料不足"
    
    info_map = get_stock_info_map(ft)
    
    d_cur = days[-1]
    now = datetime.now(timezone(timedelta(hours=8)))
    is_intra = (time(8,45)<=now.time()<time(13,30)) and (0<=now.weekday()<=4)
    allow_live_fetch = (0<=now.weekday()<=4) and (now.time() >= time(8,45))
    
    today_str = now.strftime("%Y-%m-%d")
    target_date_for_ranks = ""
    
    if now.time() < time(14, 0):
        if d_cur == today_str: target_date_for_ranks = days[-2]
        else: target_date_for_ranks = d_cur
    else:
        target_date_for_ranks = today_str

    final_codes, from_disk = get_ranks_strict(ft, target_date_for_ranks)
    
    if not final_codes and target_date_for_ranks == today_str:
        fallback_date = days[-2] if d_cur == today_str else d_cur
        final_codes, _ = get_ranks_strict(ft, fallback_date)
        msg_src = f"名單:{fallback_date}(今日未出，沿用舊單)"
    else:
        msg_src = f"名單:{target_date_for_ranks} {'(硬碟)' if from_disk else '(新抓)'}"

    pmap = {}
    data_source = "歷史"
    last_t = "無即時資料"
    api_status_code = 0 
    sj_usage_info = "無資料"
    
    is_post_market = (now.time() >= time(14, 0))
    
    if allow_live_fetch and not is_post_market:
        if sj_api:
            try:
                try: usage = sj_api.usage(); sj_usage_info = str(usage) if usage else "無法取得"
                except: sj_usage_info = "無法取得"

                contracts = []
                for c in final_codes:
                    if c in sj_api.Contracts.Stocks: contracts.append(sj_api.Contracts.Stocks[c])
                
                chunk_size = 20
                count_sj = 0
                ts_obj = datetime.now()
                
                if contracts:
                    for i in range(0, len(contracts), chunk_size):
                        chunk = contracts[i:i+chunk_size]
                        try:
                            snaps = sj_api.snapshots(chunk)
                            for s in snaps:
                                if s.close > 0: 
                                    pmap[s.code] = float(s.close)
                                    ts_obj = datetime.fromtimestamp(s.ts/1e9)
                                    count_sj += 1
                            time_module.sleep(1.0)
                        except: pass
                    
                    if count_sj > 0:
                        last_t = ts_obj.strftime("%H:%M:%S")
                        data_source = "永豐API"
                        api_status_code = 2
                    else: api_status_code = 1
                else: api_status_code = 1
            except: api_status_code = 1 
        
        if not pmap:
            pmap = get_prices_twse_mis(final_codes, info_map)
            if pmap:
                data_source = "證交所MIS(免登入)"
                last_t = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
    elif is_post_market:
        data_source = "FinMind盤後資料"
        last_t = "13:30:00"

    s_dt = (datetime.now()-timedelta(days=40)).strftime("%Y-%m-%d")
    h_c, v_c, h_p, v_p = 0, 0, 0, 0
    dtls = []
    
    for c in final_codes:
        df = get_hist(ft, c, s_dt)
        m_type = info_map.get(c, "未知")
        m_display = {"twse":"上市", "tpex":"上櫃", "emerging":"興櫃"}.get(m_type, "未知")
        
        p_price, p_ma5, p_stt = 0, 0, "-"
        
        curr_p = pmap.get(c, 0)
        
        if is_post_market and not df.empty:
            if df.iloc[-1]['date'] == today_str:
                curr_p = float(df.iloc[-1]['close'])

        if not df.empty:
            try:
                if df.iloc[-1]['date'] == target_date_for_ranks:
                    if len(df) >= 2:
                        df_temp = df.copy()
                        df_temp['MA5'] = df_temp['close'].rolling(5).mean()
                        p_price = float(df_temp.iloc[-2]['close'])
                        p_ma5 = float(df_temp.iloc[-2]['MA5'])
                    else:
                        p_price = 0; p_ma5=0
                else:
                    df_temp = df.copy()
                    df_temp['MA5'] = df_temp['close'].rolling(5).mean()
                    p_price = float(df_temp.iloc[-1]['close'])
                    p_ma5 = float(df_temp.iloc[-1]['MA5'])

                if p_price > 0 and p_ma5 > 0:
                    if p_price > p_ma5: h_p += 1; p_stt="✅"
                    else: p_stt="📉"
                    v_p += 1
            except: pass

        c_ma5, c_stt, note = 0, "-", ""
        
        if not df.empty:
            df_cur = df.copy()
            if curr_p > 0:
                if df_cur.iloc[-1]['date'] != today_str:
                     df_cur = pd.concat([df_cur, pd.DataFrame([{'date': today_str, 'close': curr_p}])], ignore_index=True)
                else:
                    pass
            elif not is_intra and curr_p == 0:
                pass
            
            if curr_p > 0 and len(df_cur) >= 5:
                df_cur['MA5'] = df_cur['close'].rolling(5).mean()
                c_ma5 = df_cur.iloc[-1]['MA5']
                if curr_p > c_ma5: h_c += 1; c_stt="✅"
                else: c_stt="📉"
                v_c += 1
            else:
                if curr_p == 0: 
                    c_stt = "⚠️無報價"
                    note += "抓取失敗 "
                if len(df_cur) < 5: c_stt = "⚠️無MA5"; note += "歷史過短 "
        else:
            c_stt = "⚠️無歷史"; note = "FinMind缺資料"

        dtls.append({
            "代號":c, "市場": m_display,
            "昨收":p_price, "昨MA5":round(p_ma5,2), "昨狀態":p_stt,
            "現價":curr_p, "今MA5":round(c_ma5,2), "今狀態":c_stt,
            "備註": note
        })

    br_c = h_c/v_c if v_c>0 else 0
    br_p = h_p/v_p if v_p>0 else 0
    
    t_cur, t_pre, slope = 0, 0, 0
    try:
        tw = get_hist(ft, "TAIEX", s_dt)
        if not tw.empty:
            t_pre = 0
            if tw.iloc[-1]['date'] == target_date_for_ranks:
                 if len(tw) >= 2: t_pre = float(tw.iloc[-2]['close'])
            else:
                 t_pre = float(tw.iloc[-1]['close'])

            if is_post_market and tw.iloc[-1]['date'] == today_str:
                t_cur = float(tw.iloc[-1]['close'])
            else:
                if data_source == "永豐API":
                    try: t_cur = float(sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0].close)
                    except: pass
                if t_cur == 0:
                    try:
                       mis_tw = get_prices_twse_mis(["t00"], {"t00":"twse"})
                       if "t00" in mis_tw: t_cur = mis_tw["t00"]
                    except: pass
                if t_cur == 0: 
                    r = tw.iloc[-1]
                    t_cur = float(r['close'])
            
            if t_cur > 0:
                if tw.iloc[-1]['date'] != today_str:
                    tw = pd.concat([tw, pd.DataFrame([{'date':today_str, 'close':t_cur}])], ignore_index=True)
                else:
                    tw.iloc[-1, tw.columns.get_loc('close')] = t_cur
            if len(tw) >= 6:
                tw['MA5'] = tw['close'].rolling(5).mean()
                slope = tw.iloc[-1]['MA5'] - tw.iloc[-2]['MA5']
    except: pass
    
    t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    rec_t = last_t if is_intra and "無" not in str(last_t) else ("13:30:00" if is_post_market else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S"))
    
    save_rec(d_cur, rec_t, br_c, t_chg, t_cur, t_pre, is_intra, v_c)
    
    return {
        "d":d_cur, "d_prev": target_date_for_ranks, 
        "br":br_c, "br_p":br_p, "h":h_c, "v":v_c, "h_p":h_p, "v_p":v_p,
        "df":pd.DataFrame(dtls), 
        "t":last_t, "tc":t_chg, "slope":slope, "src_type": data_source,
        "raw":{'Date':d_cur,'Time':rec_t,'Breadth':br_c}, "src":msg_src,
        "api_status": api_status_code, "sj_err": sj_err, "sj_usage": sj_usage_info
    }

# [新增] 策略邏輯顯示區
def display_strategy_panel(slope, open_br, br, n_state):
    st.subheader("♟️ 戰略指揮所")
    
    strategies = []
    
    # 1. 大盤趨勢 (斜率)
    if slope > 0:
        strategies.append({"sig": "MA5斜率為正 ➜ 大盤偏多", "act": "只做多單，放棄空單", "type": "success"})
    elif slope < 0:
        strategies.append({"sig": "MA5斜率為負 ➜ 大盤偏空", "act": "只做空單，放棄多單", "type": "error"})
    else:
        strategies.append({"sig": "MA5斜率持平", "act": "", "type": "info"})
        
    # 2. 開盤氣氛 (開盤廣度)
    if open_br is not None:
        if slope > 0:
            if open_br >= 0.60:
                strategies.append({"sig": "開盤廣度 > 60% ➜ 行情延續", "act": "", "type": "success"})
            else:
                strategies.append({"sig": "開盤廣度 < 60% ➜ 行情轉折", "act": "", "type": "warning"})
        elif slope < 0:
            if open_br >= 0.60:
                strategies.append({"sig": "開盤廣度 > 60% ➜ 行情轉折", "act": "", "type": "warning"})
            else:
                strategies.append({"sig": "開盤廣度 < 60% ➜ 行情延續", "act": "", "type": "error"})

    # 3. 盤中變化 (乖離)
    if open_br is not None:
        if br >= (open_br + 0.05):
            strategies.append({"sig": "廣度較開盤 +5% ➜ 今日偏多", "act": "", "type": "success"})
        elif br <= (open_br - 0.05):
            strategies.append({"sig": "廣度較開盤 -5% ➜ 今日偏空", "act": "", "type": "error"})
            
    # 4. 精細戰術 (回檔/反彈) - 根據 n_state (通知狀態) 來觸發
    #
    if slope > 0:
        # 多頭回檔買進 (高點回落 5%)
        if n_state['notified_drop_high']:
            strategies.append({
                "sig": "今日偏多 + 賣壓短暫回檔 (高點落 5%)",
                "act": "🎯 進場多單 (確認止穩後)",
                "type": "success"
            })
        # 多頭遇壓 (低點反彈 5% 但上不去) -> 這裡用反向思考，如果有低點反彈，可能代表稍早殺低了
        if n_state['notified_rise_low']:
            strategies.append({
                "sig": "今日偏空(短) + 買方短暫支撐",
                "act": "⚠️ 多單出場 / 收盤再進場",
                "type": "warning"
            })
            
    elif slope < 0:
        # 空頭反彈放空 (低點反彈 5%)
        if n_state['notified_rise_low']:
             strategies.append({
                "sig": "今日偏空 + 買方短暫反彈 (低點彈 5%)",
                "act": "🎯 進場空單 (確認止漲後)",
                "type": "error"
            })
        # 空頭遇撐 (高點回落 5%)
        if n_state['notified_drop_high']:
            strategies.append({
                "sig": "今日偏多(短) + 賣方短暫壓制",
                "act": "⚠️ 空單出場 / 收盤再進場",
                "type": "warning"
            })

    # 顯示區塊
    cols = st.columns(len(strategies))
    for i, s in enumerate(strategies):
        with cols[i]:
            title = s["sig"]
            body = s["act"]
            if s["type"] == "success":
                st.success(f"**{title}**\n\n{body}")
            elif s["type"] == "error":
                st.error(f"**{title}**\n\n{body}")
            elif s["type"] == "warning":
                st.warning(f"**{title}**\n\n{body}")
            else:
                st.info(f"**{title}**\n\n{body}")

def run_app():
    st.title(f"📈 {APP_VER}")
    
    with st.sidebar:
        st.subheader("設定")
        auto = st.checkbox("自動更新", value=False)
        fin_ok = "🟢" if get_finmind_token() else "🔴"
        st.caption(f"FinMind Token: {fin_ok}")
        tg_tok = st.text_input("TG Token", value=st.secrets.get("telegram",{}).get("token",""), type="password")
        tg_id = st.text_input("Chat ID", value=st.secrets.get("telegram",{}).get("chat_id",""))
        if tg_tok and tg_id: st.success("TG Ready")
        
        st.write("---")
        if st.button("⚡ 強制清除快取 (重抓名單)", type="primary"):
            st.cache_data.clear()
            if os.path.exists(RANK_FILE): os.remove(RANK_FILE)
            st.toast("快取已清除，正在重新抓取名單...", icon="🚀")
            time_module.sleep(1)
            st.rerun()
            
        if st.button("🗑️ 重置圖表資料"):
            if os.path.exists(HIST_FILE):
                os.remove(HIST_FILE)
                st.toast("歷史資料已刪除，請重新整理", icon="🗑️")
                time_module.sleep(1)
                st.rerun()

    if st.button("🔄 刷新"): st.rerun()

    try:
        data = fetch_all()
        if isinstance(data, str): st.error(f"❌ {data}")
        elif data:
            st.sidebar.info(f"報價來源: {data['src_type']}")
            st.sidebar.caption(f"永豐API額度: {data.get('sj_usage', '未知')}")
            
            status_code = data['api_status']
            if status_code == 2: st.sidebar.success("🟢 永豐連線正常")
            elif status_code == 1: st.sidebar.warning("🟠 流量/連線異常 (忙線)")
            else:
                if data['sj_err']: st.sidebar.error(f"🔴 連線失敗: {data['sj_err']}")
                else: st.sidebar.error("🔴 未連線")
            
            br = data['br']
            open_br = get_opening_breadth(data['d'])
            
            hist_max, hist_min = get_intraday_extremes(data['d'])
            today_max = br if hist_max is None else max(hist_max, br)
            today_min = br if hist_min is None else min(hist_min, br)
            
            n_state = load_notify_state(data['d']) 

            if tg_tok and tg_id:
                # 1. 過熱/冰點
                stt = 'normal'
                if br >= BREADTH_THR: stt = 'hot'
                elif br <= BREADTH_LOW: stt = 'cold'
                
                if stt != n_state['last_stt']:
                    msg = f"🔥 過熱: {br:.1%}" if stt=='hot' else (f"❄️ 冰點: {br:.1%}" if stt=='cold' else "")
                    if msg: send_tg(tg_tok, tg_id, msg)
                    n_state['last_stt'] = stt 
                
                # 2. 急變
                rap_msg, rid = check_rapid(data['raw'])
                if rap_msg and rid != n_state['last_rap']:
                    send_tg(tg_tok, tg_id, rap_msg)
                    n_state['last_rap'] = rid
                
                # 3. 乖離
                if open_br is not None:
                    is_dev_high = (br >= open_br + OPEN_DEV_THR)
                    is_dev_low = (br <= open_br - OPEN_DEV_THR)
                    
                    if is_dev_high and not n_state['was_dev_high']:
                        msg = f"🚀 <b>【向上乖離】</b>\n開盤廣度: {open_br:.1%}\n目前廣度: {br:.1%}\n已高於開盤 5%"
                        send_tg(tg_tok, tg_id, msg)
                        n_state['was_dev_high'] = True
                    
                    if is_dev_low and not n_state['was_dev_low']:
                        msg = f"📉 <b>【向下乖離】</b>\n開盤廣度: {open_br:.1%}\n目前廣度: {br:.1%}\n已低於開盤 5%"
                        send_tg(tg_tok, tg_id, msg)
                        n_state['was_dev_low'] = True
                
                # 4. 反轉
                if br <= (today_max - 0.05):
                    if not n_state['notified_drop_high']:
                        msg = f"📉 <b>【高點回落】</b>\n今日高點: {today_max:.1%}\n目前廣度: {br:.1%}\n已自高點回檔 5%"
                        send_tg(tg_tok, tg_id, msg)
                        n_state['notified_drop_high'] = True
                else:
                    n_state['notified_drop_high'] = False
                
                if br >= (today_min + 0.05):
                    if not n_state['notified_rise_low']:
                        msg = f"🚀 <b>【低點反彈】</b>\n今日低點: {today_min:.1%}\n目前廣度: {br:.1%}\n已自低點反彈 5%"
                        send_tg(tg_tok, tg_id, msg)
                        n_state['notified_rise_low'] = True
                else:
                    n_state['notified_rise_low'] = False
                
                save_notify_state(n_state)
            
            # [新增] 呼叫策略面板
            display_strategy_panel(data['slope'], open_br, br, n_state)

            st.subheader(f"📅 {data['d']}")
            st.caption(f"名單基準日: {data['d_prev']}") 
            st.info(f"{data['src']} | 更新: {data['t']}")
            chart = plot_chart()
            if chart: st.altair_chart(chart, use_container_width=True)
            
            c1,c2,c3 = st.columns(3)
            c1.metric("今日廣度", f"{br:.1%}", f"{data['h']}/{data['v']}")
            
            caption_str = f"昨日廣度: {data['br_p']:.1%} ({data['h_p']}/{data['v_p']})"
            if open_br:
                caption_str += f" | 開盤: {open_br:.1%}"
            else:
                caption_str += " | 開盤: 等待中..."
            c1.caption(caption_str)
            
            c2.metric("大盤漲跌", f"{data['tc']:.2%}")
            sl = data['slope']; icon = "📈 正" if sl > 0 else "📉 負"
            c3.metric("大盤MA5斜率", f"{sl:.2f}", icon)
            
            st.dataframe(data['df'], use_container_width=True, hide_index=True)
        else: st.warning("⚠️ 無資料")
    except Exception as e: st.error(f"Error: {e}")

    if auto:
        now = datetime.now(timezone(timedelta(hours=8)))
        is_intra = (time(8,45)<=now.time()<time(13,30)) and (0<=now.weekday()<=4)
        if is_intra:
            sec = 120
            with st.sidebar:
                t = st.empty()
                for i in range(sec, 0, -1):
                    t.info(f"⏳ {i}s")
                    time_module.sleep(1)
            st.rerun()
        else: st.sidebar.warning("⏸ 休市")

if __name__ == "__main__":
    if 'streamlit' in sys.modules: run_app()

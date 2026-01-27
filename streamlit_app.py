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
# 設定區 v9.24.0 (策略邏輯完美修正版)
# ==========================================
APP_VER = "v9.24.0 (策略邏輯完美修正版)"
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
        "notified_rise_low": False,
        "intraday_trend": None  
    }
    
    if not os.path.exists(NOTIFY_FILE): return default_state
    
    try:
        with open(NOTIFY_FILE, 'r') as f:
            state = json.load(f)
            if state.get("date") != today_str:
                return default_state
            if "intraday_trend" not in state:
                state["intraday_trend"] = None
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

# [核心修改] 修正戰略顯示邏輯，完全對應 Excel
def display_strategy_panel(slope, open_br, br, n_state):
    st.subheader("♟️ 戰略指揮所")
    
    strategies = []
    
    # 1. 趨勢與開盤
    if slope > 0:
        strategies.append({"sig": "MA5斜率為正 ➜ 大盤偏多", "act": "只做多單，放棄空單", "type": "success"})
    elif slope < 0:
        strategies.append({"sig": "MA5斜率為負 ➜ 大盤偏空", "act": "只做空單，放棄多單", "type": "error"})
    else:
        strategies.append({"sig": "MA5斜率持平", "act": "", "type": "info"})
        
    # 2. 盤中趨勢鎖定狀態
    trend_status = n_state.get('intraday_trend')
    if trend_status == 'up':
        strategies.append({"sig": "🔒 已觸發【開盤+5%】", "act": "今日偏多確認，留意回檔", "type": "success"})
    elif trend_status == 'down':
        strategies.append({"sig": "🔒 已觸發【開盤-5%】", "act": "今日偏空確認，留意反彈", "type": "error"})
    else:
        strategies.append({"sig": "⏳ 盤整中 (未達 +/- 5%)", "act": "觀望，等待趨勢表態", "type": "info"})

    # 3. 戰術執行
    
    # === 情境 A: MA5 > 0 (多頭戰場) ===
    if slope > 0:
        # 1. 進場邏輯：盤中也鎖定「偏多」 + 發生回檔
        if trend_status == 'up':
            if n_state['notified_drop_high']:
                strategies.append({
                    "sig": "今日偏多 + 賣壓短暫回檔 (高點落 5%)",
                    "act": "🎯 進場多單 (確認止穩後)",
                    "type": "success"
                })
        
        # 2. 出場邏輯：盤中鎖定「偏空」(逆勢) + 發生反彈 -> 趁反彈出場
        # Row 14: 今日偏空, 買方短暫支撐 -> 多單出場
        elif trend_status == 'down':
             if n_state['notified_rise_low']:
                strategies.append({
                    "sig": "今日偏空(逆勢) + 買方短暫支撐",
                    "act": "⚠️ 多單出場 / 收盤再進場多單",
                    "type": "warning"
                })

    # === 情境 B: MA5 < 0 (空頭戰場) ===
    elif slope < 0:
        # 1. 進場邏輯：盤中也鎖定「偏空」 + 發生反彈
        if trend_status == 'down':
            if n_state['notified_rise_low']:
                strategies.append({
                    "sig": "今日偏空 + 買方短暫反彈 (低點彈 5%)",
                    "act": "🎯 進場空單 (確認止漲後)",
                    "type": "error"
                })
        
        # 2. 出場邏輯：盤中鎖定「偏多」(逆勢) + 發生回檔 -> 趁回檔出場
        # Row 15: 今日偏多, 賣方短暫壓制 -> 空單出場
        elif trend_status == 'up':
            if n_state['notified_drop_high']:
                strategies.append({
                    "sig": "今日偏多(逆勢) + 賣方短暫壓制",
                    "act": "⚠️ 空單出場 / 收盤再進場空單",
                    "type": "warning"
                })

    # 顯示
    cols = st.columns(len(strategies))
    for i, s in enumerate(strategies):
        with cols[i]:
            title = s["sig"]
            body = s["act"]
            if s["type"] == "success": st.success(f"**{title}**\n\n{body}")
            elif s["type"] == "error": st.error(f"**{title}**\n\n{body}")
            elif s["type"] == "warning": st.warning(f"**{title}**\n\n{body}")
            else: st.info(f"**{title}**\n\n{body}")

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

            # [核心邏輯] 判斷並鎖定 Intraday Trend
            if open_br is not None and n_state['intraday_trend'] is None:
                if br >= (open_br + 0.05):
                    n_state['intraday_trend'] = 'up'
                    if tg_tok and tg_id: send_tg(tg_tok, tg_id, f"🔒 <b>【趨勢鎖定】</b>\n廣度先達開盤+5% (目前{br:.1%})，今日確認偏多！")
                elif br <= (open_br - 0.05):
                    n_state['intraday_trend'] = 'down'
                    if tg_tok and tg_id: send_tg(tg_tok, tg_id, f"🔒 <b>【趨勢鎖定】</b>\n廣度先達開盤-5% (目前{br:.1%})，今日確認偏空！")

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
                
                # 3. 乖離 (只通知一次)
                if open_br is not None:
                    is_dev_high = (br >= open_br + OPEN_DEV_THR)
                    is_dev_low = (br <= open_br - OPEN_DEV_THR)
                    
                    if is_dev_high and not n_state['was_dev_high']:
                        n_state['was_dev_high'] = True
                    
                    if is_dev_low and not n_state['was_dev_low']:
                        n_state['was_dev_low'] = True
                
                # 4. 反轉
                if br <= (today_max - 0.05):
                    if not n_state['notified_drop_high']:
                        # 只在符合策略情境時才發通知
                        should_notify = False
                        # 多頭回檔 (買點)
                        if data['slope'] > 0 and n_state['intraday_trend'] == 'up': should_notify = True
                        # 空頭遇壓 (逃命點)
                        if data['slope'] < 0 and n_state['intraday_trend'] == 'up': should_notify = True
                        
                        if should_notify:
                            msg = f"📉 <b>【高點回落】</b>\n今日高點: {today_max:.1%}\n目前廣度: {br:.1%}\n已回檔 5%"
                            send_tg(tg_tok, tg_id, msg)
                            
                        n_state['notified_drop_high'] = True
                else:
                    n_state['notified_drop_high'] = False
                
                if br >= (today_min + 0.05):
                    if not n_state['notified_rise_low']:
                        # 只在符合策略情境時才發通知
                        should_notify = False
                        # 空頭反彈 (空點)
                        if data['slope'] < 0 and n_state['intraday_trend'] == 'down': should_notify = True
                        # 多頭支撐 (逃命點)
                        if data['slope'] > 0 and n_state['intraday_trend'] == 'down': should_notify = True
                        
                        if should_notify:
                            msg = f"🚀 <b>【低點反彈】</b>\n今日低點: {today_min:.1%}\n目前廣度: {br:.1%}\n已反彈 5%"
                            send_tg(tg_tok, tg_id, msg)

                        n_state['notified_rise_low'] = True
                else:
                    n_state['notified_rise_low'] = False
                
                save_notify_state(n_state)
            
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

# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import shioaji as sj
import os, sys, requests, json
import altair as alt
import time as time_module
import random

# ==========================================
# 設定區 v9.33.0 (真實連線修正版)
# ==========================================
APP_VER = "v9.33.0 (真實連線修正版)"
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
    
    if not os.path.exists(NOTIFY_FILE):
        return default_state
    
    try:
        with open(NOTIFY_FILE, 'r') as f:
            state = json.load(f)
            if state.get("date") != today_str:
                return default_state
            if "intraday_trend" not in state:
                state["intraday_trend"] = None
            return state
    except:
        return default_state

def save_notify_state(state):
    try:
        with open(NOTIFY_FILE, 'w') as f:
            json.dump(state, f)
    except:
        pass

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
    if 0 <= now.weekday() <= 4 and now.time() >= time(8,0):
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

# [關鍵修正] 使用 Session 進行持久化連線，並預先獲取 Cookie
def get_prices_twse_mis(codes, info_map):
    if not codes: return {}
    
    # 1. 取得或建立 Session (這能確保我們像個真人一樣有餅乾)
    if 'mis_session' not in st.session_state:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw",
            "X-Requested-With": "XMLHttpRequest"
        })
        # [最重要的一步] 先去首頁拿 Cookie，不然直接戳 API 會被擋
        try:
            session.get("https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw", timeout=5)
        except:
            pass # 這裡失敗的話後面可能也會掛，但先不管
        st.session_state['mis_session'] = session
    
    session = st.session_state['mis_session']
    
    req_strs = []
    chunk_size = 35 # 保守一點
    
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
            # 使用已經有 Cookie 的 session 發請求
            time_module.sleep(random.uniform(0.1, 0.4))
            r = session.get(url, timeout=5)
            
            if r.status_code == 200:
                data = r.json()
                if 'msgArray' in data:
                    for item in data['msgArray']:
                        c = item.get('c', '') 
                        z = item.get('z', '-') 
                        y = item.get('y', '-') 
                        
                        val = {}
                        if y != '-': val['y'] = float(y)
                        
                        price = 0
                        if z != '-': 
                            price = float(z)
                        elif item.get('b', '-') != '-': 
                             try: price = float(item.get('b').split('_')[0])
                             except: pass
                        elif item.get('a', '-') != '-': 
                             try: price = float(item.get('a').split('_')[0])
                             except: pass
                        
                        # [修正] 如果 price 是 0，就讓它 0，不要用昨收補，這樣才知道是真的沒抓到
                        if price > 0: val['z'] = price
                        
                        if c and val: results[c] = val
        except: 
            # 如果 Session 過期或失敗，清除它以便下次重建
            if 'mis_session' in st.session_state:
                del st.session_state['mis_session']
            pass
            
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
    
    if slope > 0:
        strategies.append({"sig": "MA5斜率為正 ➜ 大盤偏多", "act": "只做多單，放棄空單", "type": "success"})
    elif slope < 0:
        strategies.append({"sig": "MA5斜率為負 ➜ 大盤偏空", "act": "只做空單，放棄多單", "type": "error"})
    else:
        strategies.append({"sig": "MA5斜率持平", "act": "", "type": "info"})
        
    trend_status = n_state.get('intraday_trend')
    if trend_status == 'up':
        strategies.append({"sig": "🔒 已觸發【開盤+5%】", "act": "今日偏多確認，留意回檔", "type": "success"})
    elif trend_status == 'down':
        strategies.append({"sig": "🔒 已觸發【開盤-5%】", "act": "今日偏空確認，留意反彈", "type": "error"})
    else:
        strategies.append({"sig": "⏳ 盤整中 (未達 +/- 5%)", "act": "觀望，等待趨勢表態", "type": "info"})

    if slope > 0:
        if trend_status == 'up':
            if n_state['notified_drop_high']:
                strategies.append({
                    "sig": "今日偏多 + 賣壓短暫回檔 (高點落 5%)",
                    "act": "🎯 進場多單 (確認止穩後)",
                    "type": "success"
                })
        elif trend_status == 'down':
             if n_state['notified_rise_low']:
                strategies.append({
                    "sig": "今日偏空(逆勢) + 買方短暫支撐",
                    "act": "⚠️ 多單出場 / 收盤再進場多單",
                    "type": "warning"
                })

    elif slope < 0:
        if trend_status == 'down':
            if n_state['notified_rise_low']:
                strategies.append({
                    "sig": "今日偏空 + 買方短暫反彈 (低點彈 5%)",
                    "act": "🎯 進場空單 (確認止漲後)",
                    "type": "error"
                })
        elif trend_status == 'up':
            if n_state['notified_drop_high']:
                strategies.append({
                    "sig": "今日偏多(逆勢) + 賣方短暫壓制",
                    "act": "⚠️ 空單出場 / 收盤再進場空單",
                    "type": "warning"
                })

    cols = st.columns(len(strategies))
    for i, s in enumerate(strategies):
        with cols[i]:
            title = s["sig"]
            body = s["act"]
            if s["type"] == "success": st.success(f"**{title}**\n\n{body}")
            elif s["type"] == "error": st.error(f"**{title}**\n\n{body}")
            elif s["type"] == "warning": st.warning(f"**{title}**\n\n{body}")
            else: st.info(f"**{title}**\n\n{body}")

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
    target_date_for_ranks = days[-2] if (now.time() < time(14, 0) and d_cur == today_str) else d_cur
    if now.time() >= time(14, 0): target_date_for_ranks = today_str

    final_codes, from_disk = get_ranks_strict(ft, target_date_for_ranks)
    if not final_codes and target_date_for_ranks == today_str:
        target_date_for_ranks = days[-2]
        final_codes, _ = get_ranks_strict(ft, target_date_for_ranks)
        msg_src = f"名單:{target_date_for_ranks}(延用舊單)"
    else:
        msg_src = f"名單:{target_date_for_ranks} {'(硬碟)' if from_disk else '(新抓)'}"

    pmap = {}
    data_source = "歷史"
    last_t = "無即時資料"
    api_status_code = 0 
    sj_usage_info = "無資料"
    
    is_post_market = (now.time() >= time(14, 0))
    
    if allow_live_fetch and not is_post_market:
        # 1. Shioaji
        if sj_api:
            try:
                usage = sj_api.usage(); sj_usage_info = str(usage) if usage else "無法取得"
                contracts = []
                for c in final_codes:
                    if c in sj_api.Contracts.Stocks: contracts.append(sj_api.Contracts.Stocks[c])
                
                if contracts:
                    for i in range(0, len(contracts), 50):
                        chunk = contracts[i:i+50]
                        snaps = sj_api.snapshots(chunk)
                        for s in snaps:
                            if s.close > 0:
                                pmap[s.code] = {
                                    'price': float(s.close),
                                    'y_close': float(s.reference_price) 
                                }
                        time_module.sleep(0.2)
                    
                    if len(pmap) > 0:
                        data_source = "永豐API"
                        last_t = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
                        api_status_code = 2
            except: pass
        
        # 2. MIS (Shioaji 沒抓到的補)
        missing_codes = [c for c in final_codes if c not in pmap]
        if missing_codes:
            mis_data = get_prices_twse_mis(missing_codes, info_map)
            for c, val in mis_data.items():
                pmap[c] = val
            
            if len(mis_data) > 0 and data_source == "歷史":
                data_source = "證交所MIS"
                last_t = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
                api_status_code = 2

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
        
        info = pmap.get(c, {})
        curr_p = info.get('price', 0)
        real_y = info.get('y_close', 0) # 優先使用即時源的昨收
        
        if is_post_market and not df.empty:
            if df.iloc[-1]['date'] == today_str:
                curr_p = float(df.iloc[-1]['close'])
                if len(df) >= 2: real_y = float(df.iloc[-2]['close'])

        # 昨收與昨 MA5
        p_price = 0
        if real_y > 0: 
            p_price = real_y
        elif not df.empty:
            p_price = float(df.iloc[-1]['close']) 

        p_ma5 = 0
        p_stt = "-"
        
        if not df.empty and p_price > 0:
            last_date_db = df.iloc[-1]['date']
            closes = []
            if last_date_db == today_str:
                 closes = df['close'].tail(6).tolist()[:-1] 
            else:
                 closes = df['close'].tail(4).tolist()
                 closes.append(p_price) 
            
            if len(closes) >= 5:
                p_ma5 = sum(closes[-5:]) / 5
                if p_price > p_ma5: h_p += 1; p_stt="✅"
                else: p_stt="📉"
                v_p += 1

        c_ma5 = 0
        c_stt = "-"
        note = ""
        
        # [絕對不補] 沒報價就沒報價，不補昨收
        if curr_p == 0: 
            c_stt = "⚠️無報價"
            if p_price > 0: note = f"昨收{p_price} "

        if curr_p > 0 and p_price > 0 and not df.empty:
            hist_closes = df['close'].tail(4).tolist()
            hist_closes.append(p_price) 
            if len(hist_closes) >= 5:
                ma5_input = hist_closes[-4:] 
                ma5_input.append(curr_p)     
                c_ma5 = sum(ma5_input) / 5
                if curr_p > c_ma5: h_c += 1; c_stt="✅"
                else: c_stt="📉"
                v_c += 1
        
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
            # 大盤也用 MIS
            mis_tw = get_prices_twse_mis(["t00"], {"t00":"twse"}) 
            t_curr = mis_tw.get("t00", {}).get("z", 0)
            t_y = mis_tw.get("t00", {}).get("y", 0)

            if t_y > 0: t_pre = t_y
            else: t_pre = float(tw.iloc[-1]['close'])

            if t_curr > 0: t_cur = t_curr
            else: t_cur = t_pre

            hist_tw = tw['close'].tail(4).tolist()
            hist_tw.append(t_pre)
            ma5_yest = 0
            if len(hist_tw) >= 5: ma5_yest = sum(hist_tw[-5:]) / 5
            
            if ma5_yest > 0:
                today_input = hist_tw[-4:]
                today_input.append(t_cur)
                ma5_today = sum(today_input) / 5
                slope = ma5_today - ma5_yest
            
    except: pass
    
    if t_cur == t_pre: t_chg = 0
    else: t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    
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

if __name__ == "__main__":
    if 'streamlit' in sys.modules: run_app()

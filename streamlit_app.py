# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import shioaji as sj
import os
import sys
import requests
import altair as alt
import time as time_module

# ==========================================
# 設定區 v8.5.0 (功能全補齊版)
# ==========================================
APP_VER = "v8.5.0 (完整功能回歸版)"
TOP_N = 300              
BREADTH_THR = 0.65 
BREADTH_LOW = 0.55 
RAPID_THR = 0.02 
EXCL_PFX = ["00", "91"]
HIST_FILE = "breadth_history_v3.csv"

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

def check_rapid(row):
    if not os.path.exists(HIST_FILE): return None, None
    try:
        df = pd.read_csv(HIST_FILE)
        if len(df) < 2: return None, None
        curr_dt = datetime.strptime(f"{row['Date']} {row['Time']}", "%Y-%m-%d %H:%M:%S")
        curr_v = float(row['Breadth'])
        target = None
        for i in range(2, min(10, len(df)+1)):
            r = df.iloc[-i]
            r_dt = datetime.strptime(f"{r['Date']} {r['Time']}", "%Y-%m-%d %H:%M:%S")
            if 170 <= (curr_dt - r_dt).total_seconds() <= 190:
                target = r; break
        if target is not None:
            prev_v = float(target['Breadth'])
            diff = curr_v - prev_v
            if abs(diff) >= RAPID_THR:
                d_str = "上漲" if diff>0 else "下跌"
                msg = f"⚡ <b>【廣度急變】</b>\n{target['Time'][:5]}廣度{prev_v:.0%}，{row['Time'][:5]}廣度{curr_v:.0%}，{d_str}{abs(diff):.0%}"
                return msg, str(curr_dt)
    except: pass
    return None, None

@st.cache_resource(ttl=3600) 
def get_api():
    api = sj.Shioaji(simulation=False)
    try: 
        api.login(api_key=st.secrets["shioaji"]["api_key"], secret_key=st.secrets["shioaji"]["secret_key"])
        return api
    except: return None

# ==========================================
# 資料處理
# ==========================================
def get_col(df, names):
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n in df.columns: return df[n]
        if n.lower() in cols: return df[cols[n.lower()]]
    return None

@st.cache_data(ttl=3600)
def get_days(token):
    api = DataLoader(); api.login_by_token(token)
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now()-timedelta(days=20)).strftime("%Y-%m-%d"))
        return sorted(df['date'].unique().tolist()) if not df.empty else []
    except: return []

@st.cache_data(ttl=86400)
def get_ranks(token, d_str, bak_d=None):
    api = DataLoader(); api.login_by_token(token)
    df = pd.DataFrame()
    try: df = api.taiwan_stock_daily(stock_id="", start_date=d_str)
    except: pass
    if df.empty and bak_d:
        try: df = api.taiwan_stock_daily(stock_id="", start_date=bak_d)
        except: pass
    if df.empty: return []
    
    df['ID'] = get_col(df, ['stock_id','code'])
    df['Money'] = get_col(df, ['Trading_money','turnover'])
    if df['ID'] is None or df['Money'] is None: return []
    
    df['ID'] = df['ID'].astype(str)
    df = df[df['ID'].str.len()==4]
    df = df[df['ID'].str.isdigit()]
    for p in EXCL_PFX: df = df[~df['ID'].str.startswith(p)]
    return df.sort_values('Money', ascending=False).head(TOP_N)['ID'].tolist()

@st.cache_data(ttl=21600)
def get_hist(token, code, start):
    api = DataLoader(); api.login_by_token(token)
    try: return api.taiwan_stock_daily(stock_id=code, start_date=start)
    except: return pd.DataFrame()

def save_rec(d, t, b, tc, t_cur, t_prev, intra):
    if t_cur == 0: return 
    row = pd.DataFrame([{'Date':d,'Time':t,'Breadth':b,'Taiex_Change':tc,'Taiex_Current':t_cur,'Taiex_Prev_Close':t_prev}])
    if not os.path.exists(HIST_FILE): 
        row.to_csv(HIST_FILE, index=False); return
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: row.to_csv(HIST_FILE, index=False); return
        last_d, last_t = str(df.iloc[-1]['Date']), str(df.iloc[-1]['Time'])
        if last_d != str(d):
            pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
        else:
            if not intra:
                df = df[df['Date'] != str(d)]
                pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
            elif last_t != str(t):
                row.to_csv(HIST_FILE, mode='a', header=False, index=False)
    except: row.to_csv(HIST_FILE, index=False)

def plot_chart():
    if not os.path.exists(HIST_FILE): return None
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: return None
        df['DT'] = pd.to_datetime(df['Date'].astype(str)+' '+df['Time'].astype(str))
        df['T_S'] = (df['Taiex_Change']*10)+0.5
        
        base_d = df.iloc[-1]['Date']
        chart_data = df[df['Date'] == base_d].copy()
        if chart_data.empty: return None

        base = alt.Chart(chart_data).encode(x=alt.X('DT', title='時間', axis=alt.Axis(format='%H:%M'), scale=alt.Scale(domain=[pd.to_datetime(f"{base_d} 09:00:00"), pd.to_datetime(f"{base_d} 14:30:00")])))
        y_ax = alt.Axis(format='%', values=[i/10 for i in range(11)], tickCount=11, labelOverlap=False)
        
        l_b = base.mark_line(color='#007bff').encode(y=alt.Y('Breadth', title=None, scale=alt.Scale(domain=[0,1], nice=False), axis=y_ax))
        p_b = base.mark_circle(color='#007bff', size=30).encode(y='Breadth', tooltip=['DT', alt.Tooltip('Breadth', format='.1%')])
        l_t = base.mark_line(color='#ffc107', strokeDash=[4,4]).encode(y=alt.Y('T_S', scale=alt.Scale(domain=[0,1])))
        p_t = base.mark_circle(color='#ffc107', size=30).encode(y='T_S', tooltip=['DT', alt.Tooltip('Taiex_Change', format='.2%')])
        rule_r = alt.Chart(pd.DataFrame({'y':[BREADTH_THR]})).mark_rule(color='red', strokeDash=[5,5]).encode(y='y')
        rule_g = alt.Chart(pd.DataFrame({'y':[BREADTH_LOW]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y')
        
        return (l_b+p_b+l_t+p_t+rule_r+rule_g).properties(height=400, title=f"走勢對照 - {base_d}").resolve_scale(y='shared')
    except: return None

def fetch_all():
    ft = get_finmind_token()
    if not ft: return "FinMind Token Error"
    
    sj_api = get_api() 
    days = get_days(ft)
    if len(days)<2: return "日期資料不足"
    
    d_cur, d_pre = days[-1], days[-2]
    now = datetime.now(timezone(timedelta(hours=8)))
    is_intra = (time(8,45)<=now.time()<time(13,30)) and (0<=now.weekday()<=4)
    
    # 取得名單
    codes_cur = get_ranks(ft, d_cur)
    codes_pre = get_ranks(ft, d_pre)
    final_codes = codes_cur if codes_cur else codes_pre
    msg_src = f"名單:{d_cur if codes_cur else d_pre}"
    
    # API 報價
    pmap = {}
    last_t = "無即時資料 (API未連線)"
    if sj_api and is_intra:
        try:
            contracts = [sj_api.Contracts.Stocks[c] for c in final_codes if c in sj_api.Contracts.Stocks]
            if contracts:
                snaps = sj_api.snapshots(contracts)
                ts_obj = datetime.now()
                for s in snaps:
                    if s.close > 0: pmap[s.code] = float(s.close); ts_obj = datetime.fromtimestamp(s.ts/1e9)
                last_t = ts_obj.strftime("%H:%M:%S")
        except: last_t = "API 讀取錯誤"

    # 資料準備
    s_dt = (datetime.now()-timedelta(days=40)).strftime("%Y-%m-%d")
    h_c, v_c, h_p, v_p = 0, 0, 0, 0
    dtls = []
    
    for c in final_codes:
        df = get_hist(ft, c, s_dt)
        if df.empty: continue
        
        # 1. 處理昨日數據
        df_pre = df[df['date'] <= d_pre].copy()
        p_price, p_ma5, p_stt = 0, 0, "-"
        if len(df_pre) >= 5:
            df_pre['MA5'] = df_pre['close'].rolling(5).mean()
            if df_pre.iloc[-1]['date'] == d_pre:
                p_price = float(df_pre.iloc[-1]['close'])
                p_ma5 = float(df_pre.iloc[-1]['MA5'])
                if p_price > p_ma5: h_p += 1; p_stt="✅"
                else: p_stt="📉"
                v_p += 1
        
        # 2. 處理今日數據
        df_cur = df.copy()
        curr_p = pmap.get(c, 0)
        
        # 如果是盤中且有報價，塞入/更新最後一筆
        if is_intra and curr_p > 0:
            if df_cur.iloc[-1]['date'] != d_cur:
                df_cur = pd.concat([df_cur, pd.DataFrame([{'date': d_cur, 'close': curr_p}])], ignore_index=True)
            else:
                df_cur.iloc[-1, df_cur.columns.get_loc('close')] = curr_p
        elif not is_intra:
            # 盤後：直接用歷史資料的最後一筆 (如果是今天)
            row = df_cur[df_cur['date'] == d_cur]
            if not row.empty: curr_p = float(row.iloc[0]['close'])
        
        c_ma5, c_stt = 0, "-"
        if curr_p > 0 and len(df_cur) >= 5:
            df_cur['MA5'] = df_cur['close'].rolling(5).mean()
            c_ma5 = df_cur.iloc[-1]['MA5']
            if curr_p > c_ma5: h_c += 1; c_stt="✅"
            else: c_stt="📉"
            v_c += 1
            
        dtls.append({
            "代號":c, 
            "昨收":p_price, "昨MA5":round(p_ma5,2), "昨狀態":p_stt,
            "現價":curr_p, "今MA5":round(c_ma5,2), "今狀態":c_stt
        })

    br_c = h_c/v_c if v_c>0 else 0
    br_p = h_p/v_p if v_p>0 else 0
    
    # 大盤 (斜率回歸)
    t_cur, t_pre, slope = 0, 0, 0
    try:
        tw = get_hist(ft, "TAIEX", s_dt)
        if not tw.empty:
            t_pre = float(tw[tw['date']==d_pre].iloc[0]['close']) if not tw[tw['date']==d_pre].empty else 0
            
            # 決定今日大盤價
            if sj_api and is_intra:
                try: t_cur = float(sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0].close)
                except: pass
            if t_cur == 0: 
                r = tw[tw['date']==d_cur]
                if not r.empty: t_cur = float(r.iloc[0]['close'])
            
            # 計算斜率：先把今日價塞入/更新
            if t_cur > 0:
                if tw.iloc[-1]['date'] != d_cur:
                    tw = pd.concat([tw, pd.DataFrame([{'date':d_cur, 'close':t_cur}])], ignore_index=True)
                else:
                    tw.iloc[-1, tw.columns.get_loc('close')] = t_cur
            
            # 算 MA5 斜率
            if len(tw) >= 6:
                tw['MA5'] = tw['close'].rolling(5).mean()
                slope = tw.iloc[-1]['MA5'] - tw.iloc[-2]['MA5']
            
    except: pass
    
    t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    
    # 存檔
    rec_t = last_t if is_intra and "無" not in str(last_t) else ("14:30:00" if not is_intra else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S"))
    save_rec(d_cur, rec_t, br_c, t_chg, t_cur, t_pre, is_intra)
    
    return {
        "d":d_cur, "br":br_c, "br_p":br_p, "h":h_c, "v":v_c, "df":pd.DataFrame(dtls), 
        "t":last_t, "tc":t_chg, "slope":slope, # 回傳斜率
        "raw":{'Date':d_cur,'Time':rec_t,'Breadth':br_c}, "src":msg_src, "sj_ok": True if sj_api else False
    }

# ==========================================
# 主程式
# ==========================================
def run_app():
    st.title(f"📈 {APP_VER}")
    if 'last_stt' not in st.session_state: st.session_state['last_stt'] = 'normal'
    if 'last_rap' not in st.session_state: st.session_state['last_rap'] = ""

    with st.sidebar:
        st.subheader("設定")
        auto = st.checkbox("自動更新", value=False)
        fin_ok = "🟢" if get_finmind_token() else "🔴"
        st.caption(f"FinMind Token: {fin_ok}")
        tg_tok = st.text_input("TG Token", value=st.secrets.get("telegram",{}).get("token",""), type="password")
        tg_id = st.text_input("Chat ID", value=st.secrets.get("telegram",{}).get("chat_id",""))
        if tg_tok and tg_id: st.success("TG Ready")

    if st.button("🔄 刷新"): st.rerun()

    try:
        data = fetch_all()
        if isinstance(data, str): st.error(f"❌ {data}")
        elif data:
            sj_status = "🟢 連線中" if data['sj_ok'] else "🔴 未連線 (歷史數據)"
            st.sidebar.caption(f"永豐 API: {sj_status}")
            
            br = data['br']
            if tg_tok and tg_id:
                stt = 'normal'
                if br >= BREADTH_THR: stt = 'hot'
                elif br <= BREADTH_LOW: stt = 'cold'
                if stt != st.session_state['last_stt']:
                    msg = f"🔥 過熱: {br:.1%}" if stt=='hot' else (f"❄️ 冰點: {br:.1%}" if stt=='cold' else "")
                    if msg: send_tg(tg_tok, tg_id, msg)
                    st.session_state['last_stt'] = stt
                rap_msg, rid = check_rapid(data['raw'])
                if rap_msg and rid != st.session_state['last_rap']:
                    send_tg(tg_tok, tg_id, rap_msg); st.session_state['last_rap'] = rid

            st.subheader(f"📅 {data['d']}")
            st.info(f"{data['src']} | {data['t']}")
            chart = plot_chart()
            if chart: st.altair_chart(chart, use_container_width=True)
            
            c1,c2,c3 = st.columns(3)
            c1.metric("今日廣度", f"{br:.1%}", f"{data['h']}/{data['v']}")
            c1.caption(f"昨日廣度: {data['br_p']:.1%}")
            c2.metric("大盤漲跌", f"{data['tc']:.2%}")
            
            # 斜率顯示
            slope_val = data['slope']
            slope_icon = "📈 正" if slope_val > 0 else "📉 負"
            c3.metric("大盤MA5斜率", f"{slope_val:.2f}", slope_icon)
            
            st.dataframe(data['df'], use_container_width=True, hide_index=True)
        else: st.warning("⚠️ 無資料")
    except Exception as e: st.error(f"Error: {e}")

    if auto:
        now = datetime.now(timezone(timedelta(hours=8)))
        is_intra = (time(8,45)<=now.time()<time(13,30)) and (0<=now.weekday()<=4)
        if is_intra:
            sec = 60 if (time(9,0)<=now.time()<time(10,0) or time(12,30)<=now.time()<time(13,30)) else 180
            with st.sidebar:
                t = st.empty()
                for i in range(sec, 0, -1):
                    t.info(f"⏳ {i}s")
                    time_module.sleep(1)
            st.rerun()
        else: st.sidebar.warning("⏸ 休市")

if __name__ == "__main__":
    if 'streamlit' in sys.modules: run_app()

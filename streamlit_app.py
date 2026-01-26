# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import shioaji as sj
import os, sys, requests
import altair as alt
import time as time_module

# --- 設定區 ---
APP_VER = "v8.2.5 (極速瘦身版)"
TOP_N = 300              
BREADTH_THR = 0.65 
BREADTH_LOW = 0.55 
RAPID_THR = 0.02 
EXCL_PFX = ["00", "91"]
HIST_FILE = "breadth_history_v3.csv"

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# --- 核心函式 ---
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
    try: api.login(api_key=st.secrets["shioaji"]["api_key"], secret_key=st.secrets["shioaji"]["secret_key"])
    except: return None
    return api

# --- 資料處理 ---
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
    df = df[df['ID'].str.len()==4 & df['ID'].str.isdigit()]
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
    if not os.path.exists(HIST_FILE): row.to_csv(HIST_FILE, index=False)
    else:
        try:
            df = pd.read_csv(HIST_FILE)
            if not df.empty and str(df.iloc[-1]['Date'])==str(d):
                if not intra: df = df[:-1]; pd.concat([df,row], ignore_index=True).to_csv(HIST_FILE, index=False)
                elif str(df.iloc[-1]['Time'])!=str(t): row.to_csv(HIST_FILE, mode='a', header=False, index=False)
            else: row.to_csv(HIST_FILE, index=False)
        except: row.to_csv(HIST_FILE, index=False)

def plot_chart():
    if not os.path.exists(HIST_FILE): return None
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: return None
        df['DT'] = pd.to_datetime(df['Date'].astype(str)+' '+df['Time'].astype(str))
        df['T_S'] = (df['Taiex_Change']*10)+0.5
        base_d = df.iloc[0]['Date']
        base = alt.Chart(df).encode(x=alt.X('DT', title='時間', axis=alt.Axis(format='%H:%M'), scale=alt.Scale(domain=[pd.to_datetime(f"{base_d} 09:00:00"), pd.to_datetime(f"{base_d} 14:30:00")])))
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
    ft = get_finmind_token(); sj_api = get_api()
    if not ft or not sj_api: return None
    days = get_days(ft)
    if len(days)<2: return None
    d_cur, d_pre = days[-1], days[-2]
    now = datetime.now(timezone(timedelta(hours=8)))
    is_intra = (time(8,45)<=now.time()<time(13,30)) and (0<=now.weekday()<=4)
    
    codes_pre = get_ranks(ft, d_pre, days[-3])
    if not codes_pre: return None
    codes_cur = get_ranks(ft, d_cur) if not is_intra else []
    codes = codes_cur if codes_cur else codes_pre
    
    pmap = {}
    last_t = "無即時資料"
    if sj_api:
        try:
            snaps = sj_api.snapshots([sj_api.Contracts.Stocks[c] for c in codes if c in sj_api.Contracts.Stocks])
            ts_obj = datetime.now()
            for s in snaps:
                if s.close>0: pmap[s.code]=float(s.close); ts_obj=datetime.fromtimestamp(s.ts/1e9)
            last_t = ts_obj.strftime("%H:%M:%S")
        except: pass

    hits, valid, dtls = 0, 0, []
    s_dt = (datetime.now()-timedelta(days=30)).strftime("%Y-%m-%d")
    
    for c in codes:
        cur_p = pmap.get(c, 0)
        df = get_hist(ft, c, s_dt)
        stt, ma5 = "無資料", 0
        if not df.empty:
            if is_intra and cur_p>0:
                df = pd.concat([df[df['date']<d_cur], pd.DataFrame([{'date':d_cur,'close':cur_p}])], ignore_index=True)
            if len(df)>=5:
                df['MA5'] = df['close'].rolling(5).mean()
                ma5 = df.iloc[-1]['MA5']; fin_p = float(df.iloc[-1]['close'])
                if fin_p > ma5: hits+=1; stt="✅"
                else: stt="📉"
                valid+=1
        dtls.append({"代號":c, "現價":cur_p, "MA5":round(ma5,2), "狀態":stt})

    br = hits/valid if valid>0 else 0
    t_cur, t_pre = 0, 0
    try:
        tw = get_hist(ft, "TAIEX", s_dt)
        if not tw.empty: t_pre = float(tw[tw['date']==d_pre].iloc[0]['close']) if not tw[tw['date']==d_pre].empty else 0
        if sj_api:
            s = sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0]
            if s.close>0: t_cur = float(s.close)
        if t_cur==0 and not tw.empty:
            r = tw[tw['date']==d_cur]
            if not r.empty: t_cur = float(r.iloc[0]['close'])
    except: pass
    
    t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    rec_t = last_t if is_intra and "無" not in str(last_t) else ("14:30:00" if not is_intra else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S"))
    save_rec(d_cur, rec_t, br, t_chg, t_cur, t_pre, is_intra)
    
    return {"d":d_cur, "br":br, "h":hits, "v":valid, "df":pd.DataFrame(dtls), "t":last_t, "tc":t_chg, "raw":{'Date':d_cur,'Time':rec_t,'Breadth':br}, "src":f"名單:{d_cur if codes_cur else d_pre}"}

# --- 主程式 ---
def run_app():
    st.title(f"📈 盤中權證進場判斷 {APP_VER}")
    if 'last_stt' not in st.session_state: st.session_state['last_stt'] = 'normal'
    if 'last_rap' not in st.session_state: st.session_state['last_rap'] = ""

    with st.sidebar:
        st.subheader("設定")
        auto = st.checkbox("自動更新", value=False)
        tg_tok = st.text_input("TG Token", value=st.secrets.get("telegram",{}).get("token",""), type="password")
        tg_id = st.text_input("Chat ID", value=st.secrets.get("telegram",{}).get("chat_id",""))
        if tg_tok and tg_id: st.success("TG Ready")

    if st.button("🔄 刷新"): st.rerun()

    try:
        data = fetch_all()
        if data:
            br = data['br']
            if tg_tok and tg_id:
                stt = 'normal'
                if br >= BREADTH_THR: stt = 'hot'
                elif br <= BREADTH_LOW: stt = 'cold'
                
                if stt != st.session_state['last_stt']:
                    if stt == 'hot': send_tg(tg_tok, tg_id, f"🔥 過熱: {br:.1%}")
                    elif stt == 'cold': send_tg(tg_tok, tg_id, f"❄️ 冰點: {br:.1%}")
                    st.session_state['last_stt'] = stt
                
                rap_msg, rid = check_rapid(data['raw'])
                if rap_msg and rid != st.session_state['last_rap']:
                    send_tg(tg_tok, tg_id, rap_msg)
                    st.session_state['last_rap'] = rid

            st.subheader(f"📅 {data['d']}")
            st.caption(data['src'])
            chart = plot_chart()
            if chart: st.altair_chart(chart, use_container_width=True)
            c1,c2,c3 = st.columns(3)
            c1.metric("今日廣度", f"{br:.1%}", f"{data['h']}/{data['v']}")
            c2.metric("大盤漲跌", f"{data['tc']:.2%}")
            c3.metric("狀態", "🔥" if br>=0.65 else ("❄️" if br<=0.55 else "---"))
            st.caption(f"Update: {data['t']}")
            st.dataframe(data['df'], use_container_width=True, hide_index=True)
        else: st.warning("⚠️ 無資料或非交易日")
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

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
# 設定區 v8.4.0 (功能完全復原版)
# ==========================================
APP_VER = "v8.4.0 (功能復原版)"
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
        # 抓取最近 20 天的交易日，確保有足夠的日期可以回推
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now()-timedelta(days=20)).strftime("%Y-%m-%d"))
        return sorted(df['date'].unique().tolist()) if not df.empty else []
    except: return []

@st.cache_data(ttl=86400)
def get_ranks(token, d_str, bak_d=None):
    api = DataLoader(); api.login_by_token(token)
    df = pd.DataFrame()
    
    # 1. 嘗試抓指定日期 (d_str)
    try: df = api.taiwan_stock_daily(stock_id="", start_date=d_str)
    except: pass
    
    # 2. 如果指定日期沒資料 (例如FinMind還沒更新)，才用備份日期 (bak_d)
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
    
    # 確保檔案存在
    if not os.path.exists(HIST_FILE): 
        row.to_csv(HIST_FILE, index=False)
        return

    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty:
            row.to_csv(HIST_FILE, index=False)
            return

        last_date = str(df.iloc[-1]['Date'])
        last_time = str(df.iloc[-1]['Time'])
        
        # 如果是新的一天 -> Append
        if last_date != str(d):
            df = pd.concat([df, row], ignore_index=True)
            df.to_csv(HIST_FILE, index=False)
        else:
            # 同一天
            if not intra:
                # 盤後模式：覆蓋當天最後一筆 (更新成收盤價)
                # 移除當天所有資料，只留最新這筆收盤
                df = df[df['Date'] != str(d)]
                df = pd.concat([df, row], ignore_index=True)
                df.to_csv(HIST_FILE, index=False)
            elif last_time != str(t):
                # 盤中模式：時間不同就 Append
                row.to_csv(HIST_FILE, mode='a', header=False, index=False)
    except: 
        row.to_csv(HIST_FILE, index=False)

def plot_chart():
    if not os.path.exists(HIST_FILE): return None
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: return None
        df['DT'] = pd.to_datetime(df['Date'].astype(str)+' '+df['Time'].astype(str))
        df['T_S'] = (df['Taiex_Change']*10)+0.5
        
        base_d = df.iloc[-1]['Date'] # 取最新日期的圖表
        chart_data = df[df['Date'] == base_d].copy()
        
        if chart_data.empty: return None

        start_t = pd.to_datetime(f"{base_d} 09:00:00")
        end_t = pd.to_datetime(f"{base_d} 14:30:00")
        
        base = alt.Chart(chart_data).encode(x=alt.X('DT', title='時間', axis=alt.Axis(format='%H:%M'), scale=alt.Scale(domain=[start_t, end_t])))
        y_ax = alt.Axis(format='%', values=[i/10 for i in range(11)], tickCount=11, labelOverlap=False)
        
        l_b = base.mark_line(color='#007bff').encode(y=alt.Y('Breadth', title=None, scale=alt.Scale(domain=[0,1], nice=False), axis=y_ax))
        p_b = base.mark_circle(color='#007bff', size=30).encode(y='Breadth', tooltip=['DT', alt.Tooltip('Breadth', format='.1%')])
        l_t = base.mark_line(color='#ffc107', strokeDash=[4,4]).encode(y=alt.Y('T_S', scale=alt.Scale(domain=[0,1])))
        p_t = base.mark_circle(color='#ffc107', size=30).encode(y='T_S', tooltip=['DT', alt.Tooltip('Taiex_Change', format='.2%')])
        
        rule_r = alt.Chart(pd.DataFrame({'y':[BREADTH_THR]})).mark_rule(color='red', strokeDash=[5,5]).encode(y='y')
        rule_g = alt.Chart(pd.DataFrame({'y':[BREADTH_LOW]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y')
        
        return (l_b+p_b+l_t+p_t+rule_r+rule_g).properties(height=400, title=f"走勢對照 - {base_d}").resolve_scale(y='shared')
    except: return None

def calc_breadth(df_hist, codes, target_date, price_map=None, is_intra=False):
    """
    通用廣度計算函式：可算今日，也可算昨日
    """
    hits, valid = 0, 0
    # 為了效能，一次篩選出相關代號的歷史資料
    if df_hist.empty: return 0, 0
    
    for code in codes:
        # 取得該股歷史資料
        df = df_hist[df_hist['stock_id'] == code].copy()
        if df.empty: continue
        
        curr_p = 0
        ma5 = 0
        
        # 判斷是用 API 報價 還是 歷史收盤價
        if is_intra and price_map and code in price_map:
            # 盤中模式：用即時價
            curr_p = price_map[code]
            # 把即時價塞入歷史資料算 MA5
            if curr_p > 0:
                new_row = pd.DataFrame([{'date': target_date, 'close': curr_p}])
                # 確保不重複
                if df.iloc[-1]['date'] != target_date:
                    df = pd.concat([df, new_row], ignore_index=True)
        else:
            # 盤後/昨日模式：用該日期的收盤價
            # 找出 target_date 當天的資料
            row = df[df['date'] == target_date]
            if not row.empty:
                curr_p = float(row.iloc[0]['close'])
            else:
                continue # 沒那天資料就跳過

        # 計算 MA5
        if len(df) >= 5:
            # 確保 MA5 是算到 target_date 當天
            # 如果是算昨日廣度，資料只會切到昨日，所以取最後一筆即可
            df['MA5'] = df['close'].rolling(5).mean()
            # 找到 target_date 對應的 MA5
            target_row = df[df['date'] == target_date]
            if not target_row.empty:
                ma5 = float(target_row.iloc[0]['MA5'])
                if curr_p > ma5: hits += 1
                valid += 1
    
    return hits, valid

def fetch_all():
    ft = get_finmind_token()
    if not ft: return "FinMind Token Error"
    
    sj_api = get_api() 
    
    days = get_days(ft)
    if len(days)<2: return "日期資料不足"
    
    d_cur, d_pre = days[-1], days[-2]
    now = datetime.now(timezone(timedelta(hours=8)))
    # 判斷是否為盤中 (週一~週五 08:45~13:30)
    is_intra = (time(8,45)<=now.time()<time(13,30)) and (0<=now.weekday()<=4)
    
    # 取得名單 (優先抓今日，抓不到抓昨日)
    codes_cur = get_ranks(ft, d_cur)
    codes_pre = get_ranks(ft, d_pre)
    
    # 如果今天是盤中，優先用今日名單；如果是盤後或假日，還是優先用今日(最新)名單
    # 只有當今日名單完全抓不到時，才用昨日名單
    final_codes = codes_cur if codes_cur else codes_pre
    msg_src = f"名單:{d_cur if codes_cur else d_pre}"
    
    # 取得即時報價 (如果 API 連線成功)
    pmap = {}
    last_t = "無即時資料 (API未連線)"
    
    if sj_api and is_intra:
        try:
            contracts = []
            for c in final_codes:
                if c in sj_api.Contracts.Stocks: contracts.append(sj_api.Contracts.Stocks[c])
            if contracts:
                snaps = sj_api.snapshots(contracts)
                ts_obj = datetime.now()
                for s in snaps:
                    if s.close > 0: 
                        pmap[s.code] = float(s.close)
                        ts_obj = datetime.fromtimestamp(s.ts/1e9)
                last_t = ts_obj.strftime("%H:%M:%S")
        except: last_t = "API 讀取錯誤"

    # 準備歷史資料 (一次撈取所有成分股，減少迴圈內 I/O)
    # 這裡做個優化：因為要算昨日跟今日，所以一次把資料傳進去
    s_dt = (datetime.now()-timedelta(days=40)).strftime("%Y-%m-%d")
    
    # === 關鍵：回復「昨日廣度」計算 ===
    # 為了計算昨天的，我們需要昨天的名單 (通常跟今天差不多，暫用 final_codes)
    # 下載歷史資料 (比較花時間，但必要)
    # 為了避免太慢，我們只針對前 300 檔跑迴圈 get_hist (有 Cache 頂著)
    
    # 1. 算今日廣度
    hits_cur, valid_cur = 0, 0
    # 2. 算昨日廣度
    hits_pre, valid_pre = 0, 0
    
    dtls = []
    
    for code in final_codes:
        df = get_hist(ft, code, s_dt)
        if df.empty: continue
        
        # --- 算昨日 (d_pre) ---
        # 篩選出 <= d_pre 的資料
        df_pre = df[df['date'] <= d_pre].copy()
        if len(df_pre) >= 5:
            df_pre['MA5'] = df_pre['close'].rolling(5).mean()
            last_row = df_pre.iloc[-1]
            if last_row['date'] == d_pre: # 確保有昨天的資料
                if last_row['close'] > last_row['MA5']: hits_pre += 1
                valid_pre += 1
        
        # --- 算今日 (d_cur) ---
        # 準備資料：包含歷史 + (如果是盤中) 即時價
        df_cur = df.copy()
        curr_p = 0
        
        if is_intra and code in pmap:
            curr_p = pmap[code]
            if curr_p > 0:
                # 檢查最後一筆是不是今天
                if df_cur.iloc[-1]['date'] != d_cur:
                    new_row = pd.DataFrame([{'date': d_cur, 'close': curr_p}])
                    df_cur = pd.concat([df_cur, new_row], ignore_index=True)
                else:
                    # 更新今天收盤價
                    df_cur.iloc[-1, df_cur.columns.get_loc('close')] = curr_p
        else:
            # 盤後/API斷線：直接用 FinMind 裡的 d_cur 資料
            row = df_cur[df_cur['date'] == d_cur]
            if not row.empty:
                curr_p = float(row.iloc[0]['close'])
        
        stt, ma5 = "無資料", 0
        if curr_p > 0 and len(df_cur) >= 5:
            df_cur['MA5'] = df_cur['close'].rolling(5).mean()
            ma5 = df_cur.iloc[-1]['MA5']
            if curr_p > ma5: 
                hits_cur += 1
                stt = "✅"
            else: 
                stt = "📉"
            valid_cur += 1
            
        dtls.append({"代號":code, "現價":curr_p, "MA5":round(ma5,2), "狀態":stt})

    br_cur = hits_cur/valid_cur if valid_cur>0 else 0
    br_pre = hits_pre/valid_pre if valid_pre>0 else 0 # 找回昨日廣度
    
    # 大盤資料
    t_cur, t_pre = 0, 0
    try:
        tw = get_hist(ft, "TAIEX", s_dt)
        if not tw.empty: 
            t_pre = float(tw[tw['date']==d_pre].iloc[0]['close']) if not tw[tw['date']==d_pre].empty else 0
        
        if sj_api and is_intra: 
            try:
                s = sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0]
                if s.close>0: t_cur = float(s.close)
            except: pass
            
        if t_cur == 0 and not tw.empty: 
            r = tw[tw['date']==d_cur]
            if not r.empty: t_cur = float(r.iloc[0]['close'])
    except: pass
    
    t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    
    # 決定記錄時間
    if is_intra:
        rec_t = last_t if "無" not in str(last_t) else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
    else:
        rec_t = "14:30:00"
        
    save_rec(d_cur, rec_t, br_cur, t_chg, t_cur, t_pre, is_intra)
    
    return {
        "d":d_cur, "br":br_cur, "br_prev": br_pre, # 傳回昨日廣度
        "h":hits_cur, "v":valid_cur, "h_p": hits_pre, "v_p": valid_pre,
        "df":pd.DataFrame(dtls), 
        "t":last_t, "tc":t_chg, "raw":{'Date':d_cur,'Time':rec_t,'Breadth':br_cur}, 
        "src":msg_src,
        "sj_ok": True if sj_api else False
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
        if isinstance(data, str):
            st.error(f"❌ 錯誤: {data}")
        elif data:
            sj_status = "🟢 連線中" if data['sj_ok'] else "🔴 未連線 (使用歷史數據)"
            st.sidebar.caption(f"永豐 API: {sj_status}")
            
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
            st.info(f"{data['src']} | {data['t']}")
            
            chart = plot_chart()
            if chart: st.altair_chart(chart, use_container_width=True)
            
            c1,c2,c3 = st.columns(3)
            # 顯示今日廣度 + 昨日廣度
            c1.metric("今日廣度", f"{br:.1%}", f"{data['h']}/{data['v']}")
            # 這裡把昨日廣度補回去
            c1.caption(f"昨日廣度: {data['br_prev']:.1%}")
            
            c2.metric("大盤漲跌", f"{data['tc']:.2%}")
            c3.metric("狀態", "🔥" if br>=0.65 else ("❄️" if br<=0.55 else "---"))
            
            st.dataframe(data['df'], use_container_width=True, hide_index=True)
        else:
            st.warning("⚠️ 無資料 (未知錯誤)")
            
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

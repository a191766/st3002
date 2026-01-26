# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import shioaji as sj
import os, sys, requests
import altair as alt
import yfinance as yf
import time as time_module

# ==========================================
# 設定區 v9.1.0 (流量管制優化版)
# ==========================================
APP_VER = "v9.1.0 (流量管制版)"
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
        curr_dt = datetime.strptime(f"{row['Date']} {row['Time']}", "%Y-%m-%d %H:%M")
        curr_v = float(row['Breadth'])
        target = None
        for i in range(2, min(10, len(df)+1)):
            r = df.iloc[-i]
            try: r_t = r['Time'] if len(str(r['Time']))==5 else r['Time'][:5]
            except: continue
            r_dt = datetime.strptime(f"{r['Date']} {r_t}", "%Y-%m-%d %H:%M")
            if 170 <= (curr_dt - r_dt).total_seconds() <= 190:
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

@st.cache_resource(ttl=3600) 
def get_api():
    api = sj.Shioaji(simulation=False)
    try: 
        api.login(api_key=st.secrets["shioaji"]["api_key"], secret_key=st.secrets["shioaji"]["secret_key"])
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

# Yahoo 雙規抓取 (TSE + OTC)
def get_prices_yf_robust(codes):
    if not codes: return {}
    results = {}
    unknown_codes = []
    chunk_size = 50
    
    # 1. TSE
    for i in range(0, len(codes), chunk_size):
        chunk = codes[i:i+chunk_size]
        tickers = [f"{c}.TW" for c in chunk]
        try:
            data = yf.download(tickers, period="1d", progress=False, threads=True)
            if 'Close' in data and not data['Close'].empty:
                last_row = data['Close'].iloc[-1]
                for t in tickers:
                    code_raw = t.replace(".TW", "")
                    try:
                        val = float(last_row[t])
                        if not np.isnan(val) and val > 0: results[code_raw] = val
                        else: unknown_codes.append(code_raw)
                    except: unknown_codes.append(code_raw)
            else: unknown_codes.extend(chunk)
        except: unknown_codes.extend(chunk)
    
    # 2. OTC
    if unknown_codes:
        unknown_codes = list(set(unknown_codes))
        for i in range(0, len(unknown_codes), chunk_size):
            chunk = unknown_codes[i:i+chunk_size]
            tickers_two = [f"{c}.TWO" for c in chunk]
            try:
                data = yf.download(tickers_two, period="1d", progress=False, threads=True)
                if 'Close' in data and not data['Close'].empty:
                    last_row = data['Close'].iloc[-1]
                    for t in tickers_two:
                        code_raw = t.replace(".TWO", "")
                        try:
                            val = float(last_row[t])
                            if not np.isnan(val) and val > 0: results[code_raw] = val
                        except: pass
            except: pass
            
    return results

def save_rec(d, t, b, tc, t_cur, t_prev, intra):
    if t_cur == 0: return 
    t_short = t[:5] 
    row = pd.DataFrame([{'Date':d,'Time':t_short,'Breadth':b,'Taiex_Change':tc,'Taiex_Current':t_cur,'Taiex_Prev_Close':t_prev}])
    if not os.path.exists(HIST_FILE): 
        row.to_csv(HIST_FILE, index=False); return
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: row.to_csv(HIST_FILE, index=False); return
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
        end_t = pd.to_datetime(f"{base_d} 14:30:00")
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
    
    d_cur, d_pre = days[-1], days[-2]
    now = datetime.now(timezone(timedelta(hours=8)))
    is_intra = (time(8,45)<=now.time()<time(13,30)) and (0<=now.weekday()<=4)
    
    codes_cur = get_ranks(ft, d_cur)
    codes_pre = get_ranks(ft, d_pre)
    final_codes = codes_cur if codes_cur else codes_pre
    msg_src = f"名單:{d_cur if codes_cur else d_pre}"
    
    pmap = {}
    data_source = "歷史"
    last_t = "無即時資料"
    api_status_code = 0 
    sj_usage_info = "無資料" # 用量資訊
    
    if is_intra:
        # 1. 永豐 API (分批抓取模式)
        if sj_api:
            try:
                # [新增] 抓取用量資訊
                try:
                    usage = sj_api.usage()
                    if usage: sj_usage_info = str(usage)
                except: sj_usage_info = "無法取得"

                # 準備合約
                contracts = []
                for c in final_codes:
                    if c in sj_api.Contracts.Stocks: contracts.append(sj_api.Contracts.Stocks[c])
                
                # [關鍵修正] 分批抓取 (Batch Snapshots)
                # 每次抓 80 檔，避免觸發 50次/5秒 限制 (如果是算總量) 或是封包過大
                chunk_size = 80 
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
                            # [關鍵] 休息一下，避免被鎖
                            time_module.sleep(0.5) 
                        except: pass
                    
                    if count_sj > 0:
                        last_t = ts_obj.strftime("%H:%M:%S")
                        data_source = "永豐API"
                        api_status_code = 2
                    else: api_status_code = 1
                else: api_status_code = 1
            except: api_status_code = 1 
        
        # 2. Yahoo 備援
        if not pmap:
            pmap = get_prices_yf_robust(final_codes)
            if pmap:
                data_source = "Yahoo備援(雙規)"
                last_t = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")

    s_dt = (datetime.now()-timedelta(days=40)).strftime("%Y-%m-%d")
    h_c, v_c, h_p, v_p = 0, 0, 0, 0
    dtls = []
    
    for c in final_codes:
        df = get_hist(ft, c, s_dt)
        # 昨日
        p_price, p_ma5, p_stt = 0, 0, "-"
        if not df.empty:
            df_pre = df[df['date'] <= d_pre].copy()
            if len(df_pre) >= 5:
                df_pre['MA5'] = df_pre['close'].rolling(5).mean()
                if df_pre.iloc[-1]['date'] == d_pre:
                    p_price = float(df_pre.iloc[-1]['close'])
                    p_ma5 = float(df_pre.iloc[-1]['MA5'])
                    if p_price > p_ma5: h_p += 1; p_stt="✅"
                    else: p_stt="📉"
                    v_p += 1
        # 今日
        curr_p = pmap.get(c, 0)
        c_ma5, c_stt, note = 0, "-", ""
        if not df.empty:
            df_cur = df.copy()
            if is_intra and curr_p > 0:
                if df_cur.iloc[-1]['date'] != d_cur:
                    df_cur = pd.concat([df_cur, pd.DataFrame([{'date': d_cur, 'close': curr_p}])], ignore_index=True)
                else:
                    df_cur.iloc[-1, df_cur.columns.get_loc('close')] = curr_p
            elif not is_intra:
                row = df_cur[df_cur['date'] == d_cur]
                if not row.empty: curr_p = float(row.iloc[0]['close'])
            if curr_p > 0 and len(df_cur) >= 5:
                df_cur['MA5'] = df_cur['close'].rolling(5).mean()
                c_ma5 = df_cur.iloc[-1]['MA5']
                if curr_p > c_ma5: h_c += 1; c_stt="✅"
                else: c_stt="📉"
                v_c += 1
            else:
                if curr_p == 0: c_stt = "⚠️無報價"; note += "抓取失敗 "
                if len(df_cur) < 5: c_stt = "⚠️無MA5"; note += "歷史過短 "
        else:
            c_stt = "⚠️無歷史"; note = "FinMind缺資料"

        dtls.append({
            "代號":c, 
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
            t_pre = float(tw[tw['date']==d_pre].iloc[0]['close']) if not tw[tw['date']==d_pre].empty else 0
            if data_source == "永豐API":
                try: t_cur = float(sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0].close)
                except: pass
            if t_cur == 0: 
                try: 
                    yf_tw = yf.download("^TWII", period="1d", progress=False)['Close']
                    if not yf_tw.empty: t_cur = float(yf_tw.iloc[-1])
                except: pass
            if t_cur == 0: 
                r = tw[tw['date']==d_cur]
                if not r.empty: t_cur = float(r.iloc[0]['close'])
            if t_cur > 0:
                if tw.iloc[-1]['date'] != d_cur:
                    tw = pd.concat([tw, pd.DataFrame([{'date':d_cur, 'close':t_cur}])], ignore_index=True)
                else:
                    tw.iloc[-1, tw.columns.get_loc('close')] = t_cur
            if len(tw) >= 6:
                tw['MA5'] = tw['close'].rolling(5).mean()
                slope = tw.iloc[-1]['MA5'] - tw.iloc[-2]['MA5']
    except: pass
    
    t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    rec_t = last_t if is_intra and "無" not in str(last_t) else ("14:30:00" if not is_intra else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S"))
    save_rec(d_cur, rec_t, br_c, t_chg, t_cur, t_pre, is_intra)
    
    return {
        "d":d_cur, "d_prev": d_pre,
        "br":br_c, "br_p":br_p, "h":h_c, "v":v_c, "h_p":h_p, "v_p":v_p,
        "df":pd.DataFrame(dtls), 
        "t":last_t, "tc":t_chg, "slope":slope, "src_type": data_source,
        "raw":{'Date':d_cur,'Time':rec_t,'Breadth':br_c}, "src":msg_src,
        "api_status": api_status_code, "sj_err": sj_err, "sj_usage": sj_usage_info
    }

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
        st.write("---")
        if st.button("🗑️ 重置圖表資料", type="primary"):
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
            # 顯示 API 用量
            st.sidebar.caption(f"永豐API額度: {data.get('sj_usage', '未知')}")
            
            status_code = data['api_status']
            if status_code == 2: st.sidebar.success("🟢 永豐連線正常")
            elif status_code == 1: st.sidebar.warning("🟠 流量/連線異常 (忙線)")
            else:
                if data['sj_err']: st.sidebar.error(f"🔴 連線失敗: {data['sj_err']}")
                else: st.sidebar.error("🔴 未連線")
            
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
            st.caption(f"昨日基準: {data['d_prev']}")
            st.info(f"{data['src']} | 更新: {data['t']}")
            chart = plot_chart()
            if chart: st.altair_chart(chart, use_container_width=True)
            
            c1,c2,c3 = st.columns(3)
            c1.metric("今日廣度", f"{br:.1%}", f"{data['h']}/{data['v']}")
            c1.caption(f"昨日廣度: {data['br_p']:.1%} ({data['h_p']}/{data['v_p']})")
            
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

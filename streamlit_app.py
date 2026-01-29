# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import shioaji as sj
import os, sys, json, subprocess, traceback
import altair as alt
import time as time_module
import random
import io 

# 引入 curl_cffi 
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    st.error("缺少 curl_cffi 套件！請在 requirements.txt 中加入 'curl_cffi'")
    st.stop()

# ==========================================
# 設定區 v9.55.28 (漲跌停價格終極修復版)
# ==========================================
APP_VER = "v9.55.28 (漲跌停價格終極修復版)"
TOP_N = 300              
BREADTH_THR = 0.65 
BREADTH_LOW = 0.55 
RAPID_THR = 0.03 
OPEN_DEV_THR = 0.05 
OPEN_COUNT_THR = 290 

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
        r = cffi_requests.post(url, json={"chat_id": chat_id, "text": msg, "parse_mode": "HTML"}, impersonate="chrome")
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
            
            if 180 <= seconds_diff <= 420:
                target = r; break
                
        if target is not None:
            prev_v = float(target['Breadth'])
            diff = curr_v - prev_v
            
            if abs(diff) >= RAPID_THR:
                d_str = "上漲" if diff > 0 else "下跌"
                time_diff_min = int(seconds_diff // 60)
                msg = f"⚡ <b>【廣度急變】</b>\n{target['Time'][:5]} ({prev_v:.1%}) ➜ {row['Time']} ({curr_v:.1%})\n{time_diff_min}分鐘內{d_str} {abs(diff):.1%}"
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
# 籌碼面資料處理
# ==========================================
def call_finmind_api_try_versions(dataset_candidates, data_id, start_date, token):
    versions = ["v4", "v3", "v2"]
    last_error = ""
    for dataset in dataset_candidates:
        for v in versions:
            url = f"https://api.finmindtrade.com/api/{v}/data"
            params = {"dataset": dataset, "start_date": start_date, "token": token}
            if data_id: params["data_id"] = data_id
            try:
                r = cffi_requests.get(url, params=params, impersonate="chrome", timeout=10)
                if r.status_code == 200:
                    res_json = r.json()
                    if "data" in res_json and len(res_json["data"]) > 0:
                        return pd.DataFrame(res_json["data"]), f"{dataset} ({v})"
            except Exception as e: last_error = str(e)
    return pd.DataFrame(), last_error

def get_taifex_pc_ratio(target_date_str):
    try:
        end_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=10)
        
        url = "https://www.taifex.com.tw/cht/3/pcRatio"
        payload = {
            'queryStartDate': start_dt.strftime("%Y/%m/%d"),
            'queryEndDate': end_dt.strftime("%Y/%m/%d"),
            'queryDate': end_dt.strftime("%Y/%m/%d")
        }
        
        r = cffi_requests.post(url, data=payload, impersonate="chrome", timeout=10)
        
        if r.status_code == 200:
            dfs = pd.read_html(io.StringIO(r.text))
            for df in dfs:
                if df.shape[1] >= 7:
                    top_row = df.iloc[0] 
                    try:
                        val = float(top_row.iloc[6]) 
                        return val, f"期交所官網 ({top_row.iloc[0]})"
                    except: continue
    except Exception as e:
        return None, str(e)
    return None, "找不到表格"

@st.cache_data(ttl=43200) 
def get_chips_data(token, target_date_str):
    diagnosis = [] 
    if not token:
        diagnosis.append("❌ 錯誤: 未設定 FinMind Token")
        return None, diagnosis
    
    start_date = (datetime.strptime(target_date_str, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    res = {}
    
    # 1. 期貨
    fut_candidates = ["TaiwanFuturesInstitutional", "TaiwanFuturesInstitutionalInvestors"]
    df_fut, fut_src = call_finmind_api_try_versions(fut_candidates, "TX", start_date, token)
    if df_fut.empty:
        diagnosis.append(f"❌ 期貨: 無資料")
    else:
        col_name = None
        for c in ['institutional_investors', 'name', 'institutional_investor']:
            if c in df_fut.columns: col_name = c; break
        
        if col_name:
            df_foreign = df_fut[df_fut[col_name].astype(str).str.contains('外資|Foreign', case=False)].sort_values('date')
            if df_foreign.empty: diagnosis.append("⚠️ 期貨: 找不到外資")
            else:
                latest = df_foreign.iloc[-1]
                prev = df_foreign.iloc[-2] if len(df_foreign) >= 2 else latest
                try:
                    curr_long = float(latest.get('long_open_interest_balance_volume', 0))
                    curr_short = float(latest.get('short_open_interest_balance_volume', 0))
                    
                    if curr_long==0 and curr_short==0 and 'open_interest' in latest:
                        res['fut_oi'] = int(latest['open_interest'])
                        prev_oi = int(prev.get('open_interest', 0))
                        res['fut_oi_chg'] = res['fut_oi'] - prev_oi
                    else:
                        prev_long = float(prev.get('long_open_interest_balance_volume', 0))
                        prev_short = float(prev.get('short_open_interest_balance_volume', 0))
                        res['fut_oi'] = int(curr_long - curr_short)
                        res['fut_oi_chg'] = res['fut_oi'] - int(prev_long - prev_short)
                    diagnosis.append(f"✅ 期貨(外資): 成功 ({res['fut_oi']})")
                except: diagnosis.append("❌ 期貨: 計算錯誤")
        else: diagnosis.append("❌ 期貨: 欄位錯誤")

    # 2. 選擇權
    pc_val = None
    df_opt, _ = call_finmind_api_try_versions(["TaiwanOptionDaily"], "TXO", start_date, token)
    if not df_opt.empty:
        latest = df_opt[df_opt['date'] == df_opt['date'].max()]
        cp_col = 'call_put' if 'call_put' in latest.columns else 'CallPut'
        if cp_col in latest.columns:
            put = latest[latest[cp_col].str.lower()=='put']['open_interest'].sum()
            call = latest[latest[cp_col].str.lower()=='call']['open_interest'].sum()
            if call > 0: 
                pc_val = round((put/call)*100, 2)
                diagnosis.append(f"✅ 選擇權(FinMind): {pc_val}%")

    if pc_val is None or pc_val == 0:
        taifex_val, taifex_msg = get_taifex_pc_ratio(target_date_str)
        if taifex_val is not None:
            pc_val = taifex_val
            diagnosis.append(f"✅ 選擇權(期交所): {pc_val}%")
        else:
            if pc_val is None: diagnosis.append(f"❌ 選擇權: 全數失敗 ({taifex_msg})")
            
    if pc_val is not None:
        res['pc_ratio'] = pc_val

    # 3. 維持率
    maint_candidates = ["TaiwanTotalExchangeMarginMaintenance"]
    df_maint, _ = call_finmind_api_try_versions(maint_candidates, None, start_date, token)
    if not df_maint.empty:
        latest = df_maint.iloc[-1]
        col = 'TotalExchangeMarginMaintenance'
        if col not in latest: col = 'margin_maintenance_ratio'
        if col in latest:
            res['margin_ratio'] = float(latest[col])
            diagnosis.append(f"✅ 維持率: {res['margin_ratio']}%")

    # 4. 融資餘額
    df_margin, _ = call_finmind_api_try_versions(["TaiwanStockTotalMarginPurchaseShortSale"], None, start_date, token)
    if not df_margin.empty:
        df_m = df_margin[df_margin['name'] == 'MarginPurchaseMoney'].sort_values('date')
        if not df_m.empty:
            curr = float(df_m.iloc[-1]['TodayBalance'])
            prev = float(df_m.iloc[-2]['TodayBalance']) if len(df_m)>1 else curr
            res['margin_bal'] = round(curr/1e8, 1)
            res['margin_chg'] = round((curr-prev)/1e8, 2)
            diagnosis.append(f"✅ 融資餘額: {res['margin_bal']}億")

    return res, diagnosis

def get_chip_strategy(ma5_slope, chips):
    if not chips: return None
    fut_oi = chips.get('fut_oi', 0)
    fut_chg = chips.get('fut_oi_chg', 0)
    pc = chips.get('pc_ratio', 100)
    m_ratio = chips.get('margin_ratio', 0)
    m_chg = chips.get('margin_chg', 0)
    
    sig, act, color = "籌碼中性", "觀察技術面為主", "info"
    
    if ma5_slope <= 0 and fut_oi < -10000 and m_chg > 0:
        sig, act, color = "📉 殺戮盤 (散戶接刀)", "主力殺、散戶接，全力放空。", "error"
    elif ma5_slope > 0 and fut_oi > 10000 and pc > 110:
        sig, act, color = "🚀 火力全開 (外資助攻)", "外資期現貨同步作多，多單抱緊。", "success"
    elif ma5_slope < 0 and ((m_ratio > 0 and m_ratio < 135) or m_chg < -15):
        sig, act, color = "💎 絕佳抄底 (斷頭清洗)", "融資斷頭清洗中，留意止跌訊號。", "primary"
    elif ma5_slope > 0 and fut_chg < -3000 and m_chg > 5:
        sig, act, color = "⚠️ 籌碼渙散 (拉高出貨)", "指數漲但外資逃，小心反轉。", "warning"
    elif abs(ma5_slope) < 10 and fut_chg > 2000 and pc > 110:
        sig, act, color = "🟩 潛伏期 (主力吃貨)", "盤整中見外資佈多，建議建倉。", "success"
    elif ma5_slope > 0 and fut_oi < -3000:
        sig, act, color = "🟨 假突破警戒", "現貨漲但期貨空，多單設停損。", "warning"
        
    return {"sig": sig, "act": act, "color": color, "data": chips}

# ==========================================
# 資料處理 (一般)
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
    if 0 <= now.weekday() <= 4:
        if not dates or today_str > dates[-1]: dates.append(today_str)
    return dates

@st.cache_data(ttl=86400)
def get_stock_info_map(token):
    base_map = {"2330":"twse", "2317":"twse", "2454":"twse", "0050":"twse", "0056":"twse"}
    api = DataLoader()
    if token: api.login_by_token(token)
    try:
        df = api.taiwan_stock_info()
        if not df.empty:
            df['stock_id'] = df['stock_id'].astype(str)
            base_map.update(dict(zip(df['stock_id'], df['type'])))
    except: pass
    return base_map

def get_ranks_strict(token, target_date_str, min_count=0):
    if min_count == 0 and os.path.exists(RANK_FILE):
        try:
            with open(RANK_FILE, 'r') as f:
                data = json.load(f)
                if data.get("date") == target_date_str and data.get("ranks"): return data["ranks"], True
        except: pass

    api = DataLoader()
    if token: api.login_by_token(token)
    try: df = api.taiwan_stock_daily(stock_id="", start_date=target_date_str)
    except: return [], False
    
    if df.empty: return [], False
    if min_count > 0 and len(df) < min_count: return [], False

    df['ID'] = get_col(df, ['stock_id','code'])
    df['Money'] = get_col(df, ['Trading_money','turnover'])
    if df['ID'] is None or df['Money'] is None: return [], False
    
    df['ID'] = df['ID'].astype(str)
    df = df[df['ID'].str.len()==4]
    df = df[df['ID'].str.isdigit()]
    for p in EXCL_PFX: df = df[~df['ID'].str.startswith(p)]
     
    ranks = df.sort_values('Money', ascending=False).head(TOP_N)['ID'].tolist()
    if ranks and (min_count == 0 or len(df) > 1500):
        try:
            with open(RANK_FILE, 'w') as f: json.dump({"date": target_date_str, "ranks": ranks}, f)
        except: pass
    return ranks, False

@st.cache_data(ttl=43200)
def get_hist(token, code, start):
    api = DataLoader()
    if token: api.login_by_token(token)
    try: return api.taiwan_stock_daily(stock_id=code, start_date=start)
    except: return pd.DataFrame()

def get_prices_twse_mis(codes, info_map):
    if not codes: return {}, {}
    session = cffi_requests.Session(impersonate="chrome")
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw"
    })
    try:
        ts_now = int(time_module.time() * 1000)
        session.get(f"https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw&_={ts_now}", timeout=10)
        time_module.sleep(1)
    except: return {}, {c: "Init Fail" for c in codes}

    req_strs = []
    results = {}
    debug_log = {}
    for i in range(0, len(codes), 50):
        chunk = codes[i:i+50]
        q_list = []
        for c in chunk:
            m_type = info_map.get(c, "twse").lower()
            q_list.append(f"tse_{c}.tw" if "twse" in m_type else f"otc_{c}.tw")
        req_strs.append("|".join(q_list))
    
    base_url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
    for q_str in req_strs:
        ts = int(time_module.time() * 1000)
        try:
            time_module.sleep(random.uniform(0.3, 0.8))
            r = session.get(base_url, params={"json": "1", "delay": "0", "_": ts, "ex_ch": q_str}, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if 'msgArray' not in data: 
                    for c in chunk: debug_log[c] = "MIS空"
                    continue
                
                for item in data['msgArray']:
                    c = item.get('c', '')
                    z = item.get('z', '-')
                    y = item.get('y', '-')
                    pz = item.get('pz', '-')
                    
                    val = {}
                    if y!='-' and y!='': val['y'] = float(y)
                    price = 0
                    note = ""
                    
                    # [終極修正] 漲跌停無價判斷邏輯
                    # 1. 優先嘗試成交價
                    if z and z != '-' and z.replace('.','').isdigit(): 
                        price = float(z); note="成交"
                    # 2. 其次嘗試試撮價
                    elif pz and pz != '-' and pz.replace('.','').isdigit(): 
                        price = float(pz); note="試撮"
                    
                    # 3. 若仍無價，檢查是否漲停(只有買單) 或 跌停(只有賣單)
                    if price == 0:
                        b_str = item.get('b','').split('_')[0]
                        a_str = item.get('a','').split('_')[0]
                        
                        # 檢查買單 (漲停鎖死通常 b 有值, a 無值/0)
                        if b_str and b_str != '-' and b_str != '0':
                            try:
                                price = float(b_str)
                                note = "漲停試算"
                            except: pass
                        
                        # 若還是 0，檢查賣單 (跌停鎖死通常 a 有值, b 無值/0)
                        if price == 0 and a_str and a_str != '-' and a_str != '0':
                            try:
                                price = float(a_str)
                                note = "跌停試算"
                            except: pass
                    
                    if price > 0:
                        val['z'] = price; val['note'] = note
                        results[c] = val
                    else: debug_log[c] = "無價"
        except Exception as e: 
            # 這裡不噴錯，只紀錄
            pass
            
    return results, debug_log

def save_rec(d, t, b, tc, t_cur, t_prev, intra, total_v):
    if t_cur == 0: return 
    t_short = t[:5] 
    row = pd.DataFrame([{'Date':d, 'Time':t_short, 'Breadth':b, 'Taiex_Change':tc, 'Taiex_Current':t_cur, 'Taiex_Prev_Close':t_prev, 'Total': total_v}])
    if not os.path.exists(HIST_FILE): row.to_csv(HIST_FILE, index=False); return
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: row.to_csv(HIST_FILE, index=False); return
        df['Date'] = df['Date'].astype(str); df['Time'] = df['Time'].astype(str)
        last_d = str(df.iloc[-1]['Date']); last_t = str(df.iloc[-1]['Time'])[:5]
        if last_d != str(d): pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
        else:
            if not intra: df = df[df['Date'] != str(d)]; pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
            elif last_t != str(t_short): row.to_csv(HIST_FILE, mode='a', header=False, index=False)
    except: row.to_csv(HIST_FILE, index=False)

def display_strategy_panel(slope, open_br, br, n_state, chip_strategy, chip_diag):
    st.subheader("♟️ 戰略指揮所")
    strategies = []
    trend_status = n_state.get('intraday_trend')
    if slope > 0: strategies.append({"sig": "MA5斜率為正 ➜ 大盤偏多", "act": "只做多單", "type": "success"})
    elif slope < 0: strategies.append({"sig": "MA5斜率為負 ➜ 大盤偏空", "act": "只做空單", "type": "error"})
    else: strategies.append({"sig": "MA5斜率持平", "act": "觀望", "type": "info"})
    if trend_status == 'up': strategies.append({"sig": "🔒 趨勢鎖定：偏多", "act": "留意回檔", "type": "success"})
    elif trend_status == 'down': strategies.append({"sig": "🔒 趨勢鎖定：偏空", "act": "留意反彈", "type": "error"})
    else: strategies.append({"sig": "⏳ 趨勢未鎖定", "act": "等待 +/- 5%", "type": "info"})
    if slope > 0 and trend_status == 'up' and n_state['notified_drop_high']:
        strategies.append({"sig": "偏多回檔 (高點-5%)", "act": "🎯 進場多單", "type": "success"})
    elif slope < 0 and trend_status == 'down' and n_state['notified_rise_low']:
        strategies.append({"sig": "偏空反彈 (低點+5%)", "act": "🎯 進場空單", "type": "error"})

    cols = st.columns(len(strategies))
    for i, s in enumerate(strategies):
        with cols[i]:
            if s["type"] == "success": st.success(f"**{s['sig']}**\n\n{s['act']}")
            elif s["type"] == "error": st.error(f"**{s['sig']}**\n\n{s['act']}")
            elif s["type"] == "info": st.info(f"**{s['sig']}**\n\n{s['act']}")
            else: st.warning(f"**{s['sig']}**\n\n{s['act']}")

    st.markdown("---")
    st.subheader("♟️ 籌碼氣象站 (Sponsor)")
    if chip_strategy and chip_strategy['data']:
        d = chip_strategy['data']
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("外資期貨淨OI", f"{d.get('fut_oi',0):,}", f"{d.get('fut_oi_chg',0):,}")
        c2.metric("P/C Ratio", f"{d.get('pc_ratio',0)}%")
        if d.get('margin_ratio', 0) > 0: c3.metric("融資維持率", f"{d.get('margin_ratio',0)}%")
        else: c3.metric("融資餘額(億)", f"{d.get('margin_bal',0)}", f"{d.get('margin_chg',0)}")
        s_col = chip_strategy['color']
        with c4:
            msg = f"**{chip_strategy['sig']}**\n\n{chip_strategy['act']}"
            if s_col == 'success': st.success(msg)
            elif s_col == 'error': st.error(msg)
            elif s_col == 'warning': st.warning(msg)
            elif s_col == 'primary': st.info(msg, icon="💎")
            else: st.info(msg)
        with st.expander("查看詳細數據來源"):
            for msg in chip_diag: st.text(msg)
    else: st.error("⚠️ 無籌碼資料"); st.write(chip_diag)

def plot_chart():
    chart_data = pd.DataFrame(); base_d = ""
    if os.path.exists(HIST_FILE):
        try:
            df = pd.read_csv(HIST_FILE)
            if not df.empty:
                df['Date'] = df['Date'].astype(str)
                df['Time'] = df['Time'].astype(str)
                df['Time'] = df['Time'].apply(lambda x: str(x)[:5])
                df_today = df[df['Time'] >= "09:00"].copy()
                if not df_today.empty:
                    df_today = df_today.sort_values(['Date', 'Time'])
                    last_date = df_today.iloc[-1]['Date']
                    chart_data = df_today[df_today['Date'] == last_date].copy()
                    chart_data['DT'] = pd.to_datetime(chart_data['Date'] + ' ' + chart_data['Time'], errors='coerce')
                    chart_data = chart_data.dropna(subset=['DT'])
                    chart_data['T_S'] = (chart_data['Taiex_Change']*10)+0.5
                    base_d = last_date
        except: pass

    if base_d == "": base_d = datetime.now().strftime("%Y-%m-%d")
    start = pd.to_datetime(f"{base_d} 09:00:00")
    end = pd.to_datetime(f"{base_d} 13:30:00")
    if not chart_data.empty: chart_data = chart_data[chart_data['DT'] >= start] 

    x_scale = alt.Scale(domain=[start, end])
    y_vals = [i/10 for i in range(11)]
    y_axis = alt.Axis(format='%', values=y_vals, tickCount=11, title=None)
    
    if chart_data.empty:
        base = alt.Chart(pd.DataFrame({'DT': [start, end]})).mark_point(opacity=0).encode(
            x=alt.X('DT:T', title=None, axis=alt.Axis(format='%H:%M'), scale=x_scale),
            y=alt.Y('val:Q', axis=y_axis, scale=alt.Scale(domain=[0, 1]))
        )
    else:
        base = alt.Chart(chart_data).encode(x=alt.X('DT:T', title=None, axis=alt.Axis(format='%H:%M'), scale=x_scale))

    layers = []
    if not chart_data.empty:
        l_b = base.mark_line(color='#ffc107').encode(y=alt.Y('Breadth', scale=alt.Scale(domain=[0,1], nice=False), axis=y_axis))
        p_b = base.mark_circle(color='#ffc107', size=20).encode(y='Breadth', tooltip=['DT', alt.Tooltip('Breadth', format='.1%')])
        l_t = base.mark_line(color='#007bff', strokeDash=[4,4]).encode(y=alt.Y('T_S', scale=alt.Scale(domain=[0,1])))
        p_t = base.mark_circle(color='#007bff', size=20).encode(y='T_S', tooltip=['DT', alt.Tooltip('Taiex_Change', format='.2%')])
        layers = [l_b, p_b, l_t, p_t]
    else: layers = [base]

    rule_r = alt.Chart(pd.DataFrame({'y':[BREADTH_THR]})).mark_rule(color='red', strokeDash=[5,5]).encode(y='y')
    rule_g = alt.Chart(pd.DataFrame({'y':[BREADTH_LOW]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y')
    layers.extend([rule_r, rule_g])
    return alt.layer(*layers).properties(height=400, title=f"走勢對照 - {base_d}").resolve_scale(y='shared')

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
    date_prev = days[-2] if len(days) > 1 else (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    ranks_curr, _ = get_ranks_strict(ft, date_prev) 
    msg_src = f"名單:{date_prev}(昨日/盤中)"
    if now.time() >= time(14, 0) and d_cur == today_str:
        r_today, _ = get_ranks_strict(ft, today_str, min_count=1500)
        if r_today: ranks_curr = r_today; msg_src = f"名單:{today_str}(今日完整)"
    
    all_targets = list(set(ranks_curr))
    pmap = {}; mis_debug = {}; src_type = "歷史"; last_t = "無即時資料"
    if allow_live_fetch:
        if sj_api:
            try:
                for i in range(0, len(all_targets), 50):
                    snaps = sj_api.snapshots([sj_api.Contracts.Stocks[c] for c in all_targets[i:i+50] if c in sj_api.Contracts.Stocks])
                    for s in snaps:
                        if s.close > 0: pmap[s.code] = {'price': float(s.close), 'y_close': float(s.reference_price)}
                if pmap: src_type = "永豐API"; last_t = now.strftime("%H:%M:%S")
            except: pass
        missing = [c for c in all_targets if c not in pmap]
        if missing:
            mis_data, m_debug = get_prices_twse_mis(missing, info_map)
            pmap.update(mis_data)
            mis_debug.update(m_debug)
            if mis_data and src_type=="歷史": src_type="證交所MIS"; last_t = now.strftime("%H:%M:%S")

    s_dt = (datetime.now()-timedelta(days=40)).strftime("%Y-%m-%d")
    h_c, v_c = 0, 0
    dtls = []
    
    h_p, v_p = 0, 0
    
    for c in ranks_curr:
        info = pmap.get(c, {})
        curr_p = info.get('z', info.get('price', 0))
        y_close = info.get('y', info.get('y_close', 0))
        
        df = get_hist(ft, c, s_dt)
        m_type = info_map.get(c, "未知")
        m_display = {"twse":"上市", "tpex":"上櫃", "emerging":"興櫃"}.get(m_type, "未知")
        
        p_price = y_close if y_close > 0 else (float(df.iloc[-1]['close']) if not df.empty else 0)
        p_ma5 = 0; p_stt = "-"
        
        if not df.empty and p_price > 0:
            closes = df['close'].tail(5).tolist()
            if len(closes) >= 5:
                p_ma5 = sum(closes) / 5
            if p_price > p_ma5: p_stt="✅"
            else: p_stt="📉"

        c_ma5 = 0; c_stt = "-"; note = ""
        if curr_p == 0: 
            c_stt = "⚠️無報價"
            reason = mis_debug.get(c, "非交易時間" if not allow_live_fetch else "MIS未回傳")
            note = f"⚠️{reason} | 昨收{p_price}"
        else: note = f"昨收{p_price}"
        
        source_note = info.get('note', '')
        if source_note: note = f"📝{source_note} " + note

        if curr_p > 0 and p_price > 0 and not df.empty:
            hist_closes = df['close'].tail(4).tolist()
            if len(hist_closes) >= 4:
                ma5_input = hist_closes + [curr_p]
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
        
        if not df.empty:
            try:
                df_prev = df[df['date'] == date_prev]
                if not df_prev.empty:
                    idx = df.index.get_loc(df_prev.index[0])
                    if idx >= 4:
                        prev_c = float(df_prev.iloc[0]['close'])
                        prev_m = df['close'].iloc[idx-4:idx+1].mean()
                        if prev_c > prev_m: h_p += 1
                        v_p += 1
            except: pass

    br_c = h_c/v_c if v_c>0 else 0
    br_p = h_p/v_p if v_p>0 else 0 

    t_cur, t_pre, slope = 0, 0, 0
    try:
        tw = get_hist(ft, "TAIEX", s_dt)
        if not tw.empty:
            mis_tw, _ = get_prices_twse_mis(["t00"], {"t00":"twse"})
            t_now = mis_tw.get("t00", {}).get("z", 0)
            t_pre = float(tw.iloc[-1]['close'])
            if t_now > 0: t_cur = t_now
            else: t_cur = t_pre 
            h_tw = tw['close'].tail(6).tolist()
            if len(h_tw) >= 6:
                ma5_prev = sum(h_tw[-6:-1])/5
                if t_cur!=t_pre: ma5_curr = (sum(h_tw[-4:]) + t_cur)/5
                else: ma5_curr = sum(h_tw[-5:])/5
                slope = ma5_curr - ma5_prev
    except: pass
    t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    
    rec_t = last_t if "無" not in str(last_t) else now.strftime("%H:%M:%S")
    save_rec(d_cur, rec_t, br_c, t_chg, t_cur, t_pre, is_intra, v_c)
    
    chips_data, chips_diag = get_chips_data(ft, d_cur)
    chip_strategy = get_chip_strategy(slope, chips_data)
    
    return {
        "d":d_cur, "br":br_c, "h":h_c, "v":v_c, "br_p":br_p, "h_p":h_p, "v_p":v_p,
        "df":pd.DataFrame(dtls), "t":last_t, "tc":t_chg, "slope":slope, "src":msg_src, "src_type":src_type,
        "raw":{'Date':d_cur,'Time':rec_t,'Breadth':br_c},
        "chip_strat": chip_strategy, "chip_diag": chips_diag
    }

def run_app():
    st.title(f"📈 {APP_VER}")
    with st.sidebar:
        st.subheader("設定")
        auto = st.checkbox("自動更新", value=False)
        st.caption(f"FinMind: {'🟢' if get_finmind_token() else '🔴'}")
        tg_tok = st.text_input("TG Token", value=st.secrets.get("telegram",{}).get("token",""), type="password")
        tg_id = st.text_input("Chat ID", value=st.secrets.get("telegram",{}).get("chat_id",""))
        if st.button("清除快取"): st.cache_data.clear(); st.rerun()
        if st.button("重置資料"): 
            if os.path.exists(HIST_FILE): os.remove(HIST_FILE)
            st.rerun()
    if st.button("🔄 刷新"): st.rerun()

    try:
        data = fetch_all()
        if data:
            st.sidebar.info(f"來源: {data['src_type']}")
            br = data['br']
            n_state = load_notify_state(data['d'])
            open_br = get_opening_breadth(data['d'])
            hist_max, hist_min = get_intraday_extremes(data['d'])
            today_min = min(hist_min, br) if hist_min is not None else br
            today_max = max(hist_max, br) if hist_max is not None else br

            if tg_tok and tg_id:
                if open_br is not None and n_state['intraday_trend'] is None:
                    if br >= (open_br + 0.05):
                        n_state['intraday_trend'] = 'up'
                        send_tg(tg_tok, tg_id, f"🔒 <b>【趨勢鎖定】</b>\n廣度達開盤+5% (目前{br:.1%})，今日偏多！")
                    elif br <= (open_br - 0.05):
                        n_state['intraday_trend'] = 'down'
                        send_tg(tg_tok, tg_id, f"🔒 <b>【趨勢鎖定】</b>\n廣度達開盤-5% (目前{br:.1%})，今日偏空！")
                stt = 'normal'
                if br >= BREADTH_THR: stt = 'hot'
                elif br <= BREADTH_LOW: stt = 'cold'
                if stt != n_state['last_stt']:
                    msg = f"🔥 過熱: {br:.1%}" if stt=='hot' else (f"❄️ 冰點: {br:.1%}" if stt=='cold' else "")
                    if msg: send_tg(tg_tok, tg_id, msg)
                    n_state['last_stt'] = stt
                rap_msg, rid = check_rapid(data['raw'])
                if rap_msg and rid != n_state['last_rap']:
                    send_tg(tg_tok, tg_id, rap_msg)
                    n_state['last_rap'] = rid
                if n_state['intraday_trend'] == 'down': 
                    if br >= (today_min + 0.05):
                        if not n_state['notified_rise_low']:
                            send_tg(tg_tok, tg_id, f"🚀 <b>【低點反彈】</b>\n低點: {today_min:.1%} ➜ 目前: {br:.1%}\n已反彈 > 5%")
                            n_state['notified_rise_low'] = True
                    else: n_state['notified_rise_low'] = False
                elif n_state['intraday_trend'] == 'up':
                    if br <= (today_max - 0.05):
                        if not n_state['notified_drop_high']:
                            send_tg(tg_tok, tg_id, f"📉 <b>【高點回落】</b>\n高點: {today_max:.1%} ➜ 目前: {br:.1%}\n已回檔 > 5%")
                            n_state['notified_drop_high'] = True
                    else: n_state['notified_drop_high'] = False
                save_notify_state(n_state)

            display_strategy_panel(data['slope'], open_br, br, n_state, data['chip_strat'], data['chip_diag'])
            st.subheader(f"📅 {data['d']}")
            st.caption(f"{data['src']} | {data['t']}")
            chart = plot_chart()
            if chart: st.altair_chart(chart, use_container_width=True)
            
            c1,c2,c3 = st.columns(3)
            c1.metric("今日廣度", f"{br:.1%}", f"{data['h']}/{data['v']}")
            c2.metric("大盤漲跌", f"{data['tc']:.2%}")
            c3.metric("大盤MA5斜率", f"{data['slope']:.2f}", "📈" if data['slope']>0 else "📉")
            
            caption_str = f"昨日廣度: {data['br_p']:.1%} ({data['h_p']}/{data['v_p']})"
            if open_br: caption_str += f" | 開盤: {open_br:.1%}"
            else: caption_str += " | 開盤: 等待中..."
            caption_str += f"\n今日目前最高廣度: {today_max:.1%}"
            caption_str += f"\n今日目前最低廣度: {today_min:.1%}"
            c1.caption(caption_str)
            
            st.dataframe(data['df'], use_container_width=True, hide_index=True)
        else: st.warning("⚠️ 無資料")
    except Exception as e:
        st.error(f"Error: {e}")
        st.text(traceback.format_exc())

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
    try:
        from streamlit.web import cli as stcli
    except ImportError:
        try: import streamlit.cli as stcli
        except: pass
    if 'streamlit' in sys.modules and any('streamlit' in arg for arg in sys.argv): run_app()
    else:
        print("Starting...")
        try: subprocess.call(["streamlit", "run", __file__])
        except: pass

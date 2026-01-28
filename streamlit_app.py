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

# 引入 curl_cffi 
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    st.error("缺少 curl_cffi 套件！請在 requirements.txt 中加入 'curl_cffi'")
    st.stop()

# ==========================================
# 設定區 v9.55.18 (維持率欄位修正+圖表防呆版)
# ==========================================
APP_VER = "v9.55.18 (維持率欄位修正+圖表防呆版)"
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
# 籌碼面資料處理 (Auto-Discovery + Column Logic)
# ==========================================
def call_finmind_api_try_versions(dataset_candidates, data_id, start_date, token):
    """
    自動掃描機制：嘗試不同的 dataset 名稱與 API 版本
    """
    versions = ["v4", "v3", "v2"]
    last_error = ""
    
    for dataset in dataset_candidates:
        for v in versions:
            url = f"https://api.finmindtrade.com/api/{v}/data"
            params = {
                "dataset": dataset,
                "start_date": start_date,
                "token": token
            }
            if data_id: params["data_id"] = data_id
            
            try:
                r = cffi_requests.get(url, params=params, impersonate="chrome", timeout=10)
                if r.status_code == 200:
                    res_json = r.json()
                    if "data" in res_json and len(res_json["data"]) > 0:
                        return pd.DataFrame(res_json["data"]), f"{dataset} ({v})"
                    elif "msg" in res_json:
                        last_error = f"{dataset} ({v}): {res_json['msg']}"
                else:
                    last_error = f"{dataset} ({v}) HTTP {r.status_code}"
            except Exception as e:
                last_error = str(e)
                
    return pd.DataFrame(), last_error

@st.cache_data(ttl=43200) 
def get_chips_data(token, target_date_str):
    diagnosis = [] 
    if not token:
        diagnosis.append("❌ 錯誤: 未設定 FinMind Token")
        return None, diagnosis
    
    start_date = (datetime.strptime(target_date_str, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    res = {}
    
    # 1. 外資期貨 (掃描 + 正確欄位) - 回歸 v9.55.14 邏輯
    fut_candidates = ["TaiwanFuturesInstitutional", "TaiwanFuturesInstitutionalInvestors"]
    df_fut, fut_src = call_finmind_api_try_versions(fut_candidates, "TX", start_date, token)
    
    if df_fut.empty:
        diagnosis.append(f"❌ 期貨: 掃描失敗 ({fut_src})")
    else:
        # 尋找外資
        col_name = None
        for c in ['institutional_investors', 'name', 'institutional_investor']:
            if c in df_fut.columns:
                col_name = c
                break
        
        if col_name:
            df_foreign = df_fut[df_fut[col_name].astype(str).str.contains('外資|Foreign', case=False)].sort_values('date')
            if df_foreign.empty:
                diagnosis.append("⚠️ 期貨: 找不到外資資料")
            else:
                latest = df_foreign.iloc[-1]
                prev = df_foreign.iloc[-2] if len(df_foreign) >= 2 else latest
                
                # 正確欄位計算
                try:
                    curr_long = float(latest.get('long_open_interest_balance_volume', 0))
                    curr_short = float(latest.get('short_open_interest_balance_volume', 0))
                    
                    # 舊版欄位兼容
                    if curr_long == 0 and curr_short == 0 and 'open_interest' in latest:
                        res['fut_oi'] = int(latest['open_interest'])
                        prev_oi = int(prev.get('open_interest', 0))
                        res['fut_oi_chg'] = res['fut_oi'] - prev_oi
                    else:
                        prev_long = float(prev.get('long_open_interest_balance_volume', 0))
                        prev_short = float(prev.get('short_open_interest_balance_volume', 0))
                        
                        res['fut_oi'] = int(curr_long - curr_short)
                        prev_net_oi = int(prev_long - prev_short)
                        res['fut_oi_chg'] = res['fut_oi'] - prev_net_oi
                    
                    diagnosis.append(f"✅ 期貨(外資): 成功 (OI: {res['fut_oi']}, 變動: {res['fut_oi_chg']})")
                except:
                    diagnosis.append(f"❌ 期貨: 數值計算錯誤")
        else:
            diagnosis.append(f"❌ 期貨: 找不到法人欄位 {list(df_fut.columns)}")

    # 2. 選擇權
    df_opt, opt_src = call_finmind_api_try_versions(["TaiwanOptionDaily"], "TXO", start_date, token)
    if not df_opt.empty:
        last_date = df_opt['date'].max()
        df_today = df_opt[df_opt['date'] == last_date]
        cp_col = 'call_put' if 'call_put' in df_today.columns else 'CallPut'
        if cp_col in df_today.columns:
            put_oi = df_today[df_today[cp_col].str.lower() == 'put']['open_interest'].sum()
            call_oi = df_today[df_today[cp_col].str.lower() == 'call']['open_interest'].sum()
            if call_oi > 0:
                res['pc_ratio'] = round((put_oi / call_oi) * 100, 2)
                diagnosis.append(f"✅ 選擇權: 成功 (PC={res['pc_ratio']}%)")

    # 3. 融資維持率 (使用正確的大盤名稱 + 正確欄位)
    maint_candidates = ["TaiwanTotalExchangeMarginMaintenance"] # 這是您腳本中正確的名稱
    df_maint, maint_src = call_finmind_api_try_versions(maint_candidates, None, start_date, token)
    
    if not df_maint.empty:
        latest = df_maint.iloc[-1]
        # [關鍵修正] 使用您截圖中顯示的正確欄位名稱
        col_maint = 'TotalExchangeMarginMaintenance' 
        
        if col_maint in latest:
            res['margin_ratio'] = float(latest[col_maint])
            diagnosis.append(f"✅ 維持率(官方): 成功 ({res['margin_ratio']}%)")
        else:
            # 如果找不到，嘗試其他變體 (雖然不太可能)
            alt_cols = [c for c in latest.index if 'Margin' in c]
            if alt_cols:
                 res['margin_ratio'] = float(latest[alt_cols[0]])
                 diagnosis.append(f"✅ 維持率(自動匹配): 成功 ({res['margin_ratio']}%)")
            else:
                 diagnosis.append(f"❌ 維持率: 欄位異常 {list(latest.keys())}")
    else:
        diagnosis.append(f"❌ 維持率: 抓取失敗 ({maint_src})")
    
    # 4. 融資餘額
    df_margin, margin_src = call_finmind_api_try_versions(["TaiwanStockTotalMarginPurchaseShortSale"], None, start_date, token)
    if not df_margin.empty:
        df_money = df_margin[df_margin['name'] == 'MarginPurchaseMoney'].sort_values('date')
        if not df_money.empty:
            latest = df_money.iloc[-1]
            prev = df_money.iloc[-2] if len(df_money) >= 2 else latest
            curr_bal = float(latest['TodayBalance'])
            prev_bal = float(prev['TodayBalance'])
            res['margin_chg'] = round((curr_bal - prev_bal) / 100000000, 2) 
            res['margin_bal'] = round(curr_bal / 100000000, 1)
            diagnosis.append(f"✅ 大盤融資餘額: 成功 (總額:{res['margin_bal']}億, 變動:{res['margin_chg']}億)")

    return res, diagnosis

def get_chip_strategy(ma5_slope, chips):
    if not chips: return None
    
    fut_oi = chips.get('fut_oi', 0)
    fut_chg = chips.get('fut_oi_chg', 0)
    pc_ratio = chips.get('pc_ratio', 100)
    margin_ratio = chips.get('margin_ratio', 0) 
    margin_chg = chips.get('margin_chg', 0)
    
    sig = "籌碼中性"
    act = "觀察技術面為主"
    color = "info"
    
    # 1. 殺盤 (空頭順勢)
    if ma5_slope <= 0 and fut_oi < -10000 and margin_chg > 0:
        sig = "📉 殺戮盤 (散戶接刀)"
        act = "主力殺、散戶接，籌碼極亂。全力放空，不要猜底。"
        color = "error"
    # 2. 多頭燃料充足
    elif ma5_slope > 0 and fut_oi > 10000 and pc_ratio > 110:
        sig = "🚀 火力全開 (外資助攻)"
        act = "外資期現貨同步作多，支撐強勁。多單抱緊，甚至加碼。"
        color = "success"
    # 3. 斷頭/籌碼清洗
    elif ma5_slope < 0 and ((margin_ratio > 0 and margin_ratio < 135) or margin_chg < -15):
        sig = "💎 絕佳抄底 (斷頭清洗)"
        reason = f"維持率{margin_ratio}%" if margin_ratio > 0 else f"融資大減{abs(margin_chg)}億"
        act = f"{reason}，浮額清洗中。通常是波段低點，留意止跌訊號。"
        color = "primary"
    # 4. 多頭力竭
    elif ma5_slope > 0 and fut_chg < -3000 and margin_chg > 5: 
        sig = "⚠️ 籌碼渙散 (拉高出貨)"
        act = "指數漲但外資大逃亡，散戶在接最後一棒。獲利了結，小心反轉。"
        color = "warning"
    # 5. 潛伏期
    elif abs(ma5_slope) < 10 and fut_chg > 2000 and pc_ratio > 110:
        sig = "🟩 潛伏期 (主力吃貨)"
        act = "盤整中見外資偷佈局多單。建議提前建倉，等待噴出。"
        color = "success"
    # 6. 假突破
    elif ma5_slope > 0 and fut_oi < -3000:
        sig = "🟨 假突破警戒"
        act = "現貨漲但期貨空單留倉。可能是假突破，多單要設緊停損。"
        color = "warning"

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
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
    return dates

@st.cache_data(ttl=86400)
def get_stock_info_map(token):
    base_map = {
        "2330":"twse", "2317":"twse", "2454":"twse", "2303":"twse", "2308":"twse",
        "0050":"twse", "0056":"twse", "00878":"twse", "t00": "twse"
    }
    api = DataLoader()
    if token: api.login_by_token(token)
    try:
        df = api.taiwan_stock_info()
        if df.empty: return base_map
        df['stock_id'] = df['stock_id'].astype(str)
        api_map = dict(zip(df['stock_id'], df['type']))
        base_map.update(api_map)
        return base_map
    except: return base_map

def get_ranks_strict(token, target_date_str, min_count=0):
    if min_count == 0 and os.path.exists(RANK_FILE):
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

    if min_count > 0 and len(df) < min_count:
        print(f"DEBUG: {target_date_str} 資料量 {len(df)} 不足 (預期 > {min_count})，判定未更新完畢")
        return [], False

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
    if not codes: return {}, {}
    
    session = cffi_requests.Session(impersonate="chrome")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw",
        "Host": "mis.twse.com.tw",
        "X-Requested-With": "XMLHttpRequest",
    }
    session.headers.update(headers)
    
    try:
        ts_now = int(time_module.time() * 1000)
        session.get(f"https://mis.twse.com.tw/stock/fibest.jsp?lang=zh_tw&_={ts_now}", timeout=10)
        time_module.sleep(1)
    except:
        return {}, {c: "初始化失敗" for c in codes}

    req_strs = []
    chunk_size = 50 
    results = {}
    debug_log = {}

    for i in range(0, len(codes), chunk_size):
        chunk = codes[i:i+chunk_size]
        q_list = []
        for c in chunk:
            c = str(c).strip()
            if not c: continue
            
            m_type = info_map.get(c, "twse").lower()
            if "twse" in m_type:
                q_list.append(f"tse_{c}.tw")
            else:
                q_list.append(f"otc_{c}.tw")
                 
        if q_list:
            req_strs.append("|".join(q_list))
    
    base_url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
    
    for idx, q_str in enumerate(req_strs):
        ts = int(time_module.time() * 1000)
        params = {"json": "1", "delay": "0", "_": ts, "ex_ch": q_str}
        
        try:
            time_module.sleep(random.uniform(0.3, 0.8))
            r = session.get(base_url, params=params, timeout=10)
            
            if r.status_code == 200:
                try:
                    data = r.json()
                    if 'msgArray' not in data: continue
                    
                    for item in data['msgArray']:
                        c = item.get('c', '') 
                        z = item.get('z', '-') 
                        pz = item.get('pz', '-') 
                        y = item.get('y', '-') 
                        
                        val = {}
                        if y != '-' and y != '':
                            try: val['y'] = float(y)
                            except: pass
                        
                        price = 0
                        source_note = ""

                        if z != '-' and z != '':
                             try: 
                                price = float(z)
                                source_note = "來源:成交"
                             except: pass
                        
                        if price == 0 and pz != '-' and pz != '':
                            try:
                                price = float(pz)
                                source_note = "來源:試撮"
                            except: pass

                        if price == 0:
                            try:
                                b = item.get('b', '-').split('_')[0]
                                if b != '-' and b: 
                                    price = float(b)
                                    source_note = "來源:委買"
                                else:
                                    a = item.get('a', '-').split('_')[0]
                                    if a != '-' and a: 
                                        price = float(a)
                                        source_note = "來源:委賣"
                            except: pass
                            
                            if price == 0:
                                 source_note = "無成交/無掛單"
                        
                        if price > 0: 
                            val['z'] = price
                            val['note'] = source_note
                        elif source_note:
                            debug_log[c] = source_note
                       
                        if c and val: results[c] = val

                except: pass
        except: pass
             
    return results, debug_log

def save_rec(d, t, b, tc, t_cur, t_prev, intra, total_v):
    if t_cur == 0: return 
    t_short = t[:5] 
    row = pd.DataFrame([{
        'Date':d, 'Time':t_short, 'Breadth':b, 
        'Taiex_Change':tc, 'Taiex_Current':t_cur, 'Taiex_Prev_Close':t_prev,
        'Total': total_v
    }])
    if not os.path.exists(HIST_FILE): 
        row.to_csv(HIST_FILE, index=False)
        return

    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: 
            row.to_csv(HIST_FILE, index=False)
            return

        if 'Total' not in df.columns: df['Total'] = 0
        df['Date'] = df['Date'].astype(str)
        df['Time'] = df['Time'].astype(str)
        
        last_d = str(df.iloc[-1]['Date'])
        last_t = str(df.iloc[-1]['Time'])[:5]
        
        if last_d != str(d): 
            pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
        else:
            if not intra: 
                df = df[df['Date'] != str(d)]
                pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
            elif last_t != str(t_short): 
                row.to_csv(HIST_FILE, mode='a', header=False, index=False)
    except: row.to_csv(HIST_FILE, index=False)

def display_strategy_panel(slope, open_br, br, n_state, chip_strategy, chip_diag):
    st.subheader("♟️ 戰略指揮所")
    strategies = []
    
    # 1. 技術面
    if slope > 0:
        strategies.append({"sig": "MA5斜率為正 ➜ 大盤偏多", "act": "只做多單，放棄空單", "type": "success"})
    elif slope < 0:
        strategies.append({"sig": "MA5斜率為負 ➜ 大盤偏空", "act": "只做空單，放棄多單", "type": "error"})
    else:
        strategies.append({"sig": "MA5斜率持平", "act": "", "type": "info"})
    
    # 2. 日內趨勢
    trend_status = n_state.get('intraday_trend')
    if trend_status == 'up':
        strategies.append({"sig": "🔒 已觸發【開盤+5%】", "act": "今日偏多確認，留意回檔", "type": "success"})
    elif trend_status == 'down':
        strategies.append({"sig": "🔒 已觸發【開盤-5%】", "act": "今日偏空確認，留意反彈", "type": "error"})
    else:
        strategies.append({"sig": "⏳ 盤整中 (未達 +/- 5%)", "act": "觀望，等待趨勢表態", "type": "info"})

    # 3. 動態戰術
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
            elif s["type"] == "primary": st.info(f"**{title}**\n\n{body}")
            else: st.info(f"**{title}**\n\n{body}")
    
    # 4. 籌碼氣象站 (Sponsor)
    st.markdown("---")
    st.subheader("♟️ 籌碼氣象站 (Sponsor)")
    
    if chip_strategy and chip_strategy['data']:
        d = chip_strategy['data']
        has_missing = (d.get('fut_oi',0) == 0)
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("外資期貨淨OI", f"{d.get('fut_oi',0):,}", f"{d.get('fut_oi_chg',0):,}")
        c2.metric("P/C Ratio", f"{d.get('pc_ratio',0)}%")
        
        if d.get('margin_ratio', 0) > 0:
             c3.metric("融資維持率", f"{d.get('margin_ratio',0)}%")
        else:
             c3.metric("融資餘額(億)", f"{d.get('margin_bal',0)}", f"{d.get('margin_chg',0)}")
        
        sig = chip_strategy['sig']
        act = chip_strategy['act']
        color = chip_strategy['color']
        
        with c4:
            if color == 'success': st.success(f"**{sig}**\n\n{act}")
            elif color == 'error': st.error(f"**{sig}**\n\n{act}")
            elif color == 'warning': st.warning(f"**{sig}**\n\n{act}")
            elif color == 'primary': st.info(f"**{sig}**\n\n{act}", icon="💎")
            else: st.info(f"**{sig}**\n\n{act}")
            
        if has_missing:
             with st.expander("⚠️ 部分數據缺失 (點擊查看原因)", expanded=True):
                 for msg in chip_diag:
                     if "⚠️" in msg or "❌" in msg or "HTTP" in msg:
                         st.error(msg)
                     else:
                         st.caption(msg)
        else:
             with st.expander("查看詳細數據來源"):
                 for msg in chip_diag:
                     st.text(msg)
    else:
        st.error("⚠️ 無籌碼資料，請展開查看診斷報告")
        with st.expander("🔍 連線診斷報告", expanded=True):
            for msg in chip_diag:
                st.write(msg)

def plot_chart():
    chart_data = pd.DataFrame()
    base_d = ""
    
    if os.path.exists(HIST_FILE):
        try:
            df = pd.read_csv(HIST_FILE)
            if not df.empty:
                df['Date'] = df['Date'].astype(str)
                df['Time'] = df['Time'].astype(str)
                df['Time'] = df['Time'].apply(lambda x: x[:5])
                
                # 優先顯示今日 09:00 後的資料
                df_today = df[df['Time'] >= "09:00"].copy()
                
                if not df_today.empty:
                    # 如果今天有資料，就用今天
                    df_today = df_today.sort_values(['Date', 'Time'])
                    last_date = df_today.iloc[-1]['Date']
                    
                    # 檢查這一天是否為「今天」
                    # 如果今天還沒開盤，則使用最近一個交易日
                    chart_data = df_today[df_today['Date'] == last_date].copy()
                    base_d = last_date
        except: pass

    # 如果還是沒資料 (完全沒歷史檔或盤前)，建立空圖表
    if chart_data.empty or base_d == "":
        base_d = datetime.now().strftime("%Y-%m-%d")
        start = datetime.strptime(f"{base_d} 09:00", "%Y-%m-%d %H:%M")
        end = datetime.strptime(f"{base_d} 13:30", "%Y-%m-%d %H:%M")
        chart_data = pd.DataFrame() # 確保是空 DataFrame
    else:
        start = pd.to_datetime(f"{base_d} 09:00:00")
        end = pd.to_datetime(f"{base_d} 13:30:00")
        
        chart_data['DT'] = pd.to_datetime(chart_data['Date'] + ' ' + chart_data['Time'], errors='coerce')
        chart_data = chart_data.dropna(subset=['DT'])
        chart_data['T_S'] = (chart_data['Taiex_Change']*10)+0.5

    # --- Altair 圖表設定 (嚴格遵守使用者要求) ---
    
    # 1. 座標軸設定
    x_scale = alt.Scale(domain=[start, end])
    y_vals = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] # 10%一格
    y_axis = alt.Axis(format='%', values=y_vals, tickCount=11, title=None) # 取消"廣度"文字
    
    if chart_data.empty:
        # 空底圖: 只畫框線與警戒線
        base = alt.Chart(pd.DataFrame({'DT': [start, end]})).mark_point(opacity=0).encode(
            x=alt.X('DT:T', title=None, axis=alt.Axis(format='%H:%M'), scale=x_scale),
            y=alt.Y('val:Q', axis=y_axis, scale=alt.Scale(domain=[0, 1]))
        )
    else:
        # 有資料圖表
        base = alt.Chart(chart_data).encode(
            x=alt.X('DT:T', title=None, axis=alt.Axis(format='%H:%M'), scale=x_scale)
        )
        
    # 2. 繪製線條 (黃色廣度, 藍色大盤)
    layers = []
    
    # 警戒線 (紅虛線65%, 綠虛線55%)
    rule_r = alt.Chart(pd.DataFrame({'y':[BREADTH_THR]})).mark_rule(color='red', strokeDash=[5,5]).encode(y='y')
    rule_g = alt.Chart(pd.DataFrame({'y':[BREADTH_LOW]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y')
    
    if not chart_data.empty:
        # 廣度: 黃色線 + 黃色點
        l_b = base.mark_line(color='#ffc107').encode(
            y=alt.Y('Breadth', title=None, scale=alt.Scale(domain=[0,1], nice=False), axis=y_axis)
        )
        p_b = base.mark_circle(color='#ffc107', size=20).encode(
            y='Breadth', 
            tooltip=['DT', alt.Tooltip('Breadth', format='.1%')]
        )
        
        # 大盤: 藍色虛線 + 藍色點
        l_t = base.mark_line(color='#007bff', strokeDash=[4,4]).encode(
            y=alt.Y('T_S', scale=alt.Scale(domain=[0,1]))
        )
        p_t = base.mark_circle(color='#007bff', size=20).encode(
            y='T_S', 
            tooltip=['DT', alt.Tooltip('Taiex_Change', format='.2%')]
        )
        layers = [l_b, p_b, l_t, p_t, rule_r, rule_g]
    else:
        layers = [base, rule_r, rule_g]

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
    
    if len(days) > 1:
        date_prev = days[-2]
    else:
        date_prev = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    ranks_prev, _ = get_ranks_strict(ft, date_prev) 
    
    ranks_curr = ranks_prev 
    msg_src = f"名單:{date_prev}(昨日/盤中)"
    
    if now.time() >= time(14, 0) and d_cur == today_str:
        ranks_today, _ = get_ranks_strict(ft, today_str, min_count=1500)
        if ranks_today:
            ranks_curr = ranks_today
            msg_src = f"名單:{today_str}(今日完整)"
    
    all_targets = list(set(ranks_curr + ranks_prev))

    pmap = {}
    mis_debug_map = {} 
    
    data_source = "歷史"
    last_t = "無即時資料"
    api_status_code = 0 
    sj_usage_info = "無資料"
    
    is_post_market = (now.time() >= time(14, 0))
    
    if allow_live_fetch:
        # 1. Shioaji
        if sj_api:
            try:
                usage = sj_api.usage(); sj_usage_info = str(usage) if usage else "無法取得"
                contracts = []
                for c in all_targets: 
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
        
        # 2. MIS
        missing_codes = [c for c in all_targets if c not in pmap]
        if missing_codes:
            mis_data, debug_log = get_prices_twse_mis(missing_codes, info_map)
            mis_debug_map = debug_log 

            for c, val in mis_data.items():
                pmap[c] = val
            
            if len(mis_data) > 0 and data_source == "歷史":
                data_source = "證交所MIS"
                last_t = datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
                api_status_code = 2

    if is_post_market:
        if data_source == "歷史": 
             data_source = "FinMind盤後"
             last_t = "13:30:00"

    s_dt = (datetime.now()-timedelta(days=40)).strftime("%Y-%m-%d")
    
    # --- 計算今日廣度 ---
    h_c, v_c = 0, 0
    dtls = []
    
    for c in ranks_curr:
        df = get_hist(ft, c, s_dt) 
        m_type = info_map.get(c, "未知")
        m_display = {"twse":"上市", "tpex":"上櫃", "emerging":"興櫃"}.get(m_type, "未知")
        
        info = pmap.get(c, {})
        curr_p = info.get('z', info.get('price', 0)) 
        real_y = info.get('y', info.get('y_close', 0)) 
        
        p_price = 0
        if real_y > 0: 
            p_price = real_y
        elif not df.empty:
            if df.iloc[-1]['date'] == today_str and len(df) >= 2:
                 p_price = float(df.iloc[-2]['close'])
            else:
                 p_price = float(df.iloc[-1]['close']) 

        p_ma5 = 0
        p_stt = "-"
        
        if not df.empty and p_price > 0:
            closes = []
            if df.iloc[-1]['date'] == today_str:
                closes = df['close'].iloc[:-1].tail(5).tolist() 
            else:
                closes = df['close'].tail(5).tolist()
            if len(closes) >= 5:
                p_ma5 = sum(closes[-5:]) / 5
            if p_price > p_ma5: p_stt="✅"
            else: p_stt="📉"

        c_ma5 = 0
        c_stt = "-"
        note = ""
        
        if curr_p == 0: 
            c_stt = "⚠️無報價"
            reason = ""
            if not allow_live_fetch: 
                reason = "非交易時間"
            else:
                if c in mis_debug_map:
                    reason = mis_debug_map[c] 
                elif c not in pmap:
                    reason = "MIS未回傳"
            
            if reason: note = f"⚠️{reason} | 昨收{p_price}"
            else: note = f"昨收{p_price}"
        
        source_note = info.get('note', '')
        if source_note: note = f"📝{source_note} " + note

        if curr_p > 0 and p_price > 0 and not df.empty:
            hist_closes = []
            if df.iloc[-1]['date'] == today_str:
                 hist_closes = df['close'].iloc[:-1].tail(4).tolist()
            else:
                 hist_closes = df['close'].tail(4).tolist()
                 
            if len(hist_closes) >= 4:
                ma5_input = hist_closes 
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

    # --- 計算昨日廣度 ---
    h_p, v_p = 0, 0
    for c in ranks_prev:
        df = get_hist(ft, c, s_dt)
        if df.empty: continue
        
        has_today = (df.iloc[-1]['date'] == today_str)
        prev_close = 0
        prev_ma5 = 0
        
        if has_today:
            if len(df) >= 2: prev_close = float(df.iloc[-2]['close'])
            if len(df) >= 6:
                prev_ma5 = df['close'].iloc[-6:-1].mean()
        else:
            prev_close = float(df.iloc[-1]['close'])
            if len(df) >= 5:
                prev_ma5 = df['close'].iloc[-5:].mean()
        
        if prev_close > 0 and prev_ma5 > 0:
            if prev_close > prev_ma5: h_p += 1
            v_p += 1

    br_c = h_c/v_c if v_c>0 else 0
    br_p = h_p/v_p if v_p>0 else 0
    
    t_cur, t_pre, slope = 0, 0, 0
    try:
        tw = get_hist(ft, "TAIEX", s_dt)
        if not tw.empty:
            mis_tw, _ = get_prices_twse_mis(["t00"], {"t00":"twse"}) 
            t_curr = mis_tw.get("t00", {}).get("z", 0)
            
            if tw.iloc[-1]['date'] == today_str:
                 t_pre = float(tw.iloc[-2]['close'])
                 if t_curr == 0: t_curr = float(tw.iloc[-1]['close'])
            else:
                 t_pre = float(tw.iloc[-1]['close'])

            if t_curr > 0: t_cur = t_curr
            else: t_cur = t_pre

            hist_tw = tw['close'].tail(6).tolist()
            if len(hist_tw) >= 6:
                closes_for_prev = hist_tw[-6:-1]
                ma5_prev = sum(closes_for_prev) / 5
                
                closes_for_curr = hist_tw[-5:]
                if t_cur > 0:
                    closes_for_curr[-1] = t_cur
                
                ma5_curr = sum(closes_for_curr) / 5
                slope = ma5_curr - ma5_prev
            
    except: pass
    
    if t_cur == t_pre: t_chg = 0
    else: t_chg = (t_cur-t_pre)/t_pre if t_pre>0 else 0
    
    rec_t = last_t if is_intra and "無" not in str(last_t) else ("13:30:00" if is_post_market else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S"))
    
    save_rec(d_cur, rec_t, br_c, t_chg, t_cur, t_pre, is_intra, v_c)
    
    # 籌碼面處理 (Sponsor) - 帶回診斷日誌
    chips_data, chips_diag = get_chips_data(ft, d_cur)
    chip_strategy = get_chip_strategy(slope, chips_data)
    
    return {
        "d":d_cur, "d_prev": date_prev, 
        "br":br_c, "br_p":br_p, "h":h_c, "v":v_c, "h_p":h_p, "v_p":v_p,
        "df":pd.DataFrame(dtls), 
        "t":last_t, "tc":t_chg, "slope":slope, "src_type": data_source,
        "raw":{'Date':d_cur,'Time':rec_t,'Breadth':br_c}, "src":msg_src,
        "api_status": api_status_code, "sj_err": sj_err, "sj_usage": sj_usage_info,
        "chip_strat": chip_strategy,
        "chip_diag": chips_diag 
    }

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
            if status_code == 2: st.sidebar.success("🟢 連線正常")
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

            if open_br is not None and n_state['intraday_trend'] is None:
                if br >= (open_br + 0.05):
                    n_state['intraday_trend'] = 'up'
                    if tg_tok and tg_id: send_tg(tg_tok, tg_id, f"🔒 <b>【趨勢鎖定】</b>\n廣度先達開盤+5% (目前{br:.1%})，今日確認偏多！")
                elif br <= (open_br - 0.05):
                     n_state['intraday_trend'] = 'down'
                     if tg_tok and tg_id: send_tg(tg_tok, tg_id, f"🔒 <b>【趨勢鎖定】</b>\n廣度先達開盤-5% (目前{br:.1%})，今日確認偏空！")

            if tg_tok and tg_id:
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
                
                if open_br is not None:
                    is_dev_high = (br >= open_br + OPEN_DEV_THR)
                    is_dev_low = (br <= open_br - OPEN_DEV_THR)
                
                    if is_dev_high and not n_state['was_dev_high']:
                        n_state['was_dev_high'] = True
                    
                    if is_dev_low and not n_state['was_dev_low']:
                        n_state['was_dev_low'] = True
                
                    if br <= (today_max - 0.05):
                        if not n_state['notified_drop_high']:
                            should_notify = False
                            if data['slope'] > 0 and n_state['intraday_trend'] == 'up': should_notify = True
                            if data['slope'] < 0 and n_state['intraday_trend'] == 'up': should_notify = True
                
                            if should_notify:
                                msg = f"📉 <b>【高點回落】</b>\n今日高點: {today_max:.1%}\n目前廣度: {br:.1%}\n已回檔 5%"
                                send_tg(tg_tok, tg_id, msg)
                                
                            n_state['notified_drop_high'] = True
                    else:
                        n_state['notified_drop_high'] = False
                    
                    if br >= (today_min + 0.05):
                        if not n_state['notified_rise_low']:
                            should_notify = False
                            if data['slope'] < 0 and n_state['intraday_trend'] == 'down': should_notify = True
                            if data['slope'] > 0 and n_state['intraday_trend'] == 'down': should_notify = True
                            
                            if should_notify:
                                msg = f"🚀 <b>【低點反彈】</b>\n今日低點: {today_min:.1%}\n目前廣度: {br:.1%}\n已反彈 5%"
                                send_tg(tg_tok, tg_id, msg)

                            n_state['notified_rise_low'] = True
                    else:
                        n_state['notified_rise_low'] = False
            
                save_notify_state(n_state)
            
            # 將診斷日誌傳入 UI
            display_strategy_panel(data['slope'], open_br, br, n_state, data['chip_strat'], data['chip_diag'])

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
        else: st.sidebar.warning("⏸ 休市")

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
        try:
            import streamlit.cli as stcli
        except:
            pass

    if 'streamlit' in sys.modules and any('streamlit' in arg for arg in sys.argv):
        run_app()
    else:
        print("正在啟動 Streamlit 介面 (維持率欄位修正+圖表防呆版)...")
        try:
            subprocess.call(["streamlit", "run", __file__])
        except Exception as e:
            print(f"啟動失敗: {e}")
            print("請確認已安裝 streamlit (pip install streamlit) 和 curl_cffi (pip install curl_cffi)")
        
        input("\n程式執行結束 (或發生錯誤)，請按 Enter 鍵離開...")

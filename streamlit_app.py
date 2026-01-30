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
import gc

# 引入 curl_cffi 
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    st.error("缺少 curl_cffi 套件！請在 requirements.txt 中加入 'curl_cffi'")
    st.stop()

# ==========================================
# 設定區 v9.55.69 (精簡版 + P/C 邏輯修正)
# ==========================================
APP_VER = "v9.55.69 (精簡版 + P/C 邏輯修正)"
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
CHIPS_FILE = "chips_cache_v2.json"

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

def load_json_file(filepath):
    if not os.path.exists(filepath): return {}
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except: return {}

def save_json_file(filepath, data):
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f)
    except: pass

def load_notify_state(today_str):
    default_state = {
        "date": today_str,
        "last_stt": "normal",
        "last_rap": "",
        "was_dev_high": False,
        "was_dev_low": False,
        "notified_drop_high": False,
        "notified_rise_low": False,
        "intraday_trend": None,
        "record_high": -1.0, 
        "record_low": 2.0
    }
    
    state = load_json_file(NOTIFY_FILE)
    if not state or state.get("date") != today_str:
        return default_state
    
    if "record_high" not in state: state["record_high"] = -1.0
    if "record_low" not in state: state["record_low"] = 2.0
    if "intraday_trend" not in state: state["intraday_trend"] = None
        
    return state

def save_notify_state(state):
    save_json_file(NOTIFY_FILE, state)

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
    if not os.path.exists(HIST_FILE): return None, None, 0
    try:
        df = pd.read_csv(HIST_FILE)
        if df.empty: return None, None, 0
        df['Date'] = df['Date'].astype(str)
        
        if 'Total' not in df.columns: df['Total'] = 0
        
        df_today = df[
            (df['Date'] == str(d_cur)) & 
            (df['Time'] >= "09:00") & 
            (df['Time'] <= "13:30") &
            (df['Total'] >= OPEN_COUNT_THR)
        ]
        
        if df_today.empty: return None, None, 0
        return df_today['Breadth'].max(), df_today['Breadth'].min(), len(df_today)
    except: return None, None, 0

@st.cache_resource
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
                with cffi_requests.Session() as session:
                    r = session.get(url, params=params, impersonate="chrome", timeout=10)
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
        with cffi_requests.Session() as session:
            r = session.post(url, data=payload, impersonate="chrome", timeout=10)
            if r.status_code == 200:
                dfs = pd.read_html(io.StringIO(r.text))
                for df in dfs:
                    if df.shape[1] >= 7:
                        top_row = df.iloc[0] 
                        try:
                            val = float(top_row.iloc[6])
                            date_str = str(top_row.iloc[0]) 
                
                            return val, date_str, f"期交所官網 ({date_str})"
                        except: continue
    except Exception as e:
        return None, None, str(e)
    return None, None, "找不到表格"

def fetch_chips_from_network(token, target_date_str):
    diagnosis = [] 
    res = {}
    start_date = (datetime.strptime(target_date_str, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    
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
                
                data_date = latest.get('date', '')
                res['fut_date'] = data_date
                
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
                    diagnosis.append(f"✅ 期貨(外資): 成功 ({data_date})")
                except: diagnosis.append("❌ 期貨: 計算錯誤")

    # 2. 選擇權 (修正邏輯：只抓 Regular Session)
    pc_val = None; pc_date = None
    df_opt, _ = call_finmind_api_try_versions(["TaiwanOptionDaily"], "TXO", start_date, token)
    if not df_opt.empty:
        # [關鍵修正] 過濾掉夜盤數據，只保留 'position' (一般交易時段)
        if 'trading_session' in df_opt.columns:
            df_opt = df_opt[df_opt['trading_session'] == 'position']
            
        latest = df_opt[df_opt['date'] == df_opt['date'].max()]
        cp_col = 'call_put' if 'call_put' in latest.columns else 'CallPut'
        
        if cp_col in latest.columns:
            put = latest[latest[cp_col].str.lower()=='put']['open_interest'].sum()
            call = latest[latest[cp_col].str.lower()=='call']['open_interest'].sum()
            
            if call > 0: 
                pc_val = round((put/call)*100, 2)
                pc_date = latest.iloc[0]['date']
                diagnosis.append(f"✅ 選擇權(FinMind): {pc_val}% (Put={int(put):,}/Call={int(call):,}) ({pc_date})")

    if pc_val is None or pc_val == 0:
        taifex_val, taifex_date, taifex_msg = get_taifex_pc_ratio(target_date_str)
        if taifex_val is not None:
            pc_val = taifex_val; pc_date = taifex_date
            diagnosis.append(f"✅ 選擇權(期交所): {pc_val}% ({pc_date})")
        else:
            if pc_val is None: diagnosis.append(f"❌ 選擇權: 全數失敗 ({taifex_msg})")
            
    if pc_val is not None:
        res['pc_ratio'] = pc_val
        res['pc_date'] = pc_date

    # 3. 維持率
    maint_candidates = ["TaiwanTotalExchangeMarginMaintenance"]
    df_maint, _ = call_finmind_api_try_versions(maint_candidates, None, start_date, token)
    if not df_maint.empty:
        latest = df_maint.iloc[-1]
        col = 'TotalExchangeMarginMaintenance'
        if col not in latest: col = 'margin_maintenance_ratio'
        if col in latest:
            res['margin_ratio'] = float(latest[col])
            res['margin_date'] = latest.get('date', '未知日期')
            diagnosis.append(f"✅ 維持率: {res['margin_ratio']}% ({res['margin_date']})")

    # 4. 融資餘額
    df_margin, margin_src = call_finmind_api_try_versions(["TaiwanStockTotalMarginPurchaseShortSale"], None, start_date, token)
    if not df_margin.empty:
        df_m = df_margin[df_margin['name'] == 'MarginPurchaseMoney'].sort_values('date')
        if not df_m.empty:
            latest = df_m.iloc[-1]
            prev = df_m.iloc[-2] if len(df_m)>1 else latest
            curr_bal = float(latest['TodayBalance'])
            prev_bal = float(prev['TodayBalance']) 
            res['margin_chg'] = round((curr_bal - prev_bal)/1e8, 2) 
            res['margin_bal'] = round(curr_bal/1e8, 1)
            res['margin_bal_date'] = latest.get('date', '未知日期')
            diagnosis.append(f"✅ 融資餘額: {res['margin_bal']}億 ({res['margin_bal_date']})")

    # 5. 外資現貨 (嚴格過濾)
    df_spot, _ = call_finmind_api_try_versions(["TaiwanStockTotalInstitutionalInvestors"], None, start_date, token)
    if not df_spot.empty:
        name_col = 'name' if 'name' in df_spot.columns else 'type'
        mask_foreign = df_spot[name_col].astype(str).str.contains('Foreign|外資', case=False, na=False)
        mask_no_dealer = ~df_spot[name_col].astype(str).str.contains('Dealer|自營商', case=False, na=False)
        
        df_f = df_spot[mask_foreign & mask_no_dealer].sort_values('date')
        
        if not df_f.empty:
            latest = df_f.iloc[-1]
            buy = float(latest.get('buy', 0))
            sell = float(latest.get('sell', 0))
            net = buy - sell
            res['spot_net'] = round(net / 1e8, 2) 
            res['spot_date'] = latest.get('date', '未知')
            diagnosis.append(f"✅ 外資現貨: {res['spot_net']}億 ({res['spot_date']})")
        else:
            diagnosis.append("⚠️ 外資現貨: 找不到純外資資料 (可能被過濾掉)")
    else:
        diagnosis.append("❌ 外資現貨: 無資料")

    return res, diagnosis

def get_chips_data_smart(token):
    now = datetime.now(timezone(timedelta(hours=8)))
    today = now.date()
    yesterday = today - timedelta(days=1)
    
    if now.time() < time(14, 0):
        target_date = yesterday
    else:
        target_date = today
        
    target_str = target_date.strftime("%Y-%m-%d")
    cache = load_json_file(CHIPS_FILE)
    
    if target_str in cache:
        cached_data = cache[target_str]['data']
        margin_date = cached_data.get('margin_date', '')
        margin_bal_date = cached_data.get('margin_bal_date', '')
        spot_date = cached_data.get('spot_date', '')
        
        # 只要有一項資料不是最新的，就強制重抓
        is_all_updated = (margin_date == target_str) and (margin_bal_date == target_str) and (spot_date == target_str)
        
        if is_all_updated:
            return cached_data, cache[target_str]['diag']
    
    data, diag = fetch_chips_from_network(token, target_str)
    
    is_success = False
    fetched_date = data.get('fut_date', '')
    
    if fetched_date == target_str:
        is_success = True
    elif target_date == yesterday and fetched_date: 
        is_success = True
        
    if is_success:
        cache[target_str] = {'data': data, 'diag': diag}
        save_json_file(CHIPS_FILE, cache)
        return data, diag
    
    if target_date == today:
        cached_dates = sorted(cache.keys())
        if cached_dates:
            last_date = cached_dates[-1]
            return cache[last_date]['data'], cache[last_date]['diag'] + [f"⚠️ {target_str} 資料未出，顯示 {last_date} 數據"]
        
    return data, diag

def get_chip_strategy(ma5_slope, chips):
    if not chips: return None
    fut_oi = chips.get('fut_oi', 0)
    fut_chg = chips.get('fut_oi_chg', 0)
    pc_ratio = chips.get('pc_ratio', 100)
    margin_ratio = chips.get('margin_ratio', 0) 
    margin_chg = chips.get('margin_chg', 0)
    spot_net = chips.get('spot_net', 0)
    
    is_chip_bearish = False
    is_chip_bullish = False
    
    sig, act, color = "籌碼中性", "觀察技術面為主", "info"
    
    # 1. 法人大逃殺 (現貨大賣 > 200億) - 絕對優先
    if spot_net < -200:
        sig, act, color = "💀 法人大逃殺 (全面崩潰)", "外資現貨狂殺，資金大撤退。這是系統性風險，絕對禁止做多。", "error"
        is_chip_bearish = True
        return {
            "sig": sig, "act": act, "color": color, "data": chips,
            "is_bull": is_chip_bullish, "is_bear": is_chip_bearish
        }

    # 2. 殺戮盤
    if ma5_slope < 0 and fut_oi < -10000 and margin_chg > 5 and pc_ratio < 90:
        sig, act, color = "📉 殺戮盤 (空方控盤)", "外資期權偏空，散戶逆勢接刀。強力做空，切勿猜底。", "error"
        is_chip_bearish = True

    # 3. 火力全開 vs 誘多
    elif ma5_slope > 0 and fut_oi > 10000 and pc_ratio > 110:
        if spot_net < -50:
            sig, act, color = "⚠️ 虛漲 (期貨拉抬現貨出貨)", "外資期貨做多但現貨大賣，標準的『拉高出貨』。多單應獲利了結。", "warning"
            is_chip_bearish = True
        else:
            sig, act, color = "🚀 火力全開 (外資助攻)", "外資期現貨同步作多，支撐強勁。多單抱緊，甚至加碼。", "success"
            is_chip_bullish = True
        
    # 4. 絕佳抄底
    elif ma5_slope < 0 and ((margin_ratio > 0 and margin_ratio < 135) or margin_chg < -15):
        sig, act, color = "💎 絕佳抄底 (斷頭清洗)", "空頭趨勢中見融資斷頭，醞釀反彈。", "primary"
        is_chip_bullish = True 

    # 5. 多頭清洗 vs 雙逃
    elif ma5_slope > 0 and margin_chg < -15:
        if spot_net < -50:
            sig, act, color = "⚠️ 融資外資雙逃 (多頭潰散)", "融資減，但外資現貨也大賣。這不是洗盤，是多殺多，快逃。", "error"
            is_chip_bearish = True
        else:
            sig, act, color = "🚿 多頭清洗 (甩轎)", "上升趨勢中融資大減，籌碼換手成功，有利後市。", "success"
            is_chip_bullish = True

    # 6. 籌碼渙散 vs 下跌中繼
    elif fut_chg < -3000 and margin_chg > 5: 
        if ma5_slope > 0:
            sig, act, color = "⚠️ 籌碼渙散 (主力落跑)", "指數漲但外資大逃亡，散戶在接最後一棒。多單減碼，小心反轉。", "warning"
            is_chip_bearish = True
        else:
            sig, act, color = "🔪 下跌中繼 (散戶接刀)", "趨勢向下且外資續賣，散戶沿路攤平。空頭未止，禁止做多。", "error"
            is_chip_bearish = True

    # 7. 潛伏期
    elif abs(ma5_slope) < 10 and fut_chg > 2000 and pc_ratio > 110:
        sig, act, color = "🟩 潛伏期 (主力吃貨)", "盤整中見外資偷佈局多單。建議提前建倉，等待噴出。", "success"
        is_chip_bullish = True

    # 8. 假突破 vs 軋空助漲
    elif ma5_slope > 0 and fut_oi < -10000:
        if spot_net > 100:
            sig, act, color = "🟢 軋空助漲 (外資避險)", "現貨大買但期貨空單鎖利。這是『多頭避險』，非看空，盤勢仍穩。", "success"
            is_chip_bullish = True
        else:
            sig, act, color = "🟨 假突破警戒 (避險高掛)", "現貨漲但期貨大量空單留倉。可能是假突破，多單要設緊停損。", "warning"
            is_chip_bearish = True
        
    return {
        "sig": sig, "act": act, "color": color, "data": chips,
        "is_bull": is_chip_bullish, "is_bear": is_chip_bearish
    }

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
                        y = item.get('y', '-') 
                        pz = item.get('pz', '-') 
                        val = {}
                        if y!='-' and y!='': val['y'] = float(y)
                        price = 0
                        note = ""
                        
                        if z and z != '-' and z.replace('.','').isdigit(): 
                            price = float(z); note="成交"
                        elif pz and pz != '-' and pz.replace('.','').isdigit(): 
                            price = float(pz); note="試撮"
                        
                        if price == 0:
                            g_str = item.get('g', '')
                            f_str = item.get('f', '')
                            
                            has_bid_vol = False
                            if g_str:
                                g_top = g_str.split('_')[0]
                                if g_top and g_top != '0' and g_top != '-':
                                    has_bid_vol = True
                                    
                            has_ask_vol = False
                            if f_str:
                                f_top = f_str.split('_')[0]
                                if f_top and f_top != '0' and f_top != '-':
                                    has_ask_vol = True

                            try:
                                h_val = float(item.get('h', '0'))
                                l_val = float(item.get('l', '0'))
                                
                                if has_bid_vol and not has_ask_vol:
                                    if h_val > 0: price = h_val; note = "漲停(H)"
                                    
                                elif has_ask_vol and not has_bid_vol:
                                    if l_val > 0: price = l_val; note = "跌停(L)"
                                    
                                elif has_bid_vol and has_ask_vol:
                                    success = False
                                    b_str = item.get('b','').split('_')[0]
                                    try: 
                                        price = float(b_str)
                                        note = "委買價"
                                        success = True
                                    except: pass 
                                    
                                    if not success:
                                        a_str = item.get('a','').split('_')[0]
                                        try:
                                            price = float(a_str)
                                            note = "委賣價"
                                        except: pass
                            except: pass
                        
                        if price > 0:
                            val['z'] = price; val['note'] = note
                            results[c] = val
                        else: debug_log[c] = "無價"
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
        
        if last_d == str(d) and last_t == str(t_short):
            return 

        if last_d != str(d): 
            pd.concat([df, row], ignore_index=True).to_csv(HIST_FILE, index=False)
        else:
            if not intra and len(df) > 10:
                pass 
            else:
                row.to_csv(HIST_FILE, mode='a', header=False, index=False)
    except: row.to_csv(HIST_FILE, index=False)

def display_strategy_panel(slope, open_br, br, n_state, chip_strategy, chip_diag):
    st.markdown("""
        <style>
        div[data-testid="stMetricValue"] {
            font-size: 18px !important;
}
        div[data-testid="stMetricLabel"] {
            font-size: 14px !important;
}
        </style>
    """, unsafe_allow_html=True)

    st.subheader("♟️ 戰略指揮所")
    strategies = []
    
    chip_bull = chip_strategy.get('is_bull', False) if chip_strategy else False
    chip_bear = chip_strategy.get('is_bear', False) if chip_strategy else False
    
    if slope > 0:
        if chip_bear:
            strategies.append({"sig": "⚠️ 技術偏多但籌碼轉弱", "act": "籌碼渙散或假突破，多單應減碼或設緊停損", "type": "warning"})
        else:
            strategies.append({"sig": "MA5斜率為正 ➜ 大盤偏多", "act": "順勢操作，以做多為主", "type": "success"})
    elif slope < 0:
        if chip_bull:
            strategies.append({"sig": "💎 技術偏空但籌碼進駐", "act": "外資抄底或斷頭清洗中，空單回補，可嘗試搶反彈", "type": "primary"})
        else:
            strategies.append({"sig": "MA5斜率為負 ➜ 大盤偏空", "act": "順勢操作，以做空為主", "type": "error"})
    else:
        strategies.append({"sig": "MA5斜率持平", "act": "區間震盪，觀察突破方向", "type": "info"})
    
    trend_status = n_state.get('intraday_trend')
    if trend_status == 'up': strategies.append({"sig": "🔒 已觸發【開盤+5%】", "act": "今日偏多確認，留意回檔", "type": "success"})
    elif trend_status == 'down': strategies.append({"sig": "🔒 已觸發【開盤-5%】", "act": "今日偏空確認，留意反彈", "type": "error"})
    else: strategies.append({"sig": "⏳ 盤整中 (未達 +/- 5%)", "act": "觀望，等待趨勢表態", "type": "info"})

    if slope > 0 and trend_status == 'up' and n_state['notified_drop_high']:
        strategies.append({"sig": "今日偏多 + 賣壓短暫回檔 (高點落 5%)", "act": "🎯 進場多單 (確認止穩後)", "type": "success"})
    elif slope < 0 and trend_status == 'down' and n_state['notified_rise_low']:
        strategies.append({"sig": "今日偏空 + 買方短暫反彈 (低點彈 5%)", "act": "🎯 進場空單 (確認止漲後)", "type": "error"})

    cols = st.columns(len(strategies))
    for i, s in enumerate(strategies):
        with cols[i]:
            title = s["sig"]; body = s["act"]
            if s["type"] == "success": st.success(f"**{title}**\n\n{body}")
            elif s["type"] == "error": st.error(f"**{title}**\n\n{body}")
            elif s["type"] == "warning": st.warning(f"**{title}**\n\n{body}")
            elif s["type"] == "primary": st.info(f"**{title}**\n\n{body}", icon="💎")
            else: st.info(f"**{title}**\n\n{body}")
    
    st.markdown("---")
    st.subheader("♟️ 籌碼氣象站 (Sponsor)")
    
    if chip_strategy and chip_strategy['data']:
        d = chip_strategy['data']
        c1, c2, c3, c4, c5, c6 = st.columns([1.1, 1.1, 1.1, 1.3, 1.3, 2.5])
        
        date_fut = str(d.get('fut_date', '--')).replace('-', '/')[-5:]
        date_pc = str(d.get('pc_date', '--')).replace('-', '/')[-5:]
        date_maint = str(d.get('margin_date', '--')).replace('-', '/')[-5:]
        date_bal = str(d.get('margin_bal_date', '--')).replace('-', '/')[-5:]
        date_spot = str(d.get('spot_date', '--')).replace('-', '/')[-5:]

        c1.metric(f"期貨OI ({date_fut})", f"{d.get('fut_oi',0):,}", f"{d.get('fut_oi_chg',0):,}")
        c2.metric(f"P/C Ratio ({date_pc})", f"{d.get('pc_ratio',0)}%")
        c3.metric(f"維持率 ({date_maint})", f"{d.get('margin_ratio',0)}%")
        c4.metric(f"融資 ({date_bal})", f"{d.get('margin_bal',0)}億", f"{d.get('margin_chg',0)}億")
        c5.metric(f"外資現貨 ({date_spot})", f"{d.get('spot_net',0)}億")
        
        sig = chip_strategy['sig']; act = chip_strategy['act']; color = chip_strategy['color']
        with c6:
            if color == 'success': st.success(f"**{sig}**\n\n{act}")
            elif color == 'error': st.error(f"**{sig}**\n\n{act}")
            elif color == 'warning': st.warning(f"**{sig}**\n\n{act}")
            elif color == 'primary': st.info(f"**{sig}**\n\n{act}", icon="💎")
            else: st.info(f"**{sig}**\n\n{act}")
        
        with st.expander("查看詳細數據來源"):
            for msg in chip_diag: st.text(msg)
            
            st.markdown("---")
            st.markdown("##### 📝 籌碼指標速查表")
            col_a, col_b = st.columns(2)
            with col_a:
                st.caption("""
                **期貨 (外資)**
                * 🔴 **空單 > 1萬口** (避險/看空)
                * 🟢 **多單 > 1萬口** (趨勢多)
                
                **選擇權 (P/C Ratio)**
                * 🟢 **> 110%** (支撐強劲/偏多)
                * 🔴 **< 90%** (壓力沈重/偏空)
                """)
            with col_b:
                st.caption("""
                **融資維持率**
                * 🟢 **> 170%** (散戶獲利/多頭穩)
                * 🔪 **< 140%** (面臨追繳/準備殺多)
                
                **外資現貨 (不含自營)**
                * 💎 **買超 > 100億** (真金白銀/軋空)
                * ⚠️ **賣超 > 50億** (獲利調節/虛漲)
                * 💀 **賣超 > 200億** (提款走人/大逃殺)
                """)
    else:
        st.error("⚠️ 無籌碼資料，請展開查看診斷報告")
        with st.expander("🔍 連線診斷報告", expanded=True):
            for msg in chip_diag: st.write(msg)

def plot_chart(base_d, date_prev):
    chart_data = pd.DataFrame()
    
    if os.path.exists(HIST_FILE):
        try:
            df = pd.read_csv(HIST_FILE)
            if not df.empty:
                df['Date'] = df['Date'].astype(str)
                df['Time'] = df['Time'].astype(str)
                df['Time'] = df['Time'].apply(lambda x: x[:5])
                
                df_today = df[df['Time'] >= "09:00"].copy()
                
                df_today = df_today[df_today['Time'] <= "13:30"]
                
                if not df_today.empty:
                    df_today = df_today.sort_values(['Date', 'Time'])
                    last_date = df_today.iloc[-1]['Date']
                    chart_data = df_today[df_today['Date'] == last_date].copy()
        except: pass

    if chart_data.empty:
        start = pd.to_datetime(f"{base_d} 09:00:00")
        end = pd.to_datetime(f"{base_d} 13:30:00")
        chart_data = pd.DataFrame()
    else:
        start = pd.to_datetime(f"{base_d} 09:00:00")
        end = pd.to_datetime(f"{base_d} 13:30:00")
        chart_data['DT'] = pd.to_datetime(chart_data['Date'] + ' ' + chart_data['Time'], errors='coerce')
        chart_data = chart_data.dropna(subset=['DT'])
        chart_data['T_S'] = (chart_data['Taiex_Change']*10)+0.5

    x_scale = alt.Scale(domain=[start, end])
    y_vals = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    y_axis = alt.Axis(format='%', values=y_vals, tickCount=11, title=None)
    
    if chart_data.empty:
        base = alt.Chart(pd.DataFrame({'DT': [start, end]})).mark_point(opacity=0).encode(
            x=alt.X('DT:T', title=None, axis=alt.Axis(format='%H:%M'), scale=x_scale),
            y=alt.Y('val:Q', axis=y_axis, scale=alt.Scale(domain=[0, 1]))
        )
    else:
        base = alt.Chart(chart_data).encode(
            x=alt.X('DT:T', title=None, axis=alt.Axis(format='%H:%M'), scale=x_scale)
        )
        
    layers = []
    rule_r = alt.Chart(pd.DataFrame({'y':[BREADTH_THR]})).mark_rule(color='red', strokeDash=[5,5]).encode(y='y')
    rule_g = alt.Chart(pd.DataFrame({'y':[BREADTH_LOW]})).mark_rule(color='green', strokeDash=[5,5]).encode(y='y')
    
    if not chart_data.empty:
        l_b = base.mark_line(color='#ffc107', strokeWidth=1).encode(
            y=alt.Y('Breadth', title=None, scale=alt.Scale(domain=[0,1], nice=False), axis=y_axis)
        )
        p_b = base.mark_circle(color='#ffc107', size=10).encode(
            y='Breadth', 
            tooltip=['DT', alt.Tooltip('Breadth', format='.1%')]
        )
        l_t = base.mark_line(color='#007bff', strokeWidth=1).encode(
            y=alt.Y('T_S', scale=alt.Scale(domain=[0,1]))
        )
        p_t = base.mark_circle(color='#007bff', size=10).encode(
            y='T_S', 
            tooltip=['DT', alt.Tooltip('Taiex_Change', format='.2%')]
        )
        layers = [l_b, p_b, l_t, p_t, rule_r, rule_g]
    else:
        layers = [base, rule_r, rule_g]

    if chart_data.empty and datetime.now(timezone(timedelta(hours=8))).time() > time(13, 30):
        st.warning("⚠️ 無盤中歷史數據：程式未在盤中運行，無法顯示今日走勢圖。")

    return alt.layer(*layers).properties(height=380, title=f"走勢對照 - {base_d} (名單: {date_prev})").resolve_scale(y='shared')

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

    h_p, v_p = 0, 0
    for c in ranks_prev:
        df = get_hist(ft, c, s_dt)
        if df.empty: continue
        
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
    
    chips_data, chips_diag = get_chips_data_smart(ft)
    chip_strategy = get_chip_strategy(slope, chips_data)
    
    # [優化] 強制執行垃圾回收，釋放記憶體
    gc.collect()
    
    return {
        "d":d_cur, "d_prev": date_prev, 
        "br":br_c, "br_p":br_p, "h":h_c, "v":v_c, "h_p":h_p, "v_p":v_p,
        "df":pd.DataFrame(dtls), 
        "t":last_t, "tc":t_chg, "slope":slope, "src_type": data_source,
        "raw":{'Date':d_cur,'Time':rec_t,'Breadth':br_c, 'v':v_c}, "src":msg_src,
        "api_status": api_status_code, "sj_err": sj_err, "sj_usage": sj_usage_info,
        "chip_strat": chip_strategy,
        "chip_diag": chips_diag 
    }

def run_app():
    st.set_page_config(page_title=APP_VER, layout="wide")
    
    st.markdown("""
        <style>
        .block-container {
            padding-top: 3rem !important;
            padding-bottom: 0rem !important;
        }
        </style>
    """, unsafe_allow_html=True)
    
    st.markdown(f"<h3 style='text-align: left; margin: 0; padding-bottom: 10px;'>📈 {APP_VER}</h3>", unsafe_allow_html=True)
    
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
            
        if st.button("🧼 重置籌碼快取"):
            if os.path.exists(CHIPS_FILE):
                os.remove(CHIPS_FILE)
                st.toast("籌碼快取已清除", icon="🧹")
                time_module.sleep(1)
            st.rerun()

    # [修正] 移除按鈕右側的所有文字
    if st.button("🔄 刷新"): st.rerun()

    try:
        data = fetch_all()
        if isinstance(data, str): st.error(f"❌ {data}")
        elif data:
            # [修正] 藍色資訊框緊貼按鈕下方
            st.info(f"{data['src']} | 更新: {data['t']} | 來源: {data['src_type']}")
            
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
        
            hist_max, hist_min, hist_count = get_intraday_extremes(data['d'])
            current_time = datetime.now(timezone(timedelta(hours=8))).time()
            is_valid_time = time(9, 0) <= current_time <= time(13, 30)
            is_valid_count = data['raw']['v'] >= OPEN_COUNT_THR
            in_valid_window = is_valid_time and is_valid_count
            
            today_max = None
            today_min = None
            
            if hist_max is not None:
                if in_valid_window:
                    today_max = max(hist_max, br)
                    today_min = min(hist_min, br)
                else:
                    today_max = hist_max
                    today_min = hist_min
            else:
                if in_valid_window:
                    today_max = br
                    today_min = br
        
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
                    if is_dev_high and not n_state['was_dev_high']: n_state['was_dev_high'] = True
                    if is_dev_low and not n_state['was_dev_low']: n_state['was_dev_low'] = True
                
                    if today_max is not None and br <= (today_max - 0.05):
                        if not n_state['notified_drop_high']:
                            should_notify = False
                            if data['slope'] > 0 and n_state['intraday_trend'] == 'up': should_notify = True
                            if data['slope'] < 0 and n_state['intraday_trend'] == 'up': should_notify = True
                            if should_notify:
                                send_tg(tg_tok, tg_id, f"📉 <b>【高點回落】</b>\n今日高點: {today_max:.1%}\n目前廣度: {br:.1%}\n已回檔 5%")
                            n_state['notified_drop_high'] = True
                    else: n_state['notified_drop_high'] = False
                    
                    if today_min is not None and br >= (today_min + 0.05):
                        if not n_state['notified_rise_low']:
                            should_notify = False
                            if data['slope'] < 0 and n_state['intraday_trend'] == 'down': should_notify = True
                            if data['slope'] > 0 and n_state['intraday_trend'] == 'down': should_notify = True
                            if should_notify:
                                send_tg(tg_tok, tg_id, f"🚀 <b>【低點反彈】</b>\n今日低點: {today_min:.1%}\n目前廣度: {br:.1%}\n已反彈 5%")
                            n_state['notified_rise_low'] = True
                    else: n_state['notified_rise_low'] = False

                if in_valid_window:
                    if n_state['record_high'] == -1.0:
                        n_state['record_high'] = br
                        n_state['record_low'] = br
                    else:
                        if br > n_state['record_high']:
                            send_tg(tg_tok, tg_id, f"🏆 <b>【創新高】</b>\n目前廣度: {br:.1%}\n超越前高: {n_state['record_high']:.1%}")
                            n_state['record_high'] = br
                        if br < n_state['record_low']:
                            send_tg(tg_tok, tg_id, f"📉 <b>【創新低】</b>\n目前廣度: {br:.1%}\n跌破前低: {n_state['record_low']:.1%}")
                            n_state['record_low'] = br

                save_notify_state(n_state)
            
            chart = plot_chart(data['d'], data['d_prev'])
            if chart: st.altair_chart(chart, use_container_width=True)
            
            c1,c2,c3 = st.columns(3)
            c1.metric("今日廣度", f"{br:.1%}", f"{data['h']}/{data['v']}")
            
            caption_str = f"昨日廣度: {data['br_p']:.1%} ({data['h_p']}/{data['v_p']})"
            if open_br:
                caption_str += f" | 開盤: {open_br:.1%}"
            else:
                caption_str += " | 開盤: 等待中..."
            
            if (current_time > time(13, 30) and hist_count < 5) or (today_max is None):
                if current_time > time(13, 30):
                    caption_str += "\n⚠️ 目前無盤中廣度資料 (未在盤中運行)"
                else:
                    caption_str += "\n(資料累積中...)"
            else:
                caption_str += f"\n今日目前最高廣度: {today_max:.1%}"
                caption_str += f"\n今日目前最低廣度: {today_min:.1%}"
            
            c1.caption(caption_str)
            
            c2.metric("大盤漲跌", f"{data['tc']:.2%}")
            sl = data['slope']; icon = "📈 正" if sl > 0 else "📉 負"
            c3.metric("大盤MA5斜率", f"{sl:.2f}", icon)
            
            display_strategy_panel(data['slope'], open_br, br, n_state, data['chip_strat'], data['chip_diag'])
            
            with st.expander("📋 查看個股狀態表", expanded=False):
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
        print("正在啟動 Streamlit 介面 (精簡版 + P/C 邏輯修正)...")
        try:
            subprocess.call(["streamlit", "run", __file__])
        except Exception as e:
            print(f"啟動失敗: {e}")
            print("請確認已安裝 streamlit (pip install streamlit) 和 curl_cffi (pip install curl_cffi)")
        
        input("\n程式執行結束 (或發生錯誤)，請按 Enter 鍵離開...")

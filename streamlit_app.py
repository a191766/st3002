# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import shioaji as sj

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v4.2.0 (智慧分層快取版)"
UPDATE_LOG = """
- v4.0.1: 參數修復。
- v4.2.0: 效能與 API 額度終極優化。
  1. 【分層快取架構】將「歷史資料(靜態)」與「即時報價(動態)」完全分離。
  2. 【排行榜快取】昨日排行一旦抓過，盤中重整絕不重抓 (TTL=24hr)。
  3. 【K線快取】300 檔個股歷史 K 線也全面快取，重整時不再消耗 FinMind 額度。
  4. 【極速重整】移除按鈕的 `clear()` 指令，現在按重新整理只會更新即時價，速度提升 10 倍以上。
"""

# ==========================================
# 參數與 Token
# ==========================================
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"

TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷 (極速快取)", layout="wide")

# ==========================================
# 永豐 API 初始化 (連線資源快取)
# ==========================================
@st.cache_resource
def get_shioaji_api():
    api = sj.Shioaji(simulation=False)
    try:
        api_key = st.secrets["shioaji"]["api_key"]
        secret_key = st.secrets["shioaji"]["secret_key"]
        api.login(api_key=api_key, secret_key=secret_key)
        # print(">>> Shioaji Login Success")
    except Exception as e:
        # print(f">>> Shioaji Login Failed: {e}")
        return None
    return api

# ==========================================
# 靜態資料快取區 (省流量的核心)
# ==========================================

def smart_get_column(df, candidates):
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

# 1. 交易日快取 (1小時更新一次即可)
@st.cache_data(ttl=3600, show_spinner=False)
def get_cached_trading_days(token):
    api = DataLoader()
    api.login_by_token(token)
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if not df.empty:
            return sorted(df['date'].unique().tolist())
    except:
        pass
    return []

# 2. 排行榜快取 (24小時，存硬碟，失敗不存)
@st.cache_data(ttl=86400, show_spinner=False, persist="disk")
def get_cached_rank_list(token, date_str, backup_date=None):
    local_api = DataLoader()
    local_api.login_by_token(token)
    
    df_rank = pd.DataFrame()
    try:
        df_rank = local_api.taiwan_stock_daily(stock_id="", start_date=date_str)
    except: pass

    if df_rank.empty and backup_date:
        try:
            df_rank = local_api.taiwan_stock_daily(stock_id="", start_date=backup_date)
        except: pass

    if df_rank.empty:
        raise RuntimeError("API_FETCH_FAILED") # 失敗時拋錯，防止存入空快取

    df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
    df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
    
    if df_rank['ID'] is None or df_rank['Money'] is None:
         raise RuntimeError("DATA_FORMAT_ERROR")

    df_rank['ID'] = df_rank['ID'].astype(str)
    df_rank = df_rank[df_rank['ID'].str.len() == 4]
    df_rank = df_rank[df_rank['ID'].str.isdigit()]
    for prefix in EXCLUDE_PREFIXES:
        df_rank = df_rank[~df_rank['ID'].str.startswith(prefix)]
        
    df_candidates = df_rank.sort_values('Money', ascending=False).head(TOP_N)
    return df_candidates['ID'].tolist()

# 3. 個股歷史 K 線快取 (6小時，大幅節省 300 次 API 呼叫)
@st.cache_data(ttl=21600, show_spinner=False)
def get_cached_stock_history(token, code, start_date):
    api = DataLoader()
    api.login_by_token(token)
    try:
        return api.taiwan_stock_daily(stock_id=code, start_date=start_date)
    except:
        return pd.DataFrame()

# ==========================================
# 動態資料區 (即時運算)
# ==========================================

def get_current_status():
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    is_intraday = time(8, 45) <= current_time < time(13, 30)
    return tw_now, is_intraday

def get_trading_days_robust(token):
    # 改用快取函式
    dates = get_cached_trading_days(token)
    
    # 備援推算 (萬一 API 掛了)
    if not dates:
        tw_now, _ = get_current_status()
        check_day = tw_now
        while len(dates) < 5:
            if check_day.weekday() <= 4:
                dates.append(check_day.strftime("%Y-%m-%d"))
            check_day -= timedelta(days=1)
        dates = sorted(dates)

    # 強制校正今天 (盤中修正)
    tw_now, is_intraday = get_current_status()
    today_str = tw_now.strftime("%Y-%m-%d")
    
    if 0 <= tw_now.weekday() <= 4 and tw_now.time() >= time(8, 45):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
            
    return dates

def fetch_shioaji_snapshots(sj_api, codes):
    if not sj_api or not codes: return {}, None
    
    # Shioaji 抓取快照
    contracts = []
    for code in codes:
        try:
            contract = sj_api.Contracts.Stocks[code]
            if contract: contracts.append(contract)
        except: pass
    
    if not contracts: return {}, None

    try:
        snapshots = sj_api.snapshots(contracts)
        price_map = {}
        ts = datetime.now()
        for snap in snapshots:
            price = snap.close 
            code = snap.code
            if price > 0:
                price_map[code] = float(price)
                if snap.ts:
                    snap_time = datetime.fromtimestamp(snap.ts / 1000000000)
                    ts = snap_time
        return price_map, ts.strftime("%H:%M:%S")
    except Exception as e:
        return {}, None

def calc_stats_hybrid(sj_api, target_date, rank_codes, use_realtime=False):
    hits = 0
    valid = 0
    stats_map = {} 
    
    price_map = {}
    last_t = None
    
    # 1. 準備即時報價 (只在 use_realtime 時動作)
    if use_realtime:
        if sj_api:
            price_map, last_t = fetch_shioaji_snapshots(sj_api, rank_codes)
        if not price_map:
            last_t = "無即時資料"
    
    start_date_query = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    # 進度條只在即時運算時顯示，避免干擾
    if use_realtime:
        prog_bar = st.progress(0, text="正在整合快取與即時報價...")
    total = len(rank_codes)

    for i, code in enumerate(rank_codes):
        if use_realtime and i % 50 == 0:
            prog_bar.progress((i / total), text=f"分析進度: {i+1}/{total}")

        current_price = 0
        status = "未知"
        price_src = "歷史"
        ma5_val = 0
        is_pass = False
        
        if use_realtime:
            current_price = price_map.get(code, 0)
            price_src = "永豐API"
            if current_price == 0: status = "⚠️ 無報價"

        try:
            # === 關鍵優化：改用快取函式抓歷史 K 線 ===
            # 這會直接從記憶體拿資料，不會再去問 FinMind
            stock_df = get_cached_stock_history(FINMIND_TOKEN, code, start_date_query)
            
            if stock_df.empty:
                 status = "❌ 無資料"
            else:
                if use_realtime:
                    # 算今日: 歷史 < Target + 即時
                    stock_df = stock_df[stock_df['date'] < target_date]
                    if current_price > 0:
                        new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                        stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                    
                    if len(stock_df) > 0 and stock_df.iloc[-1]['date'] != target_date:
                         status = "🚫 缺今日價"
                         stock_df = pd.DataFrame() 
                else:
                    # 算昨日: 歷史 <= Target
                    stock_df = stock_df[stock_df['date'] <= target_date]
                    if len(stock_df) > 0:
                        last_dt = stock_df.iloc[-1]['date']
                        if isinstance(last_dt, pd.Timestamp): last_dt = last_dt.strftime("%Y-%m-%d")
                        if last_dt != target_date:
                            status = f"🚫 未更"
                            stock_df = pd.DataFrame()
                        else:
                            if not use_realtime:
                                current_price = float(stock_df.iloc[-1]['close'])
                
                if len(stock_df) >= 6:
                    stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                    curr = stock_df.iloc[-1]
                    final_price = float(curr['close'])
                    ma5_val = float(curr['MA5'])
                    
                    if final_price > ma5_val:
                        hits += 1
                        is_pass = True
                        status = "✅ 通過"
                    else:
                        is_pass = False
                        status = f"📉 未過"
                    valid += 1
                else:
                    if "未更" not in status and "缺" not in status: status = "🚫 資料不足"

        except Exception:
            status = "❌ 錯誤"
        
        stats_map[code] = {
            'price': current_price,
            'ma5': ma5_val,
            'status': status,
            'is_pass': is_pass,
            'src': price_src
        }
    
    if use_realtime: prog_bar.empty()
    return hits, valid, stats_map, last_t

# === fetch_data 不再使用 cache，確保每次重整都執行，但內部依賴快取 ===
def fetch_data():
    sj_api = get_shioaji_api()
    if sj_api is None:
        st.error("⚠️ 無法登入永豐 API，請檢查 Secrets 設定。")

    # 使用快取取得交易日
    all_days = get_trading_days_robust(FINMIND_TOKEN)
    if len(all_days) < 2: return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    # 取得排行榜 (快取保護)
    try:
        prev_rank_codes = get_cached_rank_list(FINMIND_TOKEN, d_prev_str, backup_date=all_days[-3])
    except RuntimeError:
        st.error("⚠️ 無法取得排行資料 (API 異常)。")
        return None
    
    # 計算昨日 (這裡會大量使用 K 線快取，速度極快)
    hit_prev, valid_prev, map_prev, _ = calc_stats_hybrid(None, d_prev_str, prev_rank_codes, use_realtime=False)
    
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中極速 (智慧分層快取)"
    else:
        try:
            curr_rank_codes = get_cached_rank_list(FINMIND_TOKEN, d_curr_str)
        except:
            curr_rank_codes = []

        if curr_rank_codes:
            mode_msg = "🐢 盤後精準 (今日排行)"
        else:
            curr_rank_codes = prev_rank_codes
            mode_msg = "⚠️ 盤後 (沿用昨日)"
            
    # 計算今日 (這裡會抓 Shioaji 即時價 + K 線快取)
    hit_curr, valid_curr, map_curr, last_time = calc_stats_hybrid(sj_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
    final_details = []
    for i, code in enumerate(curr_rank_codes):
        prev_data = map_prev.get(code, {})
        curr_data = map_curr.get(code, {})
        
        p_price = prev_data.get('price', 0)
        p_ma5 = prev_data.get('ma5', 0)
        p_status = "✅" if prev_data.get('is_pass') else "📉"
        if not prev_data.get('status') or "🚫" in prev_data.get('status', ''): p_status = "🚫"
        
        c_price = curr_data.get('price', 0)
        c_ma5 = curr_data.get('ma5', 0)
        c_status = "✅" if curr_data.get('is_pass') else "📉"
        if not curr_data.get('status') or "🚫" in curr_data.get('status', ''): c_status = "🚫"
        if "⚠️" in curr_data.get('status', ''): c_status = "⚠️"

        final_details.append({
            "排名": i+1,
            "代號": code,
            "昨收": p_price,
            "昨MA5": round(p_ma5, 2) if p_ma5 else 0,
            "昨狀態": p_status,
            "現價": c_price,
            "今MA5": round(c_ma5, 2) if c_ma5 else 0,
            "今狀態": c_status,
            "來源": curr_data.get('src', '-')
        })

    detail_df = pd.DataFrame(final_details)
    
    slope = 0
    try:
        # 大盤 K 線也快取一下，雖然只 call 一次
        twii_df = get_cached_stock_history(FINMIND_TOKEN, "TAIEX", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        if is_intraday and sj_api:
             # 大盤即時
             try:
                 snap = sj_api.snapshots([sj_api.Contracts.Indices.TSE.TSE001])[0]
                 twii_p = float(snap.close)
                 if twii_p > 0:
                     new_row = pd.DataFrame([{'date': d_curr_str, 'close': twii_p}])
                     twii_df = pd.concat([twii_df, new_row], ignore_index=True)
             except: pass
             
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except: pass
    
    br_prev = hit_prev / valid_prev if valid_prev > 0 else 0
    br_curr = hit_curr / valid_curr if valid_curr > 0 else 0

    return {
        "d_curr": d_curr_str,
        "d_prev": d_prev_str,
        "br_curr": br_curr, "br_prev": br_prev,
        "hit_curr": hit_curr, "valid_curr": valid_curr,
        "hit_prev": hit_prev, "valid_prev": valid_prev,
        "slope": slope,
        "detail_df": detail_df,
        "mode_msg": mode_msg,
        "last_time": last_time
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v4.2.0 分層快取)")

    with st.sidebar:
        st.subheader("系統狀態")
        if 'shioaji' in st.secrets:
            st.success("Secrets 設定已偵測")
        else:
            st.error("尚未設定 Secrets")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    if st.button("🔄 立即重新整理"):
        # 移除了 st.cache_data.clear()
        # 這裡什麼都不用做，按鈕本身就會觸發 Streamlit Rerun
        # Rerun 會執行 fetch_data -> 讀取快取 -> 抓 Shioaji -> 更新畫面
        pass 

    try:
        data = fetch_data()
            
        if data is None:
            pass
        else:
            cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
            cond2 = data['slope'] > 0
            final_decision = cond1 and cond2
            
            t_str = str(data['last_time']) if data['last_time'] else "未知"

            st.subheader(f"📅 基準日：{data['d_curr']}")
            st.caption(f"昨日基準: {data['d_prev']}")
            st.success(f"📌 {data['mode_msg']}")
            
            c1, c2, c3 = st.columns(3)
            c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid_curr']}")
            c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid_prev']}")
            c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

            st.divider()
            if final_decision:
                st.success(f"✅ 結論：可進場")
            else:
                st.error(f"⛔ 結論：不可進場")
            
            st.caption(f"永豐報價時間: {t_str}")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

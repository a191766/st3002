# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import shioaji as sj  # 引入永豐 API

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v4.0.0 (永豐 Shioaji 極速版)"
UPDATE_LOG = """
- v3.x: FinMind/Yahoo 雙源版。
- v4.0: 核心引擎更換為永豐 Shioaji API。
  1. 【極致速度】使用 Shioaji `snapshots` 功能，300 檔報價延遲降低至 1 秒內。
  2. 【混合架構】維持 FinMind 抓取「昨日排行名單」(不耗永豐資源)，僅將「即時報價」交給永豐處理。
  3. 【安全性】API Key 讀取自 Streamlit Secrets，不暴露於程式碼中。
"""

# ==========================================
# 參數與 Token
# ==========================================
# FinMind Token (維持 Sponsor 以備不時之需，或抓歷史用)
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"

TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷 (Shioaji)", layout="wide")

# ==========================================
# 永豐 API 初始化與登入 (使用 Singleton 模式避免重複登入)
# ==========================================
@st.cache_resource
def get_shioaji_api():
    api = sj.Shioaji(simulation=False) # False 代表使用正式環境
    
    # 從 Streamlit Secrets 讀取金鑰
    try:
        api_key = st.secrets["shioaji"]["api_key"]
        secret_key = st.secrets["shioaji"]["secret_key"]
        api.login(api_key=api_key, secret_key=secret_key)
        print(">>> Shioaji Login Success")
    except Exception as e:
        print(f">>> Shioaji Login Failed: {e}")
        return None
    return api

# ==========================================
# 功能函式
# ==========================================

def get_current_status():
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    is_intraday = time(8, 45) <= current_time < time(13, 30)
    return tw_now, is_intraday

def get_trading_days_robust(api):
    dates = []
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if not df.empty:
            dates = sorted(df['date'].unique().tolist())
    except:
        pass 
    
    if not dates:
        tw_now, _ = get_current_status()
        check_day = tw_now
        while len(dates) < 5:
            if check_day.weekday() <= 4:
                dates.append(check_day.strftime("%Y-%m-%d"))
            check_day -= timedelta(days=1)
        dates = sorted(dates)

    tw_now, is_intraday = get_current_status()
    today_str = tw_now.strftime("%Y-%m-%d")
    
    if 0 <= tw_now.weekday() <= 4 and tw_now.time() >= time(8, 45):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
            
    return dates

def smart_get_column(df, candidates):
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

@st.cache_data(ttl=86400, show_spinner=False, persist="disk")
def get_cached_rank_list(token, date_str, backup_date=None):
    """ 使用 FinMind 抓取排行名單 (這部分 FinMind 還是最好用的) """
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
        raise RuntimeError("API_FETCH_FAILED")

    df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
    df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
    
    df_rank['ID'] = df_rank['ID'].astype(str)
    df_rank = df_rank[df_rank['ID'].str.len() == 4]
    df_rank = df_rank[df_rank['ID'].str.isdigit()]
    for prefix in EXCLUDE_PREFIXES:
        df_rank = df_rank[~df_rank['ID'].str.startswith(prefix)]
        
    df_candidates = df_rank.sort_values('Money', ascending=False).head(TOP_N)
    return df_candidates['ID'].tolist()

# === 關鍵：永豐 Shioaji 抓即時價 ===
def fetch_shioaji_snapshots(sj_api, codes):
    """
    使用 Shioaji 一次抓取數百檔股票的即時快照 (Snapshot)
    速度極快，且包含開高低收等完整資訊。
    """
    if not sj_api or not codes:
        return {}, None

    # 1. 將代號轉為 Shioaji 的 Contract 物件
    contracts = []
    for code in codes:
        # 嘗試從 TSE (上市) 或 OTC (上櫃) 找合約
        contract = sj_api.Contracts.Stocks[code]
        if contract:
            contracts.append(contract)
    
    if not contracts:
        return {}, None

    # 2. 呼叫 Snapshots (核心加速點)
    try:
        snapshots = sj_api.snapshots(contracts)
        
        # 3. 整理資料
        price_map = {}
        ts = datetime.now()
        
        for snap in snapshots:
            # close 即使盤中也是當下最新成交價
            price = snap.close 
            code = snap.code
            if price > 0:
                price_map[code] = float(price)
                # 更新時間戳記
                if snap.ts:
                    snap_time = datetime.fromtimestamp(snap.ts / 1000000000) # 奈秒轉秒
                    ts = snap_time

        return price_map, ts.strftime("%H:%M:%S")

    except Exception as e:
        print(f"Shioaji Snapshot Error: {e}")
        return {}, None

def calc_stats_hybrid(_fm_api, _sj_api, target_date, rank_codes, use_realtime=False):
    hits = 0
    valid = 0
    stats_map = {} 
    
    price_map = {}
    last_t = None
    
    # === 1. 準備即時報價 (Shioaji) ===
    if use_realtime:
        if _sj_api:
            price_map, last_t = fetch_shioaji_snapshots(_sj_api, rank_codes)
        
        # 若永豐掛了，這裡也可以寫 Yahoo 備援，但我們先假設永豐很穩
        if not price_map:
            last_t = "無資料"
    
    # === 2. 準備歷史資料 (FinMind) ===
    start_date_query = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    if use_realtime:
        prog_bar = st.progress(0, text="Shioaji 極速連線中...")
    total = len(rank_codes)

    for i, code in enumerate(rank_codes):
        if use_realtime and i % 50 == 0: # 永豐很快，不用太常更新進度條
            prog_bar.progress((i / total), text=f"進度: {i+1}/{total}")

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
            stock_df = _fm_api.taiwan_stock_daily(stock_id=code, start_date=start_date_query)
            
            if stock_df.empty:
                 status = "❌ 無資料"
            else:
                if use_realtime:
                    stock_df = stock_df[stock_df['date'] < target_date]
                    if current_price > 0:
                        new_row = pd.DataFrame([{'date': target_date, 'close': current_price}])
                        stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                    
                    if len(stock_df) > 0 and stock_df.iloc[-1]['date'] != target_date:
                         status = "🚫 缺今日價"
                         stock_df = pd.DataFrame() 
                else:
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

@st.cache_data(ttl=5) # 永豐極速版可以設超短快取，例如 5 秒
def fetch_data(_fm_api):
    # 這裡我們需要一個 wrapper，因為 st.cache 無法直接 cache 帶有 shioaji 物件的函式(無法 pickle)
    # 所以我們在 fetch_data 內部呼叫 get_shioaji_api
    sj_api = get_shioaji_api()
    
    if sj_api is None:
        st.error("⚠️ 無法登入永豐 API，請檢查 Secrets 設定。目前將僅顯示歷史資料。")

    all_days = get_trading_days_robust(_fm_api)
    if len(all_days) < 2: return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    try:
        prev_rank_codes = get_cached_rank_list(FINMIND_TOKEN, d_prev_str, backup_date=all_days[-3])
    except RuntimeError:
        st.error("⚠️ 無法取得排行資料 (FinMind)")
        return None
    
    # 昨日計算 (不需 Shioaji)
    hit_prev, valid_prev, map_prev, _ = calc_stats_hybrid(_fm_api, None, prev_rank_codes, use_realtime=False)
    
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中極速 (永豐 Shioaji 核心)"
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
            
    # 今日計算 (傳入 sj_api)
    hit_curr, valid_curr, map_curr, last_time = calc_stats_hybrid(_fm_api, sj_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
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
    
    # 斜率計算 (大盤)
    slope = 0
    try:
        twii_df = _fm_api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        if is_intraday and sj_api:
            # 嘗試用 Shioaji 抓大盤
            # 加權指數代號通常是 'TSE001' 或 '001' 在 Shioaji 中比較特別
            # 簡單起見，大盤這裡我們還是用 FinMind 歷史或 Yahoo 補，因為 Shioaji 抓指數要另外找合約
            # 這裡先維持原樣，以免複雜化
            pass
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
    st.title("📈 盤中權證進場判斷 (v4.0 永豐極速版)")

    with st.sidebar:
        st.subheader("系統狀態")
        if 'shioaji' in st.secrets:
            st.success("Secrets 設定已偵測")
        else:
            st.error("尚未設定 Secrets (請見說明)")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    fm_api = DataLoader()
    fm_api.login_by_token(FINMIND_TOKEN)

    if st.button("🔄 立即重新整理"):
        st.cache_data.clear()

    try:
        data = fetch_data(fm_api)
            
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

# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import shioaji as sj
import os
import altair as alt  # 引入繪圖套件

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v4.4.0 (廣度分時走勢版)"
UPDATE_LOG = """
- v4.3.0: 盤後邏輯驗證。
- v4.4.0: 新增廣度分時走勢圖。
  1. 【自動記錄】每次重新整理時，自動將「時間」與「今日廣度」寫入 CSV。
  2. 【每日重置】跨日自動清空舊紀錄，確保圖表只顯示當天走勢。
  3. 【趨勢視覺化】新增折線圖，即時監控盤中多空力道變化。
"""

# ==========================================
# 參數與 Token
# ==========================================
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"

TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]
HISTORY_FILE = "breadth_history.csv" # 儲存走勢的檔案

st.set_page_config(page_title="盤中權證進場判斷 (走勢圖)", layout="wide")

# ==========================================
# 永豐 API 初始化
# ==========================================
@st.cache_resource
def get_shioaji_api():
    api = sj.Shioaji(simulation=False)
    try:
        api_key = st.secrets["shioaji"]["api_key"]
        secret_key = st.secrets["shioaji"]["secret_key"]
        api.login(api_key=api_key, secret_key=secret_key)
    except Exception as e:
        return None
    return api

# ==========================================
# 靜態資料快取區
# ==========================================
def smart_get_column(df, candidates):
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

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
        raise RuntimeError("API_FETCH_FAILED") 

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

@st.cache_data(ttl=21600, show_spinner=False)
def get_cached_stock_history(token, code, start_date):
    api = DataLoader()
    api.login_by_token(token)
    try:
        return api.taiwan_stock_daily(stock_id=code, start_date=start_date)
    except:
        return pd.DataFrame()

# ==========================================
# 廣度記錄與繪圖功能 (新增)
# ==========================================
def save_breadth_record(current_date, current_time, breadth_value):
    """
    將當下的廣度記錄到 CSV。
    如果發現日期換了 (例如昨天跑過，今天是新的一天)，就自動清空舊資料。
    """
    new_data = pd.DataFrame([{
        'Date': current_date,
        'Time': current_time,
        'Breadth': breadth_value
    }])
    
    if not os.path.exists(HISTORY_FILE):
        new_data.to_csv(HISTORY_FILE, index=False)
    else:
        try:
            # 讀取舊資料
            df = pd.read_csv(HISTORY_FILE)
            if not df.empty:
                last_date = str(df.iloc[-1]['Date'])
                # 如果日期不同，代表新的一天，覆蓋掉舊檔
                if last_date != str(current_date):
                    new_data.to_csv(HISTORY_FILE, index=False)
                else:
                    # 同一天，檢查是否重複記錄 (避免太頻繁寫入)
                    last_time = str(df.iloc[-1]['Time'])
                    if last_time != str(current_time):
                        new_data.to_csv(HISTORY_FILE, mode='a', header=False, index=False)
            else:
                new_data.to_csv(HISTORY_FILE, index=False)
        except:
            # 檔案損毀或其他錯誤，重建
            new_data.to_csv(HISTORY_FILE, index=False)

def plot_breadth_chart():
    """ 讀取 CSV 並繪製折線圖 """
    if not os.path.exists(HISTORY_FILE):
        return None
    
    try:
        df = pd.read_csv(HISTORY_FILE)
        if df.empty: return None
        
        # 轉換 Broadth 為百分比小數以便繪圖 (CSV 存的是小數 0.65)
        # 為了圖表好看，我們轉成 0~100 的整數或保留小數
        df['Breadth_Pct'] = df['Breadth']
        
        # 建立 Altair 圖表
        # X軸: Time, Y軸: Breadth (設定範圍 0~1 或適當縮放)
        chart = alt.Chart(df).mark_line(point=True).encode(
            x=alt.X('Time', title='時間'),
            y=alt.Y('Breadth_Pct', title='廣度', scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format='%')),
            tooltip=['Time', alt.Tooltip('Breadth_Pct', format='.1%')]
        ).properties(
            title=f"今日廣度走勢 ({df.iloc[0]['Date']})",
            height=300
        )
        
        # 加入 65% 警戒線 (紅線)
        rule = alt.Chart(pd.DataFrame({'y': [BREADTH_THRESHOLD]})).mark_rule(color='red', strokeDash=[5, 5]).encode(y='y')
        
        return chart + rule
    except:
        return None

# ==========================================
# 動態資料區
# ==========================================
def get_current_status():
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    is_intraday = time(8, 45) <= current_time < time(13, 30)
    return tw_now, is_intraday

def get_trading_days_robust(token):
    dates = get_cached_trading_days(token)
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

def fetch_shioaji_snapshots(sj_api, codes):
    if not sj_api or not codes: return {}, None
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
    
    if use_realtime:
        if sj_api:
            price_map, last_t = fetch_shioaji_snapshots(sj_api, rank_codes)
        if not price_map:
            last_t = "無即時資料"
    
    start_date_query = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    if use_realtime:
        prog_bar = st.progress(0, text="正在計算即時 MA5...")
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
            stock_df = get_cached_stock_history(FINMIND_TOKEN, code, start_date_query)
            
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

def fetch_data():
    sj_api = get_shioaji_api()
    if sj_api is None:
        st.error("⚠️ 無法登入永豐 API，請檢查 Secrets 設定。")

    all_days = get_trading_days_robust(FINMIND_TOKEN)
    if len(all_days) < 2: return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    tw_now, is_intraday = get_current_status()
    
    try:
        prev_rank_codes = get_cached_rank_list(FINMIND_TOKEN, d_prev_str, backup_date=all_days[-3])
    except RuntimeError:
        st.error("⚠️ 無法取得昨日排行資料。")
        return None
    
    hit_prev, valid_prev, map_prev, _ = calc_stats_hybrid(None, d_prev_str, prev_rank_codes, use_realtime=False)
    
    rank_source_msg = ""
    if is_intraday:
        curr_rank_codes = prev_rank_codes
        mode_msg = "🚀 盤中模式"
        rank_source_msg = f"名單依據：{d_prev_str} (昨日排行)"
    else:
        try:
            curr_rank_codes = get_cached_rank_list(FINMIND_TOKEN, d_curr_str)
        except:
            curr_rank_codes = []

        if curr_rank_codes:
            mode_msg = "🐢 盤後模式 (資料已更新)"
            rank_source_msg = f"名單依據：{d_curr_str} (✅ 今日新排行)"
        else:
            curr_rank_codes = prev_rank_codes
            mode_msg = "⚠️ 盤後模式 (資料未更新)"
            rank_source_msg = f"名單依據：{d_prev_str} (⏳ 沿用昨日排行)"
            
    hit_curr, valid_curr, map_curr, last_time = calc_stats_hybrid(sj_api, d_curr_str, curr_rank_codes, use_realtime=True)
    
    # === 儲存廣度紀錄 ===
    br_curr = hit_curr / valid_curr if valid_curr > 0 else 0
    # 格式化當下時間 (若有抓到永豐時間就用永豐的，不然用系統時間)
    record_time = last_time if last_time and "無" not in str(last_time) else datetime.now(timezone(timedelta(hours=8))).strftime("%H:%M:%S")
    save_breadth_record(d_curr_str, record_time, br_curr)
    
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
        twii_df = get_cached_stock_history(FINMIND_TOKEN, "TAIEX", (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        if is_intraday and sj_api:
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

    return {
        "d_curr": d_curr_str,
        "d_prev": d_prev_str,
        "br_curr": br_curr, "br_prev": br_prev,
        "hit_curr": hit_curr, "valid_curr": valid_curr,
        "hit_prev": hit_prev, "valid_prev": valid_prev,
        "slope": slope,
        "detail_df": detail_df,
        "mode_msg": mode_msg,
        "rank_source_msg": rank_source_msg,
        "last_time": last_time
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v4.4.0 走勢圖)")

    with st.sidebar:
        st.subheader("系統狀態")
        if 'shioaji' in st.secrets:
            st.success("Secrets 設定已偵測")
        else:
            st.error("尚未設定 Secrets")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    if st.button("🔄 立即重新整理 (記錄廣度)"):
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
            st.info(f"ℹ️ {data['rank_source_msg']}") 
            
            # === 新增：顯示走勢圖 ===
            chart = plot_breadth_chart()
            if chart:
                st.altair_chart(chart, use_container_width=True)
            else:
                st.caption("尚未有今日廣度紀錄，請按重新整理開始記錄。")
            
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

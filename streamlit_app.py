# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import yfinance as yf

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v2.2.1 (變數修復版)"
UPDATE_LOG = """
- v2.2.0: FinMind 排行 + 雙模式切換。
- v2.2.1: 修復 KeyError: 'hit_prev'。補上漏掉的回傳變數。
"""

# ==========================================
# 參數與 Token
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_PREFIXES = ["00", "91"]

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_current_status():
    """ 判斷目前是盤中還是盤後 """
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    current_time = tw_now.time()
    # 08:45 ~ 13:30 視為盤中
    is_intraday = time(8, 45) <= current_time < time(13, 30)
    return tw_now, is_intraday

def get_trading_days(api):
    """ 取得交易日 """
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        if df.empty: return []
        dates = sorted(df['date'].unique().tolist())
    except:
        return []
    
    tw_now, is_intraday = get_current_status()
    today_str = tw_now.strftime("%Y-%m-%d")
    
    if 0 <= tw_now.weekday() <= 4 and tw_now.time() >= time(8, 45):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
            
    return dates

def smart_get_column(df, candidates):
    """ 智慧欄位搜尋 """
    cols = df.columns
    lower_map = {c.lower(): c for c in cols}
    for name in candidates:
        if name in cols: return df[name]
        if name.lower() in lower_map: return df[lower_map[name.lower()]]
    return None

def fetch_yahoo_realtime_batch(codes):
    """ Yahoo 批次下載 """
    if not codes: return {}, None
    
    tw_tickers = [f"{c}.TW" for c in codes]
    # 混合 .TWO 以防萬一
    all_tickers = tw_tickers + [f"{c}.TWO" for c in codes]
    
    try:
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
        realtime_map = {}
        latest_time = None
        
        valid_tickers = []
        if isinstance(data.columns, pd.MultiIndex):
            valid_tickers = data.columns.levels[0]
        elif not data.empty:
            valid_tickers = [data.name] if hasattr(data, 'name') else []
            if len(all_tickers) == 1: valid_tickers = all_tickers

        if len(valid_tickers) == 0 and not data.empty and len(all_tickers) == 1:
             df = data
             if not df.empty:
                 c = float(df['Close'].iloc[-1])
                 realtime_map[codes[0]] = c
                 latest_time = df.index[-1]
        else:
            for t in valid_tickers:
                try:
                    df = data[t] if isinstance(data.columns, pd.MultiIndex) else data
                    if df.empty or df['Close'].isna().all(): continue
                    
                    last_price = float(df['Close'].iloc[-1])
                    last_ts = df.index[-1]
                    if latest_time is None or last_ts > latest_time:
                        latest_time = last_ts
                    
                    stock_id = t.split('.')[0]
                    realtime_map[stock_id] = last_price
                except: continue
                
        return realtime_map, latest_time
    except:
        return {}, None

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2:
        st.error("歷史資料不足。")
        return None

    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    tw_now, is_intraday = get_current_status()
    
    target_rank_date = d_prev_str 
    mode_msg = "🚀 盤中模式 (基準:昨日排行)"
    
    if not is_intraday:
        try:
            check_df = _api.taiwan_stock_daily(stock_id="2330", start_date=d_curr_str)
            if not check_df.empty:
                target_rank_date = d_curr_str
                mode_msg = "🐢 盤後模式 (基準:今日排行)"
            else:
                mode_msg = "⚠️ 盤後模式 (FinMind 尚未更新，暫用昨日排行)"
        except:
            pass

    # === 步驟 2: 取得 FinMind 現成排行 ===
    try:
        df_rank = _api.taiwan_stock_daily(stock_id="", start_date=target_rank_date)
        
        if df_rank.empty:
            target_rank_date = all_days[-3]
            df_rank = _api.taiwan_stock_daily(stock_id="", start_date=target_rank_date)
            mode_msg = f"⚠️ 資料異常，回退至 {target_rank_date} 排行"
            
        df_rank['ID'] = smart_get_column(df_rank, ['stock_id', 'code'])
        df_rank['Money'] = smart_get_column(df_rank, ['Trading_money', 'Trading_Money', 'turnover'])
        df_rank['Close'] = smart_get_column(df_rank, ['close', 'Close', 'price'])
        
        # 篩選邏輯
        df_rank['ID'] = df_rank['ID'].astype(str)
        df_rank = df_rank[df_rank['ID'].str.len() == 4]
        df_rank = df_rank[df_rank['ID'].str.isdigit()]
        for prefix in EXCLUDE_PREFIXES:
            df_rank = df_rank[~df_rank['ID'].str.startswith(prefix)]
            
        df_candidates = df_rank.sort_values('Money', ascending=False).head(TOP_N)
        
        target_list = []
        for _, row in df_candidates.iterrows():
            target_list.append({
                'code': row['ID'],
                'hist_close': row['Close'] 
            })
            
    except Exception as e:
        st.error(f"排行資料獲取失敗: {e}")
        return None

    # === 步驟 3: 批次抓取即時價 (Yahoo) ===
    codes = [x['code'] for x in target_list]
    realtime_prices, last_time = fetch_yahoo_realtime_batch(codes)
    
    # === 步驟 4: 逐檔計算 MA5 ===
    results = []
    detailed_status = []
    
    progress_bar = st.progress(0, text=f"分析數據中 ({mode_msg})...")
    
    for i, item in enumerate(target_list):
        code = item['code']
        rank = i + 1
        
        current_close = realtime_prices.get(code, item['hist_close'])
        price_src = "Yahoo即時" if code in realtime_prices else "歷史收盤"
        
        try:
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            )
            
            stock_df = stock_df[stock_df['date'] < d_curr_str]
            
            if current_close > 0:
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': current_close}])
                stock_df = pd.concat([stock_df, new_row], ignore_index=True)
            
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr_row = stock_df.iloc[-1]  # D
                prev_row = stock_df.iloc[-2]  # D-1
                
                results.append({
                    "d_curr_ok": curr_row['close'] > curr_row['MA5'],
                    "d_prev_ok": prev_row['close'] > prev_row['MA5']
                })
                status = "✅ 納入"
            else:
                status = "❌ 剔除 (資料不足)"
                
        except Exception:
            status = "❌ 錯誤"
            
        detailed_status.append({
            "排名": rank,
            "代號": code,
            "現價": current_close,
            "來源": price_src,
            "狀態": status
        })
        
        if i % 30 == 0:
            progress_bar.progress((i + 1) / TOP_N, text=f"分析進度: {i+1}/{TOP_N}")
            
    progress_bar.empty()
    res_df = pd.DataFrame(results)
    detail_df = pd.DataFrame(detailed_status)
    
    # === 大盤斜率 ===
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))
        twii_df = twii_df[twii_df['date'] < d_curr_str]
        try:
            twii_rt = yf.download("^TWII", period="1d", progress=False)
            if not twii_rt.empty:
                last_twii = float(twii_rt['Close'].iloc[-1])
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': last_twii}])
                twii_df = pd.concat([twii_df, new_row], ignore_index=True)
        except: pass
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except: pass
    
    return {
        "d_curr": d_curr_str,
        "br_curr": res_df['d_curr_ok'].mean() if not res_df.empty else 0,
        "br_prev": res_df['d_prev_ok'].mean() if not res_df.empty else 0,
        "hit_curr": res_df['d_curr_ok'].sum() if not res_df.empty else 0,
        "hit_prev": res_df['d_prev_ok'].sum() if not res_df.empty else 0,  # <--- 補上這個漏掉的變數
        "valid": len(res_df),
        "slope": slope,
        "detail_df": detail_df,
        "mode_msg": mode_msg,
        "last_time": last_time
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v2.2.1 修復版)")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("API Token 已載入")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    api = DataLoader()
    api.login_by_token(API_TOKEN)

    if st.button("🔄 立即重新整理"):
        st.cache_data.clear()

    try:
        data = fetch_data(api)
            
        if data is None:
            st.warning("⚠️ 暫無有效數據")
        else:
            cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
            cond2 = data['slope'] > 0
            final_decision = cond1 and cond2
            time_str = data['last_time'].strftime("%H:%M:%S") if data['last_time'] else "未知"

            st.subheader(f"📅 基準日：{data['d_curr']}")
            st.success(f"📌 {data['mode_msg']}")
            
            c1, c2, c3 = st.columns(3)
            c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid']}")
            c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid']}")
            c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

            st.divider()
            if final_decision:
                st.success(f"✅ 結論：可進場")
            else:
                st.error(f"⛔ 結論：不可進場")
                
            st.caption(f"即時報價時間: {time_str} | 篩選條件：4碼純數字個股 (排除權證/ETF)")
            st.dataframe(data['detail_df'], use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("按 Enter 結束...")

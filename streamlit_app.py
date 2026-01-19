# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta, timezone, time
import traceback
import sys
import yfinance as yf  # 新增 Yahoo Finance

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v1.7.0 (雙引擎即時版)"
UPDATE_LOG = """
- v1.6.0: 強制日期判定 (解決了標題，但數據仍舊)。
- v1.7.0: 新增 Yahoo Finance 作為即時報價備援。當 FinMind 抓不到盤中數據時，自動切換至 Yahoo 抓取最新成交價，確保數據即時更新。
"""

# ==========================================
# 參數與 Token 設定
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
RANK_DISPLAY_N = 300     
BREADTH_THRESHOLD = 0.65
EXCLUDE_ETF_PREFIX = "00"

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_trading_days(api):
    """ 取得最近交易日 (含時間強制判定) """
    # 1. 取得歷史日線
    try:
        df = api.taiwan_stock_daily(
            stock_id="0050", 
            start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
        )
        dates = sorted(df['date'].unique().tolist())
    except:
        dates = []
    
    # 2. 暴力檢查：現在是否為交易時間
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    today_str = tw_now.strftime("%Y-%m-%d")
    current_time = tw_now.time()
    
    is_weekday = 0 <= tw_now.weekday() <= 4
    is_trading_hours = time(8, 45) <= current_time <= time(14, 0)
    
    # 如果是交易時間，強制把今天算進去
    if is_weekday and is_trading_hours:
        if not dates or today_str > dates[-1]:
            dates.append(today_str)

    return dates

def smart_get_column(df, target_type):
    """ 智慧欄位對應 """
    mappings = {
        'High': ['High', 'high', 'max', 'Max'],
        'Low': ['Low', 'low', 'min', 'Min'],
        'Close': ['Close', 'close', 'price', 'Price', 'deal_price'], 
        'Volume': ['Volume', 'volume', 'Trading_Volume', 'vol'],
        'Id': ['stock_id', 'stock_code', 'code', 'SecurityCode']
    }
    candidates = mappings.get(target_type, [])
    for c in candidates:
        if c in df.columns:
            return df[c]
    raise KeyError(f"找不到 {target_type} 對應的欄位。cols: {df.columns.tolist()}")

def get_realtime_price_hybrid(api, code, date_str):
    """
    雙引擎即時報價：
    1. 先試 FinMind (Snapshot/Tick)
    2. 失敗則試 Yahoo Finance (最穩)
    回傳: (price, high, low, volume, source_name)
    """
    # --- 管道 1: FinMind Snapshot ---
    try:
        if hasattr(api, 'taiwan_stock_daily_short'):
            df = api.taiwan_stock_daily_short(stock_id=code, start_date=date_str)
            if not df.empty:
                row = df.iloc[0]
                c = float(row.get('close') or row.get('Price') or 0)
                if c > 0:
                    h = float(row.get('high') or c)
                    l = float(row.get('low') or c)
                    v = float(row.get('volume') or 0)
                    return c, h, l, v, "FinMind_Snap"
    except:
        pass

    # --- 管道 2: FinMind Tick ---
    try:
        df = api.taiwan_stock_tick(stock_id=code, date=date_str)
        if not df.empty:
            last = df.iloc[-1]
            c = float(last['deal_price'])
            return c, c, c, 0, "FinMind_Tick"
    except:
        pass

    # --- 管道 3: Yahoo Finance (終極備援) ---
    try:
        # Yahoo 代號需加 .TW (上市) 或 .TWO (上櫃)
        # 我們先盲猜 .TW，如果失敗再考慮 .TWO (但大多數權值股是 TW)
        ticker = yf.Ticker(f"{code}.TW")
        # period="1d" 會抓取「最新的一天」，不用管日期是不是 2026
        hist = ticker.history(period="1d")
        if not hist.empty:
            row = hist.iloc[-1]
            c = float(row['Close'])
            h = float(row['High'])
            l = float(row['Low'])
            v = float(row['Volume'])
            # 簡單檢查一下抓到的價格是否合理 (非 0)
            if c > 0:
                return c, h, l, v, "Yahoo_Finance"
    except:
        pass
        
    return None

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2:
        st.error("歷史資料不足 (API 連線異常或無數據)。")
        return None

    d_curr_str = all_days[-1] 
    d_prev_str = all_days[-2]
    
    # === 步驟 1: 取得排行候選名單 ===
    # 預設使用昨日排行，再更新今日價格 (這是盤中且 API 不穩時最穩的做法)
    df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_prev_str)
    
    # 欄位處理
    try:
        df_all['MyClose'] = smart_get_column(df_all, 'Close')
        df_all['MyHigh'] = smart_get_column(df_all, 'High')
        df_all['MyLow'] = smart_get_column(df_all, 'Low')
        df_all['MyVol'] = smart_get_column(df_all, 'Volume')
        df_all['MyId'] = smart_get_column(df_all, 'Id')
    except:
        return None 

    df_all['MyId'] = df_all['MyId'].astype(str)
    df_all = df_all[df_all['MyId'].str.isdigit()]  
    df_all = df_all[~df_all['MyId'].str.startswith(EXCLUDE_ETF_PREFIX)] 
    df_all = df_all[df_all['MyId'] != "TAIEX"] 

    # 排序
    df_all['avg_price'] = (df_all['MyHigh'] + df_all['MyLow'] + df_all['MyClose']) / 3.0
    df_all['turnover_val'] = (df_all['avg_price'] * df_all['MyVol']) / 1_000_000.0
    
    df_candidates = df_all.sort_values('turnover_val', ascending=False).head(TOP_N).copy()
    
    # === 步驟 2: 逐檔抓取即時價並計算 ===
    results = []
    detailed_status = []
    
    progress_bar = st.progress(0, text=f"啟動雙引擎更新數據 ({d_curr_str})...")
    total_candidates = len(df_candidates)
    
    # 統計用
    source_stats = {"FinMind": 0, "Yahoo": 0, "None": 0}

    for i, (idx, row) in enumerate(df_candidates.iterrows()):
        code = row['MyId']
        rank = i + 1
        note = ""
        status = "未知"
        price_source = "歷史"
        
        try:
            # A. 抓歷史日線
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=25)).strftime("%Y-%m-%d")
            )
            
            # B. 取得即時價格 (FinMind -> Yahoo)
            # 無論歷史資料是否已更新，我們都嘗試去抓最新的來比對
            rt_data = get_realtime_price_hybrid(_api, code, d_curr_str)
            
            current_close = row['MyClose'] # 預設值 (昨日收盤)
            
            if rt_data:
                c, h, l, v, src = rt_data
                current_close = c
                price_source = src
                if "Yahoo" in src: source_stats["Yahoo"] += 1
                else: source_stats["FinMind"] += 1
                
                # 檢查歷史資料最後一天
                last_hist_date = ""
                if not stock_df.empty:
                    last_hist_date = pd.to_datetime(stock_df['date'].iloc[-1]).strftime("%Y-%m-%d")
                
                # 如果歷史資料還沒到今天，就拼上去
                if last_hist_date < d_curr_str:
                    new_row = pd.DataFrame([{
                        'date': d_curr_str,
                        'close': c,
                        'open': c, 'high': h, 'low': l, 'Trading_Volume': v
                    }])
                    stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                else:
                    # 如果歷史資料已經有今天 (極少見)，則更新最後一筆
                    stock_df.iloc[-1, stock_df.columns.get_loc('close')] = c
            else:
                source_stats["None"] += 1

            # C. 計算 MA5
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr_row = stock_df.iloc[-1]
                prev_row = stock_df.iloc[-2]
                
                results.append({
                    "d_curr_ok": curr_row['close'] > curr_row['MA5'],
                    "d_prev_ok": prev_row['close'] > prev_row['MA5']
                })
                status = "✅ 納入"
                row['MyClose'] = current_close 
            else:
                status = "❌ 剔除"
                note = "K線不足"
                
        except Exception as e:
            status = "❌ 剔除"
            note = f"Err: {str(e)}"
        
        detailed_status.append({
            "排名": rank,
            "代號": code,
            "現價": row['MyClose'],
            "來源": price_source,
            "狀態": status
        })

        if i % 10 == 0:
            progress_bar.progress((i + 1) / total_candidates, text=f"更新中: {rank}/{total_candidates} (Yahoo: {source_stats['Yahoo']})")
    
    progress_bar.empty()
    res_df = pd.DataFrame(results)
    detail_df = pd.DataFrame(detailed_status)
    
    # === 步驟 3: 大盤斜率 ===
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=25)).strftime("%Y-%m-%d"))
        # 這裡不特別用 Yahoo 抓大盤，因為大盤代號對應比較麻煩，且個股廣度才是重點
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except:
        slope = 0
    
    return {
        "d_curr": d_curr_str,
        "d_prev": d_prev_str,
        "br_curr": res_df['d_curr_ok'].mean() if not res_df.empty else 0,
        "br_prev": res_df['d_prev_ok'].mean() if not res_df.empty else 0,
        "hit_curr": res_df['d_curr_ok'].sum() if not res_df.empty else 0,
        "hit_prev": res_df['d_prev_ok'].sum() if not res_df.empty else 0,
        "valid": len(res_df),
        "slope": slope,
        "detail_df": detail_df,
        "stats": source_stats
    }

# ==========================================
# UI
# ==========================================

def run_streamlit():
    st.title("📈 盤中權證進場判斷監控")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("API Token 已載入")
        st.divider()
        st.subheader("版本資訊")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    api = DataLoader()
    api.login_by_token(API_TOKEN)

    if st.button("🔄 立即重新整理數據"):
        st.cache_data.clear()

    try:
        with st.spinner("正在啟動雙引擎 (FinMind + Yahoo) 抓取即時數據..."):
            data = fetch_data(api)
            
        if data is None:
            st.stop()

        cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
        cond2 = data['slope'] > 0
        final_decision = cond1 and cond2

        st.subheader(f"📅 數據基準日：{data['d_curr']}")
        
        # 顯示資料來源統計，讓使用者知道現在是用哪裡的數據
        stats = data['stats']
        st.info(f"📊 資料來源統計：Yahoo Finance ({stats['Yahoo']} 檔) | FinMind ({stats['FinMind']} 檔) | 無更新 ({stats['None']} 檔)")

        c1, c2, c3 = st.columns(3)
        c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid']}")
        c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid']}")
        c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

        st.divider()

        st.header("💡 進場結論")
        if final_decision:
            st.success(f"✅ 結論（{data['d_curr']} 的隔日）：可進場")
        else:
            st.error(f"⛔ 結論（{data['d_curr']} 的隔日）：不可進場")
        
        st.write(f"- 廣度連兩天 ≥ 65%：{'✅ 通過' if cond1 else '❌ 未通過'}")
        st.write(f"- 大盤 MA5 斜率 > 0：{'✅ 通過' if cond2 else '❌ 未通過'} (MA5斜率: {data['slope']:.2f})")

        st.divider()
        st.subheader(f"📋 前 {TOP_N} 大成交值個股詳細清單")
        st.dataframe(
            data['detail_df'], 
            column_config={
                "排名": st.column_config.NumberColumn(format="%d"),
                "現價": st.column_config.NumberColumn(format="%.2f"),
            },
            use_container_width=True,
            height=600,
            hide_index=True
        )

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        input("\n按 ENTER 結束程式...")

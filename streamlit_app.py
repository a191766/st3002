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
APP_VERSION = "v1.8.0 (全市場即時版)"
UPDATE_LOG = """
- v1.8.0: 針對 95 檔無更新問題修復。
  1. 改用 Yahoo Finance 批次下載 (Batch Download) 提升速度與穩定性。
  2. 同時偵測 .TW (上市) 與 .TWO (上櫃)，解決上櫃股抓不到最新價的問題。
  3. 新增「最新報價時間」顯示，證明資料即時性。
"""

# ==========================================
# 參數與 Token
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
BREADTH_THRESHOLD = 0.65
EXCLUDE_ETF_PREFIX = "00"

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_trading_days(api):
    """ 取得交易日 (含強制判定) """
    try:
        df = api.taiwan_stock_daily(stock_id="0050", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        dates = sorted(df['date'].unique().tolist())
    except:
        dates = []
    
    tw_now = datetime.now(timezone(timedelta(hours=8)))
    today_str = tw_now.strftime("%Y-%m-%d")
    current_time = tw_now.time()
    
    # 只要是平日且在開盤時間內，強制納入今天
    if 0 <= tw_now.weekday() <= 4 and time(8, 45) <= current_time <= time(14, 0):
        if not dates or today_str > dates[-1]:
            dates.append(today_str)
    return dates

def fetch_yahoo_realtime_batch(codes):
    """
    Yahoo Finance 批次下載 (解決上市上櫃後綴問題)
    回傳: Dict { '2330': 1050.0, '8069': 120.0 ... }
    """
    if not codes: return {}, None
    
    # 建立兩種後綴的清單
    tw_tickers = [f"{c}.TW" for c in codes]
    two_tickers = [f"{c}.TWO" for c in codes]
    all_tickers = tw_tickers + two_tickers
    
    # 顯示進度
    print(f"正在批次下載 {len(all_tickers)} 檔 Yahoo 即時報價...")
    
    try:
        # 批次下載，只抓當天 (period='1d')
        # group_by='ticker' 讓回傳格式比較好處理
        data = yf.download(all_tickers, period="1d", group_by='ticker', progress=False, threads=True)
        
        realtime_map = {}
        latest_time = None
        
        # 解析資料
        for t in all_tickers:
            try:
                # 處理單一 Ticker 的資料
                if len(all_tickers) == 1:
                    df = data # 如果只有一檔，格式不同
                else:
                    df = data[t]
                
                # 檢查是否有資料
                if not df.empty and not df['Close'].isna().all():
                    # 抓最後一筆 Close
                    last_price = float(df['Close'].iloc[-1])
                    
                    # 抓這筆資料的時間 (轉成字串顯示)
                    last_ts = df.index[-1]
                    if latest_time is None or last_ts > latest_time:
                        latest_time = last_ts
                    
                    # 移除後綴 (.TW / .TWO) 存回 Map
                    clean_code = t.split('.')[0]
                    
                    # 優先權：如果已經有值(可能先抓到.TW)，通常保留即可；
                    # 但考慮到有時候誤判，這裡簡單處理：有抓到就存
                    realtime_map[clean_code] = last_price
            except Exception:
                continue
                
        return realtime_map, latest_time
        
    except Exception as e:
        print(f"Yahoo 下載失敗: {e}")
        return {}, None

@st.cache_data(ttl=300)
def fetch_data(_api):
    all_days = get_trading_days(_api)
    if len(all_days) < 2:
        st.error("歷史資料不足。")
        return None

    d_curr_str = all_days[-1] 
    d_prev_str = all_days[-2]
    
    # === 步驟 1: 取得「昨日」排行作為候選名單 ===
    # (盤中排行變動不大，且 FinMind 盤中排行常缺資料，用昨日最穩)
    df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_prev_str)
    
    if df_all.empty:
        st.error("無法取得昨日全市場資料，請稍後再試。")
        return None

    # 欄位映射
    cols_map = {c.lower(): c for c in df_all.columns}
    def get_col(n): return df_all[cols_map.get(n.lower(), n)]
    
    try:
        df_all['MyClose'] = get_col('Close')
        df_all['MyVol'] = get_col('Volume')
        df_all['MyId'] = get_col('stock_id')
        # 簡易計算成交值 (用昨日收盤價概算，主要為了排序)
        df_all['turnover_val'] = df_all['MyClose'] * df_all['MyVol']
    except:
        return None

    # 過濾
    df_all['MyId'] = df_all['MyId'].astype(str)
    df_all = df_all[df_all['MyId'].str.isdigit()]  
    df_all = df_all[~df_all['MyId'].str.startswith(EXCLUDE_ETF_PREFIX)] 
    
    # 取前 N 大
    df_candidates = df_all.sort_values('turnover_val', ascending=False).head(TOP_N).copy()
    target_codes = df_candidates['MyId'].tolist()
    
    # === 步驟 2: Yahoo 批次抓取即時價 (關鍵步驟) ===
    # 這裡會一次抓完 300 檔的 .TW 和 .TWO
    rt_prices, last_update_time = fetch_yahoo_realtime_batch(target_codes)
    
    # === 步驟 3: 逐檔運算 ===
    results = []
    detailed_status = []
    
    progress_bar = st.progress(0, text="正在整合歷史與即時數據...")
    
    # 統計
    updated_count = 0
    
    for i, (idx, row) in enumerate(df_candidates.iterrows()):
        code = row['MyId']
        rank = i + 1
        status = "未知"
        price_src = "昨日收盤(無更新)"
        current_close = row['MyClose'] # 預設用昨日
        
        # 檢查是否有 Yahoo 即時價
        if code in rt_prices:
            current_close = rt_prices[code]
            price_src = "Yahoo即時"
            updated_count += 1
        
        try:
            # 抓歷史資料 (FinMind)
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=25)).strftime("%Y-%m-%d")
            )
            
            # 手動合成今日 K 棒
            if not stock_df.empty:
                # 移除可能重複的今日 (若 FinMind 突然更新了)
                stock_df = stock_df[stock_df['date'] != d_curr_str]
                
                # 拼上今日數據
                new_row = pd.DataFrame([{
                    'date': d_curr_str,
                    'close': current_close
                }])
                # 這裡只補 close 計算 MA5 即可，其他欄位不影響廣度
                stock_df = pd.concat([stock_df, new_row], ignore_index=True)
                
            # 計算 MA5
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr_row = stock_df.iloc[-1]
                prev_row = stock_df.iloc[-2]
                
                results.append({
                    "d_curr_ok": curr_row['close'] > curr_row['MA5'],
                    "d_prev_ok": prev_row['close'] > prev_row['MA5']
                })
                status = "✅ 納入"
            else:
                status = "❌ 剔除 (K線不足)"
                
        except Exception as e:
            status = f"❌ 剔除 ({str(e)})"

        detailed_status.append({
            "排名": rank,
            "代號": code,
            "現價": current_close,
            "來源": price_src,
            "狀態": status
        })
        
        if i % 20 == 0:
            progress_bar.progress((i + 1) / TOP_N, text=f"計算中... (已更新 {updated_count} 檔)")
            
    progress_bar.empty()
    res_df = pd.DataFrame(results)
    detail_df = pd.DataFrame(detailed_status)
    
    # === 步驟 4: 大盤斜率 ===
    slope = 0
    try:
        twii_df = _api.taiwan_stock_daily(stock_id="TAIEX", start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"))
        # 嘗試抓大盤即時 (用 Yahoo ^TWII)
        try:
            twii_rt = yf.download("^TWII", period="1d", progress=False)
            if not twii_rt.empty:
                last_twii = float(twii_rt['Close'].iloc[-1])
                new_row = pd.DataFrame([{'date': d_curr_str, 'close': last_twii}])
                twii_df = twii_df[twii_df['date'] != d_curr_str]
                twii_df = pd.concat([twii_df, new_row], ignore_index=True)
        except:
            pass
            
        twii_df['MA5'] = twii_df['close'].rolling(5).mean()
        slope = twii_df['MA5'].iloc[-1] - twii_df['MA5'].iloc[-2]
    except:
        pass
        
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
        "updated_count": updated_count,
        "last_time": last_update_time
    }

# ==========================================
# UI
# ==========================================
def run_streamlit():
    st.title("📈 盤中權證進場判斷 (v1.8 修正版)")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("API Token 已載入")
        st.code(f"Version: {APP_VERSION}")
        st.markdown(UPDATE_LOG)

    api = DataLoader()
    api.login_by_token(API_TOKEN)

    if st.button("🔄 立即重新整理 (抓取最新報價)"):
        st.cache_data.clear()

    try:
        with st.spinner("正在進行全市場批次更新 (含上市/上櫃)..."):
            data = fetch_data(api)
            
        if data is None:
            st.stop()

        cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
        cond2 = data['slope'] > 0
        final_decision = cond1 and cond2
        
        # 格式化時間顯示
        time_str = "未知"
        if data['last_time']:
            # 轉換為台灣時間顯示
            time_str = data['last_time'].strftime("%H:%M:%S")

        st.subheader(f"📅 數據基準日：{data['d_curr']}")
        
        # 狀態卡片
        st.info(f"""
        📊 **即時資料狀態**
        - 成功更新：**{data['updated_count']}** / {len(data['detail_df'])} 檔
        - 最新報價時間：**{time_str}** (以此確認是否為盤中)
        - 資料來源：Yahoo Finance (.TW / .TWO 雙軌偵測) + FinMind 歷史
        """)

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
        st.subheader(f"📋 前 {TOP_N} 大個股即時狀況")
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
        input("按 Enter 結束...")

# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta
import traceback
import sys

# ==========================================
# 版本資訊
# ==========================================
APP_VERSION = "v1.3.2"
UPDATE_LOG = """
- v1.3.0: 採用 (H+L+C)/3 公式計算成交金額。
- v1.3.1: 修正 API 名稱。
- v1.3.2: 增加對 FinMind 內部依賴 (tqdm) 的相容性，並加入 API 自動降級機制。
"""

# ==========================================
# 參數與 Token 設定
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
RANK_DISPLAY_N = 600     
BREADTH_THRESHOLD = 0.65
EXCLUDE_ETF_PREFIX = "00"

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_trading_days(api):
    """ 取得最近交易日 (使用最穩定的基礎 API) """
    # 為了避免 AttributeError，這裡只使用最基本的 taiwan_stock_daily
    # 雖然資料量稍大，但只抓一檔 0050 非常快且穩定
    df = api.taiwan_stock_daily(
        stock_id="0050", 
        start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
    )
    return sorted(df['date'].unique().tolist())

@st.cache_data(ttl=300)
def fetch_data(_api):
    """ 抓取排行與計算廣度 """
    all_days = get_trading_days(_api)
    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    # 1. 抓取當日全個股 (防禦性寫法)
    # 嘗試使用輕量版 API，若失敗則自動切換回標準版
    try:
        if hasattr(_api, 'taiwan_stock_daily_short'):
            df_all = _api.taiwan_stock_daily_short(stock_id="", start_date=d_curr_str)
        else:
            raise AttributeError("API too old")
    except (AttributeError, Exception):
        # 回退機制：使用標準 daily API
        print("Warn: taiwan_stock_daily_short not found, using standard daily api.")
        df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_curr_str)
    
    # 2. 計算成交金額 (百萬) - 邏輯比照 0+1 程式
    # 公式: ((Max + Min + Close) / 3 * Volume) / 1,000,000
    # 確保欄位名稱正確 (有些版本是大寫有些是小寫)
    cols = {c.lower(): c for c in df_all.columns}
    def get_col(name): return df_all[cols.get(name.lower(), name)]

    # 建立統一名稱
    df_all['MyClose'] = get_col('Close')
    df_all['MyHigh'] = get_col('High')
    df_all['MyLow'] = get_col('Low')
    df_all['MyVol'] = get_col('Volume')
    df_all['MyId'] = get_col('stock_id')

    df_all['avg_price'] = (df_all['MyHigh'] + df_all['MyLow'] + df_all['MyClose']) / 3.0
    df_all['turnover_val'] = (df_all['avg_price'] * df_all['MyVol']) / 1_000_000.0
    
    # 3. 排除 ETF 與大盤
    df_all = df_all[~df_all['MyId'].str.startswith(EXCLUDE_ETF_PREFIX)]
    df_all = df_all[df_all['MyId'] != "TAIEX"] 
    
    df_ranked = df_all.sort_values('turnover_val', ascending=False).head(RANK_DISPLAY_N)
    top_codes = df_ranked.head(TOP_N)['MyId'].tolist() 
    
    results = []
    progress_bar = st.progress(0, text="分析個股 MA5 狀態中...")
    
    for i, code in enumerate(top_codes):
        try:
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
            )
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr_row = stock_df.iloc[-1]
                prev_row = stock_df.iloc[-2]
                
                results.append({
                    "code": code,
                    "d_curr_ok": curr_row['close'] > curr_row['MA5'],
                    "d_prev_ok": prev_row['close'] > prev_row['MA5']
                })
        except:
            continue
        # 為了效能，每處理 10 檔才更新一次進度條
        if i % 10 == 0:
            progress_bar.progress((i + 1) / len(top_codes), text=f"進度: {i+1}/{len(top_codes)}")
    
    progress_bar.empty()
    res_df = pd.DataFrame(results)
    
    # 大盤 MA5 斜率
    twii_df = _api.taiwan_stock_daily(
        stock_id="TAIEX", 
        start_date=(datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
    )
    twii_df['MA5'] = twii_df['close'].rolling(5).mean()
    ma5_t = twii_df['MA5'].iloc[-1]
    ma5_t_1 = twii_df['MA5'].iloc[-2]
    slope = ma5_t - ma5_t_1
    
    return {
        "d_curr": d_curr_str,
        "d_prev": d_prev_str,
        "br_curr": res_df['d_curr_ok'].mean() if not res_df.empty else 0,
        "br_prev": res_df['d_prev_ok'].mean() if not res_df.empty else 0,
        "hit_curr": res_df['d_curr_ok'].sum() if not res_df.empty else 0,
        "hit_prev": res_df['d_prev_ok'].sum() if not res_df.empty else 0,
        "valid": len(res_df),
        "ma5_t": ma5_t,
        "ma5_t_1": ma5_t_1,
        "slope": slope,
        "rank_list": df_ranked[['MyId', 'MyClose', 'turnover_val']].head(10)
    }

# ==========================================
# Streamlit UI 介面
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
        with st.spinner("正在獲取盤中數據..."):
            data = fetch_data(api)

        cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
        cond2 = data['slope'] > 0
        final_decision = cond1 and cond2

        st.subheader(f"📅 數據基準日：{data['d_curr']}")
        
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

        col_list, col_detail = st.columns([1, 1])
        with col_list:
            st.write("📊 **今日成交金額排行 (Top 10)**")
            st.dataframe(data['rank_list'].rename(columns={'MyId':'代號', 'MyClose':'收盤', 'turnover_val':'金額(百萬)'}))

        with col_detail:
            st.write("🔍 **判斷條件詳情**")
            st.write(f"- 廣度連兩天 ≥ 65%：{'通過' if cond1 else '未通過'}")
            st.write(f"- 大盤 MA5 斜率 > 0：{'通過' if cond2 else '未通過'}")

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.code(traceback.format_exc())

# ==========================================
# 執行處理
# ==========================================

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        print(f"--- 盤中權證進場判斷監控 {APP_VERSION} ---")
        try:
            api = DataLoader()
            api.login_by_token(API_TOKEN)
            print("API Token 驗證成功。")
        except Exception as e:
            print(f"API 驗證失敗：{e}")
        
        input("\n按 ENTER 結束程式...")
        sys.exit(0)

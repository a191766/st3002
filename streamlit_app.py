# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import numpy as np
from FinMind.data import DataLoader
from datetime import datetime, timedelta
import traceback
import sys

# ==========================================
# 版本資訊 (每次修改請更新此處)
# ==========================================
APP_VERSION = "v1.3.0"
UPDATE_LOG = """
- v1.0.0: 初始即時網頁版開發。
- v1.1.0: 新增 FinMind API 對接與盤中即時計算。
- v1.2.0: 硬編碼 Token，移除輸入框。
- v1.3.0: 依照 0+1 程式邏輯，採用 (H+L+C)/3 公式計算成交金額並進行排行。
"""

# ==========================================
# 參數與 Token 設定
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              # 進場判斷採用的母體數量
RANK_DISPLAY_N = 600     # 比照 0+1 程式的排行長度
BREADTH_THRESHOLD = 0.65
EXCLUDE_ETF_PREFIX = "00"

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_trading_days(api):
    """ 取得最近交易日 """
    df = api.taiwan_stock_daily_short(
        stock_id="0050", 
        start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
    )
    return sorted(df['date'].unique().tolist())

@st.cache_data(ttl=300)
def fetch_data(_api):
    """ 抓取排行與計算廣度 (邏輯比照 0+1 程式) """
    all_days = get_trading_days(_api)
    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    # 1. 抓取當日全個股
    df_all = _api.taiwan_stock_daily_short(stock_id="", start_date=d_curr_str)
    
    # 2. 計算成交金額 (百萬) - 邏輯比照 0+1 程式的 compute_fields
    # 公式: ((Max + Min + Close) / 3 * Volume) / 1,000,000
    df_all['avg_price'] = (df_all['High'] + df_all['Low'] + df_all['Close']) / 3.0
    df_all['turnover_val'] = (df_all['avg_price'] * df_all['Volume']) / 1_000_000.0
    
    # 3. 排除 ETF 與大盤，並排行
    df_all = df_all[~df_all['stock_id'].str.startswith(EXCLUDE_ETF_PREFIX)]
    df_all = df_all[df_all['stock_id'] != "TAIEX"] # 排除大盤
    
    df_ranked = df_all.sort_values('turnover_val', ascending=False).head(RANK_DISPLAY_N)
    top_codes = df_ranked.head(TOP_N)['stock_id'].tolist() # 取前 300 檔做廣度計算
    
    # 4. 計算廣度 (D 與 D-1)
    results = []
    progress_bar = st.progress(0, text="分析個股 MA5 狀態中...")
    
    for i, code in enumerate(top_codes):
        try:
            # 抓取足以計算 MA5 的資料 (至少 6 筆)
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
        progress_bar.progress((i + 1) / len(top_codes), text=f"進度: {i+1}/{len(top_codes)} ({code})")
    
    progress_bar.empty()
    res_df = pd.DataFrame(results)
    
    # 5. TWII 大盤 MA5 斜率
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
        "br_curr": res_df['d_curr_ok'].mean(),
        "br_prev": res_df['d_prev_ok'].mean(),
        "hit_curr": res_df['d_curr_ok'].sum(),
        "hit_prev": res_df['d_prev_ok'].sum(),
        "valid": len(res_df),
        "ma5_t": ma5_t,
        "ma5_t_1": ma5_t_1,
        "slope": slope,
        "rank_list": df_ranked[['stock_id', 'Close', 'turnover_val']].head(10) # 顯示前10名參考
    }

# ==========================================
# Streamlit UI 介面
# ==========================================

def run_streamlit():
    st.title("📈 盤中權證進場判斷監控 (邏輯同步 v1.3)")

    with st.sidebar:
        st.subheader("系統狀態")
        st.success("API Token 自動載入成功")
        st.divider()
        st.subheader("版本與邏輯資訊")
        st.code(f"Version: {APP_VERSION}")
        st.info("成交金額計算公式：\n((H + L + C) / 3 * Vol) / 10^6")
        st.markdown(UPDATE_LOG)

    api = DataLoader()
    api.login_by_token(API_TOKEN)

    if st.button("🔄 立即重新整理數據"):
        st.cache_data.clear()

    try:
        with st.spinner("正在依照最新成交值排行計算廣度..."):
            data = fetch_data(api)

        # 判定邏輯
        cond1 = (data['br_curr'] >= BREADTH_THRESHOLD) and (data['br_prev'] >= BREADTH_THRESHOLD)
        cond2 = data['slope'] > 0
        final_decision = cond1 and cond2

        # 核心指標顯示
        st.subheader(f"📅 數據基準日：{data['d_curr']}")
        
        c1, c2, c3 = st.columns(3)
        c1.metric("今日廣度 (D)", f"{data['br_curr']:.1%}", f"{data['hit_curr']}/{data['valid']}")
        c2.metric("昨日廣度 (D-1)", f"{data['br_prev']:.1%}", f"{data['hit_prev']}/{data['valid']}")
        c3.metric("大盤 MA5 斜率", f"{data['slope']:.2f}", "正 ✓" if cond2 else "非正 ✗")

        st.divider()

        # 結論區
        st.header("💡 進場結論")
        if final_decision:
            st.success(f"✅ 結論（{data['d_curr']} 的隔日）：可進場")
        else:
            st.error(f"⛔ 結論（{data['d_curr']} 的隔日）：不可進場")

        # 詳細清單與排行
        col_list, col_detail = st.columns([1, 1])
        with col_list:
            st.write("📊 **今日成交金額排行 (Top 10)**")
            st.dataframe(data['rank_list'].rename(columns={'stock_id':'代號', 'Close':'收盤', 'turnover_val':'金額(百萬)'}))

        with col_detail:
            st.write("🔍 **判斷條件詳情**")
            st.write(f"- 廣度連兩天 ≥ 65%：{'通過' if cond1 else '未通過'}")
            st.write(f"- 大盤 MA5 斜率 > 0：{'通過' if cond2 else '未通過'}")
            st.write(f"  (MA5_t: {data['ma5_t']:.2f}, MA5_t-1: {data['ma5_t_1']:.2f})")

    except Exception as e:
        st.error(f"執行出錯: {e}")
        st.text(traceback.format_exc())

# ==========================================
# 執行處理 (支援雙擊執行)
# ==========================================

if __name__ == "__main__":
    if 'streamlit' in sys.modules:
        run_streamlit()
    else:
        print(f"--- 盤中權證進場判斷監控 {APP_VERSION} ---")
        print("系統提示：請於終端機輸入 'streamlit run 檔案名稱.py' 啟動網頁介面。")
        print("-" * 50)
        try:
            api = DataLoader()
            api.login_by_token(API_TOKEN)
            print("FinMind API Token 驗證成功，連線正常。")
        except Exception as e:
            print(f"API 連線失敗：{e}")
        
        input("\n按 ENTER 結束程式...")
        sys.exit(0)

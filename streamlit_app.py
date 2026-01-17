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
APP_VERSION = "v1.3.5"
UPDATE_LOG = """
- v1.3.0: 採用 (H+L+C)/3 公式計算成交金額。
- v1.3.1: 修正 API 名稱。
- v1.3.2: 增加 API 自動降級機制。
- v1.3.3: 新增智慧欄位對應。
- v1.3.4: 新增「純數字代號」濾網。
- v1.3.5: 新增「前 300 名詳細清單」，標註剔除原因（解決分母不一致的疑問）。
"""

# ==========================================
# 參數與 Token 設定
# ==========================================
API_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wMS0xNCAxOTowMDowNiIsInVzZXJfaWQiOiJcdTllYzNcdTRlYzFcdTVhMDEiLCJlbWFpbCI6ImExOTE3NjZAZ21haWwuY29tIiwiaXAiOiIifQ.JFPtMDNbxKzhl8HsxkOlA1tMlwq8y_NA6NpbRel6HCk"
TOP_N = 300              
RANK_DISPLAY_N = 300     # 配合使用者需求，這裡主要顯示前 300 檔的詳細狀況
BREADTH_THRESHOLD = 0.65
EXCLUDE_ETF_PREFIX = "00"

st.set_page_config(page_title="盤中權證進場判斷", layout="wide")

# ==========================================
# 功能函式
# ==========================================

def get_trading_days(api):
    """ 取得最近交易日 """
    df = api.taiwan_stock_daily(
        stock_id="0050", 
        start_date=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
    )
    return sorted(df['date'].unique().tolist())

def smart_get_column(df, target_type):
    """ 智慧欄位對應 """
    mappings = {
        'High': ['High', 'high', 'max', 'Max'],
        'Low': ['Low', 'low', 'min', 'Min'],
        'Close': ['Close', 'close', 'price', 'Price'],
        'Volume': ['Volume', 'volume', 'Trading_Volume', 'vol'],
        'Id': ['stock_id', 'stock_code', 'code', 'SecurityCode']
    }
    candidates = mappings.get(target_type, [])
    for c in candidates:
        if c in df.columns:
            return df[c]
    raise KeyError(f"找不到 {target_type} 對應的欄位。")

@st.cache_data(ttl=300)
def fetch_data(_api):
    """ 抓取排行與計算廣度 """
    all_days = get_trading_days(_api)
    d_curr_str = all_days[-1]
    d_prev_str = all_days[-2]
    
    # 1. 抓取當日全市場資料
    try:
        if hasattr(_api, 'taiwan_stock_daily_short'):
            df_all = _api.taiwan_stock_daily_short(stock_id="", start_date=d_curr_str)
        else:
            raise AttributeError("API too old")
    except (AttributeError, Exception):
        df_all = _api.taiwan_stock_daily(stock_id="", start_date=d_curr_str)
    
    # 2. 欄位標準化
    try:
        df_all['MyClose'] = smart_get_column(df_all, 'Close')
        df_all['MyHigh'] = smart_get_column(df_all, 'High')
        df_all['MyLow'] = smart_get_column(df_all, 'Low')
        df_all['MyVol'] = smart_get_column(df_all, 'Volume')
        df_all['MyId'] = smart_get_column(df_all, 'Id')
    except KeyError as e:
        st.error(f"資料欄位解析失敗: {e}")
        return None

    # 3. 過濾雜訊
    df_all['MyId'] = df_all['MyId'].astype(str)
    df_all = df_all[df_all['MyId'].str.isdigit()]  # 只留純數字 (過濾 Electronic 等指數)
    df_all = df_all[~df_all['MyId'].str.startswith(EXCLUDE_ETF_PREFIX)] # 過濾 ETF
    df_all = df_all[df_all['MyId'] != "TAIEX"] # 過濾大盤

    # 4. 計算成交金額並排序
    df_all['avg_price'] = (df_all['MyHigh'] + df_all['MyLow'] + df_all['MyClose']) / 3.0
    df_all['turnover_val'] = (df_all['avg_price'] * df_all['MyVol']) / 1_000_000.0
    
    # 取前 300 名作為「候選名單」
    df_candidates = df_all.sort_values('turnover_val', ascending=False).head(TOP_N).copy()
    
    results = []
    detailed_status = [] # 用來存 300 檔的詳細狀態
    
    progress_bar = st.progress(0, text="逐檔檢查 K 線資料完整性...")
    total_candidates = len(df_candidates)

    # 5. 逐一檢查這 300 檔
    for i, (idx, row) in enumerate(df_candidates.iterrows()):
        code = row['MyId']
        rank = i + 1
        note = ""
        status = "未知"
        is_valid = False
        
        try:
            # 抓取個股歷史資料
            stock_df = _api.taiwan_stock_daily(
                stock_id=code,
                start_date=(datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
            )
            
            # 檢查資料長度
            if len(stock_df) >= 6:
                stock_df['MA5'] = stock_df['close'].rolling(5).mean()
                curr_row = stock_df.iloc[-1]
                prev_row = stock_df.iloc[-2]
                
                # 加入廣度計算
                results.append({
                    "d_curr_ok": curr_row['close'] > curr_row['MA5'],
                    "d_prev_ok": prev_row['close'] > prev_row['MA5']
                })
                status = "✅ 納入"
                is_valid = True
            else:
                status = "❌ 剔除"
                note = f"資料不足 (僅 {len(stock_df)} 筆，需 6 筆)"
                
        except Exception as e:
            status = "❌ 剔除"
            note = f"API 抓取失敗: {str(e)}"
        
        # 記錄詳細清單
        detailed_status.append({
            "排名": rank,
            "代號": code,
            "收盤": row['MyClose'],
            "成交額(百萬)": round(row['turnover_val'], 2),
            "狀態": status,
            "備註": note
        })

        if i % 10 == 0:
            progress_bar.progress((i + 1) / total_candidates, text=f"檢查中: 排名 {rank} ({code})")
    
    progress_bar.empty()
    
    res_df = pd.DataFrame(results)
    detail_df = pd.DataFrame(detailed_status)
    
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
        "detail_df": detail_df # 回傳完整清單
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
        with st.spinner("正在獲取並檢查前 300 檔個股資料..."):
            data = fetch_data(api)
            
        if data is None:
            st.stop()

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
        
        st.write(f"- 廣度連兩天 ≥ 65%：{'✅ 通過' if cond1 else '❌ 未通過'}")
        st.write(f"- 大盤 MA5 斜率 > 0：{'✅ 通過' if cond2 else '❌ 未通過'} (MA5斜率: {data['slope']:.2f})")

        st.divider()
        
        # 顯示完整名單與剔除原因
        st.subheader(f"📋 前 {TOP_N} 大成交值個股檢查清單")
        st.info("💡 點擊欄位標題可排序，或使用右上角搜尋框輸入「剔除」來查看被排除的股票。")
        
        # 為了讓使用者更容易看到剔除項，我們先把剔除的排在前面，或者維持排名
        df_show = data['detail_df']
        
        # 顯示 Dataframe
        st.dataframe(
            df_show, 
            column_config={
                "排名": st.column_config.NumberColumn(format="%d"),
                "成交額(百萬)": st.column_config.NumberColumn(format="$%.2f"),
                "收盤": st.column_config.NumberColumn(format="%.2f"),
            },
            use_container_width=True,
            height=600,
            hide_index=True
        )

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
